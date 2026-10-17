package bridged

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/paulscode/lightning-fork-bridge/runner"
	"github.com/paulscode/lightning-fork-bridge/store"
)

// QuoteRequest asks for a swap.
type QuoteRequest struct {
	// Invoice is the payment request the user wants paid, on whichever
	// chain it is for. Which direction that makes it is worked out by
	// asking the nodes, not by reading it here.
	Invoice string `json:"invoice"`
}

// QuoteResponse is what the user must do, and what it costs.
type QuoteResponse struct {
	// HoldInvoice is what the user pays, on the other chain. It does not
	// settle until the bridge has delivered, which is the whole mechanism.
	HoldInvoice string `json:"holdInvoice"`

	// Hash is the payment hash both legs share, and the key to ask about
	// this swap later.
	Hash string `json:"hash"`

	// IncomingMsat is what the user owes, OutgoingMsat what the
	// destination receives. They are in different units: the two chains'
	// millisatoshis are not the same thing.
	IncomingMsat uint64 `json:"incomingMsat"`
	OutgoingMsat uint64 `json:"outgoingMsat"`

	// Rate is outgoing units per incoming unit, Spread the fraction
	// charged on top.
	Rate   float64 `json:"rate"`
	Spread float64 `json:"spread"`

	// CLTVDelta is the incoming leg's required CLTV, in blocks of the
	// incoming chain.
	CLTVDelta uint32 `json:"cltvDelta"`

	// Direction names which way this swap runs, for a user who wants to
	// know which node to pay from.
	Direction string `json:"direction"`

	// ExpiresAt is the deadline for paying HoldInvoice. Pay it before this
	// and the swap proceeds at the quoted rate; leave it and the bridge
	// cancels the hold invoice, after which paying it fails.
	//
	// It only applies while the invoice is unpaid. Once the incoming HTLC
	// is locked in, the swap is no longer racing a clock in minutes and
	// the CLTV in CLTVDelta is what bounds it.
	ExpiresAt time.Time `json:"expiresAt"`
}

// SwapResponse is what the bridge believes about a swap.
type SwapResponse struct {
	Hash         string `json:"hash"`
	State        string `json:"state"`
	IncomingMsat uint64 `json:"incomingMsat"`
	OutgoingMsat uint64 `json:"outgoingMsat"`

	// Preimage is present once there is one, which is the user's proof the
	// destination was paid.
	Preimage string `json:"preimage,omitempty"`
}

// Handler is the daemon's HTTP surface.
//
// Deliberately small. A quote commits the bridge's money, so this belongs on
// loopback or behind something that authenticates; there is no authentication
// here and adding a weak one would be worse than saying so.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /quote", s.handleQuote)
	mux.HandleFunc("GET /swap/{hash}", s.handleSwap)
	mux.HandleFunc("GET /health", s.handleHealth)

	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// writeRefusal reports a quote that did not happen, with the stable code a
// client branches on next to the sentence a person reads.
func writeRefusal(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{
		"error": err.Error(), "code": string(quote.CodeOf(err)),
	})
}

func (s *Service) handleQuote(w http.ResponseWriter, r *http.Request) {
	var req QuoteRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)

		return
	}
	if req.Invoice == "" {
		writeErr(w, http.StatusBadRequest, errors.New("no invoice"))

		return
	}

	// Deliberately not the request's context. Quoting creates a journal
	// record and then a hold invoice on the node, in that order, and both
	// outlive the connection that asked for them. Abandoning it half way
	// because the caller hung up would leave a swap that only a restart
	// would notice. Bounded, so a node that stops answering cannot pin the
	// handler open.
	ctx, cancel := context.WithTimeout(s.driveCtx(), 60*time.Second)
	defer cancel()

	sd, decoded, err := s.route(ctx, req.Invoice)
	if err != nil {
		writeRefusal(w, http.StatusBadRequest, err)

		return
	}

	terms, err := sd.quoter.Quote(ctx, req.Invoice)
	if err != nil {
		// The record exists and the invoice may: drive it, so that it
		// either catches up or expires rather than holding its amount
		// against the headroom until a restart.
		if errors.Is(err, quote.ErrUntracked) {
			s.drive(sd, decoded.Hash)
		}

		// A refusal is the bridge working, not failing: too large, too
		// close to expiry, not enough headroom. It is the user's to act
		// on, so it is a 4xx with the reason intact.
		code := http.StatusBadGateway
		if errors.Is(err, quote.ErrRefused) {
			code = http.StatusConflict
		}
		writeRefusal(w, code, err)

		return
	}

	// The swap is now the bridge's to finish: the hold invoice is live and
	// the user may pay it at any moment. Drive it on a context that
	// outlives this request, or a user who disconnects after paying would
	// leave an HTLC nobody is watching.
	s.drive(sd, terms.Hash)

	writeJSON(w, http.StatusOK, QuoteResponse{
		HoldInvoice: terms.HoldInvoice, Hash: hex.EncodeToString(terms.Hash[:]),
		IncomingMsat: terms.IncomingMsat, OutgoingMsat: terms.OutgoingMsat,
		Rate: terms.Rate, Spread: terms.Spread, CLTVDelta: terms.CLTVDelta,
		Direction: sd.name, ExpiresAt: terms.Expires,
	})
}

func (s *Service) handleSwap(w http.ResponseWriter, r *http.Request) {
	raw, err := hex.DecodeString(r.PathValue("hash"))
	if err != nil || len(raw) != len(node.Hash{}) {
		writeErr(w, http.StatusBadRequest,
			errors.New("the hash must be 32 hex-encoded bytes"))

		return
	}
	var hash node.Hash
	copy(hash[:], raw)

	rec, err := s.journal.Get(r.Context(), hash)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeErr(w, http.StatusNotFound, err)

			return
		}
		writeErr(w, http.StatusInternalServerError, err)

		return
	}

	resp := SwapResponse{
		Hash: hex.EncodeToString(rec.Hash[:]), State: rec.State.String(),
		IncomingMsat: rec.IncomingMsat, OutgoingMsat: rec.OutgoingMsat,
	}
	if rec.Preimage != (node.Preimage{}) {
		resp.Preimage = hex.EncodeToString(rec.Preimage[:])
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleHealth answers whether the daemon could quote right now, which is a
// different question from whether it is running. A bridge with no price or a
// chain it cannot see is up and unable to do anything, and saying "ok" to
// that is how it goes unnoticed.
func (s *Service) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := map[string]any{"directions": len(s.sides)}

	s.priceMu.RLock()
	priceErr := s.priceErr
	price := s.price
	s.priceMu.RUnlock()

	ready := true
	if priceErr != nil {
		out["price"] = priceErr.Error()
		ready = false
	} else {
		out["price"] = price.OutgoingPerIncoming
		out["priceSources"] = price.Sources
	}

	for _, name := range []string{"blake2b", "bitcoin"} {
		obs := s.lfChain
		if name == "bitcoin" {
			obs = s.btcChain
		}
		if _, err := s.estimate(obs); err != nil {
			out[name+"Chain"] = err.Error()
			ready = false
		} else {
			out[name+"Chain"] = "ok"
		}
	}

	// Being unable to reach a node is the other way this is up and useless.
	// Bounded: a node that accepts the connection and then says nothing
	// would otherwise hang the health check, which is the one request that
	// has to answer when things are wrong.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for _, sd := range s.sides {
		if _, err := sd.balance(ctx); err != nil {
			out[sd.name] = err.Error()
			ready = false
		} else {
			out[sd.name] = "ok"
		}
	}

	// Every quote writes to the journal, so a bridge whose journal has gone
	// is up and unable to do anything.
	//
	// This checks the file is still there, which catches the operational
	// failure that actually happens: a deleted file, or a volume that went
	// away under a running daemon. It does not catch a full disk, because
	// the only way to learn that is to write, and health checks that write
	// to the record a swap depends on are their own hazard. Reading the
	// journal would catch neither: Pending answers from an in-memory index
	// and never touches disk.
	if s.cfg.Journal != "" {
		if _, err := os.Stat(s.cfg.Journal); err != nil {
			out["journal"] = err.Error()
			ready = false
		} else {
			out["journal"] = "present"
		}
	}

	out["ready"] = ready
	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, out)
}

// drive finishes a swap in the background, on the service's own context
// rather than on a request's: the hold invoice is live the moment it is
// returned and the user may pay it after disconnecting.
func (s *Service) drive(sd *side, hash node.Hash) {
	ctx := s.driveCtx()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		err := sd.runner.Drive(ctx, hash)
		switch {
		case err == nil:
		case errors.Is(err, runner.ErrBusy):
			// Already being driven, which is what should happen when a
			// user asks twice for the same invoice. The runner's rule is
			// one goroutine per hash, and this is that rule working.
			s.log.Debug("already driving", "direction", sd.name,
				"hash", hex.EncodeToString(hash[:]))
		case errors.Is(err, context.Canceled):
			// Shutdown. The record is always at least as far along as the
			// nodes are, so the next process picks it up.
			s.log.Info("stopped driving for shutdown", "direction", sd.name,
				"hash", hex.EncodeToString(hash[:]))
		default:
			s.log.Error("swap did not finish", "direction", sd.name,
				"hash", hex.EncodeToString(hash[:]), "err", err)
		}
	}()
}

// ctxOrBackground is used where a caller has not started the service.
func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return ctx
}
