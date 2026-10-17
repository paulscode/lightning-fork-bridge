package bridged

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/paulscode/lightning-fork-bridge/chainrate"
	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/inventory"
	"github.com/paulscode/lightning-fork-bridge/lnd"
	"github.com/paulscode/lightning-fork-bridge/lndrpc/lnrpc"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/paulscode/lightning-fork-bridge/rate"
	"github.com/paulscode/lightning-fork-bridge/runner"
	"github.com/paulscode/lightning-fork-bridge/store"
)

// ErrNoDirection is returned when no enabled direction can pay an invoice.
var ErrNoDirection = errors.New("no enabled direction can pay this invoice")

// side is one direction of the bridge, fully wired.
type side struct {
	name string

	// in receives, out pays. Which node is which is the direction.
	in  node.Incoming
	out node.Outgoing

	// balance reports what the paying node can still send, which is the
	// side the bridge's own money leaves from and so the side the spread
	// is priced against. A function rather than a connection so the wiring
	// above it can be exercised without two Lightning nodes.
	balance func(context.Context) (uint64, error)

	quoter *quote.Quoter
	runner *runner.Runner

	// dir is what this direction does to the position, which is what the
	// inventory policy prices.
	dir inventory.Direction

	// invert is set on the direction whose rate is the reciprocal of the
	// one the sources quote.
	invert bool

	// paysBLAKE2b is whether this direction pays out on the BLAKE2b chain,
	// which is how an invoice is matched to its direction.
	paysBLAKE2b bool
}

// Service is the running daemon.
type Service struct {
	cfg Config
	log *slog.Logger

	lfConn, btcConn *grpc.ClientConn
	journal         *store.Journal

	// sides holds whichever directions are enabled, in the order a quote
	// request tries them.
	sides []*side

	// disabled names the directions that are configured and turned off, so
	// an invoice for one can be refused by name. "No enabled direction can
	// pay this" sends an operator looking at their nodes; "toBlake2b is
	// configured but not enabled" sends them to the line they changed.
	disabled []string

	oracle   *rate.Oracle
	lfChain  *chainrate.Observer
	btcChain *chainrate.Observer

	// price is the latest combined reading, or the error that stopped it
	// being one. Guarded because the poller writes it and quotes read it.
	priceMu  sync.RWMutex
	price    rate.Reading
	priceErr error

	// ctx is the service's own lifetime, which a swap driven from an HTTP
	// request must outlive: the user may disconnect the moment after they
	// pay, and the HTLC does not go away with their connection.
	//
	// Guarded because Run replaces it with a cancellable one while the
	// handler may already be serving. A caller that uses Handler directly,
	// which the tests do, races with Run otherwise.
	ctxMu sync.RWMutex
	ctx   context.Context

	wg sync.WaitGroup

	closers []func() error

	// chainOf caches, per payment hash, whether a recorded swap pays out on
	// the BLAKE2b chain, so headroom can be counted per direction without
	// decoding every pending invoice on every quote.
	chainMu sync.Mutex
	chainOf map[node.Hash]bool
}

// New dials the nodes, opens the journal and wires both directions. It does
// not start anything: Run does that, so a caller can check a configuration
// without the daemon beginning to quote on it.
func New(ctx context.Context, cfg Config, log *slog.Logger) (*Service, error) {
	if err := cfg.Valid(); err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}

	s := &Service{cfg: cfg, log: log, ctx: context.Background()}

	// Anything already opened has to be closed if a later step fails, or a
	// configuration error leaves a node connection and a journal behind.
	fail := func(err error) (*Service, error) {
		s.Close()

		return nil, err
	}

	var err error
	if s.lfConn, err = lnd.Dial(ctx, cfg.Blake2b.toLND()); err != nil {
		return fail(fmt.Errorf("dialling the BLAKE2b node: %w", err))
	}
	s.closers = append(s.closers, s.lfConn.Close)

	if s.btcConn, err = lnd.Dial(ctx, cfg.Bitcoin.toLND()); err != nil {
		return fail(fmt.Errorf("dialling the Bitcoin node: %w", err))
	}
	s.closers = append(s.closers, s.btcConn.Close)

	if s.journal, err = loadJournal(cfg.Journal, log); err != nil {
		return fail(err)
	}
	s.closers = append(s.closers, s.journal.Close)

	if s.oracle, err = rate.New(cfg.Rate); err != nil {
		return fail(fmt.Errorf("building the rate oracle: %w", err))
	}
	if s.lfChain, err = chainrate.New(cfg.Blake2bChain.toParams()); err != nil {
		return fail(fmt.Errorf("the BLAKE2b chain observer: %w", err))
	}
	if s.btcChain, err = chainrate.New(cfg.BitcoinChain.toParams()); err != nil {
		return fail(fmt.Errorf("the Bitcoin chain observer: %w", err))
	}

	// A quote that has never had a price must not be answered with a stale
	// zero, so start refused and let the first poll clear it.
	s.priceErr = errors.New("no price has been read yet")

	// The bridge's own two node keys, so neither can be paid through it.
	var own []string
	for _, conn := range []*grpc.ClientConn{s.lfConn, s.btcConn} {
		info, err := lnrpc.NewLightningClient(conn).GetInfo(ctx,
			&lnrpc.GetInfoRequest{})
		if err != nil {
			return fail(fmt.Errorf("reading a node's identity: %w", err))
		}
		own = append(own, info.GetIdentityPubkey())
	}

	// Both directions price against what the bridge holds on Bitcoin. That
	// is the position the inventory policy describes: toBitcoin spends it,
	// toBlake2b puts it back, and pricing toBlake2b against its own BTCB2
	// balance instead would offer its biggest discount when the BTCB2 side
	// was nearly empty, the opposite of what that side needs.
	position := channelBalance(s.btcConn)

	if cfg.ToBitcoin.Enabled {
		s.sides = append(s.sides, s.build("toBitcoin", cfg.ToBitcoin,
			s.lfConn, s.btcConn, position, inventory.Draining, false, own))
	} else {
		s.disabled = append(s.disabled, "toBitcoin")
	}
	if cfg.ToBlake2b.Enabled {
		// The sources quote BTCB2 in BTC, so the direction that pays in
		// BTCB2 uses the reciprocal. And it is the direction that puts
		// back what the other one spends.
		s.sides = append(s.sides, s.build("toBlake2b", cfg.ToBlake2b,
			s.btcConn, s.lfConn, position, inventory.Replenishing, true,
			own))
	} else {
		s.disabled = append(s.disabled, "toBlake2b")
	}

	return s, nil
}

// loadJournal opens the journal and says so if opening it had to repair it.
//
// A repair drops a record torn by a crash mid-write. Nothing reported durable
// is lost, so it does not stop the daemon, but it means the last process died
// writing, and an operator looking at what happened then needs to know.
func loadJournal(path string, log *slog.Logger) (*store.Journal, error) {
	j, err := store.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening the journal %s: %w", path, err)
	}
	if j.Repaired {
		log.Warn("the journal ended in a torn record, from a process "+
			"that died writing it; the record was never reported durable "+
			"and was dropped", "journal", path, "bytes", j.RepairedBytes)
	}

	return j, nil
}

// build wires one direction. inConn receives and outConn pays.
func (s *Service) build(name string, d Direction, inConn, outConn *grpc.ClientConn,
	position func(context.Context) (uint64, error),
	dir inventory.Direction, invert bool, own []string) *side {

	sd := &side{
		name: name, in: lnd.NewIncoming(inConn), out: lnd.NewOutgoing(outConn),
		balance: channelBalance(outConn), dir: dir, invert: invert,
		// The direction that inverts the rate is the one paying in BTCB2.
		paysBLAKE2b: invert,
	}

	sd.quoter = &quote.Quoter{
		In: sd.in, Out: sd.out, Store: s.journal,
		Price:       s.pricer(sd, d.Inventory, position),
		Spacing:     quote.Rates(s.spacing(invert)),
		Room:        s.headroom(sd),
		Margin:      s.cfg.Margin,
		Policy:      d.Policy,
		PaysBLAKE2b: sd.paysBLAKE2b,
		Own:         own,
	}
	sd.runner = &runner.Runner{
		Driver: &driver.Driver{
			In: sd.in, Out: sd.out, Store: s.journal, Policy: s.cfg.Margin,
		},
		Store: s.journal, Rates: runner.Rates(s.spacing(invert)),
		Log: s.log.With("direction", name),
	}

	return sd
}

// pricer combines the market price with what the outgoing side is holding.
//
// The spread is the larger of the two the bridge computes: the rate oracle
// widens for volatility, the inventory policy widens for how drained the
// outgoing side is, and both are reasons to charge more. Taking the larger
// charges enough for whichever risk is bigger; taking either alone would
// underprice the other.
func (s *Service) pricer(sd *side, inv inventory.Policy,
	position func(context.Context) (uint64, error)) quote.Pricer {

	return func(ctx context.Context) (rate.Reading, error) {
		s.priceMu.RLock()
		r, err := s.price, s.priceErr
		s.priceMu.RUnlock()
		if err != nil {
			return rate.Reading{}, err
		}

		// The oracle bounds how old an observation may be when it combines
		// them. Nothing bounds how old the combined reading is by the time
		// a quote uses it, and a poller that has stopped producing leaves
		// the last one in place indefinitely. Refuse rather than quote on
		// a price the policy itself would not have accepted.
		if max := s.cfg.Rate.MaxAge; max > 0 {
			if age := time.Since(r.At); age > max {
				return rate.Reading{}, fmt.Errorf("%w: the last usable "+
					"price is %v old and the policy allows %v, so the "+
					"price feed has stopped", quote.ErrRefused,
					age.Round(time.Second), max)
			}
		}

		if sd.invert {
			if r.OutgoingPerIncoming <= 0 {
				return rate.Reading{}, fmt.Errorf("%w: cannot invert a "+
					"rate of %g", quote.ErrRefused, r.OutgoingPerIncoming)
			}
			r.OutgoingPerIncoming = 1 / r.OutgoingPerIncoming
		}

		held, err := position(ctx)
		if err != nil {
			return rate.Reading{}, fmt.Errorf("%w: %w", quote.ErrRefused,
				err)
		}
		pos, err := inv.Spread(inventory.State{
			OutgoingMsat: held, At: time.Now(),
		}, sd.dir, time.Now())
		if err != nil {
			return rate.Reading{}, err
		}
		if pos.Spread > r.Spread {
			r.Spread = pos.Spread
		}

		return r, nil
	}
}

// channelBalance reads what a node can still send over its channels.
func channelBalance(conn *grpc.ClientConn) func(context.Context) (uint64, error) {
	return func(ctx context.Context) (uint64, error) {
		bal, err := lnrpc.NewLightningClient(conn).ChannelBalance(ctx,
			&lnrpc.ChannelBalanceRequest{})
		if err != nil {
			return 0, fmt.Errorf("reading the outgoing balance: %w", err)
		}

		return uint64(bal.GetLocalBalance().GetMsat()), nil
	}
}

// headroom is the outgoing balance less what is already promised to swaps
// that have not finished. The second part is the one that is easy to forget
// and expensive to forget.
func (s *Service) headroom(sd *side) quote.Headroom {
	return func(ctx context.Context) (uint64, error) {
		held, err := sd.balance(ctx)
		if err != nil {
			return 0, err
		}
		committed, err := store.CommittedWhere(ctx, s.journal,
			func(r store.Record) bool {
				return s.paysBLAKE2b(ctx, sd, r) == sd.paysBLAKE2b
			})
		if err != nil {
			return 0, err
		}
		if held < committed {
			return 0, nil
		}

		return held - committed, nil
	}
}

// paysBLAKE2b reports whether a recorded swap pays out on the BLAKE2b chain,
// by reading its outgoing invoice once and remembering the answer.
//
// An invoice that cannot be read is counted against the asking side. That
// over-reserves, which refuses a quote it need not have; the other way
// under-reserves, which sells the same money twice.
func (s *Service) paysBLAKE2b(ctx context.Context, sd *side,
	r store.Record) bool {

	s.chainMu.Lock()
	if s.chainOf == nil {
		s.chainOf = map[node.Hash]bool{}
	}
	known, ok := s.chainOf[r.Hash]
	s.chainMu.Unlock()
	if ok {
		return known
	}

	dec, err := sd.out.Decode(ctx, r.Invoice)
	if err != nil {
		return sd.paysBLAKE2b
	}

	s.chainMu.Lock()
	s.chainOf[r.Hash] = dec.BLAKE2b
	s.chainMu.Unlock()

	return dec.BLAKE2b
}

// spacing reports both chains' block spacing, with the legs the right way
// round for the direction. Using one chain's spacing for both would size a
// swap against a chain it is not on.
//
// quote.Rates and runner.Rates are the same signature declared twice, so one
// function serves both and is converted at each use.
func (s *Service) spacing(invert bool) func(context.Context) (driver.Rates, error) {
	return func(ctx context.Context) (driver.Rates, error) {
		lf, err := s.lfChain.Estimate(time.Now())
		if err != nil {
			return driver.Rates{}, fmt.Errorf("the BLAKE2b chain's "+
				"spacing: %w", err)
		}
		btc, err := s.btcChain.Estimate(time.Now())
		if err != nil {
			return driver.Rates{}, fmt.Errorf("the Bitcoin chain's "+
				"spacing: %w", err)
		}

		in, out := lf.Bounds, btc.Bounds
		if invert {
			in, out = btc.Bounds, lf.Bounds
		}

		return driver.Rates{Incoming: in, Outgoing: out, As: time.Now()}, nil
	}
}

// Close releases whatever was opened, in reverse order.
//
// Call it after Run returns, not alongside it: it closes the node connections
// that swaps in flight are using. Run waits for those itself. Calling it twice
// is harmless, which matters because New closes on a partial failure and the
// caller's defer will close again.
func (s *Service) Close() error {
	var errs []error
	for i := len(s.closers) - 1; i >= 0; i-- {
		if err := s.closers[i](); err != nil {
			errs = append(errs, err)
		}
	}
	s.closers = nil

	return errors.Join(errs...)
}

// driveCtx is the lifetime a swap is driven on.
func (s *Service) driveCtx() context.Context {
	s.ctxMu.RLock()
	defer s.ctxMu.RUnlock()

	return s.ctx
}

// setDriveCtx is called once, by Run.
func (s *Service) setDriveCtx(ctx context.Context) {
	s.ctxMu.Lock()
	defer s.ctxMu.Unlock()

	s.ctx = ctx
}

// route picks the direction that can pay an invoice, by asking each paying
// node to decode it.
//
// Deliberately not a parser in this process. The node that can read an
// invoice is the node that will pay it, which is the same reason quote takes
// the payment hash from the paying node rather than reading it here.
func (s *Service) route(ctx context.Context, invoice string) (*side, node.Decoded, error) {
	var last error
	for _, sd := range s.sides {
		dec, err := sd.out.Decode(ctx, invoice)
		if err != nil {
			last = err

			continue
		}
		// Both chains' invoices share a prefix and both nodes read every
		// one, so decoding says nothing about the chain. option_blake2b
		// does.
		if dec.BLAKE2b != sd.paysBLAKE2b {
			last = quote.ErrWrongChain

			continue
		}

		return sd, dec, nil
	}

	// Say so if the direction exists and is switched off, which is the
	// difference between "this daemon cannot do that" and "you turned that
	// off".
	if len(s.disabled) > 0 {
		return nil, node.Decoded{}, fmt.Errorf("%w: %s configured but not "+
			"enabled, and no enabled direction could read it",
			ErrNoDirection, strings.Join(s.disabled, " and "))
	}

	if last == nil {
		return nil, node.Decoded{}, ErrNoDirection
	}

	// No node could read it at all, as opposed to every node reading it as
	// the other chain's: that is an invoice problem, not a direction one.
	if !errors.Is(last, quote.ErrWrongChain) {
		last = fmt.Errorf("%w: %w", quote.ErrInvalidInvoice, last)
	}

	return nil, node.Decoded{}, fmt.Errorf("%w: %w", ErrNoDirection, last)
}
