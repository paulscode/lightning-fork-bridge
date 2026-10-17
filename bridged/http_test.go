package bridged

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/inventory"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/paulscode/lightning-fork-bridge/rate"
	"github.com/paulscode/lightning-fork-bridge/runner"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// A record has to carry a preimage that actually hashes to its hash, which
// the store checks, so these are a real pair rather than two constants.
var (
	testPreimage = node.Preimage{7, 7, 7, 7}
	testHash     = node.Hash(sha256.Sum256(testPreimage[:]))
)

// fakeIn issues hold invoices and remembers what it was asked for.
type fakeIn struct {
	addErr error
}

func (f *fakeIn) AddHoldInvoice(context.Context, node.HoldInvoice) (string, error) {
	if f.addErr != nil {
		return "", f.addErr
	}

	return "lnblakert1holdinvoice", nil
}

func (f *fakeIn) LookupInvoice(context.Context, node.Hash) (node.Invoice, error) {
	return node.Invoice{}, node.ErrUnknownHash
}
func (f *fakeIn) SettleInvoice(context.Context, node.Preimage) error { return nil }
func (f *fakeIn) CancelInvoice(context.Context, node.Hash) error     { return nil }
func (f *fakeIn) ForgetInvoice(context.Context, node.Hash) error     { return nil }

func (f *fakeIn) BlockHeight(context.Context) (int32, error) { return 500, nil }

// quotableService is a service whose one direction can actually produce a
// quote, so the handlers can be exercised without two Lightning nodes.
func quotableService(t *testing.T) *Service {
	t.Helper()

	dir := t.TempDir()
	j := openJournal(t, dir)
	t.Cleanup(func() { j.Close() })

	out := &fakeOut{
		accepts: "lnbc",
		decoded: node.Decoded{
			Hash:        testHash,
			AmountMsat:  100_000_000,
			CLTVDelta:   40,
			Expiry:      time.Now().Add(time.Hour),
			Destination: "03destination",
			Description: "a coffee",
		},
	}

	s := &Service{
		log: quiet(), ctx: context.Background(), journal: j,
		cfg: Config{Journal: filepath.Join(dir, "swaps.journal")},
	}
	s.price = rate.Reading{
		OutgoingPerIncoming: 0.00308078, At: time.Now(), Sources: 2,
		Spread: 0.01,
	}
	oracle, err := rate.New(rate.DefaultPolicy)
	if err != nil {
		t.Fatal(err)
	}
	s.oracle = oracle
	s.lfChain = observerAt(t, 155*time.Second)
	s.btcChain = observerAt(t, 600*time.Second)

	sd := &side{
		name: "toBitcoin", in: &fakeIn{}, out: out, dir: inventory.Draining,
		balance: func(context.Context) (uint64, error) {
			return 10_000_000_000, nil
		},
	}
	marg := margin.Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
		HoldExpiryDelta: margin.DefaultHoldExpiryDelta}
	sd.quoter = &quote.Quoter{
		In: sd.in, Out: sd.out, Store: j,
		Price:   s.pricer(sd, inventory.DefaultPolicy, sd.balance),
		Spacing: quote.Rates(s.spacing(false)),
		Room:    s.headroom(sd),
		Margin:  marg,
		Policy:  quote.DefaultPolicy,
	}
	sd.runner = &runner.Runner{
		Driver: &driver.Driver{
			In: sd.in, Out: sd.out, Store: j, Policy: marg,
		},
		Store: j, Rates: runner.Rates(s.spacing(false)), Log: quiet(),
	}
	s.sides = append(s.sides, sd)

	return s
}

func post(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/quote",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	return w
}

func TestQuoteReturnsTermsAndRecordsTheSwap(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	w := post(t, s.Handler(), `{"invoice":"lnbcrt1500n1p4invoice"}`)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}

	var got QuoteResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.HoldInvoice == "" {
		t.Error("no hold invoice, so the user has nothing to pay")
	}
	if got.Hash != hex.EncodeToString(testHash[:]) {
		t.Errorf("hash %s does not match the invoice's %x", got.Hash, testHash)
	}
	if got.OutgoingMsat != 100_000_000 {
		t.Errorf("outgoing %d, wanted what the invoice asked for",
			got.OutgoingMsat)
	}
	// The payer brings BTCB2, which is worth far less each, so there must be
	// more of them. Getting this backwards is the expensive mistake.
	if got.IncomingMsat <= got.OutgoingMsat {
		t.Errorf("incoming %d is not more than outgoing %d at a rate of %g",
			got.IncomingMsat, got.OutgoingMsat, got.Rate)
	}
	if got.Direction != "toBitcoin" {
		t.Errorf("direction %q", got.Direction)
	}

	// The swap has to be in the journal before the response is sent: the
	// hold invoice is live, and a crash now must leave something to resume.
	if _, err := s.journal.Get(context.Background(), testHash); err != nil {
		t.Errorf("the swap was not recorded: %v", err)
	}
}

func TestQuoteRefusals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "not JSON", body: `{`,
			want: http.StatusBadRequest,
		},
		{
			// A field the daemon does not know is a caller expecting
			// something it will not get.
			name: "unknown field",
			body: `{"invoice":"lnbcrt1x","chain":"bitcoin"}`,
			want: http.StatusBadRequest,
		},
		{
			name: "no invoice", body: `{"invoice":""}`,
			want: http.StatusBadRequest,
		},
		{
			name: "an invoice no node can read",
			body: `{"invoice":"ltc1somethingelse"}`,
			want: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			s := quotableService(t)
			w := post(t, s.Handler(), test.body)
			if w.Code != test.want {
				t.Errorf("got %d, wanted %d: %s", w.Code, test.want,
					w.Body.String())
			}
			if !strings.Contains(w.Body.String(), "error") {
				t.Errorf("a refusal should carry a reason: %s", w.Body.String())
			}
		})
	}
}

// A bridge that cannot deliver must refuse with the reason rather than fail
// as though it were broken: the caller can act on "too large", not on a 502.
func TestQuoteRefusedIsAConflictNotAFailure(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	// Nothing to pay out with, so the quote is refused on headroom.
	s.sides[0].balance = func(context.Context) (uint64, error) { return 0, nil }

	w := post(t, s.Handler(), `{"invoice":"lnbcrt1500n1p4invoice"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("got %d, wanted 409: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "headroom") {
		t.Errorf("the body should say what it ran out of: %s", w.Body.String())
	}
}

// Asking twice for the same invoice must not produce a second hold invoice
// for a hash that already has one.
func TestQuotingTheSameInvoiceTwiceIsRefused(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	h := s.Handler()

	if w := post(t, h, `{"invoice":"lnbcrt1500n1p4invoice"}`); w.Code != http.StatusOK {
		t.Fatalf("the first quote should succeed, got %d: %s", w.Code,
			w.Body.String())
	}
	w := post(t, h, `{"invoice":"lnbcrt1500n1p4invoice"}`)
	if w.Code == http.StatusOK {
		t.Fatal("the second quote for the same hash should be refused")
	}
}

func TestSwapLookup(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	now := time.Now()
	err := s.journal.Put(context.Background(), store.Record{
		Hash: testHash, State: swap.Settled, OutgoingCLTVLimit: 40,
		Invoice: "lnbc1fake", IncomingMsat: 49_000_000,
		OutgoingMsat: 150_000, Rate: 0.00308078, Spread: 0.01,
		Preimage: testPreimage, Created: now, Updated: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		path string
		want int
	}{
		{name: "found", path: "/swap/" + hex.EncodeToString(testHash[:]),
			want: http.StatusOK},
		{name: "not found",
			path: "/swap/" + strings.Repeat("ab", 32),
			want: http.StatusNotFound},
		{name: "not hex", path: "/swap/zzzz", want: http.StatusBadRequest},
		{name: "wrong length", path: "/swap/abcd",
			want: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, req)

			if w.Code != test.want {
				t.Fatalf("got %d, wanted %d: %s", w.Code, test.want,
					w.Body.String())
			}
			if test.want != http.StatusOK {
				return
			}

			var got SwapResponse
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.State != "settled" {
				t.Errorf("state %q", got.State)
			}
			// The preimage is not the asker's to have: anyone who can
			// reach this can ask about any hash, and the payer's own
			// node learns it by being settled.
			if strings.Contains(w.Body.String(),
				hex.EncodeToString(testPreimage[:])) ||
				strings.Contains(w.Body.String(), "preimage") {

				t.Errorf("the preimage was returned: %s", w.Body.String())
			}
		})
	}
}

// Health has to distinguish "running" from "able to do anything", because a
// bridge with no price is up and useless and that is how it goes unnoticed.
func TestHealthReportsUnreadyWithoutAPrice(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	s.priceErr = errors.New("every source failed")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, wanted 503: %s", w.Code, w.Body.String())
	}

	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["ready"] != false {
		t.Errorf("ready should be false: %v", got)
	}
	if !strings.Contains(w.Body.String(), "every source failed") {
		t.Errorf("health should say what is wrong: %s", w.Body.String())
	}
}

func TestHealthReportsUnreadyWhenANodeIsUnreachable(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	s.priceErr = nil
	s.sides[0].balance = func(context.Context) (uint64, error) {
		return 0, errors.New("connection refused")
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, wanted 503: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "connection refused") {
		t.Errorf("health should name the unreachable node: %s",
			w.Body.String())
	}
}

func TestHealthIsReadyWhenEverythingAnswers(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	s.priceErr = nil

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d, wanted 200: %s", w.Code, w.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["ready"] != true {
		t.Errorf("ready should be true: %v", got)
	}
}

// The surface is small on purpose, so the methods it does not serve should
// say so rather than doing something surprising.
func TestUnsupportedMethods(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	req := httptest.NewRequest(http.MethodGet, "/quote", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /quote returned %d, wanted 405", w.Code)
	}
}

// A caller that hangs up mid-quote must not abandon the swap. Quoting writes
// a journal record and then creates a hold invoice on the node, and both
// outlive the connection: dropping it half way leaves a swap that only a
// restart would notice.
func TestQuoteSurvivesTheCallerHangingUp(t *testing.T) {
	t.Parallel()

	s := quotableService(t)

	// A request whose context is already dead, which is what the server
	// hands the handler once the client has gone.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/quote",
		strings.NewReader(`{"invoice":"lnbcrt1500n1p4invoice"}`)).WithContext(ctx)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got %d, wanted the quote to be made anyway: %s", w.Code,
			w.Body.String())
	}
	if _, err := s.journal.Get(context.Background(), testHash); err != nil {
		t.Errorf("the swap should have been recorded even though the caller "+
			"was gone: %v", err)
	}
}

// New closes on a partial failure and the caller's defer closes again, so
// closing twice has to be harmless.
func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	var closed int
	s := &Service{log: quiet()}
	s.closers = []func() error{
		func() error { closed++; return nil },
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("closing twice should be harmless: %v", err)
	}
	if closed != 1 {
		t.Errorf("the closer ran %d times, wanted once", closed)
	}
}

// Closing reports what failed rather than stopping at the first one, or a
// journal that would not flush could be hidden by a connection that would
// not close.
func TestCloseReportsEveryFailure(t *testing.T) {
	t.Parallel()

	s := &Service{log: quiet()}
	s.closers = []func() error{
		func() error { return errors.New("journal") },
		func() error { return errors.New("bitcoin node") },
	}

	err := s.Close()
	if err == nil {
		t.Fatal("wanted the failures reported")
	}
	for _, want := range []string{"journal", "bitcoin node"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error should mention %q: %v", want, err)
		}
	}
}

// A journal that has gone means every quote fails, so health has to say so
// rather than reporting a bridge that is up and useless.
func TestHealthReportsAMissingJournal(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	s.priceErr = nil

	// A volume that went away under a running daemon, which is the failure
	// this actually catches.
	if err := os.Remove(s.cfg.Journal); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, wanted 503: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"journal"`) {
		t.Errorf("health should name the journal: %s", w.Body.String())
	}
}

// TestARefusalCarriesItsCode: a client branches on the code and shows the
// sentence, so both have to be there.
func TestARefusalCarriesItsCode(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	s.sides[0].quoter.Policy.MaxSwapMsat = 1_000_000
	w := post(t, s.Handler(), `{"invoice":"lnbcrt1500n1p4invoice"}`)

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["code"] != string(quote.CodeTooLarge) {
		t.Errorf("code %q, want %q", body["code"], quote.CodeTooLarge)
	}
	if body["error"] == "" {
		t.Error("no sentence")
	}
}

// TestAnUntrackedSwapIsDriven: the record exists and the hold invoice may, so
// the daemon drives it rather than leaving its amount reserved until a
// restart. Driving it here, with nothing on the node, lets it expire.
func TestAnUntrackedSwapIsDriven(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	sd := s.sides[0]
	in := &fakeIn{addErr: errors.New("timed out")}
	sd.in = in
	sd.quoter.In = in
	sd.runner.Driver.In = in
	expired := time.Now().Add(-time.Second)
	sd.quoter.Now = func() time.Time { return expired.Add(-quote.DefaultPolicy.Validity) }

	w := post(t, s.Handler(), `{"invoice":"lnbcrt1500n1p4invoice"}`)
	if w.Code == http.StatusOK {
		t.Fatalf("a failed hold invoice was reported as a quote")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := s.journal.Get(context.Background(), testHash)
		if err == nil && rec.State == swap.Expired {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	rec, _ := s.journal.Get(context.Background(), testHash)
	t.Fatalf("the untracked swap was left %v; it should have been driven "+
		"to expired", rec.State)
}

// TestEachCallerHasItsOwnLimits: a quote commits the bridge's money and costs
// the caller nothing until it pays, so each address that asks is limited on
// its own. One over its limit is told so with 429, while another address is
// unaffected; the swap records which address asked.
func TestEachCallerHasItsOwnLimits(t *testing.T) {
	t.Parallel()

	s := quotableService(t)
	s.sides[0].quoter.Limits = quote.Limits{PerMinute: 1}
	h := s.Handler()

	from := func(addr string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/quote",
			strings.NewReader(`{"invoice":"lnbcrt1500n1p4invoice"}`))
		req.RemoteAddr = addr
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		return w
	}

	if w := from("192.0.2.1:4000"); w.Code != http.StatusOK {
		t.Fatalf("the first quote: %d %s", w.Code, w.Body.String())
	}
	rec, err := s.journal.Get(context.Background(), testHash)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Participant != "addr:192.0.2.1" {
		t.Errorf("recorded as %q", rec.Participant)
	}

	// The same address on another connection is the same caller.
	w := from("192.0.2.1:4001")
	if w.Code != http.StatusTooManyRequests ||
		!strings.Contains(w.Body.String(), `"limit"`) {

		t.Errorf("a second quote in the minute: %d %s", w.Code,
			w.Body.String())
	}

	// Another address is not limited by the first. It is refused only
	// because the invoice already has a swap.
	w = from("198.51.100.7:4000")
	if w.Code != http.StatusConflict ||
		!strings.Contains(w.Body.String(), `"in_progress"`) {

		t.Errorf("another caller: %d %s", w.Code, w.Body.String())
	}
}
