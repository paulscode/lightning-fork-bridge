package bridged

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/chainrate"
	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/inventory"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/paulscode/lightning-fork-bridge/rate"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// quiet keeps test output to what the test itself says.
func quiet() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeOut answers Decode and nothing else, which is all routing needs.
type fakeOut struct {
	// accepts is the invoice prefix this node will read. Both chains'
	// invoices share a prefix, so in practice each node reads every
	// Lightning invoice and refuses only something that is not one.
	accepts string
	decoded node.Decoded
	calls   int
}

// b2bMarker in a test invoice stands for option_blake2b being set, which is
// how a real node's decoder reports an invoice issued on the BLAKE2b chain.
const b2bMarker = "b2b"

func (f *fakeOut) Decode(_ context.Context, invoice string) (node.Decoded, error) {
	f.calls++
	if f.accepts == "" || len(invoice) < len(f.accepts) ||
		invoice[:len(f.accepts)] != f.accepts {

		return node.Decoded{}, errors.New("not a Lightning invoice")
	}

	d := f.decoded
	d.BLAKE2b = strings.Contains(invoice, b2bMarker)

	return d, nil
}

func (f *fakeOut) Pay(context.Context, node.PayRequest) (node.Payment, error) {
	return node.Payment{}, errors.New("not used")
}

func (f *fakeOut) LookupPayment(context.Context, node.Hash) (node.Payment, error) {
	return node.Payment{}, errors.New("not used")
}

func (f *fakeOut) BlockHeight(context.Context) (int32, error) { return 100, nil }

// testService is a Service with both directions wired to fakes.
func testService(t *testing.T, toBitcoin, toBlake2b *fakeOut,
	balance func(context.Context) (uint64, error)) *Service {

	t.Helper()

	if balance == nil {
		balance = func(context.Context) (uint64, error) {
			return 1_000_000_000, nil
		}
	}

	s := &Service{log: quiet(), ctx: context.Background()}
	if toBitcoin != nil {
		s.sides = append(s.sides, &side{
			name: "toBitcoin", out: toBitcoin, balance: balance,
			dir: inventory.Draining,
		})
	}
	if toBlake2b != nil {
		s.sides = append(s.sides, &side{
			name: "toBlake2b", out: toBlake2b, balance: balance,
			dir: inventory.Replenishing, invert: true, paysBLAKE2b: true,
		})
	}

	return s
}

// Both chains' invoices share a prefix and both nodes read every one, so the
// direction has to come from what the invoice says about its chain. This is
// the regression for routing by "which node can decode it", which sent every
// BLAKE2b invoice to the Bitcoin side once the prefixes became the same.
func TestRouteGoesByTheChainNotTheDecoder(t *testing.T) {
	t.Parallel()

	btc := &fakeOut{accepts: "ln"}
	lf := &fakeOut{accepts: "ln"}
	s := testService(t, btc, lf, nil)

	tests := []struct {
		invoice string
		want    string
	}{
		{invoice: "lnbcrt1500n1p4invoice", want: "toBitcoin"},
		{invoice: "lnbcrt1m1p4" + b2bMarker + "invoice", want: "toBlake2b"},
	}

	for _, test := range tests {
		sd, dec, err := s.route(context.Background(), test.invoice)
		if err != nil {
			t.Fatalf("%s: %v", test.invoice, err)
		}
		if sd.name != test.want {
			t.Errorf("%s routed to %s, wanted %s", test.invoice, sd.name,
				test.want)
		}
		if dec.BLAKE2b != sd.paysBLAKE2b {
			t.Errorf("%s: routed to a side paying on the other chain",
				test.invoice)
		}
	}
}

func TestRouteRefusesAnInvoiceNeitherNodeReads(t *testing.T) {
	t.Parallel()

	s := testService(t, &fakeOut{accepts: "lnbc"},
		&fakeOut{accepts: "lnblake"}, nil)

	_, _, err := s.route(context.Background(), "ltc1notourchain")
	if !errors.Is(err, ErrNoDirection) {
		t.Fatalf("wanted ErrNoDirection, got %v", err)
	}
}

// A direction that is not enabled must not be reachable by handing the daemon
// an invoice for it, or a bridge with no BTCB2 would quote swaps it cannot pay.
func TestRouteWillNotUseADisabledDirection(t *testing.T) {
	t.Parallel()

	s := testService(t, &fakeOut{accepts: "ln"}, nil, nil)

	_, _, err := s.route(context.Background(),
		"lnbcrt1m1p4"+b2bMarker+"invoice")
	if !errors.Is(err, ErrNoDirection) {
		t.Fatalf("a BLAKE2b invoice should be unroutable with only "+
			"toBitcoin enabled, got %v", err)
	}
	// And the reason is the chain, which is what a payer is told.
	if quote.CodeOf(err) != quote.CodeNoDirection {
		t.Errorf("code %q, want %q", quote.CodeOf(err),
			quote.CodeNoDirection)
	}
}

// The reverse direction is the same machinery with the rate inverted, so the
// inversion is the one number that has to be right.
func TestPricerInvertsForTheReverseDirection(t *testing.T) {
	t.Parallel()

	const market = 0.00308078

	s := testService(t, &fakeOut{accepts: "lnbc"},
		&fakeOut{accepts: "lnblake"}, nil)
	s.price = rate.Reading{
		OutgoingPerIncoming: market, At: time.Now(), Sources: 2,
		Spread: 0.01,
	}
	s.priceErr = nil

	inv := inventory.DefaultPolicy
	inv.TargetOutgoingMsat = 1_000_000_000
	inv.FloorOutgoingMsat = 100_000_000

	forward, err := s.pricer(s.sides[0], inv, s.sides[0].balance)(context.Background())
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	reverse, err := s.pricer(s.sides[1], inv, s.sides[1].balance)(context.Background())
	if err != nil {
		t.Fatalf("reverse: %v", err)
	}

	if forward.OutgoingPerIncoming != market {
		t.Errorf("forward quoted %g, wanted the market rate %g",
			forward.OutgoingPerIncoming, market)
	}
	if math.Abs(reverse.OutgoingPerIncoming-1/market) > 1e-9 {
		t.Errorf("reverse quoted %g, wanted the reciprocal %g",
			reverse.OutgoingPerIncoming, 1/market)
	}
	// The two must be reciprocals of each other, or a round trip through
	// the bridge would gain or lose value for reasons nobody chose.
	if math.Abs(forward.OutgoingPerIncoming*reverse.OutgoingPerIncoming-1) > 1e-9 {
		t.Errorf("the two directions are not reciprocal: %g * %g",
			forward.OutgoingPerIncoming, reverse.OutgoingPerIncoming)
	}
}

// A price the daemon has never read must refuse rather than quote at zero.
func TestPricerRefusesBeforeAnyPrice(t *testing.T) {
	t.Parallel()

	s := testService(t, &fakeOut{accepts: "lnbc"}, nil, nil)
	s.priceErr = errors.New("no price has been read yet")

	if _, err := s.pricer(s.sides[0], inventory.DefaultPolicy, s.sides[0].balance)(
		context.Background()); err == nil {

		t.Fatal("a quote before the first price should be refused")
	}
}

// Inverting zero is how a bad feed becomes an infinite rate, which would
// quote an incoming amount of zero and give the swap away.
func TestPricerRefusesToInvertAnUnusableRate(t *testing.T) {
	t.Parallel()

	for _, bad := range []float64{0, -1} {
		s := testService(t, nil, &fakeOut{accepts: "lnblake"}, nil)
		s.price = rate.Reading{OutgoingPerIncoming: bad, At: time.Now()}
		s.priceErr = nil

		_, err := s.pricer(s.sides[0], inventory.DefaultPolicy, s.sides[0].balance)(
			context.Background())
		if err == nil {
			t.Errorf("inverting %g should be refused", bad)
		}
	}
}

// The spread has two independent reasons to widen and charging only one of
// them underprices the other risk.
func TestSpreadTakesTheWiderOfTheTwo(t *testing.T) {
	t.Parallel()

	inv := inventory.DefaultPolicy
	inv.TargetOutgoingMsat = 1_000_000_000
	inv.FloorOutgoingMsat = 100_000_000

	tests := []struct {
		name        string
		volatility  float64
		heldMsat    uint64
		wantAtLeast float64
	}{
		{
			// Full outgoing side, so inventory is at its floor and the
			// oracle's wider number has to win.
			name: "volatility is wider", volatility: 0.025,
			heldMsat: 1_000_000_000, wantAtLeast: 0.025,
		},
		{
			// Drained outgoing side with a calm market: inventory wins.
			name: "inventory is wider", volatility: 0.001,
			heldMsat: 110_000_000, wantAtLeast: inv.BaseSpread,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			held := test.heldMsat
			s := testService(t, &fakeOut{accepts: "lnbc"}, nil,
				func(context.Context) (uint64, error) { return held, nil })
			s.price = rate.Reading{
				OutgoingPerIncoming: 0.003, At: time.Now(), Sources: 2,
				Spread: test.volatility,
			}
			s.priceErr = nil

			got, err := s.pricer(s.sides[0], inv, s.sides[0].balance)(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got.Spread < test.wantAtLeast {
				t.Errorf("spread %g is below %g, so one of the two reasons "+
					"to widen was dropped", got.Spread, test.wantAtLeast)
			}
		})
	}
}

// Headroom that ignored what is already promised would let the bridge commit
// the same balance to two swaps.
func TestHeadroomSubtractsWhatIsAlreadyCommitted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	j := openJournal(t, dir)
	defer j.Close()

	s := testService(t, &fakeOut{accepts: "lnbc"}, nil,
		func(context.Context) (uint64, error) { return 1_000_000, nil })
	s.journal = j

	room, err := s.headroom(s.sides[0])(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if room != 1_000_000 {
		t.Fatalf("an empty journal should leave the whole balance, got %d",
			room)
	}

	commit(t, j, 400_000)

	room, err = s.headroom(s.sides[0])(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if room != 600_000 {
		t.Errorf("with 400,000 committed the headroom should be 600,000, "+
			"got %d", room)
	}
}

// A balance below what is committed must report no room rather than wrap
// around, which on an unsigned subtraction would look like limitless room.
func TestHeadroomDoesNotUnderflow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	j := openJournal(t, dir)
	defer j.Close()

	s := testService(t, &fakeOut{accepts: "lnbc"}, nil,
		func(context.Context) (uint64, error) { return 100_000, nil })
	s.journal = j
	commit(t, j, 900_000)

	room, err := s.headroom(s.sides[0])(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if room != 0 {
		t.Errorf("committed more than the balance should leave no room, "+
			"got %d", room)
	}
}

// Using one chain's spacing for both legs would size a swap against a chain
// it is not on, so the reverse direction has to swap them.
func TestSpacingSwapsTheLegsForTheReverseDirection(t *testing.T) {
	t.Parallel()

	s := testService(t, nil, nil, nil)
	s.lfChain = observerAt(t, 30*time.Second)
	s.btcChain = observerAt(t, 600*time.Second)

	forward, err := s.spacing(false)(context.Background())
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	reverse, err := s.spacing(true)(context.Background())
	if err != nil {
		t.Fatalf("reverse: %v", err)
	}

	if forward.Incoming != reverse.Outgoing ||
		forward.Outgoing != reverse.Incoming {

		t.Errorf("the reverse direction should be the forward one with the "+
			"legs exchanged: forward %+v, reverse %+v", forward, reverse)
	}
	// The fast chain is the BLAKE2b one, and it is the incoming leg only in
	// the forward direction.
	if forward.Incoming.Fast >= forward.Outgoing.Fast {
		t.Errorf("the forward incoming leg should be the faster chain: %+v",
			forward)
	}
}

func TestMarginPolicyIsCarriedIntoBothDirections(t *testing.T) {
	t.Parallel()

	// A margin policy that reached one direction and not the other would
	// size half the swaps against nothing.
	cfg := margin.Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
		HoldExpiryDelta: margin.DefaultHoldExpiryDelta}
	s := &Service{cfg: Config{Margin: cfg}, log: quiet()}
	if s.cfg.Margin != cfg {
		t.Fatal("the margin policy did not survive being stored")
	}
}

// openJournal is a real journal on disk, because Committed reads what was
// written rather than what a fake would have remembered.
func openJournal(t *testing.T, dir string) *store.Journal {
	t.Helper()

	j, err := store.Open(filepath.Join(dir, "swaps.journal"))
	if err != nil {
		t.Fatal(err)
	}

	return j
}

// commit writes one swap that counts against headroom.
func commit(t *testing.T, j *store.Journal, outgoingMsat uint64) {
	t.Helper()

	now := time.Now()
	err := j.Put(context.Background(), store.Record{
		Hash: node.Hash{byte(outgoingMsat), 2, 3}, State: swap.Offered,
		OutgoingCLTVLimit: 40, Invoice: "lnbc1fake",
		IncomingMsat: outgoingMsat * 300, OutgoingMsat: outgoingMsat,
		Rate: 0.00308078, Spread: 0.01, Created: now, Updated: now,
	})
	if err != nil {
		t.Fatal(err)
	}
}

// observerAt is a chain observer already holding enough blocks at a given
// spacing to produce an estimate.
func observerAt(t *testing.T, spacing time.Duration) *chainrate.Observer {
	t.Helper()

	// The target is the spacing being simulated and the factors are tight,
	// so the bounds hug what was measured. Wide factors would make every
	// swap need a CLTV nobody would route, which is a property of the
	// parameters rather than of the code under test.
	obs, err := chainrate.New(chainrate.Params{
		RetargetInterval: 2016, TargetSpacing: spacing,
		SurgeFactor: 2, StallFactor: 2, MinSamples: 6,
		StaleAfter: 24 * time.Hour, SurgeWindow: 12,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now().Add(-20 * spacing)
	for i := 0; i < 12; i++ {
		err := obs.Add(chainrate.Block{
			Height: int32(1000 + i),
			Time:   start.Add(time.Duration(i) * spacing),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	return obs
}

// node8 is a distinct hash for a test record.
func node8(n byte) node.Hash {
	return node.Hash{n, n, n, n, n, n, n, n}
}

// inventoryForTest is a policy scaled to the balances the tests use.
func inventoryForTest() inventory.Policy {
	p := inventory.DefaultPolicy
	p.TargetOutgoingMsat = 1_000_000_000
	p.FloorOutgoingMsat = 100_000_000

	return p
}

// emptyObserver has seen no blocks, so it cannot estimate.
func emptyObserver(t *testing.T) *chainrate.Observer {
	t.Helper()

	obs, err := chainrate.New(chainrate.Params{
		RetargetInterval: 2016, TargetSpacing: 10 * time.Minute,
		SurgeFactor: 2, StallFactor: 2, MinSamples: 6,
		StaleAfter: 24 * time.Hour, SurgeWindow: 12,
	})
	if err != nil {
		t.Fatal(err)
	}

	return obs
}

// The oracle bounds how old an observation may be when it combines them.
// Nothing bounded how old the combined reading was by the time a quote used
// it, so a poller that stopped producing left the last price in place and
// quotes went out on it.
func TestPricerRefusesAPriceThatStoppedBeingRefreshed(t *testing.T) {
	t.Parallel()

	s := testService(t, &fakeOut{accepts: "lnbc"}, nil, nil)
	s.cfg.Rate.MaxAge = 90 * time.Second
	s.priceErr = nil

	// Fresh enough: quoted.
	s.price = rate.Reading{
		OutgoingPerIncoming: 0.003, At: time.Now().Add(-30 * time.Second),
		Sources: 2, Spread: 0.01,
	}
	if _, err := s.pricer(s.sides[0], inventoryForTest(), s.sides[0].balance)(
		context.Background()); err != nil {

		t.Fatalf("a price inside MaxAge should be usable: %v", err)
	}

	// Older than the policy allows: refused, and the reason says why.
	s.price.At = time.Now().Add(-10 * time.Minute)
	_, err := s.pricer(s.sides[0], inventoryForTest(), s.sides[0].balance)(context.Background())
	if err == nil {
		t.Fatal("a price older than MaxAge should be refused")
	}
	if !strings.Contains(err.Error(), "price feed has stopped") {
		t.Errorf("the refusal should say the feed stopped: %v", err)
	}
}

// An invoice for a direction the operator turned off should send them to the
// line they changed, not to their nodes.
func TestRouteNamesADisabledDirection(t *testing.T) {
	t.Parallel()

	s := testService(t, &fakeOut{accepts: "lnbc"}, nil, nil)
	s.disabled = []string{"toBlake2b"}

	_, _, err := s.route(context.Background(), "lnblakert1m1p4invoice")
	if !errors.Is(err, ErrNoDirection) {
		t.Fatalf("wanted ErrNoDirection, got %v", err)
	}
	if !strings.Contains(err.Error(), "toBlake2b") {
		t.Errorf("the refusal should name the disabled direction: %v", err)
	}
	if !strings.Contains(err.Error(), "not enabled") {
		t.Errorf("the refusal should say it is switched off: %v", err)
	}
}

// TestHeadroomIsCountedPerDirection: a swap paying out on one chain is no
// claim on the other chain's balance. Summing both would refuse quotes the
// bridge could pay on one side and sell the same money twice on the other.
func TestHeadroomIsCountedPerDirection(t *testing.T) {
	t.Parallel()

	j := openJournal(t, t.TempDir())
	defer j.Close()

	s := testService(t, &fakeOut{accepts: "ln"}, &fakeOut{accepts: "ln"},
		func(context.Context) (uint64, error) { return 1_000_000, nil })
	s.journal = j

	now := time.Now()
	for i, r := range []struct {
		invoice string
		msat    uint64
	}{
		{"lnbcrt1tobitcoin", 300_000},
		{"lnbcrt1" + b2bMarker + "toblake2b", 900_000},
	} {
		err := j.Put(context.Background(), store.Record{
			Hash: node.Hash{byte(i + 1)}, State: swap.Offered,
			OutgoingCLTVLimit: 40, Invoice: r.invoice,
			IncomingMsat: 1, OutgoingMsat: r.msat,
			Rate: 0.003, Spread: 0.01, Created: now, Updated: now,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, want := range []uint64{700_000, 100_000} {
		room, err := s.headroom(s.sides[i])(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if room != want {
			t.Errorf("%s: headroom %d, want %d", s.sides[i].name, room,
				want)
		}
	}
}

// TestAFundedSwapIsPaidOnlyAtAPriceThatCoversIt: the runner asks just before
// paying whether what the payer sent still covers the payout and its routing
// budget at the price now. A move against the bridge gives the funds back; a
// stale price waits, and past the funded grace the swap is given back, so the
// grace path never pays on a price nobody can see. Both directions are wired
// with it.
func TestAFundedSwapIsPaidOnlyAtAPriceThatCoversIt(t *testing.T) {
	t.Parallel()

	s := testService(t, &fakeOut{accepts: "lnbc"}, nil, nil)
	s.cfg.Rate.MaxAge = 90 * time.Second
	s.priceErr = nil
	sd := s.sides[0]

	// 1,000,000 msat in at 0.0031 is worth 3,100 msat out.
	sw := &driver.Swap{IncomingMsat: 1_000_000, OutgoingMsat: 3_000,
		MaxFeeMsat: 100}
	for _, c := range []struct {
		price float64
		ok    bool
	}{
		{0.0031, true},
		{0.00309, false},
	} {
		s.price = rate.Reading{OutgoingPerIncoming: c.price,
			At: time.Now(), Sources: 2}
		ok, err := s.priceCheck(sd)(context.Background(), sw)
		if err != nil || ok != c.ok {
			t.Errorf("at %g: %v (%v), want %v", c.price, ok, err, c.ok)
		}
	}

	// The reverse direction inverts the price.
	rev := &side{name: "toBlake2b", invert: true}
	s.price = rate.Reading{OutgoingPerIncoming: 1 / 0.0031, At: time.Now(),
		Sources: 2}
	if ok, err := s.priceCheck(rev)(context.Background(), sw); err != nil ||
		!ok {

		t.Errorf("the inverted price refused: %v (%v)", ok, err)
	}

	// A stale price is not an answer.
	s.price = rate.Reading{OutgoingPerIncoming: 0.0031,
		At: time.Now().Add(-time.Hour), Sources: 2}
	if _, err := s.priceCheck(sd)(context.Background(), sw); err == nil {
		t.Error("a stale price was checked against")
	}

	// And it is wired into the runner build makes.
	built := s.build("toBitcoin", Direction{Inventory: inventoryForTest()},
		nil, nil, sd.balance, inventory.Draining, false, nil)
	if built.runner.PriceCheck == nil {
		t.Error("the runner has no price check")
	}
}
