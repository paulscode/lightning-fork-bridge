package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// fakeIn is a scripted incoming node.
type fakeIn struct {
	invoice   node.Invoice
	lookupErr error
	height    int32
	heightErr error

	settled   int
	cancelled int
}

func (f *fakeIn) AddHoldInvoice(context.Context, node.HoldInvoice) (string, error) {
	return "lnblakert1fake", nil
}

func (f *fakeIn) LookupInvoice(context.Context, node.Hash) (node.Invoice, error) {
	if f.lookupErr != nil {
		return node.Invoice{}, f.lookupErr
	}

	return f.invoice, nil
}

func (f *fakeIn) SettleInvoice(context.Context, node.Preimage) error {
	f.settled++

	return nil
}

func (f *fakeIn) CancelInvoice(context.Context, node.Hash) error {
	f.cancelled++

	return nil
}

func (f *fakeIn) ForgetInvoice(context.Context, node.Hash) error { return nil }

func (f *fakeIn) BlockHeight(context.Context) (int32, error) {
	return f.height, f.heightErr
}

// fakeOut is a scripted outgoing node.
type fakeOut struct {
	payment   node.Payment
	payErr    error
	lookup    node.Payment
	lookupErr error
	paid      int

	// lookups answers for specific hashes, consulted before lookup. A
	// sequenced swap's two legs have different hashes, so a fake that
	// answered the same for any hash could not show that the driver asks
	// about the right one.
	lookups map[node.Hash]node.Payment
}

func (f *fakeOut) Pay(context.Context, node.PayRequest) (node.Payment, error) {
	f.paid++
	if f.payErr != nil {
		return node.Payment{}, f.payErr
	}

	return f.payment, nil
}

func (f *fakeOut) LookupPayment(_ context.Context, hash node.Hash) (
	node.Payment, error) {

	if p, ok := f.lookups[hash]; ok {
		return p, nil
	}

	return f.lookup, f.lookupErr
}

// Decode reads a payment request. The bridge uses the paying node's own
// decoder so that no second parser can disagree about the payment hash.
func (f *fakeOut) Decode(context.Context, string) (node.Decoded, error) {
	return node.Decoded{}, nil
}

func (f *fakeOut) BlockHeight(context.Context) (int32, error) { return 0, nil }

var (
	blake    = margin.Bounds{Fast: 155 * time.Second, Slow: 1200 * time.Second}
	btc      = margin.Bounds{Fast: 300 * time.Second, Slow: 600 * time.Second}
	measured = time.Unix(1_700_000_000, 0)
	rates    = Rates{Incoming: blake, Outgoing: btc, As: measured}
	pol      = margin.Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
		HoldExpiryDelta: margin.DefaultHoldExpiryDelta}
)

func fixedNow() time.Time { return measured.Add(time.Second) }

func funded() *Swap {
	return &Swap{State: swap.Funded, OutgoingCLTVLimit: 40}
}

func TestGatePassesWithRoomToSpare(t *testing.T) {
	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500, // 500 blocks left, far more than the 179 required
	}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}

	ev, err := d.Gate(context.Background(), funded(), rates)
	if err != nil {
		t.Fatal(err)
	}
	if ev != swap.MarginPassed {
		t.Errorf("got %v, want %v", ev, swap.MarginPassed)
	}
}

// TestGateRefusesWhenTheMarginIsNotThere is the abort-rather-than-pay
// decision. It is an ordinary outcome and must not be reported as an error.
func TestGateRefusesWhenTheMarginIsNotThere(t *testing.T) {
	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1100},
		height:  1000, // 100 blocks left against the 179 required
	}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}

	ev, err := d.Gate(context.Background(), funded(), rates)
	if err != nil {
		t.Fatalf("a refusal is not an error: %v", err)
	}
	if ev != swap.MarginRefused {
		t.Errorf("got %v, want %v", ev, swap.MarginRefused)
	}
}

// TestGateRefusesWithoutAClaim: paying out against an HTLC that is not locked
// in is paying for nothing.
func TestGateRefusesWithoutAClaim(t *testing.T) {
	for _, st := range []node.InvoiceState{
		node.InvoiceOpen, node.InvoiceCancelled, node.InvoiceSettled,
	} {
		in := &fakeIn{
			invoice: node.Invoice{State: st, ExpiryHeight: 100000},
			height:  1,
		}
		d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}

		ev, err := d.Gate(context.Background(), funded(), rates)
		if err != nil {
			t.Fatalf("%v: %v", st, err)
		}
		if ev != swap.MarginRefused {
			t.Errorf("%v: got %v, want %v", st, ev, swap.MarginRefused)
		}
	}
}

// TestGateRefusesOnAnythingItCannotRead. An unreadable chain is a reason to
// not pay, never a reason to pay anyway.
func TestGateRefusesOnAnythingItCannotRead(t *testing.T) {
	boom := errors.New("node unreachable")

	cases := []struct {
		name string
		in   *fakeIn
	}{{
		name: "invoice unreadable",
		in:   &fakeIn{lookupErr: boom},
	}, {
		name: "height unreadable",
		in: &fakeIn{
			invoice:   node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
			heightErr: boom,
		},
	}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := &Driver{In: c.in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}
			ev, err := d.Gate(context.Background(), funded(), rates)
			if err == nil {
				t.Error("want an error")
			}
			if ev != swap.MarginRefused {
				t.Errorf("even on error the event must be %v, got %v",
					swap.MarginRefused, ev)
			}
		})
	}

	// Unusable bounds refuse too, and say why.
	d := &Driver{
		In: &fakeIn{
			invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
			height:  1,
		},
		Out:    &fakeOut{},
		Policy: pol,
		Now:    fixedNow,
	}
	// Stamped, so it gets past the freshness check and reaches the bounds.
	ev, err := d.Gate(context.Background(), funded(), Rates{As: measured})
	if !errors.Is(err, margin.ErrBounds) {
		t.Errorf("want ErrBounds, got %v", err)
	}
	if ev != swap.MarginRefused {
		t.Errorf("got %v, want %v", ev, swap.MarginRefused)
	}
}

// TestGateRefusesAnExpiredClaim covers the boundary where the expiry height
// has already been reached, which must not underflow into a huge block count.
func TestGateRefusesAnExpiredClaim(t *testing.T) {
	for _, expiry := range []int32{999, 1000} {
		in := &fakeIn{
			invoice: node.Invoice{
				State: node.InvoiceAccepted, ExpiryHeight: expiry,
			},
			height: 1000,
		}
		d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}

		ev, err := d.Gate(context.Background(), funded(), rates)
		if err != nil {
			t.Fatalf("expiry %d: %v", expiry, err)
		}
		if ev != swap.MarginRefused {
			t.Errorf("expiry %d at height 1000: got %v, want %v", expiry,
				ev, swap.MarginRefused)
		}
	}
}

func TestGateNeedsAFundedSwap(t *testing.T) {
	d := &Driver{In: &fakeIn{}, Out: &fakeOut{}, Policy: pol, Now: fixedNow}
	for _, st := range []swap.State{
		swap.Quoted, swap.Offered, swap.Paying, swap.Paid, swap.Settled,
	} {
		sw := &Swap{State: st, OutgoingCLTVLimit: 40}
		ev, err := d.Gate(context.Background(), sw, rates)
		if !errors.Is(err, ErrState) {
			t.Errorf("%v: want ErrState, got %v", st, err)
		}
		if ev != swap.MarginRefused {
			t.Errorf("%v: got %v, want %v", st, ev, swap.MarginRefused)
		}
	}
}

// TestResumePayingWaitsWhileInFlight: concluding anything about a payment
// still in flight is the mistake the whole design refuses to make.
func TestResumePayingWaitsWhileInFlight(t *testing.T) {
	out := &fakeOut{lookup: node.Payment{State: node.PaymentInFlight}}
	d := &Driver{In: &fakeIn{}, Out: out, Policy: pol, Now: fixedNow}
	sw := &Swap{State: swap.Paying}

	_, ok, err := d.Resume(context.Background(), sw)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("an in-flight payment must produce no event")
	}
}

// TestResumePayingWithNoRecordStops is the case that must never be guessed.
// "Never sent" and "sent and lost track of" call for opposite actions.
func TestResumePayingWithNoRecordStops(t *testing.T) {
	out := &fakeOut{lookup: node.Payment{State: node.PaymentNone}}
	d := &Driver{In: &fakeIn{}, Out: out, Policy: pol, Now: fixedNow}
	sw := &Swap{State: swap.Paying}

	_, ok, err := d.Resume(context.Background(), sw)
	if !errors.Is(err, ErrNeedsOperator) {
		t.Fatalf("want ErrNeedsOperator, got %v", err)
	}
	if ok {
		t.Error("no event may accompany a decision the bridge cannot make")
	}
}

func TestResumePayingResolves(t *testing.T) {
	pre := node.Preimage{1, 2, 3}

	out := &fakeOut{lookup: node.Payment{
		State: node.PaymentSucceeded, Preimage: pre,
	}}
	d := &Driver{In: &fakeIn{}, Out: out, Policy: pol, Now: fixedNow}
	sw := &Swap{State: swap.Paying}
	ev, ok, err := d.Resume(context.Background(), sw)
	if err != nil || !ok || ev != swap.OutgoingSucceeded {
		t.Fatalf("succeeded: %v %v %v", ev, ok, err)
	}
	if sw.Preimage != pre {
		t.Error("a resumed success must record the preimage")
	}

	out = &fakeOut{lookup: node.Payment{State: node.PaymentFailed}}
	d = &Driver{In: &fakeIn{}, Out: out, Policy: pol, Now: fixedNow}
	sw = &Swap{State: swap.Paying}
	ev, ok, err = d.Resume(context.Background(), sw)
	if err != nil || !ok || ev != swap.OutgoingFailed {
		t.Fatalf("failed: %v %v %v", ev, ok, err)
	}
}

// TestResumePaidDetectsARealisedLoss: paid out, and the claim is gone.
func TestResumePaidDetectsARealisedLoss(t *testing.T) {
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceCancelled}}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}
	sw := &Swap{State: swap.Paid, Preimage: node.Preimage{9}}

	ev, ok, err := d.Resume(context.Background(), sw)
	if err != nil || !ok {
		t.Fatalf("%v %v", ok, err)
	}
	if ev != swap.IncomingUnsettleable {
		t.Errorf("got %v, want %v", ev, swap.IncomingUnsettleable)
	}

	// And if the settle already went through before the crash, that is a
	// plain success.
	in.invoice.State = node.InvoiceSettled
	ev, ok, err = d.Resume(context.Background(), &Swap{State: swap.Paid})
	if err != nil || !ok || ev != swap.IncomingSettled {
		t.Fatalf("already settled: %v %v %v", ev, ok, err)
	}
}

func TestResumeOtherStates(t *testing.T) {
	ctx := context.Background()

	// Offered, and the payer funded it while the bridge was down.
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted}}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}
	ev, ok, err := d.Resume(ctx, &Swap{State: swap.Offered})
	if err != nil || !ok || ev != swap.IncomingAccepted {
		t.Fatalf("offered then funded: %v %v %v", ev, ok, err)
	}

	// Offered, and it lapsed.
	in.invoice.State = node.InvoiceCancelled
	ev, ok, err = d.Resume(ctx, &Swap{State: swap.Offered})
	if err != nil || !ok || ev != swap.IncomingLapsed {
		t.Fatalf("offered then lapsed: %v %v %v", ev, ok, err)
	}

	// Mid-cancel, and the cancel went through.
	for _, st := range []swap.State{swap.Failing, swap.Aborting, swap.Expiring} {
		ev, ok, err = d.Resume(ctx, &Swap{State: st})
		if err != nil || !ok || ev != swap.IncomingCancelled {
			t.Fatalf("%v: %v %v %v", st, ev, ok, err)
		}
	}

	// Terminal swaps need nothing.
	for _, st := range []swap.State{
		swap.Settled, swap.Failed, swap.Aborted, swap.Expired, swap.Lost,
	} {
		if _, ok, err := d.Resume(ctx, &Swap{State: st}); ok || err != nil {
			t.Errorf("%v: %v %v", st, ok, err)
		}
	}

	// A quote with nothing created yet is consistent, not an error.
	unknown := &fakeIn{lookupErr: node.ErrUnknownHash}
	d = &Driver{In: unknown, Out: &fakeOut{}, Policy: pol}
	if _, ok, err := d.Resume(ctx, &Swap{State: swap.Quoted}); ok || err != nil {
		t.Errorf("quoted with no invoice: %v %v", ok, err)
	}
	// But an unknown hash for a swap that should have one is an error.
	if _, _, err := d.Resume(ctx, &Swap{State: swap.Offered}); err == nil {
		t.Error("an offered swap whose invoice the node does not know is a fault")
	}
}

// TestDoRefusesToCancelWhileCommitted is the belt to the state machine's
// braces: even asked directly, the driver will not give up the claim while a
// payment might be in flight.
func TestDoRefusesToCancelWhileCommitted(t *testing.T) {
	in := &fakeIn{}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}
	sw := &Swap{State: swap.Paying}

	if err := d.Do(context.Background(), sw, swap.CancelIncoming); !errors.Is(err, ErrState) {
		t.Fatalf("want ErrState, got %v", err)
	}
	if in.cancelled != 0 {
		t.Error("the cancel reached the node anyway")
	}
}

func TestDoActions(t *testing.T) {
	ctx := context.Background()

	in := &fakeIn{}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}

	if err := d.Do(ctx, &Swap{State: swap.Funded}, swap.Nothing); err != nil {
		t.Error(err)
	}
	if err := d.Do(ctx, &Swap{State: swap.Aborting}, swap.CancelIncoming); err != nil {
		t.Error(err)
	}
	if in.cancelled != 1 {
		t.Errorf("cancelled %d times, want 1", in.cancelled)
	}

	// Settling needs a preimage.
	if err := d.Do(ctx, &Swap{State: swap.Paid}, swap.SettleIncoming); !errors.Is(err, ErrState) {
		t.Errorf("settling without a preimage: %v", err)
	}
	if in.settled != 0 {
		t.Error("a settle without a preimage reached the node")
	}
	// A preimage that settles a different hash must not reach the node: it
	// would fail, and it would fail in the one state where failing costs the
	// bridge the whole swap.
	wrong := node.Preimage{7}
	if err := d.Do(ctx, &Swap{
		Hash: node.Hash{1}, State: swap.Paid, Preimage: wrong,
	}, swap.SettleIncoming); !errors.Is(err, ErrState) {

		t.Errorf("settling with a preimage for another hash: %v", err)
	}
	if in.settled != 0 {
		t.Error("a preimage for the wrong hash reached the node")
	}

	pre := node.Preimage{7}
	sw := &Swap{State: swap.Paid, Hash: pre.Hash(), Preimage: pre}
	if err := d.Do(ctx, sw, swap.SettleIncoming); err != nil {
		t.Error(err)
	}
	if in.settled != 1 {
		t.Errorf("settled %d times, want 1", in.settled)
	}

	// Paying is not a fire-and-forget action.
	if err := d.Do(ctx, &Swap{State: swap.Paying}, swap.PayOutgoing); !errors.Is(err, ErrState) {
		t.Errorf("PayOutgoing through Do: %v", err)
	}
	if err := d.Do(ctx, &Swap{}, swap.Action(200)); !errors.Is(err, ErrState) {
		t.Errorf("unknown action: %v", err)
	}
}

// TestPayLeavesSwapPayingOnAnUnknownOutcome: a timeout is not a failure, and
// the swap must stay in the state that forbids cancelling.
func TestPayLeavesSwapPayingOnAnUnknownOutcome(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name string
		out  *fakeOut
	}{
		{"transport error", &fakeOut{payErr: errors.New("deadline exceeded")}},
		{"still in flight", &fakeOut{payment: node.Payment{State: node.PaymentInFlight}}},
		{"no record", &fakeOut{payment: node.Payment{State: node.PaymentNone}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := &Driver{In: &fakeIn{}, Out: c.out, Policy: pol}
			sw := &Swap{State: swap.Paying}

			_, ok, _ := d.Pay(ctx, sw, node.PayRequest{})
			if ok {
				t.Error("an unknown outcome must produce no event")
			}
			if sw.State != swap.Paying {
				t.Errorf("state moved to %v", sw.State)
			}
		})
	}
}

// payingDriver returns a driver whose store already records the swap as
// paying, which is what Pay requires on disk before anything leaves.
func payingDriver(t *testing.T, out node.Outgoing, sw *Swap) *Driver {
	t.Helper()

	st := store.NewMemory()
	if err := st.Put(context.Background(), *sw); err != nil {
		t.Fatal(err)
	}

	return &Driver{In: &fakeIn{}, Out: out, Store: st, Policy: pol,
		Now: fixedNow}
}

func TestPayRecordsOutcomes(t *testing.T) {
	ctx := context.Background()
	pre := node.Preimage{4, 5, 6}

	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: pre,
	}}
	sw := &Swap{Hash: pre.Hash(), State: swap.Paying, Created: fixedNow()}
	d := payingDriver(t, out, sw)
	ev, ok, err := d.Pay(ctx, sw, node.PayRequest{})
	if err != nil || !ok || ev != swap.OutgoingSucceeded {
		t.Fatalf("%v %v %v", ev, ok, err)
	}
	if sw.Preimage != pre {
		t.Error("the preimage was not recorded")
	}

	out = &fakeOut{payment: node.Payment{State: node.PaymentFailed}}
	failed := &Swap{Hash: node.Hash{9}, State: swap.Paying, Created: fixedNow()}
	d = payingDriver(t, out, failed)
	ev, ok, err = d.Pay(ctx, failed, node.PayRequest{})
	if err != nil || !ok || ev != swap.OutgoingFailed {
		t.Fatalf("%v %v %v", ev, ok, err)
	}

	// Pay refuses from any other state, so a swap cannot be paid twice.
	for _, st := range []swap.State{
		swap.Quoted, swap.Offered, swap.Funded, swap.Paid, swap.Settled,
	} {
		if _, _, err := d.Pay(ctx, &Swap{State: st}, node.PayRequest{}); !errors.Is(err, ErrState) {
			t.Errorf("%v: want ErrState, got %v", st, err)
		}
	}
}

// TestFullRunThroughTheMachine walks a swap end to end with the driver and the
// state machine together, which is how they are actually used.
func TestFullRunThroughTheMachine(t *testing.T) {
	ctx := context.Background()
	pre := node.Preimage{1}

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: pre,
	}}
	st := store.NewMemory()
	d := &Driver{In: in, Out: out, Store: st, Policy: pol, Now: fixedNow}
	sw := &Swap{
		Hash: pre.Hash(), State: swap.Quoted, OutgoingCLTVLimit: 40,
		Created: fixedNow(),
	}

	step := func(e swap.Event) swap.Action {
		t.Helper()
		action, err := d.Apply(ctx, sw, e)
		if err != nil {
			t.Fatalf("%v on %v: %v", e, sw.State, err)
		}
		// Every transition must be on disk by the time its action is
		// handed back, because the action may be the irreversible one.
		rec, err := st.Get(ctx, sw.Hash)
		if err != nil {
			t.Fatalf("after %v the swap was not recorded: %v", e, err)
		}
		if rec.State != sw.State {
			t.Fatalf("after %v memory says %v, the record says %v", e,
				sw.State, rec.State)
		}

		return action
	}

	step(swap.InvoiceOffered)
	step(swap.IncomingAccepted)

	ev, err := d.Gate(ctx, sw, rates)
	if err != nil || ev != swap.MarginPassed {
		t.Fatalf("gate: %v %v", ev, err)
	}
	if action := step(ev); action != swap.PayOutgoing {
		t.Fatalf("want PayOutgoing, got %v", action)
	}

	ev, ok, err := d.Pay(ctx, sw, node.PayRequest{Invoice: "lnbc1fake"})
	if err != nil || !ok {
		t.Fatalf("pay: %v %v", ok, err)
	}
	action := step(ev)
	if action != swap.SettleIncoming {
		t.Fatalf("want SettleIncoming, got %v", action)
	}
	if err := d.Do(ctx, sw, action); err != nil {
		t.Fatal(err)
	}
	in.invoice.State = node.InvoiceSettled
	step(swap.IncomingSettled)

	if sw.State != swap.Settled {
		t.Fatalf("ended in %v", sw.State)
	}
	if in.settled != 1 || out.paid != 1 || in.cancelled != 0 {
		t.Errorf("node calls: settled %d, paid %d, cancelled %d",
			in.settled, out.paid, in.cancelled)
	}
}

// TestGateRefusesStaleRates covers the gap found in review: Gate used to
// accept whatever bounds it was handed, with no way to tell that they had been
// measured an hour ago. A swap sized against a block rate the chain has since
// left is the failure the whole margin policy exists to prevent.
func TestGateRefusesStaleRates(t *testing.T) {
	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol, Now: fixedNow}

	// No measurement time at all.
	ev, err := d.Gate(context.Background(), funded(),
		Rates{Incoming: blake, Outgoing: btc})
	if !errors.Is(err, ErrStaleRates) {
		t.Errorf("unstamped rates: want ErrStaleRates, got %v", err)
	}
	if ev != swap.MarginRefused {
		t.Errorf("unstamped rates: got %v", ev)
	}

	// Measured longer ago than the limit.
	old := Rates{
		Incoming: blake, Outgoing: btc,
		As: measured.Add(-DefaultMaxRateAge - time.Minute),
	}
	ev, err = d.Gate(context.Background(), funded(), old)
	if !errors.Is(err, ErrStaleRates) {
		t.Errorf("stale rates: want ErrStaleRates, got %v", err)
	}
	if ev != swap.MarginRefused {
		t.Errorf("stale rates: got %v", ev)
	}

	// Just inside the limit is fine.
	fresh := Rates{
		Incoming: blake, Outgoing: btc,
		As: measured.Add(-DefaultMaxRateAge + time.Minute),
	}
	if ev, err := d.Gate(context.Background(), funded(), fresh); err != nil ||
		ev != swap.MarginPassed {

		t.Errorf("fresh rates: %v %v", ev, err)
	}

	// An operator may widen the window.
	d.MaxRateAge = time.Hour
	if ev, err := d.Gate(context.Background(), funded(), old); err != nil ||
		ev != swap.MarginPassed {

		t.Errorf("with a wider window: %v %v", ev, err)
	}
}

// TestDefaultClockIsUsed covers the branch where no clock is injected.
func TestDefaultClockIsUsed(t *testing.T) {
	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	d := &Driver{In: in, Out: &fakeOut{}, Policy: pol}

	ev, err := d.Gate(context.Background(), funded(),
		Rates{Incoming: blake, Outgoing: btc, As: time.Now()})
	if err != nil || ev != swap.MarginPassed {
		t.Errorf("%v %v", ev, err)
	}
}

// A payment the paying node never received is reported as such, not as an
// unknown outcome: there is nothing in flight to wait for.
func TestPayReportsAPaymentThatNeverLeft(t *testing.T) {
	ctx := context.Background()
	sw := &Swap{Hash: node.Hash{1}, State: swap.Paying, Created: fixedNow()}
	out := &fakeOut{payErr: fmt.Errorf("sending: %w", node.ErrNotSent)}
	d := payingDriver(t, out, sw)

	event, ok, err := d.Pay(ctx, sw, node.PayRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || event != swap.OutgoingNotSent {
		t.Fatalf("event %v (%v), want %v", event, ok, swap.OutgoingNotSent)
	}
}

// TestGateDoesNotCountTheHoldExpiryDelta: the incoming node gives up on the
// held invoice HoldExpiryDelta blocks before the HTLC expires, so an HTLC
// that has just enough blocks to the expiry, but not once those are taken
// off, is refused. Counting them let a payee hold the payout until the
// incoming claim was cancelled, and the payer kept both sides.
func TestGateDoesNotCountTheHoldExpiryDelta(t *testing.T) {
	withDelta := pol
	withDelta.HoldExpiryDelta = margin.DefaultHoldExpiryDelta

	withRequired, err := margin.RequiredIncomingBlocks(blake, margin.Leg{
		Bounds: btc, Blocks: funded().OutgoingCLTVLimit,
	}, withDelta)
	if err != nil {
		t.Fatal(err)
	}
	// What the margin alone needs, the quote's ask less the delta.
	required := withRequired - margin.DefaultHoldExpiryDelta

	for _, tc := range []struct {
		left int32
		want swap.Event
	}{
		// Enough without the delta, not with it.
		{left: int32(required), want: swap.MarginRefused},
		{left: int32(required) + margin.DefaultHoldExpiryDelta - 1,
			want: swap.MarginRefused},
		// Enough with it.
		{left: int32(required) + margin.DefaultHoldExpiryDelta,
			want: swap.MarginPassed},
	} {
		in := &fakeIn{
			invoice: node.Invoice{
				State:        node.InvoiceAccepted,
				ExpiryHeight: 1000 + tc.left,
			},
			height: 1000,
		}
		d := &Driver{In: in, Out: &fakeOut{}, Policy: withDelta, Now: fixedNow}

		ev, err := d.Gate(context.Background(), funded(), rates)
		if err != nil {
			t.Fatal(err)
		}
		if ev != tc.want {
			t.Errorf("%d blocks left: got %v, want %v", tc.left, ev,
				tc.want)
		}
	}

	// And the quote asks the payer for exactly that much more than the
	// margin alone needs: those blocks pass the check, one fewer does not.
	out := margin.Leg{Bounds: btc, Blocks: funded().OutgoingCLTVLimit}
	if err := margin.Check(margin.Leg{Bounds: blake, Blocks: required}, out,
		withDelta); err != nil {

		t.Errorf("%d blocks above the delta should pass: %v", required, err)
	}
	if err := margin.Check(margin.Leg{Bounds: blake, Blocks: required - 1},
		out, withDelta); err == nil {

		t.Errorf("%d blocks above the delta should not pass", required-1)
	}
}
