package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// swapHash is a hash with a known preimage, so records that carry one validate.
var (
	knownPreimage = node.Preimage{0xab, 0xcd}
	swapHash      = knownPreimage.Hash()
)

func recorded(t *testing.T, in node.Incoming, out node.Outgoing,
	sw *Swap) (*Driver, *store.Memory) {

	t.Helper()

	st := store.NewMemory()
	if err := st.Put(context.Background(), *sw); err != nil {
		t.Fatal(err)
	}

	return &Driver{In: in, Out: out, Store: st, Policy: pol, Now: fixedNow}, st
}

// TestTheOrderingRuleIsWhatPreventsTheLoss is the test this whole layer exists
// for, and it works by running the same facts twice.
//
// The facts: a swap whose outgoing payment has succeeded, a bridge that has
// just restarted, and enough time gone by that the margin no longer holds.
//
// The only variable is what was written to disk before the payment left. Write
// after, and the record still says funded, so the bridge re-decides from
// scratch, the margin refuses because the HTLC is now close to expiry, and the
// machine's answer is to cancel the incoming claim — against money that has
// already gone out. Write first, and the record says paying, the bridge asks
// the outgoing node instead of re-deciding, and it settles.
//
// The cancel itself now asks the paying node once more before it gives a
// claim up (Do refuses with ErrStillPaying), so the counterfactual is caught
// there too. That is a second line, not the first: it rests on the node's
// lookup, where the ordering rule rests on nothing but the record.
func TestTheOrderingRuleIsWhatPreventsTheLoss(t *testing.T) {
	ctx := context.Background()

	// The incoming HTLC is nearly out of blocks, which is what makes the
	// margin refuse. 40 outgoing blocks needs 179 incoming; there are 20.
	nearExpiry := func() *fakeIn {
		return &fakeIn{
			invoice: node.Invoice{
				State: node.InvoiceAccepted, ExpiryHeight: 520,
			},
			height: 500,
		}
	}
	// The outgoing payment did in fact succeed.
	succeeded := func() *fakeOut {
		return &fakeOut{
			lookup: node.Payment{
				State: node.PaymentSucceeded, Preimage: knownPreimage,
			},
		}
	}

	t.Run("recording after the payment loses the claim", func(t *testing.T) {
		in, out := nearExpiry(), succeeded()
		sw := &Swap{
			Hash: swapHash, State: swap.Funded, OutgoingCLTVLimit: 40,
			Created: fixedNow(),
		}
		d, _ := recorded(t, in, out, sw)

		// The bridge comes back up and finds a funded swap, so it re-decides.
		ev, err := d.Gate(ctx, sw, rates)
		if err != nil {
			t.Fatal(err)
		}
		if ev != swap.MarginRefused {
			t.Fatalf("the margin should refuse this close to expiry, got %v", ev)
		}
		action, err := d.Apply(ctx, sw, ev)
		if err != nil {
			t.Fatal(err)
		}
		// This is the loss: the machine's answer is to cancel the claim
		// on the incoming HTLC, while the outgoing node holds a succeeded
		// payment for the same hash.
		if action != swap.CancelIncoming {
			t.Fatalf("want CancelIncoming, got %v", action)
		}
		pmt, _ := out.LookupPayment(ctx, sw.Hash)
		if pmt.State != node.PaymentSucceeded {
			t.Fatal("the premise of this test is that the payment succeeded")
		}
		t.Logf("with the record behind: decided to cancel the claim on %d "+
			"msat against a payment that had already left", sw.IncomingMsat)

		// The second line holds it: the cancel asks the paying node first.
		if err := d.Do(ctx, sw, action); !errors.Is(err, ErrStillPaying) {
			t.Fatalf("want ErrStillPaying, got %v", err)
		}
		if in.cancelled != 0 {
			t.Fatal("the claim was cancelled against a succeeded payment")
		}
	})

	t.Run("recording before the payment keeps it", func(t *testing.T) {
		in, out := nearExpiry(), succeeded()
		sw := &Swap{
			Hash: swapHash, State: swap.Paying, OutgoingCLTVLimit: 40,
			Created: fixedNow(),
		}
		d, _ := recorded(t, in, out, sw)

		// Same restart, same facts, but the record says paying, so the
		// bridge asks the node rather than re-deciding.
		found, err := d.Recover(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) != 1 {
			t.Fatalf("recovered %d swaps, want 1", len(found))
		}
		r := found[0]
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		if !r.HasEvent || r.Event != swap.OutgoingSucceeded {
			t.Fatalf("want OutgoingSucceeded, got %v (has %v)", r.Event,
				r.HasEvent)
		}

		action, err := d.Apply(ctx, &r.Swap, r.Event)
		if err != nil {
			t.Fatal(err)
		}
		if action != swap.SettleIncoming {
			t.Fatalf("want SettleIncoming, got %v", action)
		}
		if err := d.Do(ctx, &r.Swap, action); err != nil {
			t.Fatal(err)
		}
		if in.settled != 1 || in.cancelled != 0 {
			t.Errorf("settled %d, cancelled %d; want 1 and 0", in.settled,
				in.cancelled)
		}
	})
}

// TestApplyReturnsNoActionItCouldNotRecord: if the write fails, the caller
// must not act, and memory must not be left claiming something the disk does
// not.
func TestApplyReturnsNoActionItCouldNotRecord(t *testing.T) {
	ctx := context.Background()
	diskGone := errors.New("no space left on device")

	sw := &Swap{
		Hash: swapHash, State: swap.Funded, OutgoingCLTVLimit: 40,
		Created: fixedNow(),
	}
	d, st := recorded(t, &fakeIn{}, &fakeOut{}, sw)
	st.OnPut = func(store.Record) error { return diskGone }

	action, err := d.Apply(ctx, sw, swap.MarginPassed)
	if !errors.Is(err, diskGone) {
		t.Fatalf("want the write error, got %v", err)
	}
	if action != swap.Nothing {
		t.Errorf("an unrecorded transition authorised %v", action)
	}
	if sw.State != swap.Funded {
		t.Errorf("memory moved to %v while the record stayed funded", sw.State)
	}

	// And because memory was rolled back, Pay still refuses.
	if _, _, err := d.Pay(ctx, sw, node.PayRequest{}); !errors.Is(err,
		ErrState) {

		t.Errorf("pay after a failed write: %v", err)
	}
}

// TestPayChecksTheRecordNotMemory closes the window an in-memory-only check
// would leave open.
func TestPayChecksTheRecordNotMemory(t *testing.T) {
	ctx := context.Background()
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: knownPreimage,
	}}

	// Memory says paying. The record, because the write never happened, still
	// says funded.
	stored := &Swap{
		Hash: swapHash, State: swap.Funded, OutgoingCLTVLimit: 40,
		Created: fixedNow(),
	}
	d, _ := recorded(t, &fakeIn{}, out, stored)

	inMemory := *stored
	inMemory.State = swap.Paying

	_, _, err := d.Pay(ctx, &inMemory, node.PayRequest{})
	if !errors.Is(err, ErrState) {
		t.Fatalf("want ErrState, got %v", err)
	}
	if out.paid != 0 {
		t.Error("the payment left despite the record being behind")
	}

	// With no record at all it must also refuse, rather than treating an
	// absent record as permission.
	empty := &Driver{In: &fakeIn{}, Out: out, Store: store.NewMemory(),
		Policy: pol, Now: fixedNow}
	if _, _, err := empty.Pay(ctx, &inMemory, node.PayRequest{}); !errors.Is(
		err, store.ErrNotFound) {

		t.Errorf("want ErrNotFound, got %v", err)
	}
	if out.paid != 0 {
		t.Error("the payment left with no record of the swap at all")
	}
}

// TestNothingWorksWithoutAStore: a driver with no durable record must refuse
// the operations whose safety depends on one, rather than running unprotected.
func TestNothingWorksWithoutAStore(t *testing.T) {
	ctx := context.Background()
	out := &fakeOut{payment: node.Payment{State: node.PaymentSucceeded}}
	d := &Driver{In: &fakeIn{}, Out: out, Policy: pol, Now: fixedNow}

	sw := &Swap{Hash: swapHash, State: swap.Funded, Created: fixedNow()}
	if _, err := d.Apply(ctx, sw, swap.MarginPassed); !errors.Is(err,
		ErrNoStore) {

		t.Errorf("apply: %v", err)
	}

	paying := &Swap{Hash: swapHash, State: swap.Paying, Created: fixedNow()}
	if _, _, err := d.Pay(ctx, paying, node.PayRequest{}); !errors.Is(err,
		ErrNoStore) {

		t.Errorf("pay: %v", err)
	}
	if out.paid != 0 {
		t.Error("a payment left from a driver with nowhere to record it")
	}

	if _, err := d.Recover(ctx); !errors.Is(err, ErrNoStore) {
		t.Errorf("recover: %v", err)
	}
}

// TestApplyRefusesAnEventThatDoesNotApply, and writes nothing when it does.
func TestApplyRefusesAnEventThatDoesNotApply(t *testing.T) {
	ctx := context.Background()
	sw := &Swap{Hash: swapHash, State: swap.Paying, Created: fixedNow()}
	d, st := recorded(t, &fakeIn{}, &fakeOut{}, sw)

	var writes int
	st.OnPut = func(store.Record) error { writes++; return nil }

	// Cancelling while paying is the transition the machine exists to
	// forbid, and it must not reach the disk either.
	if _, err := d.Apply(ctx, sw, swap.IncomingCancelled); !errors.Is(err,
		swap.ErrTransition) {

		t.Fatalf("want ErrTransition, got %v", err)
	}
	if writes != 0 {
		t.Errorf("a refused transition wrote %d records", writes)
	}
	if sw.State != swap.Paying {
		t.Errorf("state moved to %v", sw.State)
	}
}

// TestApplyStampsTimes: Created is set once and Updated every time, so the
// journal shows how long a swap sat in each state.
func TestApplyStampsTimes(t *testing.T) {
	ctx := context.Background()
	sw := &Swap{Hash: swapHash, State: swap.Quoted}
	st := store.NewMemory()
	d := &Driver{In: &fakeIn{}, Out: &fakeOut{}, Store: st, Policy: pol,
		Now: fixedNow}

	if _, err := d.Apply(ctx, sw, swap.InvoiceOffered); err != nil {
		t.Fatal(err)
	}
	if sw.Created.IsZero() || !sw.Created.Equal(fixedNow()) {
		t.Errorf("created %v", sw.Created)
	}
	first := sw.Created

	d.Now = func() time.Time { return fixedNow().Add(time.Hour) }
	if _, err := d.Apply(ctx, sw, swap.IncomingAccepted); err != nil {
		t.Fatal(err)
	}
	if !sw.Created.Equal(first) {
		t.Errorf("created moved to %v", sw.Created)
	}
	if !sw.Updated.Equal(fixedNow().Add(time.Hour)) {
		t.Errorf("updated %v", sw.Updated)
	}
}

// TestRecoverWorksThroughEverythingUnfinished, including past a swap it cannot
// decide. A bridge coming back up may hold several HTLCs against deadlines it
// did not choose, and stopping at the first ambiguous one would turn a single
// stuck swap into several expired ones.
func TestRecoverWorksThroughEverythingUnfinished(t *testing.T) {
	ctx := context.Background()

	st := store.NewMemory()
	base := time.Date(2026, 9, 14, 9, 0, 0, 0, time.UTC)
	put := func(n byte, state swap.State, age time.Duration) {
		t.Helper()
		if err := st.Put(ctx, store.Record{
			Hash: node.Hash{n}, State: state, OutgoingCLTVLimit: 40,
			Created: base.Add(age), Updated: base.Add(age),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Oldest first: the one nearest its deadline gets looked at first.
	put(1, swap.Paying, 0) // undecidable: the node has no record
	put(2, swap.Paid, time.Minute)
	put(3, swap.Expiring, 2*time.Minute)
	put(4, swap.Settled, 3*time.Minute) // finished, must not appear

	d := &Driver{
		In: &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted,
			ExpiryHeight: 1000}, height: 500},
		// PaymentNone: the outgoing node has never heard of it.
		Out:    &fakeOut{lookup: node.Payment{State: node.PaymentNone}},
		Store:  st,
		Policy: pol, Now: fixedNow,
	}

	found, err := d.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 3 {
		t.Fatalf("recovered %d swaps, want 3", len(found))
	}

	// The undecidable one is reported, not guessed at, and above all not
	// turned into a cancel.
	if !errors.Is(found[0].Err, ErrNeedsOperator) {
		t.Errorf("first: want ErrNeedsOperator, got %v", found[0].Err)
	}
	if found[0].Action != swap.Nothing || found[0].HasEvent {
		t.Errorf("first: offered %v / event %v for a swap it cannot decide",
			found[0].Action, found[0].HasEvent)
	}

	// The others are still decided, which is the point.
	if found[1].HasEvent || found[1].Action != swap.SettleIncoming {
		t.Errorf("second: want SettleIncoming reissued, got %v / %v",
			found[1].Action, found[1].HasEvent)
	}
	if found[2].HasEvent || found[2].Action != swap.CancelIncoming {
		t.Errorf("third: want CancelIncoming reissued, got %v / %v",
			found[2].Action, found[2].HasEvent)
	}
}

// TestRecoverPicksUpWhatHappenedWhileDown.
func TestRecoverPicksUpWhatHappenedWhileDown(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name    string
		state   swap.State
		invoice node.InvoiceState
		want    swap.Event
	}{
		{"funded while down", swap.Offered, node.InvoiceAccepted,
			swap.IncomingAccepted},
		{"lapsed while down", swap.Offered, node.InvoiceCancelled,
			swap.IncomingLapsed},
		{"the settle got through", swap.Paid, node.InvoiceSettled,
			swap.IncomingSettled},
		{"the claim expired while down", swap.Paid, node.InvoiceCancelled,
			swap.IncomingUnsettleable},
		{"the cancel got through", swap.Aborting, node.InvoiceCancelled,
			swap.IncomingCancelled},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := store.NewMemory()
			if err := st.Put(ctx, store.Record{
				Hash: swapHash, State: c.state, OutgoingCLTVLimit: 40,
				Created: fixedNow(),
			}); err != nil {
				t.Fatal(err)
			}
			d := &Driver{
				In: &fakeIn{invoice: node.Invoice{State: c.invoice,
					ExpiryHeight: 1000}, height: 500},
				Out: &fakeOut{}, Store: st, Policy: pol, Now: fixedNow,
			}

			found, err := d.Recover(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(found) != 1 {
				t.Fatalf("recovered %d", len(found))
			}
			if found[0].Err != nil {
				t.Fatal(found[0].Err)
			}
			if !found[0].HasEvent || found[0].Event != c.want {
				t.Errorf("want %v, got %v (has %v)", c.want, found[0].Event,
					found[0].HasEvent)
			}
		})
	}
}

// TestRecoverNeverOffersACancelForAPayingSwap is the same invariant the state
// machine enforces, checked at the layer that actually calls the node.
func TestRecoverNeverOffersACancelForAPayingSwap(t *testing.T) {
	ctx := context.Background()

	for _, pmt := range []node.PaymentState{
		node.PaymentNone, node.PaymentInFlight, node.PaymentSucceeded,
		node.PaymentFailed,
	} {
		st := store.NewMemory()
		if err := st.Put(ctx, store.Record{
			Hash: swapHash, State: swap.Paying, OutgoingCLTVLimit: 40,
			Created: fixedNow(),
		}); err != nil {
			t.Fatal(err)
		}
		in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted,
			ExpiryHeight: 1000}, height: 500}
		d := &Driver{In: in, Out: &fakeOut{lookup: node.Payment{State: pmt}},
			Store: st, Policy: pol, Now: fixedNow}

		found, err := d.Recover(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r := found[0]
		if r.Action == swap.CancelIncoming {
			t.Errorf("%v: recovery offered a cancel while paying", pmt)
		}
		// Only a definitive failure may lead to one, and then only through
		// the state machine.
		if r.HasEvent {
			_, action, err := swap.Step(swap.Paying, r.Event)
			if err != nil {
				t.Errorf("%v: %v", pmt, err)
			}
			if action == swap.CancelIncoming && pmt != node.PaymentFailed {
				t.Errorf("%v led to a cancel", pmt)
			}
		}
		if in.cancelled != 0 {
			t.Errorf("%v: recovery cancelled an invoice by itself", pmt)
		}
	}
}

// TestAWrongPreimageNeverReachesTheNode. The outgoing node reports success and
// hands back a preimage that settles a different hash. Nothing downstream may
// treat that as a claim: the record refuses to store it, so no action is
// authorised, and the direct settle refuses too.
func TestAWrongPreimageNeverReachesTheNode(t *testing.T) {
	ctx := context.Background()

	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted,
		ExpiryHeight: 1000}, height: 500}
	out := &fakeOut{lookup: node.Payment{
		State: node.PaymentSucceeded, Preimage: node.Preimage{0xff},
	}}
	sw := &Swap{
		Hash: swapHash, State: swap.Paying, OutgoingCLTVLimit: 40,
		Created: fixedNow(),
	}
	d, _ := recorded(t, in, out, sw)

	found, err := d.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := found[0]
	if !r.HasEvent || r.Event != swap.OutgoingSucceeded {
		t.Fatalf("want OutgoingSucceeded, got %v", r.Event)
	}

	// The record will not take it, so no action comes back and the swap
	// stays where it was for a human.
	if _, err := d.Apply(ctx, &r.Swap, r.Event); !errors.Is(err,
		store.ErrRecord) {

		t.Errorf("want ErrRecord, got %v", err)
	}
	if r.Swap.State != swap.Paying {
		t.Errorf("the swap moved to %v on an unrecordable preimage",
			r.Swap.State)
	}

	// And going straight at the node is refused as well.
	settle := r.Swap
	settle.State = swap.Paid
	settle.Preimage = node.Preimage{0xff}
	if err := d.Do(ctx, &settle, swap.SettleIncoming); !errors.Is(err,
		ErrState) {

		t.Errorf("want ErrState, got %v", err)
	}
	if in.settled != 0 {
		t.Error("a preimage for the wrong hash reached the node")
	}
}

// TestAnInvoiceWithoutARecordIsCaughtUp: the process died between creating the
// hold invoice and writing that down. Left alone the invoice would sit on the
// node with nothing tracking it, until it expired on a payer who had funded it.
func TestAnInvoiceWithoutARecordIsCaughtUp(t *testing.T) {
	ctx := context.Background()

	sw := &Swap{Hash: swapHash, State: swap.Quoted, OutgoingCLTVLimit: 40,
		Created: fixedNow()}
	d, _ := recorded(t, &fakeIn{
		invoice: node.Invoice{State: node.InvoiceOpen, ExpiryHeight: 1000},
		height:  500,
	}, &fakeOut{}, sw)

	found, err := d.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !found[0].HasEvent || found[0].Event != swap.InvoiceOffered {
		t.Fatalf("want InvoiceOffered, got %v (has %v)", found[0].Event,
			found[0].HasEvent)
	}

	// A quote whose invoice was never created stays quoted, because that is
	// consistent and there is nothing to catch up.
	none := &Swap{Hash: node.Hash{2}, State: swap.Quoted, Created: fixedNow()}
	clean, _ := recorded(t, &fakeIn{lookupErr: node.ErrUnknownHash},
		&fakeOut{}, none)
	found, err = clean.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if found[0].Err != nil || found[0].HasEvent ||
		found[0].Action != swap.Nothing {

		t.Errorf("a quote with no invoice produced %v / %v / %v",
			found[0].Event, found[0].HasEvent, found[0].Err)
	}
}

// TestAFundedSwapComesBackForTheGate, not for an action. Gate is what reads
// both chains again and refuses if the claim stopped being claimable while the
// bridge was down; reissuing something instead would be guessing at that.
func TestAFundedSwapComesBackForTheGate(t *testing.T) {
	ctx := context.Background()

	sw := &Swap{Hash: swapHash, State: swap.Funded, OutgoingCLTVLimit: 40,
		Created: fixedNow()}
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted,
		ExpiryHeight: 1000}, height: 500}
	d, _ := recorded(t, in, &fakeOut{}, sw)

	found, err := d.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if found[0].Err != nil || found[0].HasEvent ||
		found[0].Action != swap.Nothing {

		t.Fatalf("funded recovered as %v / %v / %v", found[0].Event,
			found[0].HasEvent, found[0].Err)
	}

	// And the gate is what decides it, still passing here.
	ev, err := d.Gate(ctx, &found[0].Swap, rates)
	if err != nil || ev != swap.MarginPassed {
		t.Errorf("gate: %v %v", ev, err)
	}

	// While a claim that lapsed during the outage is refused, which is the
	// case reissuing an action would have got wrong.
	in.invoice.State = node.InvoiceCancelled
	ev, err = d.Gate(ctx, &found[0].Swap, rates)
	if err != nil || ev != swap.MarginRefused {
		t.Errorf("gate on a lapsed claim: %v %v", ev, err)
	}
}

// TestASequencedSwapKeepsItsOwnPreimage is the behaviour that makes a payout
// possible at all.
//
// The recipient's node issued the outgoing invoice and chose its own preimage.
// That preimage settles nothing on the incoming side, and writing it over the
// one the bridge chose would replace a working claim with one the incoming
// node rejects — turning every payout into a realised loss at the last step.
func TestASequencedSwapKeepsItsOwnPreimage(t *testing.T) {
	ctx := context.Background()

	ours := node.Preimage{0x11}
	theirs := node.Preimage{0x22}
	outHash := theirs.Hash()

	sw := &Swap{
		Hash: ours.Hash(), State: swap.Paying, OutgoingCLTVLimit: 40,
		Binding: swap.Sequenced, Preimage: ours, OutgoingHash: outHash,
		Created: fixedNow(),
	}
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted,
		ExpiryHeight: 1000}, height: 500}
	out := &fakeOut{lookup: node.Payment{
		State: node.PaymentSucceeded, Preimage: theirs,
	}}
	d, _ := recorded(t, in, out, sw)

	found, err := d.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := found[0]
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if !r.HasEvent || r.Event != swap.OutgoingSucceeded {
		t.Fatalf("want OutgoingSucceeded, got %v", r.Event)
	}

	// Ours is untouched; theirs is kept as evidence that we paid.
	if r.Swap.Preimage != ours {
		t.Errorf("the incoming preimage became %x, want ours %x",
			r.Swap.Preimage, ours)
	}
	if r.Swap.OutgoingPreimage != theirs {
		t.Errorf("the outgoing preimage was not recorded: %x",
			r.Swap.OutgoingPreimage)
	}

	action, err := d.Apply(ctx, &r.Swap, r.Event)
	if err != nil {
		t.Fatal(err)
	}
	if action != swap.SettleIncoming {
		t.Fatalf("want SettleIncoming, got %v", action)
	}
	if err := d.Do(ctx, &r.Swap, action); err != nil {
		t.Fatalf("settling a sequenced swap: %v", err)
	}
	if in.settled != 1 {
		t.Error("the incoming leg was not claimed")
	}
}

// TestASequencedSwapLooksUpTheRightPayment. The legs have different hashes, so
// asking the outgoing node about the incoming one finds nothing — and "no
// record" is the answer that stops the bridge and calls an operator.
func TestASequencedSwapLooksUpTheRightPayment(t *testing.T) {
	ctx := context.Background()

	ours := node.Preimage{0x11}
	theirs := node.Preimage{0x22}
	outHash := theirs.Hash()

	out := &fakeOut{lookups: map[node.Hash]node.Payment{
		outHash: {State: node.PaymentSucceeded, Preimage: theirs},
		// Asking about the incoming hash finds nothing, which is what a
		// node would say.
		ours.Hash(): {State: node.PaymentNone},
	}}
	sw := &Swap{
		Hash: ours.Hash(), State: swap.Paying, OutgoingCLTVLimit: 40,
		Binding: swap.Sequenced, Preimage: ours, OutgoingHash: outHash,
		Created: fixedNow(),
	}
	d, _ := recorded(t, &fakeIn{invoice: node.Invoice{
		State: node.InvoiceAccepted, ExpiryHeight: 1000,
	}, height: 500}, out, sw)

	found, err := d.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if found[0].Err != nil {
		t.Fatalf("it looked up the incoming hash and stopped: %v", found[0].Err)
	}
	if found[0].Event != swap.OutgoingSucceeded {
		t.Errorf("want OutgoingSucceeded, got %v", found[0].Event)
	}
}

// TestSettlingBeforeThePaymentSucceededIsRefused.
//
// For a hash-linked swap this is belt and braces; the bridge holds no usable
// preimage before then. For a sequenced swap it is the whole protection: the
// bridge chose the incoming preimage before any money moved and could settle
// whenever it liked, so the recipient's guarantee that they are paid first
// rests on exactly this.
func TestSettlingBeforeThePaymentSucceededIsRefused(t *testing.T) {
	ctx := context.Background()
	ours := node.Preimage{0x11}

	for _, state := range []swap.State{
		swap.Quoted, swap.Offered, swap.Funded, swap.Paying,
		swap.Failing, swap.Aborting, swap.Expiring, swap.Settled,
	} {
		in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted}}
		d := &Driver{In: in, Out: &fakeOut{}, Store: store.NewMemory(),
			Policy: pol, Now: fixedNow}

		sw := &Swap{
			Hash: ours.Hash(), State: state, Binding: swap.Sequenced,
			Preimage: ours, OutgoingHash: node.Hash{0x99},
			Created: fixedNow(),
		}
		if err := d.Do(ctx, sw, swap.SettleIncoming); !errors.Is(err,
			ErrState) {

			t.Errorf("%v: settling was allowed: %v", state, err)
		}
		if in.settled != 0 {
			t.Errorf("%v: a settle reached the node before the recipient "+
				"was paid", state)
		}
	}

	// And from Paid it works, or nothing could ever complete.
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted}}
	d := &Driver{In: in, Out: &fakeOut{}, Store: store.NewMemory(),
		Policy: pol, Now: fixedNow}
	sw := &Swap{
		Hash: ours.Hash(), State: swap.Paid, Binding: swap.Sequenced,
		Preimage: ours, OutgoingHash: node.Hash{0x99}, Created: fixedNow(),
	}
	if err := d.Do(ctx, sw, swap.SettleIncoming); err != nil {
		t.Fatalf("settling from Paid: %v", err)
	}
	if in.settled != 1 {
		t.Error("the settle did not reach the node")
	}
}

// TestARefundNeverPaidDoesNotWaitForThePayingNode: a funded swap given back
// before it ever recorded Paying has no outgoing payment, so cancelling its
// incoming HTLC does not wait for a paying node that is down to say so; it
// would otherwise hold the payer's funds until that node came back or the
// hold invoice expired. One that has recorded Paying waits, as before.
func TestARefundNeverPaidDoesNotWaitForThePayingNode(t *testing.T) {
	ctx := context.Background()
	down := &fakeOut{lookupErr: errors.New("connection refused")}

	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted}}
	never := &Swap{Hash: swapHash, State: swap.Aborting, NoPayAttempt: true,
		Created: measured}
	d, _ := recorded(t, in, down, never)
	if err := d.Do(ctx, never, swap.CancelIncoming); err != nil {
		t.Fatalf("the cancel waited for the paying node: %v", err)
	}
	if in.cancelled != 1 {
		t.Errorf("cancelled %d times, want once", in.cancelled)
	}
	in.invoice.State = node.InvoiceCancelled
	ev, ok, err := d.Resume(ctx, never)
	if err != nil || !ok || ev != swap.IncomingCancelled {
		t.Errorf("resuming it: %v %v %v, want incoming cancelled", ev, ok,
			err)
	}

	maybe := &Swap{Hash: swapHash, State: swap.Aborting, Created: measured}
	in = &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted}}
	d, _ = recorded(t, in, down, maybe)
	if err := d.Do(ctx, maybe, swap.CancelIncoming); !errors.Is(err,
		ErrStillPaying) || in.cancelled != 0 {

		t.Errorf("one that may have paid was cancelled: %v, %d", err,
			in.cancelled)
	}
}

// TestRecordingPayingClearsNoPayAttempt: the write that records Paying is the
// one that says a payment may now exist, so no crash after it can leave a
// record claiming none was attempted.
func TestRecordingPayingClearsNoPayAttempt(t *testing.T) {
	ctx := context.Background()
	sw := &Swap{Hash: swapHash, State: swap.Funded, NoPayAttempt: true,
		Created: measured}
	d, st := recorded(t, &fakeIn{}, &fakeOut{}, sw)

	if _, err := d.Apply(ctx, sw, swap.MarginPassed); err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, swapHash)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != swap.Paying || got.NoPayAttempt || sw.NoPayAttempt {
		t.Errorf("recorded %v with no pay attempt %v (memory %v)",
			got.State, got.NoPayAttempt, sw.NoPayAttempt)
	}

	// And a payment the node never received leaves it cleared: it is
	// funded again, and may yet have reached the node after all.
	if _, err := d.Apply(ctx, sw, swap.OutgoingNotSent); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get(ctx, swapHash); got.NoPayAttempt {
		t.Error("funded again, it claims no payment was attempted")
	}
}

// TestAnExpiryThatSawAnHTLCSaysSo: a hold invoice the payer funded as it was
// being cancelled ends expired, unpaid as far as its state goes, and the node
// may later drop the invoice that shows the HTLC. The record says it was seen
// funded, so a new quote for the hash can refuse from the record alone.
func TestAnExpiryThatSawAnHTLCSaysSo(t *testing.T) {
	ctx := context.Background()
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceCancelled,
		HTLCs: 1}}
	sw := &Swap{Hash: swapHash, State: swap.Expiring, Created: measured}
	d, st := recorded(t, in, &fakeOut{}, sw)

	ev, ok, err := d.Resume(ctx, sw)
	if err != nil || !ok || ev != swap.IncomingCancelled {
		t.Fatalf("resume: %v %v %v", ev, ok, err)
	}
	if _, err := d.Apply(ctx, sw, ev); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Get(ctx, swapHash)
	if got.State != swap.Expired || !got.HTLCSeen {
		t.Errorf("recorded %v, HTLC seen %v; want expired and seen",
			got.State, got.HTLCSeen)
	}

	// One that lapsed with nothing paid says nothing of the kind.
	in.invoice.HTLCs = 0
	sw = &Swap{Hash: swapHash, State: swap.Expiring, Created: measured}
	d, st = recorded(t, in, &fakeOut{}, sw)
	ev, _, _ = d.Resume(ctx, sw)
	if _, err := d.Apply(ctx, sw, ev); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get(ctx, swapHash); got.HTLCSeen {
		t.Error("an unpaid expiry says an HTLC was seen")
	}
}

// TestRecordingOfferedClearsNoInvoice: a swap caught up to offered after a
// crash has an invoice, and says so in the same write.
func TestRecordingOfferedClearsNoInvoice(t *testing.T) {
	ctx := context.Background()
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceOpen}}
	sw := &Swap{Hash: swapHash, State: swap.Quoted, NoInvoice: true,
		Created: measured}
	d, st := recorded(t, in, &fakeOut{}, sw)

	ev, ok, err := d.Resume(ctx, sw)
	if err != nil || !ok || ev != swap.InvoiceOffered {
		t.Fatalf("resume: %v %v %v", ev, ok, err)
	}
	if _, err := d.Apply(ctx, sw, ev); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.Get(ctx, swapHash); got.NoInvoice {
		t.Error("an offered swap says it has no invoice")
	}
}
