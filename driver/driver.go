// Package driver turns node facts into swap events and carries out what the
// state machine decides.
//
// Two of its jobs are the ones that lose money when they are wrong.
//
// Gate runs immediately before the outgoing payment and is the last moment a
// swap can be abandoned for free. It reads both chains fresh rather than
// trusting what the quote was built from, because a quote minutes old was
// sized against a block rate that may since have changed.
//
// Resume runs after a restart and decides what a swap left mid-flight should
// do. Its hardest case is a swap recorded as Paying whose outgoing node has no
// record of the payment: that is refused rather than guessed at, because the
// two readings of it, "never sent" and "sent and lost track of", call for
// opposite actions and only one of them is recoverable.
package driver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

var (
	// ErrNeedsOperator is returned when the bridge cannot safely decide on
	// its own. It never accompanies an action; the swap is left where it is
	// for a human to look at.
	ErrNeedsOperator = errors.New("swap needs an operator")

	// ErrState is returned when a swap is asked to do something its state
	// does not allow.
	ErrState = errors.New("swap is not in a state for this")

	// ErrStaleRates is returned when the block-spacing measurement handed to
	// Gate is too old to decide on.
	ErrStaleRates = errors.New("block spacing measurement is stale")

	// ErrStillPaying is returned instead of cancelling an incoming HTLC
	// when the paying node does not say the outgoing payment is over and
	// unpaid: it is in flight, it succeeded, or the node could not be
	// asked. Transient: the next look at the swap decides it again, and a
	// payment that succeeded moves it to Paid (see Resume).
	ErrStillPaying = errors.New("the outgoing payment is not known to " +
		"have failed, so the incoming HTLC is kept")
)

// Swap is the record the driver works from.
//
// It is store.Record, deliberately, rather than a separate working copy. What
// a restart reads is the disk, so the disk is what the driver has to be
// deciding against; a second in-memory shape would be one more thing that can
// disagree with it, and every disagreement between the two resolves into
// either cancelling a claim against money already sent or sending money twice.
type Swap = store.Record

// Rates are the block-spacing bounds for both chains, with the moment they
// were measured.
//
// The moment matters: bounds are derived from a chain's recent behaviour, and
// behaviour an hour old is not evidence about now. Gate refuses rather than
// trusting a stale measurement, because a swap sized against a block rate the
// chain has since left is exactly the failure the margin policy exists to
// prevent.
type Rates struct {
	Incoming margin.Bounds
	Outgoing margin.Bounds

	// As is when these were measured. A zero value is refused.
	As time.Time
}

// DefaultMaxRateAge is how old a measurement may be before Gate refuses to
// decide on it. Blocks on the BLAKE2b chain arrive every few minutes, so a
// measurement this old still describes the present; much older and it does
// not.
const DefaultMaxRateAge = 5 * time.Minute

// Driver holds the two nodes, the durable record and the safety policy.
type Driver struct {
	In     node.Incoming
	Out    node.Outgoing
	Store  store.Store
	Policy margin.Policy

	// MaxRateAge overrides DefaultMaxRateAge. Zero means the default.
	MaxRateAge time.Duration

	// Now is the clock, for tests. Zero means time.Now.
	Now func() time.Time
}

func (d *Driver) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}

	return time.Now()
}

func (d *Driver) maxRateAge() time.Duration {
	if d.MaxRateAge > 0 {
		return d.MaxRateAge
	}

	return DefaultMaxRateAge
}

// Gate decides whether the outgoing payment may be committed.
//
// It returns swap.MarginPassed or swap.MarginRefused, which the caller feeds
// to swap.Step. A refusal is not an error: it is the abort-rather-than-pay
// decision working. An error means the decision could not be made at all,
// which must also not result in a payment.
func (d *Driver) Gate(ctx context.Context, sw *Swap, r Rates) (swap.Event, error) {
	if sw.State != swap.Funded {
		return swap.MarginRefused, fmt.Errorf("%w: gate needs %v, have %v",
			ErrState, swap.Funded, sw.State)
	}

	if r.As.IsZero() {
		return swap.MarginRefused, fmt.Errorf("%w: rates carry no "+
			"measurement time", ErrStaleRates)
	}
	if age := d.now().Sub(r.As); age > d.maxRateAge() {
		return swap.MarginRefused, fmt.Errorf("%w: measured %v ago, limit %v",
			ErrStaleRates, age, d.maxRateAge())
	}

	inv, err := d.In.LookupInvoice(ctx, sw.Hash)
	if err != nil {
		return swap.MarginRefused, fmt.Errorf("reading the incoming "+
			"invoice: %w", err)
	}
	// The gate exists to protect a claim that is actually there. If the
	// HTLC is not locked in, there is nothing to pay out against.
	if !inv.State.Claimable() {
		return swap.MarginRefused, nil
	}

	height, err := d.In.BlockHeight(ctx)
	if err != nil {
		return swap.MarginRefused, fmt.Errorf("reading the incoming "+
			"height: %w", err)
	}
	if inv.ExpiryHeight <= height {
		// Already expired, or expiring in this block.
		return swap.MarginRefused, nil
	}
	// The incoming node cancels the held invoice HoldExpiryDelta blocks
	// before the HTLC expires; only the blocks before that are margin.
	usable, ok := d.Policy.Usable(uint32(inv.ExpiryHeight - height))
	if !ok {
		return swap.MarginRefused, nil
	}

	err = margin.Check(
		margin.Leg{Bounds: r.Incoming, Blocks: usable},
		margin.Leg{Bounds: r.Outgoing, Blocks: sw.OutgoingCLTVLimit},
		d.Policy,
	)
	switch {
	case err == nil:
		return swap.MarginPassed, nil

	case errors.Is(err, margin.ErrUnsafe):
		// The margin is not there. This is the refusal the whole policy
		// exists to produce, and it is an ordinary outcome, not a fault.
		return swap.MarginRefused, nil

	default:
		// Bad bounds or bad inputs. Refuse, and say why, because an
		// unusable measurement is a reason to stop quoting.
		return swap.MarginRefused, fmt.Errorf("margin check: %w", err)
	}
}

// Resume decides what a swap found mid-flight after a restart should do.
//
// It returns an event to apply, or ErrNeedsOperator when the bridge must not
// decide on its own.
func (d *Driver) Resume(ctx context.Context, sw *Swap) (swap.Event, bool, error) {
	if sw.State.Terminal() {
		return 0, false, nil
	}

	if sw.State == swap.Paying {
		return d.resumePaying(ctx, sw)
	}

	// A swap cancelling its incoming HTLC may have been paying before. If
	// the payment succeeded after all, that is what happened, whatever the
	// invoice is doing: the swap is paid, and settles, or is lost if the
	// claim is already gone. One that never recorded Paying has no payment
	// to ask about, and is not held up by a paying node that is down.
	if cancelsAfterPaying(sw.State) && !sw.NoPayAttempt {
		pmt, err := d.Out.LookupPayment(ctx, sw.Outgoing())
		if err != nil {
			return 0, false, fmt.Errorf("reading the outgoing payment: %w",
				err)
		}
		if pmt.State == node.PaymentSucceeded {
			recordOutgoing(sw, pmt.Preimage)

			return swap.OutgoingSucceeded, true, nil
		}
	}

	// Everything else is decided by what the incoming node says.
	inv, err := d.In.LookupInvoice(ctx, sw.Hash)
	if err != nil {
		if errors.Is(err, node.ErrUnknownHash) && sw.State == swap.Quoted {
			// Nothing was ever created, which is consistent.
			return 0, false, nil
		}

		return 0, false, fmt.Errorf("reading the incoming invoice: %w", err)
	}

	switch sw.State {
	case swap.Quoted:
		// The record says nothing was created but the node has the
		// invoice, so the process died between the two. Catch the record
		// up rather than leaving an invoice nothing will ever cancel.
		return swap.InvoiceOffered, true, nil

	case swap.Offered:
		switch inv.State {
		case node.InvoiceAccepted:
			return swap.IncomingAccepted, true, nil
		case node.InvoiceCancelled:
			return swap.IncomingLapsed, true, nil
		}

	case swap.Paid:
		// The settle may already have gone through before the crash.
		if inv.State == node.InvoiceSettled {
			return swap.IncomingSettled, true, nil
		}
		if !inv.State.Claimable() {
			// Paid out and the claim is gone: a realised loss, and the
			// machine has a name for it.
			return swap.IncomingUnsettleable, true, nil
		}

	case swap.Failing, swap.Aborting, swap.Expiring:
		if inv.State == node.InvoiceCancelled {
			return swap.IncomingCancelled, true, nil
		}
	}

	// Nothing has changed since the crash; the outstanding action is
	// reissued by the caller using swap.Recoverable.
	//
	// swap.Funded is the deliberate gap. What a funded swap needs is not an
	// action but a fresh decision, and that decision is Gate's: it reads both
	// chains again, and refuses if the incoming HTLC has stopped being
	// claimable while the bridge was down. Resuming one by reissuing
	// something would be guessing at what Gate is there to work out.
	return 0, false, nil
}

// resumePaying is the delicate one. A swap was recorded as Paying, so an
// outgoing payment may or may not have left.
func (d *Driver) resumePaying(ctx context.Context, sw *Swap) (swap.Event, bool,
	error) {

	pmt, err := d.Out.LookupPayment(ctx, sw.Outgoing())
	if err != nil {
		return 0, false, fmt.Errorf("reading the outgoing payment: %w", err)
	}

	switch pmt.State {
	case node.PaymentSucceeded:
		recordOutgoing(sw, pmt.Preimage)

		return swap.OutgoingSucceeded, true, nil

	case node.PaymentFailed:
		return swap.OutgoingFailed, true, nil

	case node.PaymentInFlight:
		// Still going. Wait for it; concluding anything here is exactly
		// the mistake the state machine refuses to allow.
		return 0, false, nil

	case node.PaymentNone:
		// The swap says a payment was started and the node has no record
		// of it. Either it never left, in which case cancelling the
		// incoming HTLC is correct, or the node lost track of one that
		// did, in which case cancelling gives away the only claim
		// against money that has already gone. The two readings call for
		// opposite actions and the bridge cannot tell them apart, so it
		// stops and asks.
		//
		// Experiment S14, run against lnd v0.21.3-beta on 2026-09-15,
		// showed the record is written before any HTLC is dispatched: a
		// payment on a node with no channels at all was reported
		// IN_FLIGHT before failing to find a route. So in the ordinary
		// case "no record" does mean "never left", and cancelling would
		// be safe.
		//
		// It still stops here, because the two readings that survive
		// that evidence are the ones an operator has to rule out and
		// this process cannot: a node database restored from a backup
		// older than the payment, and payments deleted by hand. The
		// window this can even arise in is the few milliseconds between
		// the record becoming durable and the node accepting the send,
		// so escalating costs an operator's attention about once never,
		// while being wrong costs the swap.
		return 0, false, fmt.Errorf("%w: recorded as %v but the outgoing "+
			"node has no record of the payment. S14 established that this "+
			"node writes its payment record before dispatching any HTLC, "+
			"so the payment almost certainly never left and cancelling "+
			"the incoming HTLC is almost certainly safe. Confirm first "+
			"that the node's database was not restored from a backup and "+
			"that no payments were deleted by hand, because those are the "+
			"two cases where this reading is wrong and the claim is all "+
			"there is against money already gone",
			ErrNeedsOperator, swap.Paying)
	}

	return 0, false, fmt.Errorf("%w: unknown outgoing payment state %v",
		ErrNeedsOperator, pmt.State)
}

// Do carries out an action. Every action is idempotent on its node, so a
// repeat after a crash is safe.
func (d *Driver) Do(ctx context.Context, sw *Swap, a swap.Action) error {
	switch a {
	case swap.Nothing:
		return nil

	case swap.CancelIncoming:
		if sw.State.Committed() {
			// Belt and braces behind the state machine: nothing may
			// cancel while a payment might still be in flight.
			return fmt.Errorf("%w: refusing to cancel while %v",
				ErrState, sw.State)
		}
		if err := d.notPaying(ctx, sw); err != nil {
			return err
		}

		return d.In.CancelInvoice(ctx, sw.Hash)

	case swap.SettleIncoming:
		// Only ever out of Paid, which is only reachable from Paying by the
		// outgoing leg definitively succeeding.
		//
		// For a hash-linked swap this is belt and braces: the bridge does
		// not hold a usable preimage before then, so an early settle would
		// fail at the node anyway. **For a sequenced swap it is the whole
		// protection.** There the incoming preimage was chosen before any
		// money moved, so the bridge could settle whenever it liked, and the
		// recipient's guarantee that they are paid first rests on this
		// check and the state machine behind it rather than on arithmetic.
		if sw.State != swap.Paid {
			return fmt.Errorf("%w: refusing to settle from %v; the incoming "+
				"leg is claimed only after the outgoing one has succeeded",
				ErrState, sw.State)
		}

		var zero node.Preimage
		if sw.Preimage == zero {
			return fmt.Errorf("%w: no preimage to settle with", ErrState)
		}
		// A preimage that does not hash to this swap's hash settles
		// nothing. Checking costs one hash and catches the outgoing node
		// returning the wrong one, in the single state where getting it
		// wrong is a realised loss rather than a retry.
		if got := sw.Preimage.Hash(); got != sw.Hash {
			return fmt.Errorf("%w: the preimage hashes to %x, not %x",
				ErrState, got, sw.Hash)
		}

		return d.In.SettleInvoice(ctx, sw.Preimage)

	case swap.PayOutgoing:
		return fmt.Errorf("%w: paying is not a fire-and-forget action; "+
			"use Pay so the outcome is recorded", ErrState)
	}

	return fmt.Errorf("%w: unknown action %v", ErrState, a)
}

// cancelsAfterPaying reports whether a state cancels the incoming HTLC and may
// have been paying before: Failing always was, and Aborting may have been,
// through a payment the paying node never received.
func cancelsAfterPaying(s swap.State) bool {
	return s == swap.Failing || s == swap.Aborting
}

// notPaying is the last check before an incoming HTLC is given back: the
// paying node must say the outgoing payment never started or definitively
// failed.
//
// The state machine already allows a cancel only after a definitive failure,
// or before anything was sent. This guards the readings of the node it rests
// on: a failure reported for a payment that then succeeded, a payment taken
// for never sent that had in fact left. Either would cancel the bridge's only
// claim against money already gone, and asking costs one lookup.
//
// A swap that never recorded Paying (NoPayAttempt) has no payment to ask
// about, so a paying node that is down does not hold up its refund.
func (d *Driver) notPaying(ctx context.Context, sw *Swap) error {
	if !cancelsAfterPaying(sw.State) || sw.NoPayAttempt {
		return nil
	}

	pmt, err := d.Out.LookupPayment(ctx, sw.Outgoing())
	if err != nil {
		return fmt.Errorf("%w: reading the outgoing payment before "+
			"cancelling: %w", ErrStillPaying, err)
	}
	switch pmt.State {
	case node.PaymentNone, node.PaymentFailed:
		return nil
	}

	return fmt.Errorf("%w: %v while %v", ErrStillPaying, pmt.State,
		sw.State)
}

// Pay sends the outgoing payment and reports the event its outcome implies.
//
// A timeout or transport error yields no event: the payment may still be in
// flight, and only LookupPayment can say. The swap stays in Paying, which is
// precisely the state that forbids cancelling the incoming HTLC. The one
// exception is node.ErrNotSent, which yields OutgoingNotSent.
//
// Before anything leaves it checks the disk, not memory. A restart reads the
// disk, so if the disk does not already say this swap might be paying then a
// crash in the next instant would leave a record that recovery resolves by
// cancelling the incoming claim, against a payment that had in fact gone out.
// The check costs one read and closes the only window in which that can
// happen.
func (d *Driver) Pay(ctx context.Context, sw *Swap, req node.PayRequest) (
	swap.Event, bool, error) {

	if sw.State != swap.Paying {
		return 0, false, fmt.Errorf("%w: pay needs %v, have %v", ErrState,
			swap.Paying, sw.State)
	}

	if d.Store == nil {
		return 0, false, fmt.Errorf("%w: refusing to pay without a durable "+
			"record", ErrNoStore)
	}
	rec, err := d.Store.Get(ctx, sw.Hash)
	if err != nil {
		return 0, false, fmt.Errorf("re-reading the swap before paying: %w",
			err)
	}
	if rec.State != swap.Paying {
		return 0, false, fmt.Errorf("%w: memory says %v but the record says "+
			"%v; a crash now would be recovered by cancelling the incoming "+
			"claim against a payment that had already left",
			ErrState, sw.State, rec.State)
	}

	pmt, err := d.Out.Pay(ctx, req)
	if errors.Is(err, node.ErrNotSent) {
		// Nothing left, so nothing is in flight: the swap goes back to
		// being funded and is gated again, rather than waiting in
		// Paying for a node that will have no record of it.
		return swap.OutgoingNotSent, true, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("outgoing payment: %w", err)
	}

	switch pmt.State {
	case node.PaymentSucceeded:
		recordOutgoing(sw, pmt.Preimage)

		return swap.OutgoingSucceeded, true, nil

	case node.PaymentFailed:
		return swap.OutgoingFailed, true, nil

	default:
		// In flight or unknown. Nothing to conclude.
		return 0, false, nil
	}
}

// recordOutgoing files the preimage the outgoing leg produced.
//
// For a hash-linked swap it is also what claims the incoming HTLC, and the
// swap has been holding nothing until this moment. For a sequenced swap it
// settles nothing here — the incoming preimage was chosen before any money
// moved — and overwriting with it would replace a working claim with one the
// incoming node will reject. It is kept as evidence instead.
func recordOutgoing(sw *Swap, pre node.Preimage) {
	sw.OutgoingPreimage = pre
	if sw.Binding == swap.HashLinked {
		sw.Preimage = pre
	}
}
