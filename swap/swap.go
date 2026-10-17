// Package swap is the state machine for one cross-chain swap.
//
// A swap holds an incoming HTLC on one chain and pays an outgoing one on the
// other. Both carry the same payment hash, so the preimage that settles the
// outgoing leg is what lets the bridge claim the incoming one. That is what
// makes the swap atomic and it is also what makes the ordering unforgiving.
//
// One rule matters more than the rest: **once the outgoing payment is in
// flight, the incoming HTLC must not be cancelled until the outgoing leg has
// definitively failed.** An in-flight payment may still succeed, and cancelling
// the claim on the money coming in while the money going out may still leave is
// how a bridge pays twice and collects once. The transition table encodes that,
// and TestCannotCancelWhilePaying proves it over every state and event.
//
// Cancelling and settling are two-phase: the machine moves to an intermediate
// state and emits the action, and only a confirmation from the node moves it to
// a terminal state. That makes a crash between issuing and confirming
// recoverable, because resuming from the intermediate state reissues an action
// that is idempotent on both nodes.
package swap

import (
	"errors"
	"fmt"
)

// State is where a swap has got to.
type State uint8

const (
	// Quoted: a rate has been given, nothing exists on either chain.
	Quoted State = iota

	// Offered: the hold invoice exists on the incoming chain, unpaid.
	Offered

	// Funded: the incoming HTLC is locked in and claimable with the
	// preimage. Nothing has been sent.
	Funded

	// Paying: the outgoing payment is in flight. The incoming HTLC must not
	// be cancelled from here on anything short of definitive failure.
	Paying

	// Paid: the outgoing leg succeeded and the preimage is known. The
	// incoming HTLC must now be settled; failing to is a pure loss.
	Paid

	// Failing: the outgoing leg definitively failed and the incoming HTLC
	// is being cancelled.
	Failing

	// Aborting: the margin check refused before anything was sent, and the
	// incoming HTLC is being cancelled.
	Aborting

	// Expiring: the quote lapsed before the incoming HTLC was funded, and
	// the unpaid hold invoice is being cancelled.
	Expiring

	// Settled is the terminal success.
	Settled

	// Failed is terminal: nothing was sent that was not recovered, and the
	// incoming HTLC was returned.
	Failed

	// Aborted is terminal: the bridge declined before sending.
	Aborted

	// Expired is terminal: nobody funded it.
	Expired

	// Lost is terminal and is the only state that costs the bridge money:
	// the outgoing leg was paid, the preimage is known, and the incoming
	// HTLC could not be claimed. The margin policy exists to make this
	// unreachable, so arriving here means an assumption was wrong and an
	// operator has to look at why, not just at the balance.
	Lost
)

var stateNames = map[State]string{
	Quoted: "quoted", Offered: "offered", Funded: "funded",
	Paying: "paying", Paid: "paid", Failing: "failing",
	Aborting: "aborting", Expiring: "expiring", Settled: "settled",
	Failed: "failed", Aborted: "aborted", Expired: "expired",
	Lost: "lost",
}

func (s State) String() string {
	if n, ok := stateNames[s]; ok {
		return n
	}

	return fmt.Sprintf("state(%d)", uint8(s))
}

// ErrUnknownState is returned by ParseState for a name it does not recognise.
var ErrUnknownState = errors.New("unknown swap state")

// ParseState turns a state's name back into a State.
//
// Anything that stores or transmits a state should use its name and not its
// numeric value. The numbers are positions in an iota block, so inserting a
// state in the middle would silently reinterpret every record already written:
// a swap recorded as paying would come back as something else entirely, and
// the recovery for the wrong state is how money is lost. Names do not have
// that failure mode, because a name that no longer exists is an error rather
// than a different state.
func ParseState(name string) (State, error) {
	for s, n := range stateNames {
		if n == name {
			return s, nil
		}
	}

	return 0, fmt.Errorf("%w: %q", ErrUnknownState, name)
}

// Terminal reports whether a swap has finished and needs no further work.
func (s State) Terminal() bool {
	switch s {
	case Settled, Failed, Aborted, Expired, Lost:
		return true
	}

	return false
}

// Committed reports whether an outgoing payment has been started and may still
// be in flight. While this is true the incoming HTLC is the bridge's only
// recovery and must not be given up.
func (s State) Committed() bool {
	return s == Paying
}

// Event is something that happened, either on a node or in the bridge.
type Event uint8

const (
	// InvoiceOffered: the hold invoice was created on the incoming chain.
	InvoiceOffered Event = iota

	// IncomingAccepted: the incoming HTLC is locked in.
	IncomingAccepted

	// IncomingLapsed: the hold invoice expired without being paid.
	IncomingLapsed

	// QuoteExpired: the quote's validity ran out before funding.
	QuoteExpired

	// MarginPassed: the wall-clock margin check allows the outgoing
	// payment. See package margin.
	MarginPassed

	// MarginRefused: the margin check does not allow it. This is the
	// abort-rather-than-pay decision and it must happen before the outgoing
	// payment starts, never after.
	MarginRefused

	// OutgoingSucceeded: the outgoing payment completed and the preimage is
	// known.
	OutgoingSucceeded

	// OutgoingFailed: the outgoing payment failed definitively, with every
	// HTLC resolved. A payment whose outcome is merely unknown must not
	// raise this.
	OutgoingFailed

	// IncomingSettled: the incoming HTLC was claimed with the preimage.
	IncomingSettled

	// IncomingCancelled: the incoming HTLC was returned to the payer.
	IncomingCancelled

	// IncomingUnsettleable: the incoming HTLC can no longer be claimed,
	// because it expired or was resolved by its payer. Raised only after
	// the outgoing leg has already been paid, where it is a realised loss.
	IncomingUnsettleable

	// OutgoingNotSent: the outgoing payment provably never reached the
	// paying node (see node.ErrNotSent), so nothing is in flight and the
	// swap is funded and undecided again. Appended rather than placed with
	// the other outgoing events, so that every earlier event keeps its
	// number.
	OutgoingNotSent
)

var eventNames = map[Event]string{
	InvoiceOffered: "invoice offered", IncomingAccepted: "incoming accepted",
	IncomingLapsed: "incoming lapsed", QuoteExpired: "quote expired",
	MarginPassed: "margin passed", MarginRefused: "margin refused",
	OutgoingSucceeded: "outgoing succeeded", OutgoingFailed: "outgoing failed",
	IncomingSettled: "incoming settled", IncomingCancelled: "incoming cancelled",
	IncomingUnsettleable: "incoming unsettleable",
	OutgoingNotSent:      "outgoing not sent",
}

func (e Event) String() string {
	if n, ok := eventNames[e]; ok {
		return n
	}

	return fmt.Sprintf("event(%d)", uint8(e))
}

// Action is what the driver must do after a transition. Every action is
// idempotent on the node it targets, so reissuing one after a restart is safe.
type Action uint8

const (
	// Nothing to do but wait.
	Nothing Action = iota

	// CancelIncoming returns the incoming HTLC to its payer.
	CancelIncoming

	// PayOutgoing sends the outgoing payment. This is the irreversible
	// step.
	PayOutgoing

	// SettleIncoming claims the incoming HTLC with the preimage.
	SettleIncoming
)

var actionNames = map[Action]string{
	Nothing: "nothing", CancelIncoming: "cancel incoming",
	PayOutgoing: "pay outgoing", SettleIncoming: "settle incoming",
}

func (a Action) String() string {
	if n, ok := actionNames[a]; ok {
		return n
	}

	return fmt.Sprintf("action(%d)", uint8(a))
}

// ErrTransition is returned when an event does not apply to a state. It is not
// necessarily a bug: duplicate notifications from a node are ordinary, and the
// caller should treat a transition error on a terminal state as a no-op.
var ErrTransition = errors.New("event does not apply to state")

type key struct {
	state State
	event Event
}

type outcome struct {
	state  State
	action Action
}

// table is the whole machine. Anything absent from it is refused.
//
// Read the Paying row carefully: only a definitive outcome moves it, only
// OutgoingFailed produces CancelIncoming, and OutgoingNotSent returns it to
// Funded with no action.
var table = map[key]outcome{
	// Before anything exists on chain.
	{Quoted, InvoiceOffered}: {Offered, Nothing},
	{Quoted, QuoteExpired}:   {Expired, Nothing},

	// The hold invoice exists but is unpaid, so cancelling costs nobody
	// anything.
	{Offered, IncomingAccepted}: {Funded, Nothing},
	{Offered, IncomingLapsed}:   {Expiring, CancelIncoming},
	{Offered, QuoteExpired}:     {Expiring, CancelIncoming},

	// Funded and deciding. This is the last moment a swap can be walked
	// away from for free.
	{Funded, MarginPassed}:  {Paying, PayOutgoing},
	{Funded, MarginRefused}: {Aborting, CancelIncoming},
	{Funded, QuoteExpired}:  {Aborting, CancelIncoming},

	// In flight. Nothing else may move this, with one exception that is
	// not a decision: a payment the paying node provably never received
	// was never in flight, so the swap is back where it was before it,
	// funded, and is gated again (which may refuse it if time has run
	// short). It never cancels anything by itself.
	{Paying, OutgoingSucceeded}: {Paid, SettleIncoming},
	{Paying, OutgoingFailed}:    {Failing, CancelIncoming},
	{Paying, OutgoingNotSent}:   {Funded, Nothing},

	// Winding up. Each waits for the node to confirm what it was told.
	{Paid, IncomingSettled}: {Settled, Nothing},

	// Paid but unable to claim: money went out and none came back. The
	// margin policy is meant to make this unreachable; reaching it is a
	// signal to stop and investigate, not a routine outcome.
	{Paid, IncomingUnsettleable}:  {Lost, Nothing},
	{Failing, IncomingCancelled}:  {Failed, Nothing},
	{Aborting, IncomingCancelled}: {Aborted, Nothing},

	// A cancel that finds the outgoing leg paid after all. Failing follows
	// a definitive failure and Aborting a payment that never left, so this
	// should not happen; but the driver asks the paying node before every
	// such cancel, and if the payment did succeed its preimage is the
	// bridge's claim, which cancelling would give away. The swap is paid,
	// and is settled like any other.
	{Failing, OutgoingSucceeded}:  {Paid, SettleIncoming},
	{Aborting, OutgoingSucceeded}: {Paid, SettleIncoming},
	{Expiring, IncomingCancelled}: {Expired, Nothing},

	// A hold invoice that lapses on its own while being cancelled reaches
	// the same place.
	{Expiring, IncomingLapsed}: {Expired, Nothing},
}

// Step applies an event to a state.
//
// It returns the new state and the action the driver must take. An event that
// does not apply returns ErrTransition and leaves the state unchanged, which
// is the right answer for a duplicate notification.
func Step(s State, e Event) (State, Action, error) {
	if _, known := stateNames[s]; !known {
		return s, Nothing, fmt.Errorf("%w: unknown %v", ErrTransition, s)
	}
	if _, known := eventNames[e]; !known {
		return s, Nothing, fmt.Errorf("%w: unknown %v", ErrTransition, e)
	}
	out, ok := table[key{s, e}]
	if !ok {
		return s, Nothing, fmt.Errorf("%w: %v in %v", ErrTransition, e, s)
	}

	return out.state, out.action, nil
}

// Recoverable reports whether a state needs an action reissued after a
// restart, and which one. The intermediate states are exactly those that were
// told to do something and never saw it confirmed.
func Recoverable(s State) (Action, bool) {
	switch s {
	case Paid:
		return SettleIncoming, true
	case Failing, Aborting, Expiring:
		return CancelIncoming, true
	case Paying:
		// The outgoing payment may or may not be in flight. The driver
		// must ask the node which, not assume, so no action is offered
		// here.
		return Nothing, false
	}

	return Nothing, false
}
