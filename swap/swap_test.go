package swap

import (
	"errors"
	"strings"
	"testing"
)

var allStates = []State{
	Quoted, Offered, Funded, Paying, Paid, Failing, Aborting, Expiring,
	Settled, Failed, Aborted, Expired, Lost,
}

var allEvents = []Event{
	InvoiceOffered, IncomingAccepted, IncomingLapsed, QuoteExpired,
	MarginPassed, MarginRefused, OutgoingSucceeded, OutgoingFailed,
	IncomingSettled, IncomingCancelled, IncomingUnsettleable,
	OutgoingNotSent,
}

// TestCannotCancelWhilePaying is the invariant this package exists to hold.
// An outgoing payment in flight may still succeed, so giving up the incoming
// HTLC is how a bridge pays out and collects nothing. Only a definitive
// failure may lead to a cancellation from Paying.
func TestCannotCancelWhilePaying(t *testing.T) {
	for _, e := range allEvents {
		next, action, err := Step(Paying, e)
		if err != nil {
			continue // refused outright, which is safe
		}
		if action != CancelIncoming {
			continue
		}
		if e != OutgoingFailed {
			t.Errorf("%v from Paying cancels the incoming HTLC and moves "+
				"to %v; only a definitive %v may do that", e, next,
				OutgoingFailed)
		}
	}

	// And nothing but a definitive outcome may move Paying at all. Never
	// having been sent is one: it is certain, and it is the only other
	// thing the paying node can say for sure about a payment.
	for _, e := range allEvents {
		_, _, err := Step(Paying, e)
		switch e {
		case OutgoingSucceeded, OutgoingFailed, OutgoingNotSent:
			if err != nil {
				t.Errorf("%v must move Paying: %v", e, err)
			}
		default:
			if err == nil {
				t.Errorf("%v must not move Paying", e)
			}
		}
	}
}

// TestCancelOnlyWhenNothingIsOwed checks the same rule from the other side:
// every transition that cancels must come from a state where either nothing
// has been sent, or what was sent has definitively failed.
func TestCancelOnlyWhenNothingIsOwed(t *testing.T) {
	safeToCancel := map[State]bool{
		Offered: true, Funded: true, // nothing sent yet
		Paying:   true, // only via OutgoingFailed, checked above
		Failing:  true, // already cancelling
		Aborting: true,
		Expiring: true,
	}
	for _, s := range allStates {
		for _, e := range allEvents {
			_, action, err := Step(s, e)
			if err != nil || action != CancelIncoming {
				continue
			}
			if !safeToCancel[s] {
				t.Errorf("%v on %v cancels the incoming HTLC, which is "+
					"not safe from that state", e, s)
			}
		}
	}
}

// TestSettleOnlyWithAPreimage: the incoming HTLC may only be claimed once the
// outgoing leg has produced the preimage. Besides Paying, that can be found out
// while cancelling: the driver asks the paying node before giving up a claim,
// and a payment that succeeded after all is settled, not cancelled.
func TestSettleOnlyWithAPreimage(t *testing.T) {
	for _, s := range allStates {
		for _, e := range allEvents {
			_, action, err := Step(s, e)
			if err != nil || action != SettleIncoming {
				continue
			}
			if e != OutgoingSucceeded ||
				(s != Paying && s != Failing && s != Aborting) {
				t.Errorf("%v on %v settles the incoming HTLC without the "+
					"preimage having been learned", e, s)
			}
		}
	}
}

// TestPayOnlyOnceAndOnlyWhenFunded: money may only leave from a funded swap
// whose margin check passed, and only once.
func TestPayOnlyOnceAndOnlyWhenFunded(t *testing.T) {
	for _, s := range allStates {
		for _, e := range allEvents {
			_, action, err := Step(s, e)
			if err != nil || action != PayOutgoing {
				continue
			}
			if s != Funded || e != MarginPassed {
				t.Errorf("%v on %v starts an outgoing payment; only %v on "+
					"%v may", e, s, MarginPassed, Funded)
			}
		}
	}
}

// TestTerminalStatesAreFinal: nothing moves a finished swap.
func TestTerminalStatesAreFinal(t *testing.T) {
	for _, s := range allStates {
		if !s.Terminal() {
			continue
		}
		for _, e := range allEvents {
			next, action, err := Step(s, e)
			if err == nil {
				t.Errorf("%v moved terminal %v to %v", e, s, next)
			}
			if next != s || action != Nothing {
				t.Errorf("a refused transition must change nothing: "+
					"%v on %v gave %v/%v", e, s, next, action)
			}
		}
	}
}

// TestHappyPath walks a swap from quote to settlement and checks each action.
func TestHappyPath(t *testing.T) {
	steps := []struct {
		event  Event
		state  State
		action Action
	}{
		{InvoiceOffered, Offered, Nothing},
		{IncomingAccepted, Funded, Nothing},
		{MarginPassed, Paying, PayOutgoing},
		{OutgoingSucceeded, Paid, SettleIncoming},
		{IncomingSettled, Settled, Nothing},
	}
	s := Quoted
	for i, step := range steps {
		next, action, err := Step(s, step.event)
		if err != nil {
			t.Fatalf("step %d (%v): %v", i, step.event, err)
		}
		if next != step.state || action != step.action {
			t.Fatalf("step %d (%v): got %v/%v, want %v/%v", i, step.event,
				next, action, step.state, step.action)
		}
		s = next
	}
	if !s.Terminal() {
		t.Errorf("a settled swap should be terminal")
	}
}

// TestUnhappyPaths walks each way a swap can end without settling.
func TestUnhappyPaths(t *testing.T) {
	paths := []struct {
		name   string
		events []Event
		want   State
	}{{
		name:   "never funded",
		events: []Event{InvoiceOffered, IncomingLapsed, IncomingCancelled},
		want:   Expired,
	}, {
		name:   "quote lapsed before funding",
		events: []Event{InvoiceOffered, QuoteExpired, IncomingCancelled},
		want:   Expired,
	}, {
		name: "margin refused after funding",
		events: []Event{
			InvoiceOffered, IncomingAccepted, MarginRefused, IncomingCancelled,
		},
		want: Aborted,
	}, {
		name: "outgoing failed",
		events: []Event{
			InvoiceOffered, IncomingAccepted, MarginPassed, OutgoingFailed,
			IncomingCancelled,
		},
		want: Failed,
	}, {
		name:   "expired before an invoice existed",
		events: []Event{QuoteExpired},
		want:   Expired,
	}}

	for _, p := range paths {
		t.Run(p.name, func(t *testing.T) {
			s := Quoted
			for _, e := range p.events {
				next, _, err := Step(s, e)
				if err != nil {
					t.Fatalf("%v on %v: %v", e, s, err)
				}
				s = next
			}
			if s != p.want {
				t.Errorf("ended in %v, want %v", s, p.want)
			}
			if !s.Terminal() {
				t.Errorf("%v should be terminal", s)
			}
		})
	}
}

// TestDuplicateEventsAreRefusedNotApplied: nodes resend notifications, and a
// duplicate must leave the swap alone rather than advance it.
func TestDuplicateEventsAreRefusedNotApplied(t *testing.T) {
	s := Quoted
	s, _, _ = Step(s, InvoiceOffered)
	s, _, _ = Step(s, IncomingAccepted)

	again, action, err := Step(s, IncomingAccepted)
	if !errors.Is(err, ErrTransition) {
		t.Errorf("a repeated accept should be refused, got %v", err)
	}
	if again != s || action != Nothing {
		t.Errorf("a refused duplicate changed state to %v/%v", again, action)
	}
}

// TestRecoverableReissuesExactlyTheOutstandingAction covers restart: a swap
// interrupted between being told to do something and seeing it confirmed must
// reissue that action and nothing else.
func TestRecoverableReissuesExactlyTheOutstandingAction(t *testing.T) {
	want := map[State]Action{
		Paid:     SettleIncoming,
		Failing:  CancelIncoming,
		Aborting: CancelIncoming,
		Expiring: CancelIncoming,
	}
	for _, s := range allStates {
		action, ok := Recoverable(s)
		if expect, isPending := want[s]; isPending {
			if !ok || action != expect {
				t.Errorf("%v should reissue %v, got %v/%v", s, expect,
					action, ok)
			}

			continue
		}
		if ok {
			t.Errorf("%v should not reissue anything, got %v", s, action)
		}
	}

	// Paying deserves its own note: the driver has to ask the node whether
	// the payment is in flight rather than reissue blindly, because paying
	// twice is the one mistake this package cannot undo.
	if _, ok := Recoverable(Paying); ok {
		t.Error("Paying must not offer an action to reissue")
	}
	if !Paying.Committed() {
		t.Error("Paying must report itself committed")
	}
	for _, s := range allStates {
		if s != Paying && s.Committed() {
			t.Errorf("%v must not report itself committed", s)
		}
	}
}

// TestUnknownValuesRefused: a state or event read back from a corrupt store
// must not match a table entry by accident.
func TestUnknownValuesRefused(t *testing.T) {
	if _, _, err := Step(State(200), InvoiceOffered); !errors.Is(err, ErrTransition) {
		t.Errorf("unknown state: %v", err)
	}
	if _, _, err := Step(Quoted, Event(200)); !errors.Is(err, ErrTransition) {
		t.Errorf("unknown event: %v", err)
	}
	if got := State(200).String(); got != "state(200)" {
		t.Errorf("unknown state prints as %q", got)
	}
	if got := Event(200).String(); got != "event(200)" {
		t.Errorf("unknown event prints as %q", got)
	}
	if got := Action(200).String(); got != "action(200)" {
		t.Errorf("unknown action prints as %q", got)
	}
}

// TestEveryStateIsReachableAndEveryTableEntryUsed guards against a state that
// can never happen or a transition nothing can trigger, either of which means
// the machine and the intent have drifted apart.
func TestEveryStateIsReachableAndEveryTableEntryUsed(t *testing.T) {
	reached := map[State]bool{Quoted: true}
	for changed := true; changed; {
		changed = false
		for s := range reached {
			for _, e := range allEvents {
				next, _, err := Step(s, e)
				if err == nil && !reached[next] {
					reached[next] = true
					changed = true
				}
			}
		}
	}
	for _, s := range allStates {
		if !reached[s] {
			t.Errorf("%v is unreachable from %v", s, Quoted)
		}
	}

	// Every entry in the table must be exercised by some reachable state.
	for k := range table {
		if !reached[k.state] {
			t.Errorf("table entry %v on %v can never fire", k.event, k.state)
		}
	}
}

// TestNoActionWithoutATransition: an action may only accompany a state change
// the table sanctioned.
func TestNoActionWithoutATransition(t *testing.T) {
	for _, s := range allStates {
		for _, e := range allEvents {
			next, action, err := Step(s, e)
			if err != nil && action != Nothing {
				t.Errorf("%v on %v failed but still asked for %v", e, s,
					action)
			}
			if err != nil && next != s {
				t.Errorf("%v on %v failed but moved to %v", e, s, next)
			}
		}
	}
}

// TestLostIsReachableAndLoud covers the one outcome that costs money: the
// outgoing leg paid, the preimage known, and the incoming HTLC gone. The
// margin policy is meant to make this unreachable, so the machine has to name
// it rather than leave the swap stuck in Paid retrying forever.
func TestLostIsReachableAndLoud(t *testing.T) {
	s := Quoted
	for _, e := range []Event{InvoiceOffered, IncomingAccepted, MarginPassed,
		OutgoingSucceeded} {

		next, _, err := Step(s, e)
		if err != nil {
			t.Fatalf("%v on %v: %v", e, s, err)
		}
		s = next
	}
	if s != Paid {
		t.Fatalf("expected to be Paid, got %v", s)
	}

	next, action, err := Step(s, IncomingUnsettleable)
	if err != nil {
		t.Fatalf("a paid swap that cannot be claimed must have somewhere "+
			"to go: %v", err)
	}
	if next != Lost {
		t.Errorf("got %v, want %v", next, Lost)
	}
	if action != Nothing {
		t.Errorf("nothing can be done about a realised loss, got %v", action)
	}
	if !Lost.Terminal() {
		t.Error("Lost must be terminal so the driver stops retrying")
	}
	if _, ok := Recoverable(Lost); ok {
		t.Error("Lost must not reissue anything")
	}
}

// TestUnsettleableOnlyCountsAfterPaying: before the outgoing leg is paid, a
// vanished incoming HTLC is not a loss and must not be recorded as one.
func TestUnsettleableOnlyCountsAfterPaying(t *testing.T) {
	for _, s := range allStates {
		next, _, err := Step(s, IncomingUnsettleable)
		if err != nil {
			continue
		}
		if s != Paid {
			t.Errorf("%v accepted %v and moved to %v; only %v may", s,
				IncomingUnsettleable, next, Paid)
		}
	}

	// While still paying, it must not move: the outgoing outcome is not
	// known yet, so there is nothing to conclude.
	if _, _, err := Step(Paying, IncomingUnsettleable); err == nil {
		t.Error("Paying must not conclude anything from a vanished incoming " +
			"HTLC while the outgoing outcome is unknown")
	}
}

// TestTestListsAreComplete keeps the exhaustive tests honest: a state or event
// added to the package but not to these lists would silently stop being
// covered by every property test above.
func TestTestListsAreComplete(t *testing.T) {
	if len(allStates) != len(stateNames) {
		t.Errorf("allStates has %d entries, the package defines %d",
			len(allStates), len(stateNames))
	}
	seen := map[State]bool{}
	for _, s := range allStates {
		if _, ok := stateNames[s]; !ok {
			t.Errorf("allStates contains unknown %v", s)
		}
		if seen[s] {
			t.Errorf("allStates repeats %v", s)
		}
		seen[s] = true
	}

	if len(allEvents) != len(eventNames) {
		t.Errorf("allEvents has %d entries, the package defines %d",
			len(allEvents), len(eventNames))
	}
	seenE := map[Event]bool{}
	for _, e := range allEvents {
		if _, ok := eventNames[e]; !ok {
			t.Errorf("allEvents contains unknown %v", e)
		}
		if seenE[e] {
			t.Errorf("allEvents repeats %v", e)
		}
		seenE[e] = true
	}
}

// TestParseStateRoundTripsEveryState. Records are stored by name, so a state
// whose name does not survive the trip back is a record that cannot be read
// after a restart, in a state whose recovery decides whether a claim is given
// up.
func TestParseStateRoundTripsEveryState(t *testing.T) {
	for _, s := range allStates {
		name := s.String()
		if strings.HasPrefix(name, "state(") {
			t.Errorf("%d has no name, so it cannot be stored", uint8(s))

			continue
		}
		got, err := ParseState(name)
		if err != nil {
			t.Errorf("%v: %v", s, err)

			continue
		}
		if got != s {
			t.Errorf("%q parsed to %v, want %v", name, got, s)
		}
	}

	// Names are not interchangeable with numbers, which is the whole reason
	// for storing them.
	for _, name := range []string{"", "0", "3", "PAYING", "pay", "half-paid"} {
		if got, err := ParseState(name); !errors.Is(err, ErrUnknownState) {
			t.Errorf("%q parsed to %v, %v; want ErrUnknownState", name, got,
				err)
		}
	}
}

// A payment the paying node never received was never in flight: the swap goes
// back to Funded with nothing to do, so it is decided again, and nothing about
// that step cancels the incoming claim.
func TestOutgoingNotSentReturnsToFunded(t *testing.T) {
	next, action, err := Step(Paying, OutgoingNotSent)
	if err != nil {
		t.Fatal(err)
	}
	if next != Funded || action != Nothing {
		t.Fatalf("Paying on not sent went to %v with %v, want Funded "+
			"with nothing", next, action)
	}

	// It applies to Paying only: in every other state the claim on the
	// payment has moved on, and "not sent" cannot be a reason to go back.
	for _, s := range allStates {
		if s == Paying {
			continue
		}
		if _, _, err := Step(s, OutgoingNotSent); err == nil {
			t.Errorf("not sent moved %v", s)
		}
	}
}

// TestACancelThatFindsThePaymentSucceededSettles: the driver asks the paying
// node before it cancels a failing or aborting swap's incoming HTLC, and a
// payment that succeeded after all makes the swap paid, settling the claim
// rather than giving it away.
func TestACancelThatFindsThePaymentSucceededSettles(t *testing.T) {
	for _, s := range []State{Failing, Aborting} {
		next, action, err := Step(s, OutgoingSucceeded)
		if err != nil {
			t.Fatalf("%v on %v: %v", OutgoingSucceeded, s, err)
		}
		if next != Paid || action != SettleIncoming {
			t.Errorf("%v on %v moves to %v and asks for %v, want %v and "+
				"%v", OutgoingSucceeded, s, next, action, Paid,
				SettleIncoming)
		}
	}
}
