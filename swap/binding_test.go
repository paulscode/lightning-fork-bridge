package swap

import (
	"errors"
	"strings"
	"testing"
)

// TestBindingRoundTripsByName. Stored by name for the same reason states are:
// the numbers are positions in an iota block, and a record misread as the
// weaker binding would be trusted more than it earned.
func TestBindingRoundTripsByName(t *testing.T) {
	for _, b := range []Binding{HashLinked, Sequenced} {
		name := b.String()
		if strings.HasPrefix(name, "binding(") {
			t.Errorf("%d has no name, so it cannot be stored", uint8(b))

			continue
		}
		got, err := ParseBinding(name)
		if err != nil || got != b {
			t.Errorf("%q parsed to %v, %v", name, got, err)
		}
	}

	for _, name := range []string{"", "0", "linked", "HASH-LINKED", "atomic"} {
		if _, err := ParseBinding(name); !errors.Is(err, ErrUnknownBinding) {
			t.Errorf("%q was accepted", name)
		}
	}
	if got := Binding(9).String(); got != "binding(9)" {
		t.Errorf("unknown binding prints %q", got)
	}
}

// TestOnlySequencedNeedsThePreimageUpfront. A hash-linked swap must not have
// it early — not holding it is the entire security of that arrangement — and
// a sequenced one cannot work without it, because no later moment supplies it.
func TestOnlySequencedNeedsThePreimageUpfront(t *testing.T) {
	if HashLinked.NeedsPreimageUpfront() {
		t.Error("a hash-linked swap would be claiming it holds the preimage " +
			"before the outgoing leg has produced it")
	}
	if !Sequenced.NeedsPreimageUpfront() {
		t.Error("a sequenced swap has no later source for the preimage")
	}
}

// TestSettleIsOnlyEverReachableFromPaid is the invariant a sequenced swap
// rests on entirely, so it is checked over the whole machine rather than
// assumed from reading the table.
func TestSettleIsOnlyEverReachableFromPaid(t *testing.T) {
	// The action is emitted on the transition *into* Paid, so that is what
	// has to be pinned: the only move that authorises a settle is the
	// outgoing leg definitively succeeding. That is learned while paying,
	// or, should a payment succeed after all, while cancelling: the driver
	// asks the paying node before it gives up a failing or aborting swap's
	// claim.
	paidFrom := map[State]bool{Paying: true, Failing: true, Aborting: true}
	for _, s := range allStates {
		for _, e := range allEvents {
			next, action, err := Step(s, e)
			if err != nil || action != SettleIncoming {
				continue
			}
			if !paidFrom[s] || e != OutgoingSucceeded || next != Paid {
				t.Errorf("SettleIncoming is authorised by %v on %v (to %v), "+
					"so the recipient is not guaranteed to have been paid "+
					"first", e, s, next)
			}
		}
	}

	// And the only way into Paid is the outgoing leg succeeding.
	for _, s := range allStates {
		for _, e := range allEvents {
			next, _, err := Step(s, e)
			if err != nil || next != Paid {
				continue
			}
			if !paidFrom[s] || e != OutgoingSucceeded {
				t.Errorf("Paid is reachable by %v on %v, so the recipient is "+
					"not guaranteed to have been paid first", e, s)
			}
		}
	}

	// Recovery must not offer a shortcut either.
	for _, s := range allStates {
		if action, ok := Recoverable(s); ok && action == SettleIncoming &&
			s != Paid {

			t.Errorf("recovery offers a settle from %v", s)
		}
	}
}
