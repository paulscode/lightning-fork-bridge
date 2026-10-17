package quote

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/chainrate"
	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/inventory"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/rate"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// manyOut decodes every invoice to a hash of its own text, so a test can quote
// several distinct invoices through one quoter.
type manyOut struct {
	fakeOut
	mu sync.Mutex
}

func (f *manyOut) Decode(_ context.Context, invoice string) (node.Decoded,
	error) {

	f.mu.Lock()
	defer f.mu.Unlock()

	d := decoded()
	d.Hash = node.Hash(sha256.Sum256([]byte(invoice)))

	return d, nil
}

// lockedIn is a fakeIn safe for the concurrent tests.
type lockedIn struct {
	mu sync.Mutex
	fakeIn
}

func (f *lockedIn) AddHoldInvoice(ctx context.Context, r node.HoldInvoice) (
	string, error) {

	f.mu.Lock()
	defer f.mu.Unlock()

	return f.fakeIn.AddHoldInvoice(ctx, r)
}

// TestEveryRefusalHasItsCode: a payer branches on the code, so each reason
// has to come out as its own name rather than as "refused".
func TestEveryRefusalHasItsCode(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		edit func(q *Quoter, d *node.Decoded, in *fakeIn)
		want Code
	}{
		{"unreadable", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			q.Out.(*fakeOut).decodeErr = errors.New("bad checksum")
		}, CodeInvalidInvoice},
		{"other chain", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			d.BLAKE2b = true
		}, CodeNoDirection},
		{"no amount", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			d.AmountMsat = 0
		}, CodeNoAmount},
		{"too small", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			d.AmountMsat = q.Policy.MinSwapMsat - 1
		}, CodeTooSmall},
		{"too large", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			d.AmountMsat = q.Policy.MaxSwapMsat + 1
		}, CodeTooLarge},
		{"expires soon", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			d.Expiry = now.Add(time.Minute)
		}, CodeExpiresSoon},
		{"route budget", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			d.CLTVDelta = q.Policy.OutgoingCLTVLimit
		}, CodeRouteBudget},
		{"pays the bridge", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			q.Own = []string{"02aaa", d.Destination}
		}, CodeSelfPayment},
		{"no headroom", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			q.Room = func(context.Context) (uint64, error) { return 1, nil }
		}, CodeNoLiquidity},
		{"drained", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			q.Price = func(context.Context) (rate.Reading, error) {
				return rate.Reading{}, fmt.Errorf("%w: %w", ErrRefused,
					inventory.ErrDrained)
			}
		}, CodeNoLiquidity},
		{"stale price", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			q.Price = func(context.Context) (rate.Reading, error) {
				return rate.Reading{}, rate.ErrStale
			}
		}, CodePriceUnavailable},
		{"unmeasured chain", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			q.Spacing = func(context.Context) (driver.Rates, error) {
				return driver.Rates{}, chainrate.ErrInsufficient
			}
		}, CodeChainUnmeasured},
		{"too fast to size", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			q.Margin = margin.Policy{Settlement: time.Hour,
				MaxIncomingBlocks: 2}
		}, CodeChainUnmeasured},
		{"already paid", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			in.existing = &node.Invoice{State: node.InvoiceSettled}
		}, CodeAlreadyPaid},
		{"funded before", func(q *Quoter, d *node.Decoded, in *fakeIn) {
			in.existing = &node.Invoice{State: node.InvoiceCancelled,
				HTLCs: 1}
		}, CodeFundedBefore},
		{"held by something else", func(q *Quoter, d *node.Decoded,
			in *fakeIn) {

			in.existing = &node.Invoice{State: node.InvoiceAccepted}
		}, CodeInProgress},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := &fakeIn{}
			out := &fakeOut{decoded: decoded()}
			q, _ := build(t, in, out, 0.01)
			c.edit(q, &out.decoded, in)

			_, err := q.Quote(ctx, "lnbc1x")
			if err == nil {
				t.Fatal("quoted")
			}
			if got := CodeOf(err); got != c.want {
				t.Errorf("code %q, want %q (%v)", got, c.want, err)
			}
			if in.asked != nil && c.want != CodeInternal {
				t.Error("a hold invoice was created for a refused quote")
			}
		})
	}
}

// TestAnInvoiceForTheOtherChainIsNeverQuoted is the guard under routing. A
// direction that pays on Bitcoin must refuse a BLAKE2b invoice even if
// something upstream sent it there, and the other way round.
func TestAnInvoiceForTheOtherChainIsNeverQuoted(t *testing.T) {
	ctx := context.Background()

	for _, paysBLAKE2b := range []bool{false, true} {
		in := &fakeIn{}
		d := decoded()
		d.BLAKE2b = !paysBLAKE2b
		q, _ := build(t, in, &fakeOut{decoded: d}, 0.01)
		q.PaysBLAKE2b = paysBLAKE2b

		if _, err := q.Quote(ctx, "lnbc1x"); !errors.Is(err, ErrWrongChain) {
			t.Errorf("paysBLAKE2b=%v: want ErrWrongChain, got %v",
				paysBLAKE2b, err)
		}

		// And the matching chain is quoted.
		d.BLAKE2b = paysBLAKE2b
		q.Out = &fakeOut{decoded: d}
		if _, err := q.Quote(ctx, "lnbc1x"); err != nil {
			t.Errorf("paysBLAKE2b=%v: the matching chain was refused: %v",
				paysBLAKE2b, err)
		}
	}
}

// TestSelfPaymentIgnoresCase: node keys are hex, and a key written in the
// other case is the same key.
func TestSelfPaymentIgnoresCase(t *testing.T) {
	d := decoded()
	d.Destination = "03ABCDEF"
	q, _ := build(t, &fakeIn{}, &fakeOut{decoded: d}, 0.01)
	q.Own = []string{"03abcdef"}

	if _, err := q.Quote(context.Background(), "lnbc1x"); !errors.Is(err,
		ErrSelfPayment) {

		t.Fatalf("want ErrSelfPayment, got %v", err)
	}
}

// endAs moves the recorded swap for hash into a terminal state, with the node
// holding a cancelled invoice as it would after the bridge cancelled it.
func endAs(t *testing.T, st *store.Memory, in *fakeIn, state swap.State) {
	t.Helper()

	ctx := context.Background()
	rec, err := st.Get(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	rec.State = state
	if err := st.Put(ctx, rec); err != nil {
		t.Fatal(err)
	}
	in.existing = &node.Invoice{State: node.InvoiceCancelled}
}

// TestAnEndedSwapCanBeQuotedAgain is the reason the rule exists: without it an
// invoice whose first attempt expired unpaid could never be paid through the
// bridge, because the node keeps the cancelled hold invoice and refuses a
// second one for the same hash.
func TestAnEndedSwapCanBeQuotedAgain(t *testing.T) {
	ctx := context.Background()

	in := &fakeIn{}
	q, st := build(t, in, &fakeOut{decoded: decoded()}, 0.01)
	if _, err := q.Quote(ctx, "lnbc1x"); err != nil {
		t.Fatal(err)
	}
	endAs(t, st, in, swap.Expired)
	first := in.asked

	got, err := q.Quote(ctx, "lnbc1x")
	if err != nil {
		t.Fatalf("an invoice whose attempt expired unpaid could not be "+
			"quoted again: %v", err)
	}
	if in.forgotten != 1 {
		t.Errorf("the cancelled invoice was forgotten %d times, want once",
			in.forgotten)
	}
	if in.asked == first {
		t.Error("no new hold invoice was created")
	}

	rec, _ := st.Get(ctx, hash)
	if rec.State != swap.Offered {
		t.Errorf("the new attempt is %v, want offered", rec.State)
	}
	if rec.IncomingMsat != got.IncomingMsat {
		t.Error("the record does not carry the new terms")
	}
}

// TestAFundedSwapIsNeverQuotedAgain: an attempt whose hold invoice was funded
// spends the hash, however it ended. Failed and aborted are only reachable
// through funded; an expired one counts when the node's cancelled invoice
// shows an HTLC. Each is refused as funded_before and the earlier invoice is
// left where it is.
func TestAFundedSwapIsNeverQuotedAgain(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		state swap.State
		htlcs int
	}{
		{swap.Failed, 1},
		{swap.Aborted, 1},
		// The record alone decides these two: a node whose invoice
		// shows nothing does not make a funded attempt unfunded.
		{swap.Failed, 0},
		{swap.Aborted, 0},
		{swap.Expired, 1},
	} {
		name := fmt.Sprintf("%v with %d HTLCs", c.state, c.htlcs)
		t.Run(name, func(t *testing.T) {
			in := &fakeIn{}
			q, st := build(t, in, &fakeOut{decoded: decoded()}, 0.01)
			if _, err := q.Quote(ctx, "lnbc1x"); err != nil {
				t.Fatal(err)
			}
			endAs(t, st, in, c.state)
			in.existing.HTLCs = c.htlcs
			first := in.asked

			_, err := q.Quote(ctx, "lnbc1x")
			if !errors.Is(err, ErrFundedBefore) {
				t.Fatalf("want ErrFundedBefore, got %v", err)
			}
			if got := CodeOf(err); got != CodeFundedBefore {
				t.Errorf("code %q, want %q", got, CodeFundedBefore)
			}
			if in.forgotten != 0 {
				t.Error("the funded attempt's invoice was forgotten")
			}
			if in.asked != first {
				t.Error("a new hold invoice was created")
			}
			rec, _ := st.Get(ctx, hash)
			if rec.State != c.state {
				t.Errorf("the earlier record became %v", rec.State)
			}
		})
	}
}

// TestALiveOrSettledSwapIsNotQuotedAgain: everything that is not an ended,
// unpaid attempt stays refused, with the reason a payer can act on.
func TestALiveOrSettledSwapIsNotQuotedAgain(t *testing.T) {
	ctx := context.Background()

	cases := map[swap.State]Code{
		swap.Quoted: CodeInProgress, swap.Offered: CodeInProgress,
		swap.Funded: CodeInProgress, swap.Paying: CodeInProgress,
		swap.Paid: CodeInProgress, swap.Failing: CodeInProgress,
		swap.Aborting: CodeInProgress, swap.Expiring: CodeInProgress,
		swap.Settled: CodeAlreadyPaid, swap.Lost: CodeNeedsOperator,
	}
	for state, want := range cases {
		t.Run(state.String(), func(t *testing.T) {
			in := &fakeIn{}
			q, st := build(t, in, &fakeOut{decoded: decoded()}, 0.01)
			if _, err := q.Quote(ctx, "lnbc1x"); err != nil {
				t.Fatal(err)
			}
			rec, _ := st.Get(ctx, hash)
			rec.State = state
			if err := st.Put(ctx, rec); err != nil {
				t.Fatal(err)
			}

			_, err := q.Quote(ctx, "lnbc1x")
			if got := CodeOf(err); got != want {
				t.Errorf("code %q, want %q (%v)", got, want, err)
			}
			if in.forgotten != 0 {
				t.Error("an invoice was forgotten for a swap that is " +
					"not over")
			}
		})
	}
}

// TestACompactedSwapIsJudgedByTheNode: with no record of the hash, as after a
// journal started afresh, the node's invoice is all there is to go on, and its
// HTLCs say whether an earlier attempt was funded.
func TestACompactedSwapIsJudgedByTheNode(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		state  node.InvoiceState
		htlcs  int
		want   Code
		forget int
	}{
		{node.InvoiceCancelled, 0, "", 1},
		{node.InvoiceCancelled, 1, CodeFundedBefore, 0},
		{node.InvoiceSettled, 0, CodeAlreadyPaid, 0},
		{node.InvoiceOpen, 0, CodeInProgress, 0},
		{node.InvoiceAccepted, 1, CodeInProgress, 0},
	}
	for _, c := range cases {
		name := fmt.Sprintf("%v with %d HTLCs", c.state, c.htlcs)
		t.Run(name, func(t *testing.T) {
			in := &fakeIn{existing: &node.Invoice{State: c.state,
				HTLCs: c.htlcs}}
			q, _ := build(t, in, &fakeOut{decoded: decoded()}, 0.01)

			_, err := q.Quote(ctx, "lnbc1x")
			if got := CodeOf(err); got != c.want {
				t.Errorf("code %q, want %q (%v)", got, c.want, err)
			}
			if in.forgotten != c.forget {
				t.Errorf("forgotten %d times, want %d", in.forgotten,
					c.forget)
			}
		})
	}
}

// TestARefusedRequoteLeavesTheOldInvoice: the cancelled invoice is cleared
// only once the new quote is otherwise certain, so a refusal leaves the record
// of the previous attempt where an operator can still see it.
func TestARefusedRequoteLeavesTheOldInvoice(t *testing.T) {
	ctx := context.Background()
	in := &fakeIn{}
	q, st := build(t, in, &fakeOut{decoded: decoded()}, 0.01)
	if _, err := q.Quote(ctx, "lnbc1x"); err != nil {
		t.Fatal(err)
	}
	endAs(t, st, in, swap.Expired)

	q.Room = func(context.Context) (uint64, error) { return 0, nil }
	if _, err := q.Quote(ctx, "lnbc1x"); !errors.Is(err, ErrNoLiquidity) {
		t.Fatalf("want a liquidity refusal, got %v", err)
	}
	if in.forgotten != 0 {
		t.Error("the old invoice was forgotten for a quote that was refused")
	}
}

// TestALookupFailureRefusesRatherThanGuesses: if the node cannot say what it
// holds for the hash, nothing is created.
func TestALookupFailureRefusesRatherThanGuesses(t *testing.T) {
	in := &fakeIn{lookupErr: errors.New("node down")}
	q, _ := build(t, in, &fakeOut{decoded: decoded()}, 0.01)

	if _, err := q.Quote(context.Background(), "lnbc1x"); err == nil {
		t.Fatal("quoted without knowing what the node holds")
	}
	if in.asked != nil {
		t.Error("a hold invoice was created")
	}
}

// TestAFailedHoldInvoiceIsUntracked: the record exists and the invoice may, so
// the caller is told to drive it rather than given an ordinary failure it
// would drop.
func TestAFailedHoldInvoiceIsUntracked(t *testing.T) {
	ctx := context.Background()
	in := &fakeIn{addErr: errors.New("timed out")}
	q, st := build(t, in, &fakeOut{decoded: decoded()}, 0.01)

	_, err := q.Quote(ctx, "lnbc1x")
	if !errors.Is(err, ErrUntracked) {
		t.Fatalf("want ErrUntracked, got %v", err)
	}
	if CodeOf(err) != CodeInternal {
		t.Errorf("code %q, want internal", CodeOf(err))
	}
	rec, err := st.Get(ctx, hash)
	if err != nil {
		t.Fatalf("the swap was not recorded: %v", err)
	}
	if rec.State != swap.Quoted {
		t.Errorf("recorded as %v, want quoted", rec.State)
	}
}

// TestParticipantsAreRecorded: the limits are counted from the journal, so the
// participant has to be in it.
func TestParticipantsAreRecorded(t *testing.T) {
	ctx := context.Background()
	q, st := build(t, &fakeIn{}, &fakeOut{decoded: decoded()}, 0.01)
	q.Limits = DefaultLimits

	if _, err := q.QuoteFor(ctx, "alice", "lnbc1x"); err != nil {
		t.Fatal(err)
	}
	rec, _ := st.Get(ctx, hash)
	if rec.Participant != "alice" {
		t.Errorf("participant %q, want alice", rec.Participant)
	}
	// And that nothing has been paid for it, so a refund before it is
	// ever paid need not ask the paying node.
	if !rec.NoPayAttempt {
		t.Error("a new swap does not say no payment was attempted")
	}
}

// limited is a quoter over many distinct invoices with the given limits.
func limited(t *testing.T, l Limits) (*Quoter, *store.Memory, *lockedIn) {
	t.Helper()

	in := &lockedIn{}
	q, st := build(t, &in.fakeIn, &fakeOut{}, 0.01)
	q.In = in
	q.Out = &manyOut{}
	q.Limits = l

	return q, st, in
}

// TestTheUnpaidLimitStopsFreeReservations is the hole the limit closes: an
// unpaid quote reserves the bridge's liquidity and costs nothing, so one
// participant could otherwise hold all of it.
func TestTheUnpaidLimitStopsFreeReservations(t *testing.T) {
	ctx := context.Background()
	q, st, _ := limited(t, Limits{MaxUnpaid: 2})

	for i := range 2 {
		if _, err := q.QuoteFor(ctx, "alice",
			fmt.Sprintf("lnbc1-%d", i)); err != nil {

			t.Fatal(err)
		}
	}
	_, err := q.QuoteFor(ctx, "alice", "lnbc1-3")
	if CodeOf(err) != CodeLimit {
		t.Fatalf("third unpaid quote: code %q (%v)", CodeOf(err), err)
	}

	// Somebody else is not affected, and nor is the operator.
	if _, err := q.QuoteFor(ctx, "bob", "lnbc1-bob"); err != nil {
		t.Errorf("another participant was limited by alice: %v", err)
	}
	if _, err := q.Quote(ctx, "lnbc1-operator"); err != nil {
		t.Errorf("the operator was limited: %v", err)
	}

	// Paying one frees a slot.
	pending, _ := st.Pending(ctx)
	for _, r := range pending {
		if r.Participant == "alice" {
			r.State = swap.Funded
			if err := st.Put(ctx, r); err != nil {
				t.Fatal(err)
			}

			break
		}
	}
	if _, err := q.QuoteFor(ctx, "alice", "lnbc1-4"); err != nil {
		t.Errorf("a paid quote should free its slot: %v", err)
	}
}

// TestTheInProgressLimitCountsPaidSwapsToo: it bounds the value one
// participant can have outstanding, paid or not.
func TestTheInProgressLimitCountsPaidSwapsToo(t *testing.T) {
	ctx := context.Background()
	q, st, _ := limited(t, Limits{MaxInProgress: 2})

	for i := range 2 {
		if _, err := q.QuoteFor(ctx, "alice",
			fmt.Sprintf("lnbc1-%d", i)); err != nil {

			t.Fatal(err)
		}
	}
	pending, _ := st.Pending(ctx)
	for _, r := range pending {
		r.State = swap.Paying
		if err := st.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	_, err := q.QuoteFor(ctx, "alice", "lnbc1-3")
	if CodeOf(err) != CodeLimit {
		t.Fatalf("code %q (%v)", CodeOf(err), err)
	}
}

// TestThePerMinuteLimit, and that it is a window rather than a lifetime cap.
func TestThePerMinuteLimit(t *testing.T) {
	ctx := context.Background()
	q, _, _ := limited(t, Limits{PerMinute: 3})
	clock := now
	q.Now = func() time.Time { return clock }

	for i := range 3 {
		if _, err := q.QuoteFor(ctx, "alice",
			fmt.Sprintf("lnbc1-%d", i)); err != nil {

			t.Fatal(err)
		}
	}
	if _, err := q.QuoteFor(ctx, "alice", "lnbc1-3"); CodeOf(err) != CodeLimit {
		t.Fatalf("fourth in a minute: %v", err)
	}

	clock = clock.Add(time.Minute)
	if _, err := q.QuoteFor(ctx, "alice", "lnbc1-4"); err != nil {
		t.Errorf("a minute later the window should have moved: %v", err)
	}
}

// TestLimitsHoldUnderARace: sixteen quotes from one participant at once must
// not all pass a limit that only two of them fit under. Without the
// per-participant lock each would read the journal before any had written.
func TestLimitsHoldUnderARace(t *testing.T) {
	ctx := context.Background()
	q, st, _ := limited(t, Limits{MaxUnpaid: 2})

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = q.QuoteFor(ctx, "alice", fmt.Sprintf("lnbc1-%d", i))
		}()
	}
	wg.Wait()

	pending, _ := st.Pending(ctx)
	if len(pending) != 2 {
		t.Fatalf("%d swaps recorded for alice under a limit of 2",
			len(pending))
	}
}

// TestIdleParticipantsAreForgotten: a participant with no quote in hand and
// none in the last minute is dropped from memory, so callers that come and
// go, one address each, do not grow it for as long as the process runs. One
// still inside its minute is kept, and still limited.
func TestIdleParticipantsAreForgotten(t *testing.T) {
	ctx := context.Background()
	q, _, _ := limited(t, Limits{PerMinute: 1})
	clock := now
	q.Now = func() time.Time { return clock }

	for i := range 3 {
		_, _ = q.QuoteFor(ctx, fmt.Sprintf("addr:%d", i),
			fmt.Sprintf("lnbc1-%d", i))
	}
	clock = clock.Add(30 * time.Second)
	if _, err := q.QuoteFor(ctx, "alice", "lnbc1-alice"); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(45 * time.Second)
	_, _ = q.QuoteFor(ctx, "bob", "lnbc1-bob")

	q.whoMu.Lock()
	n := len(q.who)
	q.whoMu.Unlock()
	if n != 2 {
		t.Errorf("%d participants remembered, want alice and bob", n)
	}
	if _, err := q.QuoteFor(ctx, "alice", "lnbc1-alice2"); CodeOf(err) !=
		CodeLimit {

		t.Errorf("alice, inside her minute, was not limited: %v", err)
	}
}

// TestAnExpiredAttemptTheNodeForgotIsNotQuotedAgain: an attempt that expired
// with a hold invoice out, whose invoice the node no longer has, cannot be
// shown unfunded, so it is refused as funded_before. One that never had an
// invoice can be quoted again, and one the bridge saw an HTLC for is refused
// whatever the node says.
func TestAnExpiredAttemptTheNodeForgotIsNotQuotedAgain(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name      string
		noInvoice bool
		htlcSeen  bool
		node      *node.Invoice
		requote   bool
	}{
		{name: "offered, then gone from the node"},
		{name: "never offered, nothing on the node", noInvoice: true,
			requote: true},
		{name: "seen funded, the node's invoice shows nothing",
			htlcSeen: true,
			node:     &node.Invoice{State: node.InvoiceCancelled}},
		{name: "seen funded, then gone from the node", htlcSeen: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := &fakeIn{}
			q, st := build(t, in, &fakeOut{decoded: decoded()}, 0.01)
			if _, err := q.Quote(ctx, "lnbc1x"); err != nil {
				t.Fatal(err)
			}
			rec, _ := st.Get(ctx, hash)
			rec.State = swap.Expired
			rec.NoInvoice, rec.HTLCSeen = c.noInvoice, c.htlcSeen
			if err := st.Put(ctx, rec); err != nil {
				t.Fatal(err)
			}
			in.existing = c.node

			_, err := q.Quote(ctx, "lnbc1x")
			switch {
			case c.requote && err != nil:
				t.Errorf("refused: %v", err)
			case !c.requote && !errors.Is(err, ErrFundedBefore):
				t.Errorf("want ErrFundedBefore, got %v", err)
			}
		})
	}
}

// TestAQuoteSaysWhenItsInvoiceExists: a swap is recorded with no invoice, and
// the write that records it offered says it has one.
func TestAQuoteSaysWhenItsInvoiceExists(t *testing.T) {
	ctx := context.Background()

	in := &fakeIn{addErr: errors.New("node gone")}
	q, st := build(t, in, &fakeOut{decoded: decoded()}, 0.01)
	_, _ = q.Quote(ctx, "lnbc1x")
	if rec, _ := st.Get(ctx, hash); !rec.NoInvoice ||
		rec.State != swap.Quoted {

		t.Errorf("before its invoice: %v, no invoice %v", rec.State,
			rec.NoInvoice)
	}

	in.addErr = nil
	_ = st.Put(ctx, store.Record{Hash: hash, State: swap.Expired,
		NoInvoice: true, Created: now})
	if _, err := q.Quote(ctx, "lnbc1x"); err != nil {
		t.Fatal(err)
	}
	if rec, _ := st.Get(ctx, hash); rec.NoInvoice ||
		rec.State != swap.Offered {

		t.Errorf("offered: %v, no invoice %v", rec.State, rec.NoInvoice)
	}
}

// TestConcurrentQuotesCannotOversell: two quotes, each fitting the room alone
// and not together, both pass the first look at the room before either is
// recorded. The room is looked at again with the record written under one
// lock, so only one is quoted; the other is refused for liquidity rather than
// sold money already promised.
func TestConcurrentQuotesCannotOversell(t *testing.T) {
	ctx := context.Background()
	q, st, _ := limited(t, Limits{})

	// Room for one 100,000 sat payout with its routing budget, not two.
	// Having read the room the second time, a quote waits a little for the
	// other's second read, which only comes if nothing holds it back, so
	// that two reads that can overlap do.
	const balance = 150_000_000
	var looks, seconds atomic.Int32
	both := make(chan struct{})
	q.Room = func(ctx context.Context) (uint64, error) {
		committed, err := store.Committed(ctx, st)
		if err != nil || committed > balance {
			return 0, err
		}
		if looks.Add(1) > 2 {
			if seconds.Add(1) == 2 {
				close(both)
			}
			select {
			case <-both:
			case <-time.After(50 * time.Millisecond):
			}
		}

		return balance - committed, nil
	}
	// Both are priced, so past the first look at the room, before either
	// goes on.
	var priced sync.WaitGroup
	priced.Add(2)
	q.Price = func(context.Context) (rate.Reading, error) {
		priced.Done()
		priced.Wait()

		return reading(0.01), nil
	}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = q.QuoteFor(ctx, fmt.Sprintf("p%d", i),
				fmt.Sprintf("lnbc1-%d", i))
		}()
	}
	wg.Wait()

	pending, _ := st.Pending(ctx)
	if len(pending) != 1 {
		t.Fatalf("%d swaps recorded with room for one", len(pending))
	}
	refused := 0
	for _, err := range errs {
		if errors.Is(err, ErrNoLiquidity) {
			refused++
		}
	}
	if refused != 1 {
		t.Errorf("refusals %v, want one for liquidity", errs)
	}
}
