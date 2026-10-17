package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

var (
	blake = margin.Bounds{Fast: 155 * time.Second, Slow: 1200 * time.Second}
	btc   = margin.Bounds{Fast: 300 * time.Second, Slow: 600 * time.Second}
	pol   = margin.Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016}
	now   = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

	preimage = node.Preimage{0xab, 0xcd}
	hash     = preimage.Hash()
)

func clock() time.Time { return now }

func rates(context.Context) (driver.Rates, error) {
	return driver.Rates{Incoming: blake, Outgoing: btc, As: now}, nil
}

// fakeIn is a scripted incoming node, safe for concurrent use because the
// runner's whole job is to be the only thing touching a swap.
type fakeIn struct {
	mu        sync.Mutex
	invoice   node.Invoice
	lookupErr error
	height    int32
	settled   int
	cancelled int
}

func (f *fakeIn) set(fn func(*fakeIn)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *fakeIn) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.settled, f.cancelled
}

func (f *fakeIn) AddHoldInvoice(context.Context, node.HoldInvoice) (string,
	error) {

	return "lnblakert1fake", nil
}

func (f *fakeIn) LookupInvoice(context.Context, node.Hash) (node.Invoice,
	error) {

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return node.Invoice{}, f.lookupErr
	}

	return f.invoice, nil
}

// SettleInvoice refuses what a real node would refuse. A permissive fake here
// would let a settle succeed against a claim that is already gone, and hide
// the one path that costs the bridge money.
func (f *fakeIn) SettleInvoice(context.Context, node.Preimage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.invoice.State == node.InvoiceSettled {
		return nil // idempotent, as the adapter guarantees
	}
	if f.invoice.State != node.InvoiceAccepted {
		return fmt.Errorf("cannot settle an invoice that is %v",
			f.invoice.State)
	}
	f.settled++
	f.invoice.State = node.InvoiceSettled

	return nil
}

func (f *fakeIn) ForgetInvoice(context.Context, node.Hash) error { return nil }

func (f *fakeIn) CancelInvoice(context.Context, node.Hash) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.invoice.State == node.InvoiceCancelled {
		return nil // idempotent
	}
	if f.invoice.State == node.InvoiceSettled {
		return errors.New("cannot cancel an invoice that is settled")
	}
	f.cancelled++
	f.invoice.State = node.InvoiceCancelled

	return nil
}

func (f *fakeIn) BlockHeight(context.Context) (int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.height, nil
}

// fakeOut is a scripted outgoing node.
type fakeOut struct {
	mu      sync.Mutex
	payment node.Payment
	payErr  error
	lookup  node.Payment
	paid    int32
	delay   time.Duration

	// lookups answers for specific hashes, consulted before lookup. Without
	// it one fake serves every swap in a Resume test, and a preimage from
	// one swap comes back as the answer for another.
	lookups map[node.Hash]node.Payment

	// expiry is when the outgoing invoice stops being payable. Zero means
	// a day after the test clock's now.
	expiry time.Time
}

func (f *fakeOut) Pay(ctx context.Context, _ node.PayRequest) (node.Payment,
	error) {

	atomic.AddInt32(&f.paid, 1)
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return node.Payment{}, ctx.Err()
		case <-time.After(f.delay):
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.payErr != nil {
		return node.Payment{}, f.payErr
	}
	f.lookup = f.payment

	return f.payment, nil
}

func (f *fakeOut) LookupPayment(_ context.Context, hash node.Hash) (
	node.Payment, error) {

	f.mu.Lock()
	defer f.mu.Unlock()

	if p, ok := f.lookups[hash]; ok {
		return p, nil
	}

	return f.lookup, nil
}

// Decode reads a payment request. The bridge uses the paying node's own
// decoder so that no second parser can disagree about the payment hash.
func (f *fakeOut) Decode(context.Context, string) (node.Decoded, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.expiry.IsZero() {
		return node.Decoded{Expiry: now.Add(24 * time.Hour)}, nil
	}

	return node.Decoded{Expiry: f.expiry}, nil
}

func (f *fakeOut) BlockHeight(context.Context) (int32, error) { return 0, nil }

// build wires a runner around scripted nodes and a record.
func build(t *testing.T, in *fakeIn, out *fakeOut, rec store.Record) (*Runner,
	*store.Memory) {

	t.Helper()

	st := store.NewMemory()
	if err := st.Put(context.Background(), rec); err != nil {
		t.Fatal(err)
	}

	return &Runner{
		Driver: &driver.Driver{In: in, Out: out, Store: st, Policy: pol,
			Now: clock},
		Store: st, Rates: rates, Poll: time.Millisecond,
		NotSentPause: time.Millisecond, Now: clock,
	}, st
}

func record(state swap.State) store.Record {
	return store.Record{
		Hash: hash, State: state, OutgoingCLTVLimit: 40,
		Invoice: "lnbc1fake", IncomingMsat: 1_000_000, OutgoingMsat: 3080,
		MaxFeeMsat: 5000, QuoteExpiry: now.Add(time.Minute), Created: now,
	}
}

func deadline(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	return context.WithTimeout(context.Background(), 10*time.Second)
}

// TestASwapRunsToSettlement end to end, through the real driver, state machine
// and store.
func TestASwapRunsToSettlement(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Offered))

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, err := st.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != swap.Settled {
		t.Fatalf("ended in %v", final.State)
	}
	if final.Preimage != preimage {
		t.Error("the preimage was not recorded")
	}
	settled, cancelled := in.counts()
	if settled != 1 || cancelled != 0 {
		t.Errorf("settled %d, cancelled %d", settled, cancelled)
	}
	if n := atomic.LoadInt32(&out.paid); n != 1 {
		t.Errorf("paid %d times, want once", n)
	}
	if r.Active() != 0 {
		t.Errorf("%d swaps still claimed after finishing", r.Active())
	}
}

// TestAFailedOutgoingLegReturnsTheClaim.
func TestAFailedOutgoingLegReturnsTheClaim(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{State: node.PaymentFailed}}
	r, st := build(t, in, out, record(swap.Offered))

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Failed {
		t.Fatalf("ended in %v", final.State)
	}
	settled, cancelled := in.counts()
	if settled != 0 || cancelled != 1 {
		t.Errorf("settled %d, cancelled %d", settled, cancelled)
	}
}

// TestAMarginThatDoesNotHoldAbortsBeforePaying. This is the abort-rather-than-
// pay decision reaching all the way out to the nodes.
func TestAMarginThatDoesNotHoldAbortsBeforePaying(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	// 40 outgoing blocks needs 179 incoming; there are 20.
	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 520},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Offered))

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Aborted {
		t.Fatalf("ended in %v", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 0 {
		t.Errorf("paid %d times despite the margin refusing", n)
	}
	_, cancelled := in.counts()
	if cancelled != 1 {
		t.Errorf("cancelled %d times", cancelled)
	}
}

// TestAnUnfundedQuoteLapses.
func TestAnUnfundedQuoteLapses(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceOpen}, height: 500,
	}
	rec := record(swap.Offered)
	rec.QuoteExpiry = now.Add(-time.Second) // already gone
	r, st := build(t, in, &fakeOut{}, rec)

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Expired {
		t.Fatalf("ended in %v", final.State)
	}
	_, cancelled := in.counts()
	if cancelled != 1 {
		t.Errorf("cancelled %d times", cancelled)
	}
}

// TestAFundedSwapIsPaidWithinItsGrace: a swap funded before its quote
// expired is still paid shortly after, because funding is what the quote
// asked for and a node taking a few seconds to answer is not the payer's
// fault.
func TestAFundedSwapIsPaidWithinItsGrace(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	rec := record(swap.Funded)
	rec.QuoteExpiry = now.Add(-DefaultFundedGrace / 2)
	r, st := build(t, in, out, rec)

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Settled {
		t.Fatalf("ended in %v; a swap inside its grace should be paid",
			final.State)
	}
}

// TestAFundedSwapPastItsGraceIsGivenBack is the bound on how long a payer's
// HTLC can be held for nothing. Past the grace the swap is cancelled rather
// than paid, and above all nothing is sent: the case this exists for is a
// bridge that cannot measure a chain, where paying would be a decision made
// without the margin check.
func TestAFundedSwapPastItsGraceIsGivenBack(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	rec := record(swap.Funded)
	rec.QuoteExpiry = now.Add(-DefaultFundedGrace - time.Second)
	r, st := build(t, in, out, rec)
	// The measurement is down, which is the situation in question.
	r.Rates = func(context.Context) (driver.Rates, error) {
		return driver.Rates{}, errors.New("cannot measure")
	}

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Aborted {
		t.Fatalf("ended in %v, want aborted", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 0 {
		t.Fatalf("paid %d times; a swap past its grace must not pay", n)
	}
	if _, cancelled := in.counts(); cancelled != 1 {
		t.Fatalf("cancelled %d times, want once", cancelled)
	}
}

// TestTheGraceIsConfigurable: an operator who wants a tighter or looser bound
// gets it.
func TestTheGraceIsConfigurable(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	rec := record(swap.Funded)
	rec.QuoteExpiry = now.Add(-2 * time.Minute)
	r, st := build(t, in, out, rec)
	r.FundedGrace = time.Minute

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Aborted {
		t.Fatalf("ended in %v, want aborted under a one-minute grace",
			final.State)
	}
}

// TestOneDriverPerHash is the concurrency guarantee the rest of the package
// rests on. Two goroutines interleaving read-decide-record-act on the same
// swap can gate against one reading of the nodes and pay against another, and
// nothing further down survives that.
func TestOneDriverPerHash(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{
		payment: node.Payment{State: node.PaymentSucceeded,
			Preimage: preimage},
		delay: 20 * time.Millisecond, // hold the swap open while the race runs
	}
	r, st := build(t, in, out, record(swap.Offered))

	const racers = 32
	var (
		wg    sync.WaitGroup
		ran   atomic.Int32
		busy  atomic.Int32
		other atomic.Int32
	)
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := r.Drive(ctx, hash); {
			case err == nil:
				ran.Add(1)
			case errors.Is(err, ErrBusy):
				busy.Add(1)
			default:
				other.Add(1)
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if other.Load() != 0 {
		t.Fatalf("%d unexpected errors", other.Load())
	}
	// Whoever ran second found the swap already settled and returned at once,
	// so the count of clean returns is not fixed. What must be true is that
	// the work happened once.
	if n := atomic.LoadInt32(&out.paid); n != 1 {
		t.Errorf("the outgoing leg was paid %d times, want exactly once", n)
	}
	settled, cancelled := in.counts()
	if settled != 1 || cancelled != 0 {
		t.Errorf("settled %d, cancelled %d; want 1 and 0", settled, cancelled)
	}
	if ran.Load()+busy.Load() != racers {
		t.Errorf("%d ran, %d were refused, want %d between them", ran.Load(),
			busy.Load(), racers)
	}
	if busy.Load() == 0 {
		t.Log("note: no contention observed this run; the guarantee is " +
			"still asserted by the call counts above")
	}
	if r.Active() != 0 {
		t.Errorf("%d swaps still claimed", r.Active())
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Settled {
		t.Errorf("ended in %v", final.State)
	}
}

// TestASwapItCannotDecideIsLeftAlone. The outgoing node has no record of a
// payment the bridge recorded as in flight: "never left" and "left and the
// node lost track" call for opposite actions, and the wrong one gives up the
// only claim against money already gone.
func TestASwapItCannotDecideIsLeftAlone(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{lookup: node.Payment{State: node.PaymentNone}}
	r, st := build(t, in, out, record(swap.Paying))

	err := r.Drive(ctx, hash)
	if !errors.Is(err, driver.ErrNeedsOperator) {
		t.Fatalf("want ErrNeedsOperator, got %v", err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Paying {
		t.Errorf("the record moved to %v", final.State)
	}
	_, cancelled := in.counts()
	if cancelled != 0 {
		t.Error("it cancelled the claim on a swap it could not decide")
	}
}

// TestATransientFailureIsRetried, not treated as a verdict on the swap.
// Abandoning a live HTLC because a node did not answer once is how one
// expires.
func TestATransientFailureIsRetried(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice:   node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:    500,
		lookupErr: errors.New("connection refused"),
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Offered))

	// The node comes back after a few failed polls.
	go func() {
		time.Sleep(30 * time.Millisecond)
		in.set(func(f *fakeIn) { f.lookupErr = nil })
	}()

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatalf("a node blip ended the swap: %v", err)
	}
	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Settled {
		t.Errorf("ended in %v", final.State)
	}
}

// TestRatesThatCannotBeMeasuredDoNotDecideAnything. A missing measurement is
// not a refusal: the HTLC is still claimable and the margin may well hold once
// it comes back. Paying or cancelling on the strength of it would both be
// decisions the bridge has no grounds for.
func TestRatesThatCannotBeMeasuredDoNotDecideAnything(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(),
		200*time.Millisecond)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Funded))
	r.Rates = func(context.Context) (driver.Rates, error) {
		return driver.Rates{}, errors.New("the chain observer has no samples")
	}

	// It should keep waiting until the context ends, never deciding.
	if err := r.Drive(ctx, hash); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the context to be what stopped it, got %v", err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Funded {
		t.Errorf("it moved to %v without a usable measurement", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 0 {
		t.Error("it paid without a usable measurement")
	}
	_, cancelled := in.counts()
	if cancelled != 0 {
		t.Error("it cancelled without a usable measurement")
	}
}

// TestShuttingDownLeavesEverythingRecoverable.
func TestShuttingDownLeavesEverythingRecoverable(t *testing.T) {
	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceOpen}, height: 500,
	}
	rec := record(swap.Offered)
	rec.QuoteExpiry = now.Add(time.Hour) // will not lapse during the test
	r, st := build(t, in, &fakeOut{}, rec)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := r.Drive(ctx, hash); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if r.Active() != 0 {
		t.Error("the swap is still claimed after shutting down")
	}

	// And it is exactly where it was, so a restart picks it up.
	final, err := st.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != swap.Offered {
		t.Errorf("state %v", final.State)
	}
	pending, _ := st.Pending(context.Background())
	if len(pending) != 1 {
		t.Errorf("%d pending after shutdown, want 1", len(pending))
	}
}

// TestResumeWorksThroughEverythingAndPastWhatItCannotDecide.
func TestResumeWorksThroughEverythingAndPastWhatItCannotDecide(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	st := store.NewMemory()
	stuck := node.Hash{0xff}
	put := func(h node.Hash, state swap.State, age time.Duration) {
		t.Helper()
		r := record(state)
		r.Hash = h
		r.Created = now.Add(age)
		if err := st.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	put(hash, swap.Offered, 0)
	put(stuck, swap.Paying, time.Minute)

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{
		payment: node.Payment{State: node.PaymentSucceeded,
			Preimage: preimage},
		// The stuck one has no record at the node, whatever else happens.
		lookups: map[node.Hash]node.Payment{
			stuck: {State: node.PaymentNone},
		},
	}
	r := &Runner{
		Driver: &driver.Driver{In: in, Out: out, Store: st, Policy: pol,
			Now: clock},
		Store: st, Rates: rates, Poll: time.Millisecond, Now: clock,
	}

	err := r.Resume(ctx)
	if !errors.Is(err, driver.ErrNeedsOperator) {
		t.Fatalf("want the stuck swap reported, got %v", err)
	}

	// The one it could decide still finished, which is the point.
	good, _ := st.Get(context.Background(), hash)
	if good.State != swap.Settled {
		t.Errorf("the decidable swap ended in %v", good.State)
	}
	// And the one it could not is untouched.
	bad, _ := st.Get(context.Background(), stuck)
	if bad.State != swap.Paying {
		t.Errorf("the stuck swap moved to %v", bad.State)
	}
	if r.Active() != 0 {
		t.Errorf("%d swaps still claimed", r.Active())
	}
}

func TestARunnerWithoutWhatItNeedsRefuses(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemory()
	d := &driver.Driver{In: &fakeIn{}, Out: &fakeOut{}, Store: st}

	for _, c := range []struct {
		name string
		r    *Runner
	}{
		{"no driver", &Runner{Store: st, Rates: rates}},
		{"no store", &Runner{Driver: d, Rates: rates}},
		{"no rate source", &Runner{Driver: d, Store: st}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := c.r.Drive(ctx, hash); !errors.Is(err, ErrConfig) {
				t.Errorf("drive: %v", err)
			}
			if err := c.r.Resume(ctx); !errors.Is(err, ErrConfig) {
				t.Errorf("resume: %v", err)
			}
		})
	}
}

// TestFatalAndTransientAreSortedCorrectly, because the two want opposite
// responses and putting one in the other's bucket is how a bridge either
// abandons a live HTLC or repeats something it must not.
func TestFatalAndTransientAreSortedCorrectly(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"needs an operator", driver.ErrNeedsOperator, true},
		{"an inconsistent state", driver.ErrState, true},
		{"nowhere to record it", driver.ErrNoStore, true},
		{"a record that cannot be stored", store.ErrRecord, true},
		{"a corrupt journal", store.ErrCorrupt, true},
		{"a runner that cannot run", ErrConfig, true},
		{"shutting down", context.Canceled, true},
		{"out of time", context.DeadlineExceeded, true},

		{"a node that did not answer", errors.New("connection refused"), false},
		{"no usable measurement", errors.New("not enough samples"), false},
		{"stale rates", driver.ErrStaleRates, false},
		{"a swap not found", store.ErrNotFound, false},
		{"an unknown hash", node.ErrUnknownHash, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			wrapped := errors.Join(errors.New("while doing something"), c.err)
			if got := fatal(wrapped); got != c.want {
				t.Errorf("fatal(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestAClaimThatDiesWhilePayingEndsInLost.
//
// This is the only outcome that costs the bridge money, and the margin policy
// exists to make it unreachable. What matters here is that when it does
// happen, the runner recognises it, says so, and stops: the alternative is a
// swap that retries a settle against a cancelled invoice forever while nobody
// looks at why an assumption failed.
func TestAClaimThatDiesWhilePayingEndsInLost(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	// The incoming HTLC is gone. The outgoing payment succeeded.
	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceCancelled},
		height:  500,
	}
	out := &fakeOut{lookup: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Paying))

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Lost {
		t.Fatalf("ended in %v, want lost", final.State)
	}
	if final.Preimage != preimage {
		t.Error("the preimage was not kept; it is the evidence of what was paid")
	}
	settled, cancelled := in.counts()
	if settled != 0 {
		t.Errorf("it tried to settle a dead claim %d times", settled)
	}
	if cancelled != 0 {
		t.Errorf("it cancelled %d times after paying out", cancelled)
	}
}

// TestCarryOutRefusesToSkipAPayment. PayOutgoing is the one action whose
// outcome has to be recorded, so it must never be handled as fire and forget.
// Silently doing nothing would leave the swap in Paying forever.
func TestCarryOutRefusesToSkipAPayment(t *testing.T) {
	ctx := context.Background()
	in := &fakeIn{invoice: node.Invoice{State: node.InvoiceAccepted,
		ExpiryHeight: 1000}, height: 500}
	out := &fakeOut{}
	r, _ := build(t, in, out, record(swap.Funded))

	sw := record(swap.Paying)
	err := r.carryOut(ctx, &sw, swap.PayOutgoing, r.log())
	if !errors.Is(err, driver.ErrState) {
		t.Fatalf("want ErrState, got %v", err)
	}
	if !fatal(err) {
		t.Error("it would have been retried rather than surfaced")
	}
	if n := atomic.LoadInt32(&out.paid); n != 0 {
		t.Error("it paid from a path with no request to build one from")
	}
}

// TestDrivingSomethingAlreadyFinishedIsANoOp, because recovery and an operator
// can both ask at once.
func TestDrivingSomethingAlreadyFinishedIsANoOp(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	for _, state := range []swap.State{
		swap.Settled, swap.Failed, swap.Aborted, swap.Expired, swap.Lost,
	} {
		in := &fakeIn{invoice: node.Invoice{State: node.InvoiceSettled}}
		out := &fakeOut{}
		r, _ := build(t, in, out, record(state))

		if err := r.Drive(ctx, hash); err != nil {
			t.Errorf("%v: %v", state, err)
		}
		settled, cancelled := in.counts()
		if settled != 0 || cancelled != 0 || atomic.LoadInt32(&out.paid) != 0 {
			t.Errorf("%v: it touched the nodes for a finished swap", state)
		}
	}
}

// notSentOut refuses as unreached for its first attempts, then pays: a paying
// node that was down and came back.
type notSentOut struct {
	*fakeOut
	refusals int32
}

func (o *notSentOut) Pay(ctx context.Context, req node.PayRequest) (
	node.Payment, error) {

	if atomic.AddInt32(&o.refusals, -1) >= 0 {
		atomic.AddInt32(&o.paid, 1)

		return node.Payment{}, fmt.Errorf("sending: %w", node.ErrNotSent)
	}

	return o.fakeOut.Pay(ctx, req)
}

// A paying node that never received the payment leaves the swap funded rather
// than waiting in Paying for a record that will not exist, which needs an
// operator. When the node answers again the swap is gated and paid as usual,
// and nothing is cancelled on the way.
func TestAPaymentTheNodeNeverReceivedIsSentAgain(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &notSentOut{
		fakeOut: &fakeOut{payment: node.Payment{
			State: node.PaymentSucceeded, Preimage: preimage,
		}},
		refusals: 2,
	}
	r, st := build(t, in, out.fakeOut, record(swap.Offered))
	r.Driver.Out = out

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, err := st.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != swap.Settled {
		t.Fatalf("ended in %v", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 3 {
		t.Errorf("tried %d times, want two refusals and one payment", n)
	}
	settled, cancelled := in.counts()
	if settled != 1 || cancelled != 0 {
		t.Errorf("settled %d, cancelled %d", settled, cancelled)
	}
}

// Back in Funded, the margin is decided again: if the node stayed away long
// enough that paying is no longer safe, the swap is aborted and the payer
// refunded, rather than paid late.
func TestANeverReceivedPaymentIsGatedAgain(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &notSentOut{
		fakeOut: &fakeOut{payment: node.Payment{
			State: node.PaymentSucceeded, Preimage: preimage,
		}},
		refusals: 1,
	}
	r, st := build(t, in, out.fakeOut, record(swap.Offered))
	r.Driver.Out = out

	// While the node is away, the incoming HTLC's deadline closes in: by
	// the second attempt there is no longer room to pay safely.
	gated := int32(0)
	inner := r.Rates
	r.Rates = func(ctx context.Context) (driver.Rates, error) {
		if atomic.AddInt32(&gated, 1) > 1 {
			in.mu.Lock()
			in.height = 990
			in.mu.Unlock()
		}

		return inner(ctx)
	}

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Aborted {
		t.Fatalf("ended in %v, want aborted before paying late", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 1 {
		t.Errorf("tried %d times, want only the refused one", n)
	}
}

// After a refusal the runner waits NotSentPause, not a poll tick: a slow poll
// must not hold up a payment the paying node was only briefly unable to take.
func TestANeverReceivedPaymentWaitsThePauseNotThePoll(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &notSentOut{
		fakeOut: &fakeOut{payment: node.Payment{
			State: node.PaymentSucceeded, Preimage: preimage,
		}},
		refusals: 1,
	}
	r, st := build(t, in, out.fakeOut, record(swap.Offered))
	r.Driver.Out = out
	r.Poll = time.Hour

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}
	final, err := st.Get(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != swap.Settled {
		t.Fatalf("ended in %v", final.State)
	}
}

// TestAPriceThatNoLongerCoversThePayoutGivesTheFundsBack: the quote was
// funded, but at the price now what the payer sent is worth less than what
// the bridge would pay out. Nothing is paid; the payer's HTLC is cancelled.
func TestAPriceThatNoLongerCoversThePayoutGivesTheFundsBack(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Funded))
	var asked int32
	r.PriceCheck = func(_ context.Context, sw *driver.Swap) (bool, error) {
		atomic.AddInt32(&asked, 1)
		if sw.Hash != hash {
			t.Errorf("asked about %x", sw.Hash)
		}

		return false, nil
	}

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Aborted {
		t.Fatalf("ended in %v", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 0 {
		t.Errorf("paid %d times at a loss", n)
	}
	if _, cancelled := in.counts(); cancelled != 1 {
		t.Errorf("cancelled %d times", cancelled)
	}
	if atomic.LoadInt32(&asked) == 0 {
		t.Error("the price was never checked")
	}
}

// TestAPriceThatCannotBeReadDecidesNothing: a price source that does not
// answer is a reason to wait, not to pay and not to cancel.
func TestAPriceThatCannotBeReadDecidesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(),
		200*time.Millisecond)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Funded))
	r.PriceCheck = func(context.Context, *driver.Swap) (bool, error) {
		return false, errors.New("the market could not be read")
	}

	if err := r.Drive(ctx, hash); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want the context to be what stopped it, got %v", err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Funded {
		t.Errorf("it moved to %v without a price", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 0 {
		t.Error("it paid without a price")
	}
	if _, cancelled := in.counts(); cancelled != 0 {
		t.Error("it cancelled without a price")
	}
}

// TestAPriceThatStillCoversThePayoutPays.
func TestAPriceThatStillCoversThePayoutPays(t *testing.T) {
	ctx, cancel := deadline(t)
	defer cancel()

	in := &fakeIn{
		invoice: node.Invoice{State: node.InvoiceAccepted, ExpiryHeight: 1000},
		height:  500,
	}
	out := &fakeOut{payment: node.Payment{
		State: node.PaymentSucceeded, Preimage: preimage,
	}}
	r, st := build(t, in, out, record(swap.Funded))
	r.PriceCheck = func(context.Context, *driver.Swap) (bool, error) {
		return true, nil
	}

	if err := r.Drive(ctx, hash); err != nil {
		t.Fatal(err)
	}

	final, _ := st.Get(context.Background(), hash)
	if final.State != swap.Settled {
		t.Fatalf("ended in %v", final.State)
	}
	if n := atomic.LoadInt32(&out.paid); n != 1 {
		t.Errorf("paid %d times", n)
	}
}

// TestAnOutgoingInvoiceExpiringDuringThePaymentIsNotPaid: an invoice that
// lapses while the payment is out is refused by the paying node, and the swap
// would wait in Paying for an answer that is not a payment. The gate gives the
// payer's funds back instead when the invoice does not outlive a whole attempt
// and ExpirySlack, and pays when it does.
func TestAnOutgoingInvoiceExpiringDuringThePaymentIsNotPaid(t *testing.T) {
	const timeout = 2 * time.Minute

	for _, c := range []struct {
		name string
		left time.Duration
		want swap.State
	}{
		{"already expired", -time.Minute, swap.Aborted},
		{"inside the attempt", timeout / 2, swap.Aborted},
		{"inside the slack", timeout + ExpirySlack - time.Second,
			swap.Aborted},
		{"outlives both", timeout + ExpirySlack, swap.Settled},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := deadline(t)
			defer cancel()

			in := &fakeIn{
				invoice: node.Invoice{State: node.InvoiceAccepted,
					ExpiryHeight: 1000},
				height: 500,
			}
			out := &fakeOut{payment: node.Payment{
				State: node.PaymentSucceeded, Preimage: preimage,
			}, expiry: now.Add(c.left)}
			r, st := build(t, in, out, record(swap.Funded))
			r.PayTimeout = timeout

			if err := r.Drive(ctx, hash); err != nil {
				t.Fatal(err)
			}

			final, _ := st.Get(context.Background(), hash)
			if final.State != c.want {
				t.Fatalf("ended in %v, want %v", final.State, c.want)
			}
			paid := atomic.LoadInt32(&out.paid)
			_, cancelled := in.counts()
			if c.want == swap.Aborted && (paid != 0 || cancelled != 1) {
				t.Errorf("paid %d times and cancelled %d, want no "+
					"payment and one cancel", paid, cancelled)
			}
		})
	}
}

// TestACancelAsksThePayingNodeFirst: before a failing or aborting swap gives
// its incoming HTLC back, the paying node must say the payment never started
// or failed. One that succeeded after all makes the swap paid and settles the
// claim; one still in flight keeps it, and the swap waits.
func TestACancelAsksThePayingNodeFirst(t *testing.T) {
	for _, c := range []struct {
		state  swap.State
		lookup node.Payment
		want   swap.State
	}{
		{swap.Failing, node.Payment{State: node.PaymentSucceeded,
			Preimage: preimage}, swap.Settled},
		{swap.Aborting, node.Payment{State: node.PaymentSucceeded,
			Preimage: preimage}, swap.Settled},
		{swap.Failing, node.Payment{State: node.PaymentInFlight},
			swap.Failing},
		{swap.Aborting, node.Payment{State: node.PaymentInFlight},
			swap.Aborting},
		{swap.Failing, node.Payment{State: node.PaymentFailed},
			swap.Failed},
		{swap.Aborting, node.Payment{State: node.PaymentNone},
			swap.Aborted},
	} {
		name := fmt.Sprintf("%v, payment %v", c.state, c.lookup.State)
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(),
				200*time.Millisecond)
			defer cancel()

			in := &fakeIn{
				invoice: node.Invoice{State: node.InvoiceAccepted,
					ExpiryHeight: 1000},
				height: 500,
			}
			out := &fakeOut{lookup: c.lookup}
			r, st := build(t, in, out, record(c.state))

			err := r.Drive(ctx, hash)
			if c.want.Terminal() && err != nil {
				t.Fatal(err)
			}

			final, _ := st.Get(context.Background(), hash)
			if final.State != c.want {
				t.Fatalf("ended in %v, want %v", final.State, c.want)
			}
			settled, cancelled := in.counts()
			switch c.want {
			case swap.Settled:
				if settled != 1 || cancelled != 0 {
					t.Errorf("settled %d, cancelled %d", settled,
						cancelled)
				}
				if final.Preimage != preimage {
					t.Error("the preimage was not recorded")
				}
			case swap.Failing, swap.Aborting:
				if cancelled != 0 {
					t.Error("cancelled while the payment was in flight")
				}
			}
		})
	}
}
