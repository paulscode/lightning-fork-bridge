// Package runner drives swaps to completion.
//
// Everything below it decides; this is the part that acts, repeatedly, without
// supervision. Two rules shape it.
//
// One goroutine per payment hash, ever. A swap's safety comes from a sequence:
// read the nodes, decide, record, act. Two goroutines interleaving that
// sequence on the same swap can gate against one reading and pay against
// another, and no amount of care further down survives it. The runner refuses
// to start a second driver for a hash it is already driving, and that refusal
// is tested by racing many starts at once.
//
// A swap the bridge cannot decide is left alone, loudly. Stopping costs an
// operator's attention; guessing costs the swap. Anything that reaches
// ErrNeedsOperator, or any state the machine has no move for, ends the loop
// with the record exactly as it was found.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

var (
	// ErrBusy is returned when a swap is already being driven. It is not a
	// failure: it is the guarantee that makes the rest safe.
	ErrBusy = errors.New("swap is already being driven")

	// ErrConfig is returned for a runner that cannot run.
	ErrConfig = errors.New("runner is not usable")
)

// DefaultFundedGrace is how long a funded swap may wait to be paid past its
// quote's expiry. Long enough to ride out a node restart, short enough that
// the price it pays at is still recent.
const DefaultFundedGrace = 10 * time.Minute

// DefaultPoll is how often the nodes are asked what changed.
//
// Polling rather than subscribing, because the thing being watched moves at
// the speed of blocks and HTLC forwards, and a poll loop has no stream to
// leak, reconnect, or silently stop delivering. A second is far inside the
// latency a payer notices and far outside the rate at which anything here
// changes.
const DefaultPoll = time.Second

// Rates supplies the block-spacing bounds the margin check needs, freshly
// measured. It is a function rather than a value because a quote minutes old
// was sized against a block rate the chain may since have left.
type Rates func(context.Context) (driver.Rates, error)

// PriceCheck decides whether a funded swap is still worth paying.
type PriceCheck func(context.Context, *driver.Swap) (bool, error)

// Runner drives swaps.
type Runner struct {
	Driver *driver.Driver
	Store  store.Store

	// Rates measures both chains. Required.
	Rates Rates

	// PriceCheck, when set, decides just before paying whether the swap is
	// still worth paying at the price now: false when what the payer sent
	// no longer covers the payout and its routing budget. A false gives the
	// payer's funds back, as a refused margin does; an error means the
	// price cannot be read, which waits, like a chain that cannot be
	// measured, and past the funded grace the swap is given back.
	//
	// The quote fixed what the payer owes, and a payer may fund it minutes
	// later. On a market that moves a few percent in that time the bridge
	// would otherwise pay out at a loss whenever the move went against it.
	PriceCheck PriceCheck

	// PayTimeout bounds one outgoing attempt. Zero means two minutes.
	PayTimeout time.Duration

	// FundedGrace is how long past its quote's expiry a funded swap may wait
	// to be paid before it is given back instead. Zero means
	// DefaultFundedGrace.
	//
	// A funded swap normally pays within seconds. One that cannot, because
	// a chain cannot be measured or a node is not answering, would
	// otherwise wait with the payer's HTLC locked for as long as the
	// condition lasts, and then pay at a price struck long before. Giving
	// it back is free for both sides: nothing has been sent.
	FundedGrace time.Duration

	// Poll overrides DefaultPoll.
	Poll time.Duration

	// NotSentPause is how long to wait after the paying node did not
	// receive a payment before trying again. Zero means DefaultNotSentPause.
	NotSentPause time.Duration

	// Now is the clock, for tests.
	Now func() time.Time

	// Log receives what happened. Nil means discard.
	Log *slog.Logger

	mu     sync.Mutex
	active map[node.Hash]struct{}
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}

	return time.Now()
}

func (r *Runner) fundedGrace() time.Duration {
	if r.FundedGrace > 0 {
		return r.FundedGrace
	}

	return DefaultFundedGrace
}

func (r *Runner) poll() time.Duration {
	if r.Poll > 0 {
		return r.Poll
	}

	return DefaultPoll
}

func (r *Runner) notSentPause() time.Duration {
	if r.NotSentPause > 0 {
		return r.NotSentPause
	}

	return DefaultNotSentPause
}

func (r *Runner) payTimeout() time.Duration {
	if r.PayTimeout > 0 {
		return r.PayTimeout
	}

	return 2 * time.Minute
}

func (r *Runner) log() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}

	return slog.New(slog.DiscardHandler)
}

// valid reports whether the runner has what it needs.
func (r *Runner) valid() error {
	switch {
	case r.Driver == nil:
		return fmt.Errorf("%w: no driver", ErrConfig)
	case r.Store == nil:
		return fmt.Errorf("%w: no store", ErrConfig)
	case r.Rates == nil:
		return fmt.Errorf("%w: no rate source; the margin check cannot run "+
			"against bounds nobody measured", ErrConfig)
	}

	return nil
}

// claim takes exclusive charge of a hash, or reports that someone else has it.
func (r *Runner) claim(hash node.Hash) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.active == nil {
		r.active = map[node.Hash]struct{}{}
	}
	if _, taken := r.active[hash]; taken {
		return fmt.Errorf("%w: %x", ErrBusy, hash)
	}
	r.active[hash] = struct{}{}

	return nil
}

func (r *Runner) release(hash node.Hash) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.active, hash)
}

// Active reports how many swaps are being driven.
func (r *Runner) Active() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.active)
}

// Drive runs one swap until it reaches a terminal state, the context ends, or
// the bridge finds something it must not decide alone.
//
// Returning early is safe at every point: the record is always at least as far
// along as the nodes are, so a later Drive, or a restart, picks up from it.
func (r *Runner) Drive(ctx context.Context, hash node.Hash) error {
	if err := r.valid(); err != nil {
		return err
	}
	if err := r.claim(hash); err != nil {
		return err
	}
	defer r.release(hash)

	rec, err := r.Store.Get(ctx, hash)
	if err != nil {
		return fmt.Errorf("loading %x: %w", hash, err)
	}
	sw := rec

	log := r.log().With("swap", fmt.Sprintf("%x", hash[:8]))
	log.Info("driving", "state", sw.State.String())

	ticker := time.NewTicker(r.poll())
	defer ticker.Stop()

	var failures int
	for {
		if sw.State.Terminal() {
			log.Info("finished", "state", sw.State.String())

			return nil
		}

		moved, err := r.step(ctx, &sw, log)
		switch {
		case err == nil:
			failures = 0

		case fatal(err):
			log.Error("stopped", "state", sw.State.String(), "err", err)

			return err

		default:
			// A node that did not answer, a rate that could not be
			// measured. None of these say anything about the swap, and
			// giving up on one would abandon a live HTLC over a blip.
			// Wait and ask again.
			failures++
			if failures >= loudAfter {
				log.Error("still failing", "state", sw.State.String(),
					"attempts", failures, "err", err)
			} else {
				log.Warn("retrying", "state", sw.State.String(), "err", err)
			}
			moved = false
		}
		if moved {
			// Something changed, so look again immediately rather than
			// waiting out a tick: the next state may have work of its own,
			// and a funded swap has an expiry running.
			continue
		}

		// A paying node that is not taking payments is usually being
		// restarted, which takes seconds to minutes. Asking every tick
		// would write the swap back and forth through Paying each time
		// for nothing; a few seconds is nothing against an HTLC's
		// deadline, and the gate still decides every attempt.
		wait := ticker.C
		if errors.Is(err, errNotSent) {
			wait = time.After(r.notSentPause())
		}
		select {
		case <-ctx.Done():
			log.Info("interrupted", "state", sw.State.String())

			return ctx.Err()
		case <-wait:
		}
	}
}

// step advances a swap by at most one transition. It reports whether anything
// moved.
func (r *Runner) step(ctx context.Context, sw *driver.Swap,
	log *slog.Logger) (bool, error) {

	// What the nodes say happened takes precedence over anything the bridge
	// might decide, because it is fact rather than judgement.
	event, has, err := r.Driver.Resume(ctx, sw)
	if err != nil {
		return false, err
	}
	if has {
		action, err := r.apply(ctx, sw, event, log)
		if err != nil {
			return false, err
		}

		return true, r.carryOut(ctx, sw, action, log)
	}

	switch sw.State {
	case swap.Quoted, swap.Offered:
		// Only the unfunded states can lapse on price. Once an HTLC is
		// locked in the question is blocks, not minutes, and the margin
		// check is what answers it.
		// A zero expiry means the quote has no deadline of its own. Such a
		// swap still ends: the hold invoice expires on the node and comes
		// back as IncomingLapsed.
		if !sw.QuoteExpiry.IsZero() && !r.now().Before(sw.QuoteExpiry) {
			log.Info("quote expired", "at", sw.QuoteExpiry)

			action, err := r.apply(ctx, sw, swap.QuoteExpired, log)
			if err != nil {
				return false, err
			}

			return true, r.carryOut(ctx, sw, action, log)
		}

		return false, nil

	case swap.Funded:
		// Before the gate, and whether or not the gate could run: the
		// case this exists for is the one where it cannot.
		if !sw.QuoteExpiry.IsZero() &&
			!r.now().Before(sw.QuoteExpiry.Add(r.fundedGrace())) {

			log.Warn("funded too long without paying; giving it back",
				"quote_expiry", sw.QuoteExpiry)

			action, err := r.apply(ctx, sw, swap.QuoteExpired, log)
			if err != nil {
				return false, err
			}

			return true, r.carryOut(ctx, sw, action, log)
		}

		return r.gate(ctx, sw, log)

	case swap.Paying:
		// Resume already asked the node and had no answer, so the payment
		// is still out there. Nothing to do but wait, and above all not to
		// cancel.
		return false, nil

	case swap.Paid, swap.Failing, swap.Aborting, swap.Expiring:
		// Each was told to do something
		// and has not seen it confirmed. Every such action is idempotent,
		// so reissuing costs nothing and covers a crash between issuing and
		// confirming.
		action, ok := swap.Recoverable(sw.State)
		if !ok {
			return false, nil
		}

		return false, r.Driver.Do(ctx, sw, action)

	default:
		// A state this build does not know how to advance. Waiting is
		// wrong and guessing is worse, so stop and say so.
		return false, fmt.Errorf("%w: no way to advance %v",
			driver.ErrNeedsOperator, sw.State)
	}
}

// loudAfter is how many consecutive transient failures pass before the log
// stops treating them as noise.
const loudAfter = 30

// fatal reports whether an error means this swap must stop being driven.
//
// The split matters because the two kinds of failure want opposite responses.
// A node that did not answer says nothing about the swap and must be retried,
// because abandoning a live HTLC over a blip is how one expires. An
// inconsistency, or anything the bridge has said it must not decide alone,
// must not be retried at all: repeating it would either do nothing forever or
// eventually do the wrong thing.
func fatal(err error) bool {
	switch {
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		// The caller is shutting down. The record is already at least as
		// far along as the nodes are, so stopping here is safe.
		return true

	case errors.Is(err, driver.ErrNeedsOperator):
		// The bridge has said it must not decide this alone.
		return true

	case errors.Is(err, driver.ErrState):
		// Something is inconsistent: a settle with the wrong preimage, a
		// pay from a state that forbids it. Retrying cannot fix it.
		return true

	case errors.Is(err, driver.ErrNoStore),
		errors.Is(err, store.ErrRecord),
		errors.Is(err, store.ErrCorrupt),
		errors.Is(err, ErrConfig):
		// The bridge cannot record what it does, so it must not do
		// anything.
		return true
	}

	return false
}

// gate decides a funded swap and, if it passes, pays.
func (r *Runner) gate(ctx context.Context, sw *driver.Swap,
	log *slog.Logger) (bool, error) {

	rates, err := r.Rates(ctx)
	if err != nil {
		// No usable measurement is a reason to wait, not to pay and not to
		// cancel: the HTLC is still claimable and the margin may well hold
		// once the measurement comes back.
		return false, fmt.Errorf("measuring block spacing: %w", err)
	}

	if r.PriceCheck != nil {
		ok, err := r.PriceCheck(ctx, sw)
		if err != nil {
			// Waiting, not refusing: a price source that failed to
			// answer says nothing about this swap, and the funded
			// grace gives it back if the answer never comes.
			return false, fmt.Errorf("checking the price: %w", err)
		}
		if !ok {
			log.Info("gated", "decision", "price refused")
			action, err := r.apply(ctx, sw, swap.MarginRefused, log)
			if err != nil {
				return false, err
			}

			return true, r.carryOut(ctx, sw, action, log)
		}
	}

	// The outgoing invoice has to stay payable for the whole attempt. One
	// that lapses while the payment is out is refused by the paying node,
	// and the swap would wait in Paying on an answer that is not a
	// payment; refusing now gives the payer's funds back instead.
	if ok, err := r.payable(ctx, sw); err != nil {
		// Waiting, as for the price: a node that could not read the
		// invoice says nothing about it, and the funded grace gives the
		// swap back if it never can.
		return false, fmt.Errorf("reading the outgoing invoice: %w", err)
	} else if !ok {
		log.Info("gated", "decision", "outgoing invoice expires too soon")
		action, err := r.apply(ctx, sw, swap.MarginRefused, log)
		if err != nil {
			return false, err
		}

		return true, r.carryOut(ctx, sw, action, log)
	}

	event, err := r.Driver.Gate(ctx, sw, rates)
	if err != nil {
		// Gate returns MarginRefused alongside its errors, but acting on a
		// refusal the bridge could not actually compute would cancel a
		// claim on the strength of a node that failed to answer.
		return false, fmt.Errorf("gate: %w", err)
	}
	log.Info("gated", "decision", event.String())

	action, err := r.apply(ctx, sw, event, log)
	if err != nil {
		return false, err
	}
	if action != swap.PayOutgoing {
		return true, r.carryOut(ctx, sw, action, log)
	}

	if err := r.pay(ctx, sw, log); err != nil {
		// The record already says paying, which is the whole point: the
		// swap is safe where it is and the next pass asks the node what
		// became of the attempt.
		return true, err
	}

	return true, nil
}

// ExpirySlack is how long past the end of a payment attempt the outgoing
// invoice must still be payable for the attempt to start: the attempt's own
// timeout bounds it, and this covers the clocks of the bridge, the paying node
// and the invoice's issuer disagreeing by a little.
const ExpirySlack = time.Minute

// payable reports whether the outgoing invoice stays payable for a whole
// attempt plus ExpirySlack, read by the node that will pay it. An invoice with
// no readable expiry is not: the quote refused those, so one here means
// something changed, and paying against it would be the permissive reading.
func (r *Runner) payable(ctx context.Context, sw *driver.Swap) (bool, error) {
	if r.Driver.Out == nil {
		return false, fmt.Errorf("%w: no outgoing node", ErrConfig)
	}
	dec, err := r.Driver.Out.Decode(ctx, sw.Invoice)
	if err != nil {
		return false, err
	}
	if dec.Expiry.IsZero() {
		return false, nil
	}

	return dec.Expiry.Sub(r.now()) >= r.payTimeout()+ExpirySlack, nil
}

// pay sends the outgoing leg. By the time it is called the record already says
// the swap is paying, which is what makes a crash here recoverable.
func (r *Runner) pay(ctx context.Context, sw *driver.Swap,
	log *slog.Logger) error {

	log.Info("paying", "msat", sw.OutgoingMsat, "cltv", sw.OutgoingCLTVLimit)

	event, has, err := r.Driver.Pay(ctx, sw, node.PayRequest{
		Invoice:    sw.Invoice,
		MaxFeeMsat: sw.MaxFeeMsat,
		CLTVLimit:  sw.OutgoingCLTVLimit,
		Timeout:    r.payTimeout(),
	})
	if err != nil {
		// The attempt could not be made, or could not be concluded. Either
		// way the swap stays in Paying, which is the state that forbids
		// cancelling the incoming claim, and the next pass asks the node
		// what actually happened.
		return fmt.Errorf("paying: %w", err)
	}
	if !has {
		log.Info("payment in flight")

		return nil
	}

	action, err := r.apply(ctx, sw, event, log)
	if err != nil {
		return err
	}
	if event == swap.OutgoingNotSent {
		// Funded again. Reported as a transient failure rather than as
		// progress, so the swap is gated and paid again at the retry
		// pace and not at once, which against a node that is still down
		// would spin.
		return errNotSent
	}

	return r.carryOut(ctx, sw, action, log)
}

// DefaultNotSentPause is NotSentPause when none is set: a paying node that is
// not taking payments is usually restarting, which takes seconds to minutes.
const DefaultNotSentPause = 5 * time.Second

// errNotSent is why a swap is funded again after an attempt to pay: the paying
// node did not receive it. Transient, so the swap is retried, after
// NotSentPause.
var errNotSent = errors.New("the paying node did not receive the payment, " +
	"so nothing was sent; the swap is funded again and will be paid when " +
	"it answers")

// apply records a transition and reports the action it authorises. The record
// is durable by the time it returns, which is what makes acting on the action
// safe.
func (r *Runner) apply(ctx context.Context, sw *driver.Swap, event swap.Event,
	log *slog.Logger) (swap.Action, error) {

	before := sw.State
	action, err := r.Driver.Apply(ctx, sw, event)
	if err != nil {
		if errors.Is(err, swap.ErrTransition) {
			// A duplicate notification, which is ordinary. The record is
			// already past it.
			log.Debug("ignored", "event", event.String(), "state",
				before.String())

			return swap.Nothing, nil
		}

		return swap.Nothing, err
	}
	log.Info("moved", "from", before.String(), "to", sw.State.String(),
		"on", event.String(), "action", action.String())

	if sw.State == swap.Lost {
		// The one outcome that costs the bridge money. The margin policy
		// exists to make it unreachable, so reaching it means an assumption
		// was wrong and the balance is the smaller half of the problem.
		log.Error("swap lost: paid out and could not claim",
			"msat", sw.IncomingMsat, "hash", fmt.Sprintf("%x", sw.Hash))
	}

	return action, nil
}

// carryOut performs an action that is not a payment.
//
// Paying is excluded deliberately rather than by convention: it is the one
// action whose outcome has to be recorded, so it cannot be fire and forget,
// and a caller that handed it here by mistake would skip the payment in
// silence and leave the swap sitting in Paying forever. Saying so out loud
// costs an operator's attention once; the silent version costs a swap.
func (r *Runner) carryOut(ctx context.Context, sw *driver.Swap,
	action swap.Action, log *slog.Logger) error {

	switch action {
	case swap.Nothing:
		return nil

	case swap.PayOutgoing:
		return fmt.Errorf("%w: reached %v outside the gate, where there is "+
			"no request to build it from", driver.ErrState, action)

	default:
		return r.Driver.Do(ctx, sw, action)
	}
}

// Resume picks up every unfinished swap after a restart and drives each to
// completion, concurrently.
//
// One swap that cannot be decided does not stop the others. A bridge coming
// back up may hold several HTLCs against expiry deadlines it did not choose,
// and refusing to touch any of them because one is ambiguous would turn a
// single stuck swap into several expired ones. Every failure is collected and
// returned together.
func (r *Runner) Resume(ctx context.Context) error {
	if err := r.valid(); err != nil {
		return err
	}

	pending, err := r.Store.Pending(ctx)
	if err != nil {
		return fmt.Errorf("reading pending swaps: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	r.log().Info("resuming", "swaps", len(pending))

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		bad  []error
		done int
	)
	for _, rec := range pending {
		wg.Add(1)
		go func(hash node.Hash) {
			defer wg.Done()

			err := r.Drive(ctx, hash)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				bad = append(bad, fmt.Errorf("%x: %w", hash, err))

				return
			}
			done++
		}(rec.Hash)
	}
	wg.Wait()

	r.log().Info("resumed", "finished", done, "stuck", len(bad))

	return errors.Join(bad...)
}
