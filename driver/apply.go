package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// ErrNoStore is returned when a driver that needs the durable record does not
// have one.
var ErrNoStore = errors.New("driver has no store")

// Apply steps the state machine and makes the new state durable before it
// hands back the action that state authorises.
//
// That order is the whole point. The action that matters is PayOutgoing, and
// the failure it guards against is this: the payment leaves, the process dies
// before anything is written, and the record still says the swap was merely
// funded. Recovery for a funded swap is to cancel the incoming HTLC, so the
// bridge would give up its claim on the money coming in while the money going
// out had already gone. Writing first converts that into the opposite and much
// milder problem, a swap recorded as paying whose payment never left, which
// costs an operator's attention rather than the amount of the swap.
//
// An error means nothing was recorded and the caller must not act. The
// in-memory record is rolled back so that it still matches the disk.
func (d *Driver) Apply(ctx context.Context, sw *Swap, e swap.Event) (swap.Action,
	error) {

	if d.Store == nil {
		return swap.Nothing, fmt.Errorf("%w: refusing to step %v on %v",
			ErrNoStore, e, sw.State)
	}

	next, action, err := swap.Step(sw.State, e)
	if err != nil {
		return swap.Nothing, err
	}

	before := *sw
	sw.State = next
	sw.Updated = d.now()
	switch next {
	case swap.Offered:
		// The hold invoice exists, and could be funded.
		sw.NoInvoice = false
	case swap.Paying:
		// From this write on a payment may have left, and every cancel
		// has to ask the paying node first.
		sw.NoPayAttempt = false
	}
	if sw.Created.IsZero() {
		sw.Created = sw.Updated
	}

	if err := d.Store.Put(ctx, *sw); err != nil {
		*sw = before

		return swap.Nothing, fmt.Errorf("recording %v -> %v: %w", before.State,
			next, err)
	}

	return action, nil
}

// Recovery is what a restart concluded about one swap.
type Recovery struct {
	// Swap is the record as it was found, with the preimage filled in if
	// looking the outgoing payment up produced one.
	Swap Swap

	// Event is what the nodes say happened while the bridge was down, and
	// HasEvent whether there was one. Apply it before acting.
	Event    swap.Event
	HasEvent bool

	// Action is what to reissue when there is no event: the swap was told to
	// do something and never saw it confirmed. Every such action is
	// idempotent, so reissuing is safe.
	//
	// It is swap.Nothing for a funded swap, which needs Gate run again
	// rather than an action reissued, and for one that could not be decided
	// at all. Check Err before treating Nothing as "leave it alone".
	Action swap.Action

	// Err is why this swap could not be decided. A swap with an error is left
	// exactly as it was found and needs a human.
	Err error
}

// Recover works through every unfinished swap after a restart.
//
// One swap that cannot be decided does not stop the others. A bridge coming
// back up may have several HTLCs against expiry deadlines it did not choose,
// and refusing to touch any of them because one is ambiguous would turn a
// single stuck swap into several expired ones.
func (d *Driver) Recover(ctx context.Context) ([]Recovery, error) {
	if d.Store == nil {
		return nil, ErrNoStore
	}

	pending, err := d.Store.Pending(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading pending swaps: %w", err)
	}

	out := make([]Recovery, 0, len(pending))
	for _, rec := range pending {
		sw := rec
		r := Recovery{Swap: sw}

		event, has, err := d.Resume(ctx, &sw)
		r.Swap = sw
		switch {
		case err != nil:
			r.Err = err

		case has:
			r.Event, r.HasEvent = event, true

		default:
			// Nothing changed while the bridge was down, so whatever it was
			// last told to do still needs doing.
			r.Action, _ = swap.Recoverable(sw.State)
		}
		out = append(out, r)
	}

	return out, nil
}

// Load reads a swap from the durable record.
func (d *Driver) Load(ctx context.Context, sw *Swap) (store.Record, error) {
	if d.Store == nil {
		return store.Record{}, ErrNoStore
	}

	return d.Store.Get(ctx, sw.Hash)
}
