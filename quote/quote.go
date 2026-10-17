// Package quote turns "pay this invoice" into a swap, or into a refusal.
//
// It is where a swap acquires its terms, and therefore where most of the ways
// a bridge can lose money quietly are ruled out: an invoice that expires
// before the incoming leg could be funded, a routing budget wider than the
// spread it is paid out of, an incoming amount rounded in the payer's favour
// on every swap, a second hold invoice created for a hash that already has
// one.
//
// It never chooses the payment hash. The hash comes from the invoice the user
// asked to have paid, read by the node that will pay it, and the incoming hold
// invoice is built on that exact value. That is the whole mechanism: one
// preimage claims both legs.
package quote

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/rate"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

var (
	// ErrRefused is returned when the bridge will not quote this swap. It is
	// an ordinary outcome, not a fault.
	ErrRefused = errors.New("bridge will not quote this swap")

	// ErrDuplicate is returned when a swap already exists for a payment hash.
	ErrDuplicate = errors.New("a swap already exists for this payment hash")

	// ErrPolicy is returned for a quoting policy that cannot be used.
	ErrPolicy = errors.New("quote policy is not usable")
)

// Policy is the terms the bridge is willing to offer.
type Policy struct {
	// MinSwapMsat and MaxSwapMsat bound the outgoing leg, in millisatoshis of
	// the outgoing chain. That is the side the bridge's own money leaves
	// from, so it is the side a cap has to be expressed in: the maximum comes
	// from the capital model, where the per-swap cap is the inventory band
	// over ten, and no single swap may take out more than a tenth of what is
	// at risk.
	MinSwapMsat uint64
	MaxSwapMsat uint64

	// Validity is how long a quoted rate is honoured.
	Validity time.Duration

	// InvoiceExpiry is how long the hold invoice lives. It must be at least
	// Validity, or a quote could outlive the invoice it is for.
	InvoiceExpiry time.Duration

	// OutgoingCLTVLimit is the CLTV budget imposed on the outgoing route, in
	// blocks of the outgoing chain. Everything the incoming leg must outlive
	// follows from this, so smaller is cheaper.
	OutgoingCLTVLimit uint32

	// FeeFraction is the routing budget for the outgoing leg, as a fraction
	// of its amount.
	FeeFraction float64

	// MinFeeMsat is the least routing budget, whatever the fraction gives:
	// a small payout's fraction is less than one hop's base fee, so it
	// could not be routed at all. A payout whose spread does not cover
	// this is refused as too small.
	MinFeeMsat uint64

	// MinRemainingLife is how long the outgoing invoice must still be payable
	// for. A quote on an invoice about to expire sells something the bridge
	// may not be able to deliver.
	MinRemainingLife time.Duration
}

// DefaultPolicy is a starting point sized for the design's Stage 1: a $2-3k
// experiment budget gives a band of about $1,500 and a per-swap cap near $150,
// which at the measured BTCB2 price is a few thousandths of a BTC.
var DefaultPolicy = Policy{
	MinSwapMsat:       1_000_000,   // 1,000 sats
	MaxSwapMsat:       150_000_000, // 150,000 sats
	Validity:          2 * time.Minute,
	InvoiceExpiry:     10 * time.Minute,
	OutgoingCLTVLimit: 80,
	FeeFraction:       0.003,
	MinFeeMsat:        10_000, // 10 sats
	MinRemainingLife:  15 * time.Minute,
}

// Valid reports whether the policy can be used.
func (p Policy) Valid() error {
	switch {
	case p.MinSwapMsat == 0 || p.MaxSwapMsat < p.MinSwapMsat:
		return fmt.Errorf("%w: swap bounds %d..%d", ErrPolicy, p.MinSwapMsat,
			p.MaxSwapMsat)
	case p.Validity <= 0:
		return fmt.Errorf("%w: no quote validity", ErrPolicy)
	case p.InvoiceExpiry < p.Validity:
		return fmt.Errorf("%w: an invoice living %v cannot carry a quote "+
			"valid for %v", ErrPolicy, p.InvoiceExpiry, p.Validity)
	case p.OutgoingCLTVLimit == 0:
		return fmt.Errorf("%w: no outgoing CLTV limit", ErrPolicy)
	case p.FeeFraction < 0:
		return fmt.Errorf("%w: negative fee fraction", ErrPolicy)
	case p.MinRemainingLife < p.Validity:
		return fmt.Errorf("%w: quoting for %v against invoices with %v left",
			ErrPolicy, p.Validity, p.MinRemainingLife)
	}

	return nil
}

// Quote is what the bridge offers.
type Quote struct {
	// Hash is the payment hash of the invoice being paid, and of the hold
	// invoice created to fund it.
	Hash node.Hash

	// HoldInvoice is what the payer pays, on the incoming chain.
	HoldInvoice string

	// Outgoing is the invoice the bridge will pay with the proceeds, exactly
	// as it was supplied. The bridge never rewrites it.
	Outgoing string

	// IncomingMsat is what the payer owes, OutgoingMsat what the destination
	// receives. They are in different units.
	IncomingMsat uint64
	OutgoingMsat uint64

	// Rate is outgoing units per incoming unit, and Spread the fraction
	// charged over it.
	Rate   float64
	Spread float64

	// MaxFeeMsat is the routing budget, which comes out of the spread.
	MaxFeeMsat uint64

	// OutgoingCLTVLimit is the CLTV the outgoing route may use, in blocks of
	// the outgoing chain: the route the paying node found, with RouteSlack,
	// or the policy's whole budget when it cannot be asked.
	OutgoingCLTVLimit uint32

	// CLTVDelta is what the payer must leave on the final hop, in blocks of
	// the incoming chain.
	CLTVDelta uint32

	// Expires is when the rate stops being honoured.
	Expires time.Time

	// Description is the outgoing invoice's own, shown back to the payer.
	Description string
}

// RouteSlack is added to the CLTV of the route a paying node found, for the
// swap's limit: a route that changes a little between the quote and the
// payment still fits. Payment along a longer one fails, and the payer's funds
// come back.
const RouteSlack = 40

// Pricer supplies the rate to quote at, or a refusal.
type Pricer func(ctx context.Context) (rate.Reading, error)

// Headroom supplies how much of the outgoing asset may still be committed:
// the node's balance, less any reserve, less everything already promised to
// swaps that have not finished. See store.Committed for the second part, which
// is the one that is easy to forget and expensive to forget.
type Headroom func(ctx context.Context) (uint64, error)

// Rates supplies both chains' block spacing, freshly measured. Both are
// needed: the incoming CLTV is sized by comparing how long the outgoing leg
// can take against how fast the incoming chain produces blocks, and using one
// chain's spacing for both would size a swap against a chain it is not on.
type Rates func(ctx context.Context) (driver.Rates, error)

// Quoter makes quotes. Use it by pointer: it holds a lock, so a copy would
// serialise against nothing.
type Quoter struct {
	In    node.Incoming
	Out   node.Outgoing
	Store store.Store

	// Price is the rate oracle, Spacing both chains' block spacing, and Room
	// what the bridge can still commit. All three are required: a quote made
	// without any one of them is a promise the bridge has no grounds for.
	Price   Pricer
	Spacing Rates
	Room    Headroom

	// Margin is the HTLC timing policy the incoming CLTV is sized against.
	Margin margin.Policy

	// Policy is the terms offered.
	Policy Policy

	// PaysBLAKE2b is whether Out pays on the BLAKE2b chain. An invoice is
	// quoted only if it is for the chain Out pays on, which it says by
	// setting option_blake2b or not. Both chains' invoices share a prefix
	// and both nodes can read every one, so without this a BLAKE2b invoice
	// could be quoted for payment on Bitcoin, where it can never arrive.
	PaysBLAKE2b bool

	// Own is the bridge's own node keys, hex. An invoice payable to one of
	// them is refused: the bridge would be paying itself through its own
	// hold invoice, which moves nothing and ties up both sides.
	Own []string

	// Limits bounds each participant on their own. The operator's own
	// swaps, which carry no participant, are not limited.
	Limits Limits

	// Now is the clock, for tests.
	Now func() time.Time

	mu      sync.Mutex
	quoting map[node.Hash]struct{}

	// whoMu guards who and recent.
	whoMu sync.Mutex
	// who holds one lock per participant, taken for the length of a quote,
	// so two quotes from one participant cannot both pass a limit that only
	// one of them fits under.
	who map[string]*sync.Mutex
	// recent is when each participant's last quotes were asked for, for the
	// per-minute limit. In memory only: a restart forgetting it lets a
	// participant quote again sooner, which the open-quote limit, kept in
	// the journal, still bounds.
	recent map[string][]time.Time
}

// Limits is what one participant may hold. A zero field means no limit.
type Limits struct {
	// MaxUnpaid is how many quotes they may have unpaid at once. This is
	// the one that matters most: an unpaid quote reserves the bridge's
	// liquidity and costs the participant nothing.
	MaxUnpaid int

	// MaxInProgress is how many swaps they may have unfinished at once,
	// paid or not. With the policy's per-swap maximum it bounds the value
	// one participant can have outstanding.
	MaxInProgress int

	// PerMinute is how many quotes they may ask for in any minute.
	PerMinute int
}

// DefaultLimits suits a small group of named participants.
var DefaultLimits = Limits{MaxUnpaid: 2, MaxInProgress: 4, PerMinute: 6}

// enter takes a participant's lock and checks their limits. The returned
// function releases the lock. An empty participant is the operator, who is
// not limited.
func (q *Quoter) enter(ctx context.Context, who string,
	now time.Time) (func(), error) {

	if who == "" {
		return func() {}, nil
	}

	q.whoMu.Lock()
	if q.who == nil {
		q.who = map[string]*sync.Mutex{}
		q.recent = map[string][]time.Time{}
	}
	mu, ok := q.who[who]
	if !ok {
		mu = &sync.Mutex{}
		q.who[who] = mu
	}
	q.whoMu.Unlock()

	mu.Lock()
	release := mu.Unlock

	if n := q.Limits.PerMinute; n > 0 {
		q.whoMu.Lock()
		kept := q.recent[who][:0]
		for _, t := range q.recent[who] {
			if now.Sub(t) < time.Minute {
				kept = append(kept, t)
			}
		}
		q.recent[who] = kept
		over := len(kept) >= n
		if !over {
			q.recent[who] = append(q.recent[who], now)
		}
		q.whoMu.Unlock()

		if over {
			release()

			return nil, fmt.Errorf("%w: %w: %d quotes in the last "+
				"minute", ErrRefused, ErrLimit, n)
		}
	}

	if q.Limits.MaxUnpaid > 0 || q.Limits.MaxInProgress > 0 {
		pending, err := q.Store.Pending(ctx)
		if err != nil {
			release()

			return nil, fmt.Errorf("reading pending swaps: %w", err)
		}
		var unpaid, open int
		for _, r := range pending {
			if r.Participant != who {
				continue
			}
			open++
			if r.State == swap.Quoted || r.State == swap.Offered {
				unpaid++
			}
		}
		if n := q.Limits.MaxUnpaid; n > 0 && unpaid >= n {
			release()

			return nil, fmt.Errorf("%w: %w: %d quotes unpaid; pay or "+
				"let one expire first", ErrRefused, ErrLimit, unpaid)
		}
		if n := q.Limits.MaxInProgress; n > 0 && open >= n {
			release()

			return nil, fmt.Errorf("%w: %w: %d swaps in progress",
				ErrRefused, ErrLimit, open)
		}
	}

	return release, nil
}

// begin takes exclusive charge of a payment hash for the length of one quote.
//
// Without it two callers can both pass the duplicate check before either has
// written anything, and the second one's record overwrites the first's terms:
// same swap, different amount, different rate, and a hold invoice already out
// with a payer for the amount that was replaced. The node would refuse the
// second invoice for a hash it already has, so the damage is to the record
// rather than to the money, but a record that disagrees with the invoice a
// payer holds is exactly what the rest of this repository is built to avoid.
//
// This assumes one process. That is the deployment model, and the store would
// need a compare-and-set for anything else.
func (q *Quoter) begin(hash node.Hash) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.quoting == nil {
		q.quoting = map[node.Hash]struct{}{}
	}
	if _, taken := q.quoting[hash]; taken {
		return fmt.Errorf("%w: %x is being quoted now", ErrDuplicate, hash)
	}
	q.quoting[hash] = struct{}{}

	return nil
}

func (q *Quoter) end(hash node.Hash) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.quoting, hash)
}

func (q *Quoter) now() time.Time {
	if q.Now != nil {
		return q.Now()
	}

	return time.Now()
}

// Quote prices a swap for the operator, who is not limited. See QuoteFor.
func (q *Quoter) Quote(ctx context.Context, invoice string) (Quote, error) {
	return q.QuoteFor(ctx, "", invoice)
}

// QuoteFor prices a swap, creates the hold invoice, and records it, on behalf
// of a participant whose limits apply.
//
// On success the swap is in swap.Offered and ready for the runner. On any
// refusal nothing has been created on either chain, except where the error
// is ErrUntracked, which the caller must act on.
func (q *Quoter) QuoteFor(ctx context.Context, who, invoice string) (Quote,
	error) {

	if err := q.Policy.Valid(); err != nil {
		return Quote{}, err
	}
	if q.In == nil || q.Out == nil || q.Store == nil || q.Price == nil ||
		q.Spacing == nil || q.Room == nil {

		return Quote{}, fmt.Errorf("%w: quoter is missing a dependency",
			ErrPolicy)
	}

	now := q.now()

	// The paying node's own decoder, not a second one in this process: a
	// parser that disagreed by one field would have the bridge hold an
	// invoice for one hash while the node paid another.
	dec, err := q.Out.Decode(ctx, invoice)
	if err != nil {
		return Quote{}, fmt.Errorf("%w: %w: %w", ErrRefused,
			ErrInvalidInvoice, err)
	}

	if err := q.checkInvoice(dec, now); err != nil {
		return Quote{}, err
	}

	release, err := q.enter(ctx, who, now)
	if err != nil {
		return Quote{}, err
	}
	defer release()

	if err := q.begin(dec.Hash); err != nil {
		return Quote{}, err
	}
	defer q.end(dec.Hash)

	// A second hold invoice for a hash that already has a live one would let
	// the same preimage claim twice, and would leave two records fighting
	// over one swap. Refuse before anything is created. A swap that ended
	// before anyone funded it is different: see previous.
	forget, err := q.previous(ctx, dec.Hash)
	if err != nil {
		return Quote{}, err
	}

	// What the bridge can actually pay, which is not what its balance says:
	// every swap already quoted, offered or funded is a claim on the same
	// money. Checked after the duplicate guard so that the amount about to be
	// committed is not already counted in it.
	room, err := q.Room(ctx)
	if err != nil {
		return Quote{}, fmt.Errorf("%w: %w", ErrRefused, err)
	}
	if dec.AmountMsat > room {
		return Quote{}, fmt.Errorf("%w: %w: %d msat against %d msat of "+
			"headroom; the rest is promised to swaps that have not "+
			"finished", ErrRefused, ErrNoLiquidity, dec.AmountMsat, room)
	}

	reading, err := q.Price(ctx)
	if err != nil {
		return Quote{}, fmt.Errorf("%w: %w", ErrRefused, err)
	}

	terms, err := q.price(dec, reading)
	if err != nil {
		return Quote{}, err
	}
	terms.Outgoing = invoice

	spacing, err := q.Spacing(ctx)
	if err != nil {
		return Quote{}, fmt.Errorf("%w: %w", ErrRefused, err)
	}
	if spacing.As.IsZero() {
		return Quote{}, fmt.Errorf("%w: block spacing carries no measurement "+
			"time", ErrRefused)
	}

	// The route budget the chains allow now: the policy's, or less when the
	// incoming chain runs fast or the outgoing one slow. Asking for a route
	// within the policy's alone would find one that then cannot be quoted.
	budget, err := margin.MaxOutgoingBlocks(spacing.Incoming,
		spacing.Outgoing, q.Margin)
	if err != nil {
		return Quote{}, fmt.Errorf("%w: %w", ErrRefused, err)
	}
	if budget > q.Policy.OutgoingCLTVLimit {
		budget = q.Policy.OutgoingCLTVLimit
	}
	if budget == 0 {
		// Not even one block of route fits: the chains' pace, not this
		// invoice, refuses it, and that passes.
		return Quote{}, fmt.Errorf("%w: %w: the chains allow no route "+
			"budget now", ErrRefused, margin.ErrTooLong)
	}
	if budget <= dec.CLTVDelta {
		// checkInvoice already refused an invoice whose final hop fills
		// the policy's budget, so this is the chains' pace shrinking it:
		// it passes, and is said as such rather than as a fault of the
		// invoice.
		return Quote{}, fmt.Errorf("%w: %w: the destination wants %d "+
			"blocks on the final hop and the chains allow %d for the "+
			"whole route now", ErrRefused, margin.ErrTooLong,
			dec.CLTVDelta, budget)
	}

	// A route to the destination within the budgets, asked of the paying
	// node before anything is created: a payer is otherwise held, briefly,
	// for a payment the bridge then finds it cannot make. The swap's CLTV
	// limit is the route's with slack, so a short route asks a short hold
	// of the payer rather than the whole budget.
	terms.OutgoingCLTVLimit = budget
	if finder, ok := q.Out.(node.RouteFinder); ok {
		route, err := finder.FindRoute(ctx, invoice, terms.MaxFeeMsat,
			budget)
		switch {
		case errors.Is(err, node.ErrNoRoute):
			return Quote{}, fmt.Errorf("%w: %w: within %d msat of fees "+
				"and %d blocks", ErrRefused, ErrNoRoute,
				terms.MaxFeeMsat, budget)

		case err != nil:
			return Quote{}, fmt.Errorf("%w: finding a route: %w",
				ErrRefused, err)
		}
		if limit := route.CLTV + RouteSlack; route.CLTV > 0 &&
			limit < terms.OutgoingCLTVLimit {

			terms.OutgoingCLTVLimit = limit
		}
	}

	blocks, err := margin.RequiredIncomingBlocks(spacing.Incoming, margin.Leg{
		Bounds: spacing.Outgoing,
		Blocks: terms.OutgoingCLTVLimit,
	}, q.Margin)
	if err != nil {
		return Quote{}, fmt.Errorf("%w: sizing the incoming CLTV: %w",
			ErrRefused, err)
	}
	terms.CLTVDelta = blocks
	terms.Expires = now.Add(q.Policy.Validity)

	// Last, once everything else has said yes, so that a refused quote
	// leaves the previous attempt's invoice where it was. Forgetting it
	// first and then refusing would be harmless, since it was cancelled,
	// but it would erase what an operator might want to look at.
	if forget {
		if err := q.In.ForgetInvoice(ctx, dec.Hash); err != nil {
			return Quote{}, fmt.Errorf("clearing the previous attempt's "+
				"cancelled invoice: %w", err)
		}
	}

	if err := q.create(ctx, &terms, who, now); err != nil {
		return Quote{}, err
	}

	return terms, nil
}

// previous decides what an earlier swap for the same payment hash means for a
// new quote. It reports whether the previous attempt's cancelled invoice must
// be deleted before a new one can be created.
//
// At most one swap per hash may be live. One that ended before anyone paid
// its hold invoice gave nothing away and holds nothing, so quoting again is
// safe and is the only way that invoice can ever be paid through this bridge:
// the node keeps the cancelled hold invoice and would refuse a second one for
// the same hash.
//
// One whose hold invoice was funded is different, however it ended. Failed
// and aborted swaps are only reachable through Funded, and an expired one
// whose cancelled invoice shows an HTLC was paid at the last moment. The
// payer's money came back in each case, but the bridge does not put a second
// invoice under a hash an earlier one was funded under: it refuses with
// ErrFundedBefore, and the recipient issues a new invoice.
//
// The node is asked as well as the journal. Compaction keeps each swap's
// latest record, finished or not, and this depends on that; but a journal
// started afresh, or a hash quoted by something else, leaves only the node's
// invoice to go on, and its HTLCs say whether it was funded.
func (q *Quoter) previous(ctx context.Context, hash node.Hash) (bool, error) {
	rec, err := q.Store.Get(ctx, hash)
	switch {
	case err == nil:
		switch rec.State {
		case swap.Expired:
			// Ended unpaid, as far as the record knows. The node is
			// still asked below, because the record could say expired
			// while the invoice was never cancelled, which must not be
			// deleted, or while it was funded as it was being cancelled.
		case swap.Failed, swap.Aborted:
			return false, fmt.Errorf("%w: %w: %w: %x was %v", ErrRefused,
				ErrDuplicate, ErrFundedBefore, hash, rec.State)
		case swap.Settled:
			return false, fmt.Errorf("%w: %w: %w: %x", ErrRefused,
				ErrDuplicate, ErrAlreadyPaid, hash)
		case swap.Lost:
			return false, fmt.Errorf("%w: %w: %w: %x", ErrRefused,
				ErrDuplicate, ErrNeedsOperator, hash)
		default:
			return false, fmt.Errorf("%w: %w: %w: %x is %v", ErrRefused,
				ErrDuplicate, ErrInProgress, hash, rec.State)
		}

	case errors.Is(err, store.ErrNotFound):

	default:
		return false, fmt.Errorf("checking for an existing swap: %w", err)
	}

	inv, err := q.In.LookupInvoice(ctx, hash)
	switch {
	case errors.Is(err, node.ErrUnknownHash):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("checking the node for an earlier "+
			"invoice: %w", err)
	}

	switch inv.State {
	case node.InvoiceCancelled:
		if inv.HTLCs > 0 {
			// Cancelled after a payer's HTLC was locked in: funded,
			// whatever the record says.
			return false, fmt.Errorf("%w: %w: %w: the node's cancelled "+
				"invoice for %x held %d HTLCs", ErrRefused, ErrDuplicate,
				ErrFundedBefore, hash, inv.HTLCs)
		}

		return true, nil
	case node.InvoiceSettled:
		return false, fmt.Errorf("%w: %w: %w: %x", ErrRefused, ErrDuplicate,
			ErrAlreadyPaid, hash)
	default:
		// Open or held, with no live record: nothing here created it, or
		// the journal lost it. Either way it is not this quote's to touch.
		return false, fmt.Errorf("%w: %w: %w: the node holds an invoice "+
			"for %x that is %v", ErrRefused, ErrDuplicate, ErrInProgress,
			hash, inv.State)
	}
}

// checkInvoice refuses what the bridge cannot safely deliver.
func (q *Quoter) checkInvoice(dec node.Decoded, now time.Time) error {
	var zero node.Hash
	if dec.Hash == zero {
		return fmt.Errorf("%w: the invoice carries no payment hash", ErrRefused)
	}
	if dec.BLAKE2b != q.PaysBLAKE2b {
		// Checked first: every other rule below is about paying this
		// invoice, and this one says it cannot be paid from here at all.
		return fmt.Errorf("%w: %w", ErrRefused, ErrWrongChain)
	}
	for _, own := range q.Own {
		if own != "" && strings.EqualFold(own, dec.Destination) {
			return fmt.Errorf("%w: %w", ErrRefused, ErrSelfPayment)
		}
	}
	if dec.AmountMsat == 0 {
		// An invoice naming no amount would have the bridge choose what to
		// send, and price the incoming leg against its own guess.
		return fmt.Errorf("%w: %w", ErrRefused, ErrNoAmount)
	}
	if dec.AmountMsat < q.Policy.MinSwapMsat {
		return fmt.Errorf("%w: %w: %d msat is outside %d..%d", ErrRefused,
			ErrTooSmall, dec.AmountMsat, q.Policy.MinSwapMsat,
			q.Policy.MaxSwapMsat)
	}
	if dec.AmountMsat > q.Policy.MaxSwapMsat {
		return fmt.Errorf("%w: %w: %d msat is outside %d..%d", ErrRefused,
			ErrTooLarge, dec.AmountMsat, q.Policy.MinSwapMsat,
			q.Policy.MaxSwapMsat)
	}
	if dec.Expiry.IsZero() {
		// Every well-formed invoice carries a timestamp, so no expiry means
		// something could not be read. Skipping the check instead would be
		// the permissive reading of a malformed invoice.
		return fmt.Errorf("%w: the invoice has no readable expiry", ErrRefused)
	}
	if dec.Expiry.Sub(now) < q.Policy.MinRemainingLife {
		// Quoting against an invoice about to expire sells something the
		// bridge may not be able to deliver: the payer funds the incoming
		// leg and the outgoing one can no longer be paid.
		return fmt.Errorf("%w: %w: the invoice has %v left, less than the "+
			"%v a quote needs", ErrRefused, ErrExpiresSoon,
			dec.Expiry.Sub(now).Round(time.Second), q.Policy.MinRemainingLife)
	}
	if dec.CLTVDelta >= q.Policy.OutgoingCLTVLimit {
		// The destination's own final-hop delta already fills the budget,
		// leaving no blocks for the route in between.
		return fmt.Errorf("%w: %w: the destination wants %d blocks on the "+
			"final hop and the whole route budget is %d", ErrRefused,
			ErrRouteBudget, dec.CLTVDelta, q.Policy.OutgoingCLTVLimit)
	}

	return nil
}

// price works out what the payer owes.
func (q *Quoter) price(dec node.Decoded, r rate.Reading) (Quote, error) {
	if r.OutgoingPerIncoming <= 0 || math.IsNaN(r.OutgoingPerIncoming) ||
		math.IsInf(r.OutgoingPerIncoming, 0) {

		return Quote{}, fmt.Errorf("%w: unusable rate %g", ErrRefused,
			r.OutgoingPerIncoming)
	}
	if r.Spread < 0 || r.Spread >= 1 {
		return Quote{}, fmt.Errorf("%w: unusable spread %g", ErrRefused,
			r.Spread)
	}

	// The routing budget is paid by the bridge out of the spread. If it is
	// the wider of the two, every swap loses money by construction however
	// well everything else works, so refuse rather than quote it.
	if q.Policy.FeeFraction >= r.Spread {
		return Quote{}, fmt.Errorf("%w: a routing budget of %.2f%% against a "+
			"spread of %.2f%% loses on every swap", ErrRefused,
			q.Policy.FeeFraction*100, r.Spread*100)
	}

	// Incoming units are worth r each in outgoing units, so the payer owes
	// the outgoing amount divided by the rate, plus the spread.
	//
	// Rounded up, always. Rounding to nearest would give away half a
	// millisatoshi on average per swap, which is nothing once and a
	// systematic leak across every swap the bridge ever does, in the one
	// direction that never corrects.
	exact := float64(dec.AmountMsat) / r.OutgoingPerIncoming * (1 + r.Spread)
	if math.IsInf(exact, 0) || math.IsNaN(exact) ||
		exact >= math.MaxUint64/2 {

		return Quote{}, fmt.Errorf("%w: %d msat at %g does not produce a "+
			"usable amount", ErrRefused, dec.AmountMsat, r.OutgoingPerIncoming)
	}
	incoming := uint64(math.Ceil(exact))
	if incoming == 0 {
		return Quote{}, fmt.Errorf("%w: the incoming amount rounds to zero",
			ErrRefused)
	}

	fee := uint64(math.Ceil(float64(dec.AmountMsat) * q.Policy.FeeFraction))
	if fee < q.Policy.MinFeeMsat {
		fee = q.Policy.MinFeeMsat
	}
	// The floor, unlike the fraction, can outgrow the spread on a small
	// payout, and then every such swap loses money by construction.
	if float64(fee) >= float64(dec.AmountMsat)*r.Spread {
		return Quote{}, fmt.Errorf("%w: %w: %d msat cannot cover the "+
			"%d msat routing budget out of a %.2f%% spread", ErrRefused,
			ErrTooSmall, dec.AmountMsat, fee, r.Spread*100)
	}

	return Quote{
		Hash:         dec.Hash,
		IncomingMsat: incoming,
		OutgoingMsat: dec.AmountMsat,
		Rate:         r.OutgoingPerIncoming,
		Spread:       r.Spread,
		MaxFeeMsat:   fee,
		Description:  dec.Description,
	}, nil
}

// create records the swap, then builds the hold invoice, then records that it
// exists.
//
// The record comes first for the same reason it does everywhere else: a crash
// between creating the invoice and writing it down would leave an invoice on
// the node that nothing is tracking, which a payer could fund and no one would
// ever settle or cancel. Recording first makes that recoverable — the driver
// finds a swap recorded as quoted whose invoice exists, and catches the record
// up.
func (q *Quoter) create(ctx context.Context, terms *Quote, who string,
	now time.Time) error {

	rec := store.Record{
		Hash:              terms.Hash,
		State:             swap.Quoted,
		OutgoingCLTVLimit: terms.OutgoingCLTVLimit,
		Invoice:           terms.Outgoing,
		IncomingMsat:      terms.IncomingMsat,
		OutgoingMsat:      terms.OutgoingMsat,
		MaxFeeMsat:        terms.MaxFeeMsat,
		QuoteExpiry:       terms.Expires,
		Rate:              terms.Rate,
		Spread:            terms.Spread,
		Created:           now,
		Updated:           now,
		Participant:       who,
	}
	if err := q.Store.Put(ctx, rec); err != nil {
		return fmt.Errorf("recording the quote: %w", err)
	}

	hold, err := q.In.AddHoldInvoice(ctx, node.HoldInvoice{
		Hash:       terms.Hash,
		AmountMsat: terms.IncomingMsat,
		CLTVDelta:  terms.CLTVDelta,
		Expiry:     q.Policy.InvoiceExpiry,
		Memo:       terms.Description,
		Metadata:   node.InvoiceMetadata(terms.Outgoing),
	})
	if err != nil {
		// The record exists and the invoice may: a call that failed may
		// still have reached the node. Only driving the swap settles which,
		// so the caller is told to.
		return fmt.Errorf("%w: creating the hold invoice: %w", ErrUntracked,
			err)
	}
	terms.HoldInvoice = hold

	// Through the state machine rather than by assignment, so that the one
	// place that knows which transitions exist stays the only place that
	// makes them.
	next, _, err := swap.Step(rec.State, swap.InvoiceOffered)
	if err != nil {
		return fmt.Errorf("recording that the invoice exists: %w", err)
	}
	rec.State = next
	rec.Updated = q.now()
	if err := q.Store.Put(ctx, rec); err != nil {
		// The invoice exists and the record says the swap is only quoted.
		// That is recoverable and deliberately so: the driver finds a
		// quoted swap whose invoice is on the node and catches the record
		// up, rather than leaving an invoice nothing will ever cancel.
		return fmt.Errorf("recording that the invoice exists: %w", err)
	}

	return nil
}
