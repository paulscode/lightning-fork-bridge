// Package store is the durable record of every swap.
//
// The record is not bookkeeping. It is the input to recovery, and recovery is
// where a bridge loses money. After a crash the only thing that says whether an
// outgoing payment might be in flight is what was written to disk before it
// left; a swap whose record still says "funded" will be recovered by cancelling
// the incoming claim, which is exactly the wrong move if the outgoing leg
// actually went out. So the ordering matters more than the format: the state
// has to be durable before the action it authorises is taken, and the driver
// enforces that.
//
// Two implementations are here. Journal is the real one: an append-only file,
// fsynced per record, checksummed, replayed at open. Memory is for tests and
// refuses to pretend otherwise.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

var (
	// ErrNotFound is returned when no record exists for a hash.
	ErrNotFound = errors.New("no record for this payment hash")

	// ErrCorrupt is returned when the journal holds something that cannot be
	// read back. It is never repaired automatically: a damaged record in the
	// middle of the file means the bridge does not know what it did, and
	// guessing is worse than stopping.
	ErrCorrupt = errors.New("journal is corrupt")

	// ErrRecord is returned for a record that cannot be stored.
	ErrRecord = errors.New("record is not usable")

	// ErrLocked is returned when another Journal, in this process or
	// another, already has the journal open. Two writers on one journal
	// each replay it once and then append without seeing the other's
	// records, and recovery would believe whichever wrote last.
	ErrLocked = errors.New("the journal is open elsewhere")
)

// Record is one swap, in the form that survives a restart.
//
// It holds what the decisions need and what an operator would have to
// reconstruct by hand otherwise. Amounts are on both sides because the pair is
// the only evidence of what rate was actually honoured.
type Record struct {
	// Hash appears on both chains and is the primary key.
	Hash node.Hash

	// State is where the machine has got to.
	State swap.State

	// Binding is what makes the two legs one transaction, and the two kinds
	// do not offer the same assurance. See swap.Binding.
	Binding swap.Binding

	// Preimage settles the **incoming** leg. Where it comes from depends on
	// the binding: a hash-linked swap learns it from the outgoing leg and
	// holds nothing until then, while a sequenced swap chooses it before
	// issuing the hold invoice and must already have it here.
	Preimage node.Preimage

	// OutgoingHash is the payment hash of the outgoing leg, when it differs
	// from the incoming one. Zero means the legs share a hash, which is the
	// hash-linked case and the only one where sharing is possible.
	OutgoingHash node.Hash

	// OutgoingPreimage is proof the outgoing leg was paid. For a hash-linked
	// swap it is the same value as Preimage and is what makes the claim
	// possible; for a sequenced swap it settles nothing here and is kept as
	// evidence, because "we paid them" is the fact an operator needs when a
	// recipient says otherwise.
	OutgoingPreimage node.Preimage

	// OutgoingCLTVLimit is the CLTV budget imposed on the outgoing payment,
	// in blocks of the outgoing chain. The incoming leg has to outlive it.
	OutgoingCLTVLimit uint32

	// Invoice is the outgoing invoice being paid, exactly as the payer
	// supplied it. The bridge never rewrites it: its payment hash is what the
	// incoming hold invoice was built on.
	Invoice string

	// IncomingMsat is what the payer owes on the incoming chain and
	// OutgoingMsat what leaves on the outgoing one. They are in different
	// units and the gap between them, at the quoted rate, is the spread.
	IncomingMsat uint64
	OutgoingMsat uint64

	// MaxFeeMsat caps what may be spent on routing the outgoing leg. It is
	// part of the quote: a payment that cost more than this would eat the
	// spread the swap was priced with.
	MaxFeeMsat uint64

	// QuoteExpiry is when the rate stops being honoured.
	//
	// It is wall-clock rather than a block height because what it bounds is
	// price risk, and price moves with time, not with blocks. A swap that
	// reaches this unfunded is cancelled; one already funded is decided by
	// the margin check instead, which is about blocks.
	QuoteExpiry time.Time

	// Rate is the outgoing-per-incoming price the quote was struck at, and
	// Spread the fraction charged over it. Kept because a quote that was
	// honoured at a rate nobody can reproduce later is indistinguishable from
	// one that was mispriced.
	Rate   float64
	Spread float64

	// Created and Updated are wall-clock, for operators and for ageing out
	// quotes. Nothing safety-critical is decided from them; block heights do
	// that.
	Created time.Time
	Updated time.Time

	// Note is free text. The states that need a human carry the reason here.
	Note string

	// Participant is who asked for this swap, when the bridge serves more
	// than its operator: an opaque identifier of their credential. Empty for
	// the operator's own swaps.
	//
	// It is recorded so that per-participant limits survive a restart. A
	// limit that forgot every outstanding quote at each start would let a
	// participant reserve the bridge's liquidity again simply by waiting
	// for one.
	Participant string

	// NoPayAttempt is set when the swap is created and cleared in the same
	// write that records it Paying, the first moment an outgoing payment
	// may leave. While it is set no outgoing payment can exist, so giving
	// the incoming HTLC back need not wait for the paying node to say there
	// is none. A record without it, written before it existed or by hand,
	// may have paid, and the paying node is asked as before.
	NoPayAttempt bool
}

// Valid reports whether a record can be stored. It is deliberately narrow:
// this is the last check before something becomes the thing a restart trusts.
func (r Record) Valid() error {
	var zeroHash node.Hash
	if r.Hash == zeroHash {
		return fmt.Errorf("%w: zero payment hash", ErrRecord)
	}
	if _, err := swap.ParseState(r.State.String()); err != nil {
		return fmt.Errorf("%w: %w", ErrRecord, err)
	}
	if r.Created.IsZero() {
		return fmt.Errorf("%w: no creation time", ErrRecord)
	}

	// A preimage must hash to the record's hash or it settles nothing, and a
	// record carrying the wrong one would send the driver to settle with a
	// value the node will reject, in the one state where a rejection is a
	// realised loss.
	var zeroPre node.Preimage
	if r.Preimage != zeroPre && r.Preimage.Hash() != r.Hash {
		return fmt.Errorf("%w: preimage does not hash to %x", ErrRecord,
			r.Hash)
	}

	// The outgoing preimage is the evidence that the outgoing leg was paid,
	// and evidence nobody checks is not evidence. For a hash-linked swap the
	// check above already covers it, because the two are the same value; for
	// a sequenced one nothing else would ever catch a node handing back a
	// preimage for some other payment.
	if r.OutgoingPreimage != zeroPre &&
		r.OutgoingPreimage.Hash() != r.Outgoing() {

		return fmt.Errorf("%w: the outgoing preimage settles %x, not the %x "+
			"this swap was paying", ErrRecord, r.OutgoingPreimage.Hash(),
			r.Outgoing())
	}

	if _, err := swap.ParseBinding(r.Binding.String()); err != nil {
		return fmt.Errorf("%w: %w", ErrRecord, err)
	}

	switch r.Binding {
	case swap.HashLinked:
		// Sharing a hash is the whole of this binding's security. A record
		// that claims it while naming a different outgoing hash is
		// describing a swap that has no binding at all.
		if r.OutgoingHash != zeroHash && r.OutgoingHash != r.Hash {
			return fmt.Errorf("%w: a hash-linked swap cannot pay %x while "+
				"holding %x", ErrRecord, r.OutgoingHash, r.Hash)
		}

	case swap.Sequenced:
		if r.OutgoingHash == zeroHash {
			return fmt.Errorf("%w: a sequenced swap needs the outgoing hash; "+
				"without it there is nothing to look the payment up by",
				ErrRecord)
		}
		if r.OutgoingHash == r.Hash {
			// Then it is hash-linked and should say so, and be trusted
			// accordingly. Recording the stronger arrangement as the weaker
			// one is harmless; the reverse is not.
			return fmt.Errorf("%w: a sequenced swap whose legs share %x is "+
				"hash-linked and must say so", ErrRecord, r.Hash)
		}
		if r.Preimage == zeroPre {
			// There is no later moment at which it arrives: the outgoing
			// leg's preimage settles nothing here. Accepting money against
			// a claim that can never be made is the one thing this check
			// exists to stop.
			return fmt.Errorf("%w: a sequenced swap must hold the incoming "+
				"preimage before it accepts money, and this one has none",
				ErrRecord)
		}
	}

	return nil
}

// Outgoing is the payment hash of the outgoing leg, which for a hash-linked
// swap is the incoming one.
func (r Record) Outgoing() node.Hash {
	var zero node.Hash
	if r.OutgoingHash == zero {
		return r.Hash
	}

	return r.OutgoingHash
}

// Store is the durable record. Put must not return until the record would
// survive the loss of the process and of the machine.
type Store interface {
	// Put writes a record, replacing any earlier one for the same hash. It
	// returns only once the write is durable.
	Put(ctx context.Context, r Record) error

	// Get reads the current record for a hash, or ErrNotFound.
	Get(ctx context.Context, hash node.Hash) (Record, error)

	// Pending returns every non-terminal record, oldest first. This is what a
	// restart works through.
	Pending(ctx context.Context) ([]Record, error)

	// Close releases the store.
	Close() error
}

// Committed is the outgoing amount the bridge may still have to pay across
// every swap that has not finished, routing fees included: each swap counts
// its payout and its routing budget (MaxFeeMsat), since the payment leaves
// with both and the balance has to cover both.
//
// It is what makes a balance into headroom. The node's outgoing balance says
// what could be sent right now; it says nothing about the swaps already
// quoted, offered or funded that will want to be paid out of that same
// balance. Quoting against the raw balance therefore sells the same money
// repeatedly, and the bridge discovers it at the worst moment — with an
// incoming HTLC locked in and no way to pay the outgoing leg.
//
// The set is deliberately wide. A swap merely quoted has no HTLC yet, but a
// payer can fund it at any moment, and between "they might" and "they did"
// there is no chance to re-decide. Paid is excluded because by then the
// payment has left and the balance already reflects it.
func Committed(ctx context.Context, s Store) (uint64, error) {
	return CommittedWhere(ctx, s, nil)
}

// CommittedWhere is Committed over the records keep accepts, or all of them
// when keep is nil.
//
// A bridge serving both directions needs it per direction. The two pay out on
// different chains, in units that are not the same, so a sum across both is
// not an amount of anything: it reserves too much on one side and too little
// on the other.
func CommittedWhere(ctx context.Context, s Store,
	keep func(Record) bool) (uint64, error) {

	pending, err := s.Pending(ctx)
	if err != nil {
		return 0, fmt.Errorf("reading pending swaps: %w", err)
	}

	var total uint64
	for _, r := range pending {
		if keep != nil && !keep(r) {
			continue
		}
		switch r.State {
		case swap.Quoted, swap.Offered, swap.Funded, swap.Paying:
			// Refusing rather than wrapping, because an overflow here
			// would report a small commitment for an enormous one, in
			// the direction that oversells.
			owed := r.OutgoingMsat + r.MaxFeeMsat
			if owed < r.OutgoingMsat || total+owed < total {
				return 0, fmt.Errorf("%w: committed amounts overflow",
					ErrRecord)
			}
			total += owed
		}
	}

	return total, nil
}
