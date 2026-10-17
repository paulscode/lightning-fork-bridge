// Package margin decides whether a cross-chain swap's two legs are safely
// ordered in time.
//
// A swap locks an incoming HTLC on one chain and pays an outgoing HTLC on the
// other. The bridge can only claim the incoming HTLC after it learns the
// preimage, and in the worst case it learns it just before the outgoing HTLC
// expires on chain. So the incoming leg must outlive the outgoing one.
//
// Both legs express their expiry in blocks, which would make this a
// subtraction if the two chains produced blocks at the same rate. They do not.
// The BLAKE2b chain has run between 1.3x and 19x faster than its ten minute
// target since activation, and within a difficulty epoch its rate is set by
// hashrate that nothing corrects for up to 2016 blocks. A block count is
// therefore not a duration, and comparing the legs in blocks understates the
// risk by several times.
//
// Everything here converts both legs to wall-clock first, valuing the incoming
// leg at the shortest time per block it might plausibly take (it expiring
// sooner is what hurts) and the outgoing leg at the longest (it resolving
// later is what hurts), and only then compares.
package margin

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// MaxBlocks bounds the block counts this package will reason about. It is far
// above any usable CLTV and exists so that a nonsense input cannot overflow
// the nanosecond arithmetic in a time.Duration.
const MaxBlocks = 1 << 22

var (
	// ErrBounds is returned when a chain's block-spacing bounds are missing
	// or incoherent.
	ErrBounds = errors.New("block spacing bounds are not usable")

	// ErrBlocks is returned for a block count outside MaxBlocks.
	ErrBlocks = errors.New("block count out of range")

	// ErrTooLong is returned when a swap would need more incoming CLTV than
	// the policy allows. The caller should decline to quote rather than ask
	// for it.
	ErrTooLong = errors.New("required incoming CLTV exceeds the policy limit")

	// ErrUnsafe is returned when the incoming leg does not outlive the
	// outgoing one by the settlement margin. The caller must not commit the
	// outgoing payment.
	ErrUnsafe = errors.New("incoming leg does not outlive the outgoing leg")
)

// Bounds is the range of time a single block on a chain may plausibly take.
//
// Fast is the shortest and Slow the longest. They are not a confidence
// interval on the next block, which is Poisson and unbounded in both
// directions, but on the average over a window long enough to matter to an
// HTLC. chainrate derives them from observed behaviour.
type Bounds struct {
	Fast time.Duration
	Slow time.Duration
}

// Valid reports whether the bounds can be used in a calculation.
func (b Bounds) Valid() bool {
	return b.Fast > 0 && b.Slow > 0 && b.Fast <= b.Slow
}

// Leg is one side of a swap: a chain's bounds and the blocks of CLTV left
// before that leg's HTLC expires.
type Leg struct {
	Bounds Bounds

	// Blocks is the CLTV remaining, in blocks of this leg's chain.
	Blocks uint32
}

// Policy is the operator's safety settings.
type Policy struct {
	// Settlement is the time the bridge allows itself to claim the incoming
	// HTLC after learning the preimage: seeing the outgoing preimage,
	// building and broadcasting a claim, and having it confirm.
	Settlement time.Duration

	// MaxIncomingBlocks caps what a swap may demand of the incoming leg. A
	// swap needing more is refused rather than quoted, since a CLTV nobody
	// will route is worse than no quote.
	MaxIncomingBlocks uint32

	// HoldExpiryDelta is how many blocks before the incoming HTLC expires
	// the incoming node gives up on the held invoice: lnd cancels an
	// accepted hold invoice invoices.holdexpirydelta blocks early, 18 by
	// default. From then on the claim is gone though the HTLC has not
	// expired, so those blocks are no margin at all: the quote asks the
	// payer for them on top, and the gate does not count them.
	HoldExpiryDelta uint32
}

// DefaultHoldExpiryDelta is lnd's default invoices.holdexpirydelta: its
// incoming broadcast delta, 16 blocks since lnd 0.21, plus 2.
const DefaultHoldExpiryDelta = 18

// DefaultPolicy is a starting point, not a recommendation for any particular
// operator. One hour of settlement covers a claim confirming on a chain
// running at its target rate with room to spare, and 2016 blocks is LND's
// default max_cltv_expiry, so asking for more guarantees an unroutable swap.
var DefaultPolicy = Policy{
	Settlement:        time.Hour,
	MaxIncomingBlocks: 2016,
	HoldExpiryDelta:   DefaultHoldExpiryDelta,
}

// Valid reports whether the policy is usable.
//
// A HoldExpiryDelta under lnd's default is refused. No lnd gives up on a held
// invoice later than that by default, and a smaller number, zero above all,
// which is what a policy that never set it holds, would count the blocks the
// node has already given up in as margin.
func (p Policy) Valid() bool {
	return p.Settlement >= 0 && p.MaxIncomingBlocks > 0 &&
		p.MaxIncomingBlocks <= MaxBlocks &&
		p.HoldExpiryDelta >= DefaultHoldExpiryDelta &&
		p.HoldExpiryDelta < p.MaxIncomingBlocks
}

// Usable returns the blocks of an incoming HTLC that count as margin: those
// before the incoming node gives up on the held invoice. ok is false when
// there are none.
func (p Policy) Usable(remaining uint32) (blocks uint32, ok bool) {
	if remaining <= p.HoldExpiryDelta {
		return 0, false
	}

	return remaining - p.HoldExpiryDelta, true
}

// span multiplies a block count by a per-block duration without overflowing.
func span(blocks uint32, each time.Duration) (time.Duration, error) {
	if blocks > MaxBlocks {
		return 0, fmt.Errorf("%w: %d blocks", ErrBlocks, blocks)
	}
	if each <= 0 {
		return 0, fmt.Errorf("%w: %v per block", ErrBounds, each)
	}
	// MaxBlocks * each cannot overflow int64 nanoseconds for any spacing
	// short enough to be a real chain, but check rather than assume.
	if each > math.MaxInt64/time.Duration(blocks+1) {
		return 0, fmt.Errorf("%w: %d blocks at %v overflows", ErrBounds,
			blocks, each)
	}

	return time.Duration(blocks) * each, nil
}

// OutgoingExposure is how long the outgoing leg may take to resolve in the
// worst case: every one of its remaining blocks taking the longest it
// plausibly might, plus the settlement margin.
//
// This is the quantity the incoming leg has to outlive.
func OutgoingExposure(out Leg, p Policy) (time.Duration, error) {
	if !out.Bounds.Valid() {
		return 0, fmt.Errorf("%w: outgoing %+v", ErrBounds, out.Bounds)
	}
	if !p.Valid() {
		return 0, fmt.Errorf("%w: policy %+v", ErrBounds, p)
	}
	worst, err := span(out.Blocks, out.Bounds.Slow)
	if err != nil {
		return 0, fmt.Errorf("outgoing leg: %w", err)
	}
	// span alone cannot overflow, but adding the settlement margin to it
	// can, and a wrapped sum is negative, which would make an unsafe swap
	// look like it had time to spare.
	if worst > math.MaxInt64-p.Settlement {
		return 0, fmt.Errorf("%w: %v plus %v settlement overflows",
			ErrBounds, worst, p.Settlement)
	}

	return worst + p.Settlement, nil
}

// IncomingLifetime is how little time the incoming leg may have left in the
// worst case: every one of its remaining blocks arriving as quickly as it
// plausibly might.
func IncomingLifetime(in Leg) (time.Duration, error) {
	if !in.Bounds.Valid() {
		return 0, fmt.Errorf("%w: incoming %+v", ErrBounds, in.Bounds)
	}
	soonest, err := span(in.Blocks, in.Bounds.Fast)
	if err != nil {
		return 0, fmt.Errorf("incoming leg: %w", err)
	}

	return soonest, nil
}

// RequiredIncomingBlocks returns the smallest incoming CLTV, in blocks of the
// incoming chain, that outlives the outgoing leg's worst case.
//
// It is what a quote should demand of the payer. Check is what decides,
// later and with fresher numbers, whether to actually pay.
func RequiredIncomingBlocks(in Bounds, out Leg, p Policy) (uint32, error) {
	if !in.Valid() {
		return 0, fmt.Errorf("%w: incoming %+v", ErrBounds, in)
	}
	exposure, err := OutgoingExposure(out, p)
	if err != nil {
		return 0, err
	}

	// Check passes when blocks*Fast is strictly greater than the exposure,
	// so the smallest such count is floor(exposure/Fast)+1. That holds
	// whether or not the division is exact: an exact quotient needs the
	// extra block to become strictly greater, and an inexact one has its
	// remainder covered by the same block. TestRequiredIsMinimal pins this
	// against Check so the two cannot drift apart.
	// The incoming node gives up on the held invoice HoldExpiryDelta
	// blocks before the HTLC expires, so those are asked for on top.
	blocks := int64(exposure/in.Fast) + 1 + int64(p.HoldExpiryDelta)

	if blocks > int64(p.MaxIncomingBlocks) {
		return 0, fmt.Errorf("%w: need %d blocks, limit %d", ErrTooLong,
			blocks, p.MaxIncomingBlocks)
	}

	return uint32(blocks), nil
}

// Check reports whether the outgoing payment is safe to commit right now.
//
// It is the last safe moment: once the outgoing HTLC is in flight it cannot be
// recalled, so this runs immediately before committing and its failure means
// abort rather than pay. Fresh bounds and the legs' current remaining CLTV
// must be passed, not the values a quote was built from.
func Check(in, out Leg, p Policy) error {
	lifetime, err := IncomingLifetime(in)
	if err != nil {
		return err
	}
	exposure, err := OutgoingExposure(out, p)
	if err != nil {
		return err
	}
	if lifetime <= exposure {
		return fmt.Errorf("%w: incoming %d blocks is at worst %v, "+
			"outgoing %d blocks plus settlement is at worst %v",
			ErrUnsafe, in.Blocks, lifetime, out.Blocks, exposure)
	}

	return nil
}

// MaxOutgoingBlocks is the most CLTV the outgoing leg may use, in blocks of
// its chain, for the incoming leg to outlive it within the policy's
// MaxIncomingBlocks: the route budget the chains allow right now. Zero when
// not even one block fits.
//
// It moves with the measured spacing. A configured budget is a ceiling; when
// the incoming chain runs fast or the outgoing one slow, fewer blocks fit, and
// asking for a route within this rather than the ceiling finds one that can
// actually be quoted, if there is one.
func MaxOutgoingBlocks(in, out Bounds, p Policy) (uint32, error) {
	if !in.Valid() || !out.Valid() {
		return 0, fmt.Errorf("%w: incoming %+v, outgoing %+v", ErrBounds,
			in, out)
	}
	fits := func(n uint32) bool {
		_, err := RequiredIncomingBlocks(in, Leg{Bounds: out, Blocks: n}, p)
		return err == nil
	}
	if !fits(0) {
		return 0, nil
	}
	// RequiredIncomingBlocks grows with n, so the largest that fits is
	// found by bisection.
	lo, hi := uint32(0), uint32(MaxBlocks)
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if fits(mid) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}

	return lo, nil
}
