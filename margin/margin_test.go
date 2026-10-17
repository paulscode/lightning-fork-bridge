package margin

import (
	"errors"
	"math"
	"testing"
	"time"
)

// Bounds used throughout. Bitcoin's spacing is close to its target and varies
// little; the BLAKE2b chain's fast bound is its current epoch rate divided by
// the observed within-epoch surge factor (S11, 2026-09-14).
var (
	btc = Bounds{Fast: 300 * time.Second, Slow: 600 * time.Second}

	// 155 s/block fast, from 464 s/block observed over three.
	blake = Bounds{Fast: 155 * time.Second, Slow: 1200 * time.Second}
)

// TestWorkedExamples pins the numbers published in the design document. If
// these move, the document is wrong and has to move with them.
func TestWorkedExamples(t *testing.T) {
	p := Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
		HoldExpiryDelta: DefaultHoldExpiryDelta}

	cases := []struct {
		outBlocks uint32
		worst     time.Duration
		want      uint32
	}{
		{40, 24000 * time.Second, 179},  // 6 h 40 m
		{80, 48000 * time.Second, 333},  // 13 h 20 m
		{144, 86400 * time.Second, 581}, // 24 h
	}
	for _, c := range cases {
		out := Leg{Bounds: btc, Blocks: c.outBlocks}

		exposure, err := OutgoingExposure(out, p)
		if err != nil {
			t.Fatalf("%d blocks: %v", c.outBlocks, err)
		}
		if got := exposure - p.Settlement; got != c.worst {
			t.Errorf("%d BTC blocks: worst case %v, want %v",
				c.outBlocks, got, c.worst)
		}

		got, err := RequiredIncomingBlocks(blake, out, p)
		if err != nil {
			t.Fatalf("%d blocks: %v", c.outBlocks, err)
		}
		// The published numbers are the margin alone. The quote asks
		// for lnd's hold expiry delta on top, since the incoming node
		// gives up on a held invoice that many blocks before it expires.
		if want := c.want + DefaultHoldExpiryDelta; got != want {
			t.Errorf("%d BTC blocks: need %d BLAKE2b blocks, want %d",
				c.outBlocks, got, want)
		}
	}
}

// TestBlocksAreNotDurations is the whole reason this package exists. A swap
// whose incoming leg has more blocks than its outgoing leg looks safe to the
// subtraction every other implementation would do, and is not.
func TestBlocksAreNotDurations(t *testing.T) {
	p := DefaultPolicy

	// 200 BLAKE2b blocks in, 144 Bitcoin blocks out. More blocks coming in
	// than going out, so a block-count comparison calls this safe.
	in := Leg{Bounds: blake, Blocks: 200}
	out := Leg{Bounds: btc, Blocks: 144}

	if in.Blocks <= out.Blocks {
		t.Fatal("test is not exercising what it claims")
	}
	err := Check(in, out, p)
	if !errors.Is(err, ErrUnsafe) {
		t.Fatalf("200 blocks in against 144 out must be refused, got %v", err)
	}

	// The incoming leg is worth 8.6 hours; the outgoing one can take 25.
	lifetime, _ := IncomingLifetime(in)
	exposure, _ := OutgoingExposure(out, p)
	if lifetime >= exposure {
		t.Fatalf("expected the incoming leg to be shorter: %v vs %v",
			lifetime, exposure)
	}

	// With enough blocks it becomes safe, and that count is what
	// RequiredIncomingBlocks says.
	need, err := RequiredIncomingBlocks(blake, out, p)
	if err != nil {
		t.Fatal(err)
	}
	in.Blocks = need
	if err := Check(in, out, p); err != nil {
		t.Fatalf("at the required %d blocks Check must pass: %v", need, err)
	}
}

// TestRequiredIsMinimal pins RequiredIncomingBlocks against Check: the count it
// returns must pass, and one block fewer must fail. This is the invariant that
// catches an off-by-one in either function, in whichever direction.
func TestRequiredIsMinimal(t *testing.T) {
	p := Policy{Settlement: 37 * time.Minute, MaxIncomingBlocks: MaxBlocks,
		HoldExpiryDelta: DefaultHoldExpiryDelta}

	// Spacings chosen to make the division land exactly, just over and just
	// under, since that is where a rounding error hides.
	spacings := []time.Duration{
		1 * time.Second, 7 * time.Second, 60 * time.Second,
		155 * time.Second, 199 * time.Second, 600 * time.Second,
		1200 * time.Second,
	}
	for _, inFast := range spacings {
		for _, outSlow := range spacings {
			for _, outBlocks := range []uint32{0, 1, 2, 13, 40, 144, 1000} {
				in := Bounds{Fast: inFast, Slow: inFast * 4}
				out := Leg{
					Bounds: Bounds{Fast: outSlow / 4, Slow: outSlow},
					Blocks: outBlocks,
				}
				need, err := RequiredIncomingBlocks(in, out, p)
				if err != nil {
					t.Fatalf("in %v out %v %d blocks: %v",
						inFast, outSlow, outBlocks, err)
				}
				// Check counts what is left above the hold expiry
				// delta, which the requirement adds on top.
				if need <= p.HoldExpiryDelta {
					t.Fatalf("in %v out %v %d blocks: required %d, "+
						"no more than the delta", inFast, outSlow,
						outBlocks, need)
				}
				need -= p.HoldExpiryDelta
				if err := Check(Leg{Bounds: in, Blocks: need}, out, p); err != nil {
					t.Fatalf("in %v out %v %d blocks: %d should pass: %v",
						inFast, outSlow, outBlocks, need, err)
				}
				err = Check(Leg{Bounds: in, Blocks: need - 1}, out, p)
				if !errors.Is(err, ErrUnsafe) {
					t.Fatalf("in %v out %v %d blocks: %d should fail "+
						"but got %v", inFast, outSlow, outBlocks,
						need-1, err)
				}
			}
		}
	}
}

// TestMonotonic checks the direction of every input: nothing may make a swap
// look safer than it is.
func TestMonotonic(t *testing.T) {
	p := DefaultPolicy
	out := Leg{Bounds: btc, Blocks: 40}

	base, err := RequiredIncomingBlocks(blake, out, p)
	if err != nil {
		t.Fatal(err)
	}

	// A faster incoming chain means each block buys less time, so more are
	// needed.
	faster := blake
	faster.Fast = blake.Fast / 2
	more, err := RequiredIncomingBlocks(faster, out, p)
	if err != nil {
		t.Fatal(err)
	}
	if more <= base {
		t.Errorf("halving incoming spacing: %d blocks, want more than %d",
			more, base)
	}

	// A slower outgoing chain lengthens the exposure.
	slowOut := out
	slowOut.Bounds.Slow = btc.Slow * 2
	more, err = RequiredIncomingBlocks(blake, slowOut, p)
	if err != nil {
		t.Fatal(err)
	}
	if more <= base {
		t.Errorf("doubling outgoing spacing: %d blocks, want more than %d",
			more, base)
	}

	// More outgoing blocks lengthen the exposure.
	longOut := out
	longOut.Blocks = out.Blocks * 2
	more, err = RequiredIncomingBlocks(blake, longOut, p)
	if err != nil {
		t.Fatal(err)
	}
	if more <= base {
		t.Errorf("doubling outgoing blocks: %d blocks, want more than %d",
			more, base)
	}

	// A longer settlement margin lengthens the exposure.
	slowP := p
	slowP.Settlement = p.Settlement * 2
	more, err = RequiredIncomingBlocks(blake, out, slowP)
	if err != nil {
		t.Fatal(err)
	}
	if more <= base {
		t.Errorf("doubling settlement: %d blocks, want more than %d",
			more, base)
	}
}

// TestCheckIsStrict makes sure a leg that merely ties with the exposure is
// refused. Equality leaves no time at all to claim.
func TestCheckIsStrict(t *testing.T) {
	p := Policy{Settlement: 0, MaxIncomingBlocks: 2016,
		HoldExpiryDelta: DefaultHoldExpiryDelta}
	in := Leg{Bounds: Bounds{Fast: time.Minute, Slow: time.Minute}, Blocks: 10}
	out := Leg{Bounds: Bounds{Fast: time.Minute, Slow: time.Minute}, Blocks: 10}

	lifetime, _ := IncomingLifetime(in)
	exposure, _ := OutgoingExposure(out, p)
	if lifetime != exposure {
		t.Fatalf("test setup: %v vs %v", lifetime, exposure)
	}
	if err := Check(in, out, p); !errors.Is(err, ErrUnsafe) {
		t.Fatalf("a tie must be refused, got %v", err)
	}
}

func TestRejectsBadInput(t *testing.T) {
	p := DefaultPolicy
	ok := Leg{Bounds: btc, Blocks: 10}

	cases := []struct {
		name string
		in   Leg
		out  Leg
		p    Policy
		want error

		// checkOnly marks a case that only Check can catch, because
		// RequiredIncomingBlocks derives the incoming block count
		// instead of being given one.
		checkOnly bool
	}{{
		name: "incoming bounds zero",
		in:   Leg{Blocks: 10},
		out:  ok,
		p:    p,
		want: ErrBounds,
	}, {
		name: "outgoing bounds zero",
		in:   ok,
		out:  Leg{Blocks: 10},
		p:    p,
		want: ErrBounds,
	}, {
		name: "fast slower than slow",
		in:   Leg{Bounds: Bounds{Fast: time.Hour, Slow: time.Minute}, Blocks: 10},
		out:  ok,
		p:    p,
		want: ErrBounds,
	}, {
		name: "negative spacing",
		in:   Leg{Bounds: Bounds{Fast: -time.Minute, Slow: time.Hour}, Blocks: 10},
		out:  ok,
		p:    p,
		want: ErrBounds,
	}, {
		name:      "incoming blocks out of range",
		in:        Leg{Bounds: btc, Blocks: MaxBlocks + 1},
		out:       ok,
		p:         p,
		want:      ErrBlocks,
		checkOnly: true,
	}, {
		name: "outgoing blocks out of range",
		in:   ok,
		out:  Leg{Bounds: btc, Blocks: MaxBlocks + 1},
		p:    p,
		want: ErrBlocks,
	}, {
		name: "policy limit zero",
		in:   ok,
		out:  ok,
		p:    Policy{Settlement: time.Hour},
		want: ErrBounds,
	}, {
		// Under lnd's default, and above all zero, which a policy that
		// never set it holds: the blocks the incoming node has already
		// given up in would count as margin.
		name: "hold expiry delta under lnd's default",
		in:   ok,
		out:  ok,
		p: Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
			HoldExpiryDelta: DefaultHoldExpiryDelta - 1},
		want: ErrBounds,
	}, {
		name: "no hold expiry delta",
		in:   ok,
		out:  ok,
		p:    Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016},
		want: ErrBounds,
	}, {
		name: "negative settlement",
		in:   ok,
		out:  ok,
		p: Policy{Settlement: -time.Hour, MaxIncomingBlocks: 100,
			HoldExpiryDelta: DefaultHoldExpiryDelta},
		want: ErrBounds,
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := Check(c.in, c.out, c.p); !errors.Is(err, c.want) {
				t.Errorf("Check: got %v, want %v", err, c.want)
			}
			if c.checkOnly {
				return
			}
			_, err := RequiredIncomingBlocks(c.in.Bounds, c.out, c.p)
			if !errors.Is(err, c.want) {
				t.Errorf("RequiredIncomingBlocks: got %v, want %v",
					err, c.want)
			}
		})
	}
}

// TestRefusesRatherThanQuoteAbsurdly checks that a swap needing more CLTV than
// anyone would route is declined, not quoted.
func TestRefusesRatherThanQuoteAbsurdly(t *testing.T) {
	p := Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
		HoldExpiryDelta: DefaultHoldExpiryDelta}

	// A very fast incoming chain against a long outgoing leg.
	fast := Bounds{Fast: time.Second, Slow: time.Minute}
	out := Leg{Bounds: btc, Blocks: 144}

	_, err := RequiredIncomingBlocks(fast, out, p)
	if !errors.Is(err, ErrTooLong) {
		t.Fatalf("want ErrTooLong, got %v", err)
	}

	// Raising the limit makes the same swap quotable.
	p.MaxIncomingBlocks = MaxBlocks
	if _, err := RequiredIncomingBlocks(fast, out, p); err != nil {
		t.Fatalf("with a higher limit it should quote: %v", err)
	}
}

// TestSpanDoesNotOverflow feeds the largest values the package accepts and
// checks the arithmetic reports rather than wraps.
func TestSpanDoesNotOverflow(t *testing.T) {
	if _, err := span(MaxBlocks, math.MaxInt64); err == nil {
		t.Error("an overflowing span must report an error")
	}
	got, err := span(MaxBlocks, 20*time.Minute)
	if err != nil {
		t.Fatalf("a real chain's worst case must not overflow: %v", err)
	}
	if want := time.Duration(MaxBlocks) * 20 * time.Minute; got != want {
		t.Errorf("span = %v, want %v", got, want)
	}
	if _, err := span(0, time.Minute); err != nil {
		t.Errorf("zero blocks is a legitimate span: %v", err)
	}
}

// FuzzCheck looks for panics and for the one invariant that must never break:
// whenever Check passes, the incoming leg really does outlive the outgoing
// one.
func FuzzCheck(f *testing.F) {
	f.Add(uint32(200), uint32(144), int64(155), int64(600), int64(3600))
	f.Add(uint32(0), uint32(0), int64(1), int64(1), int64(0))
	f.Add(uint32(2016), uint32(40), int64(600), int64(1200), int64(60))

	f.Fuzz(func(t *testing.T, inBlocks, outBlocks uint32,
		inFastSec, outSlowSec, settleSec int64) {

		// Keep the inputs inside what the package claims to accept; the
		// rejection of everything else is covered by TestRejectsBadInput.
		if inFastSec <= 0 || outSlowSec <= 0 || settleSec < 0 {
			return
		}
		if inFastSec > 86400 || outSlowSec > 86400 || settleSec > 86400*7 {
			return
		}
		inBlocks %= MaxBlocks + 1
		outBlocks %= MaxBlocks + 1

		in := Leg{
			Bounds: Bounds{
				Fast: time.Duration(inFastSec) * time.Second,
				Slow: time.Duration(inFastSec) * time.Second * 4,
			},
			Blocks: inBlocks,
		}
		out := Leg{
			Bounds: Bounds{
				Fast: time.Duration(outSlowSec) * time.Second / 4,
				Slow: time.Duration(outSlowSec) * time.Second,
			},
			Blocks: outBlocks,
		}
		if out.Bounds.Fast <= 0 {
			out.Bounds.Fast = time.Nanosecond
		}
		p := Policy{
			Settlement:        time.Duration(settleSec) * time.Second,
			MaxIncomingBlocks: MaxBlocks,
			HoldExpiryDelta:   DefaultHoldExpiryDelta,
		}

		err := Check(in, out, p)
		if err != nil {
			return
		}
		lifetime, lerr := IncomingLifetime(in)
		exposure, eerr := OutgoingExposure(out, p)
		if lerr != nil || eerr != nil {
			t.Fatalf("Check passed but the parts failed: %v %v", lerr, eerr)
		}
		if lifetime <= exposure {
			t.Fatalf("Check passed with lifetime %v <= exposure %v",
				lifetime, exposure)
		}

		// And whenever Check passes, the required count cannot be larger
		// than what was supplied.
		need, err := RequiredIncomingBlocks(in.Bounds, out, p)
		if err != nil {
			t.Fatalf("Check passed but no requirement could be computed: %v",
				err)
		}
		// The requirement asks for the hold expiry delta on top of
		// what Check counts.
		if need-p.HoldExpiryDelta > in.Blocks {
			t.Fatalf("Check passed at %d blocks but %d are required",
				in.Blocks, need-p.HoldExpiryDelta)
		}
	})
}

// TestSettlementAdditionCannotWrap covers the overflow found in review: span
// alone is bounded, but adding the settlement margin to a very large span
// could wrap negative, and a negative exposure makes an unsafe swap look safe.
func TestSettlementAdditionCannotWrap(t *testing.T) {
	// One block at very nearly the maximum representable spacing.
	huge := time.Duration(math.MaxInt64 - 1000)
	out := Leg{Bounds: Bounds{Fast: huge, Slow: huge}, Blocks: 1}
	p := Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
		HoldExpiryDelta: DefaultHoldExpiryDelta}

	if _, err := OutgoingExposure(out, p); !errors.Is(err, ErrBounds) {
		t.Fatalf("want ErrBounds, got %v", err)
	}

	// And the wrap must not sneak through Check as a pass.
	in := Leg{Bounds: Bounds{Fast: time.Second, Slow: time.Second}, Blocks: 1}
	if err := Check(in, out, p); err == nil {
		t.Fatal("Check accepted a swap whose exposure overflows")
	}
}

// TestMaxOutgoingBlocksIsTheLargestThatFits pins it against
// RequiredIncomingBlocks, and shows it shrinking when the incoming chain runs
// faster.
func TestMaxOutgoingBlocksIsTheLargestThatFits(t *testing.T) {
	// 1776 blocks of margin, and the hold expiry delta on top.
	p := Policy{Settlement: time.Hour,
		MaxIncomingBlocks: 1776 + DefaultHoldExpiryDelta,
		HoldExpiryDelta:   DefaultHoldExpiryDelta}
	in := Bounds{Fast: 200 * time.Second, Slow: 40 * time.Minute}
	out := Bounds{Fast: 5 * time.Minute, Slow: 15 * time.Minute}

	n, err := MaxOutgoingBlocks(in, out, p)
	if err != nil {
		t.Fatal(err)
	}
	if n != 390 {
		t.Errorf("at target spacing %d blocks fit, want 390", n)
	}
	if _, err := RequiredIncomingBlocks(in, Leg{Bounds: out, Blocks: n}, p); err != nil {
		t.Errorf("%d does not fit: %v", n, err)
	}
	if _, err := RequiredIncomingBlocks(in, Leg{Bounds: out, Blocks: n + 1}, p); err == nil {
		t.Errorf("%d fits too", n+1)
	}

	// The incoming chain running 1.3x faster: fewer fit.
	fast := Bounds{Fast: in.Fast * 10 / 13, Slow: in.Slow}
	m, _ := MaxOutgoingBlocks(fast, out, p)
	if m >= n || m < 290 {
		t.Errorf("with the incoming chain faster %d fit, want fewer than %d", m, n)
	}

	// Nothing fits when the settlement alone does not.
	tight := Policy{Settlement: 1000 * time.Hour, MaxIncomingBlocks: 100,
		HoldExpiryDelta: DefaultHoldExpiryDelta}
	if m, _ := MaxOutgoingBlocks(in, out, tight); m != 0 {
		t.Errorf("%d fit under an impossible policy", m)
	}
}
