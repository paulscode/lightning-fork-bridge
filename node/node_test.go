package node

import (
	"encoding/hex"
	"math"
	"testing"
	"time"
)

func TestStateNames(t *testing.T) {
	for s, want := range invoiceStateNames {
		if got := s.String(); got != want {
			t.Errorf("invoice state %d prints %q, want %q", uint8(s), got, want)
		}
	}
	if got := InvoiceState(200).String(); got != "invoice state(200)" {
		t.Errorf("unknown invoice state prints %q", got)
	}

	for s, want := range paymentStateNames {
		if got := s.String(); got != want {
			t.Errorf("payment state %d prints %q, want %q", uint8(s), got, want)
		}
	}
	if got := PaymentState(200).String(); got != "payment state(200)" {
		t.Errorf("unknown payment state prints %q", got)
	}
}

// TestOnlyAcceptedIsClaimable: the bridge pays out against a locked-in HTLC
// and nothing else, so every other state must report itself unclaimable.
func TestOnlyAcceptedIsClaimable(t *testing.T) {
	for s := range invoiceStateNames {
		if got := s.Claimable(); got != (s == InvoiceAccepted) {
			t.Errorf("%v.Claimable() = %v", s, got)
		}
	}
	if InvoiceState(200).Claimable() {
		t.Error("an unknown invoice state must not be claimable")
	}
}

// TestPaymentHashIsSHA256OnBothChains pins the fact the whole bridge rests on.
//
// The BLAKE2b fork changed how blocks are hashed, not how payments are. Both
// networks build the same HTLC around SHA256(preimage), which is why one
// preimage claims both legs and why a swap needs no new cryptography. If this
// ever stopped being true, every swap would be a gift: the bridge would pay
// the outgoing leg and hold a preimage that settles nothing on the incoming
// one.
func TestPaymentHashIsSHA256OnBothChains(t *testing.T) {
	for _, c := range []struct {
		name string
		pre  Preimage
		want string
	}{
		{"all zeroes", Preimage{},
			"66687aadf862bd776c8fc18b8e9f8e20089714856ee233b3902a591d0d5f2925"},
		{"one then zeroes", Preimage{1},
			"01d0fabd251fcbbe2b93b4b927b26ad2a1a99077152e45ded1e678afa45dbec5"},
		{"all ones", func() Preimage {
			var p Preimage
			for i := range p {
				p[i] = 0xff
			}

			return p
		}(),
			"af9613760f72635fbdb44a5a0a63c39f12af30f950a6ee5c971be188e89c4051"},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := c.pre.Hash()
			if got := hex.EncodeToString(h[:]); got != c.want {
				t.Errorf("got %s, want %s", got, c.want)
			}
		})
	}
}

// TestDistinctPreimagesGiveDistinctHashes, because the hash is what tells two
// swaps apart in the store and on both nodes.
func FuzzPreimageHash(f *testing.F) {
	f.Add([]byte{1}, []byte{2})
	f.Fuzz(func(t *testing.T, a, b []byte) {
		var pa, pb Preimage
		copy(pa[:], a)
		copy(pb[:], b)
		if (pa == pb) != (pa.Hash() == pb.Hash()) {
			t.Errorf("%x and %x: equal preimages %v, equal hashes %v",
				pa, pb, pa == pb, pa.Hash() == pb.Hash())
		}
	})
}

// TestTimeoutSecondsRoundsUpAndNeverToNone: a timeout cut to an invoice's last
// seconds reaches lnd as whole seconds, rounded up so the attempt is not cut
// shorter than asked, and never as zero, which lnd reads as its own default of
// a minute.
func TestTimeoutSecondsRoundsUpAndNeverToNone(t *testing.T) {
	for _, c := range []struct {
		in   time.Duration
		want int32
	}{
		{-time.Second, 1},
		{0, 1},
		{time.Millisecond, 1},
		{time.Second, 1},
		{time.Second + time.Millisecond, 2},
		{11 * time.Second, 11},
		{2 * time.Minute, 120},
		{math.MaxInt32 * time.Second, math.MaxInt32},
		{math.MaxInt64, math.MaxInt32},
	} {
		if got := TimeoutSeconds(c.in); got != c.want {
			t.Errorf("%v: %d, want %d", c.in, got, c.want)
		}
	}
}
