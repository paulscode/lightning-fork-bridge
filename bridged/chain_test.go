package bridged

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/paulscode/lightning-fork-bridge/chainrate"
	"github.com/paulscode/lightning-fork-bridge/lnd"
)

// fakeChain is a node's chain: one block every spacing up to the tip, the
// last arriving now, with the difficulty doubling at every retarget.
type fakeChain struct {
	tip       int32
	spacing   time.Duration
	now       time.Time
	noHeaders bool
}

func (f *fakeChain) at(h int32) lnd.Header {
	// Doubling the difficulty halves the target: the exponent byte stays
	// and the mantissa halves, from 0x7fffff down.
	bits := uint32(0x1d000000) | (0x7fffff >> uint(h/2016%16))

	return lnd.Header{
		Height: h,
		Time:   f.now.Add(-time.Duration(f.tip-h) * f.spacing),
		Bits:   bits,
	}
}

func (f *fakeChain) Tip(context.Context) (lnd.Tip, error) {
	return lnd.Tip{Height: f.tip, Time: f.at(f.tip).Time, Synced: true}, nil
}

func (f *fakeChain) Header(_ context.Context, h int32) (lnd.Header, error) {
	if f.noHeaders {
		return lnd.Header{}, status.Error(codes.Unimplemented,
			"unknown service chainrpc.ChainKit")
	}

	return f.at(h), nil
}

// chained is a service whose two observers are fed from fake chains.
func chained(t *testing.T, lf, btc *fakeChain) *Service {
	t.Helper()

	params := chainrate.Params{
		RetargetInterval: 2016, TargetSpacing: 10 * time.Minute,
		SurgeFactor: 3, StallFactor: 4, MinSamples: MinSamplesFloor,
		StaleAfter: time.Hour, SurgeWindow: SurgeWindowFloor,
	}
	s := &Service{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	var err error
	if s.lfChain, err = chainrate.New(params); err != nil {
		t.Fatal(err)
	}
	if s.btcChain, err = chainrate.New(params); err != nil {
		t.Fatal(err)
	}
	s.chainSrcOnce.Do(func() {
		s.chainSrcs = []*chainSource{
			{name: "blake2b", reader: lf, obs: s.lfChain},
			{name: "bitcoin", reader: btc, obs: s.btcChain},
		}
	})

	return s
}

// TestABackfilledChainIsMeasurableJustAfterARetarget: twenty blocks into a
// new epoch there are far fewer than MinSamples of it, and the observer can
// estimate only by scaling the previous epoch by the difficulty change, which
// comes from the headers' bits. Backfilled history with difficulty makes a
// chain measurable at once; without the difficulty it would wait most of a
// day.
func TestABackfilledChainIsMeasurableJustAfterARetarget(t *testing.T) {
	now := time.Now()
	lf := &fakeChain{tip: 5*2016 + 20, spacing: 5 * time.Minute, now: now}
	btc := &fakeChain{tip: 7*2016 + 20, spacing: 10 * time.Minute, now: now}
	s := chained(t, lf, btc)

	s.backfill(context.Background())

	for _, c := range []struct {
		name string
		obs  *chainrate.Observer
	}{{"blake2b", s.lfChain}, {"bitcoin", s.btcChain}} {
		est, err := s.estimate(c.obs)
		if err != nil {
			t.Fatalf("%s: not measurable after the backfill: %v", c.name,
				err)
		}
		if est.Samples >= MinSamplesFloor {
			t.Errorf("%s: %d samples; the test means to be just past "+
				"a retarget", c.name, est.Samples)
		}
	}

	// The tips, sampled as they come, carry their difficulty too, and a
	// tip already read is not read again.
	lf.tip++
	s.sampleChain(context.Background(), s.chains()[0])
	if got := s.chains()[0].headerAt; got != lf.tip {
		t.Errorf("the new tip's header was not read: at %d, tip %d", got,
			lf.tip)
	}
	if _, err := s.estimate(s.lfChain); err != nil {
		t.Errorf("not measurable after the next tip: %v", err)
	}
}

// TestANodeWithoutHeadersLearnsFromTips: a stock lnd without chainrpc serves
// no headers. The daemon does not stop; it samples tips, without difficulty,
// and waits for enough of them.
func TestANodeWithoutHeadersLearnsFromTips(t *testing.T) {
	now := time.Now()
	lf := &fakeChain{tip: 5*2016 + 20, spacing: 5 * time.Minute, now: now,
		noHeaders: true}
	s := chained(t, lf, lf)

	s.backfill(context.Background())
	s.sampleChain(context.Background(), s.chains()[0])

	if n := s.lfChain.Len(); n != 1 {
		t.Errorf("%d blocks observed, want the one tip", n)
	}
	if !s.chains()[0].noHeaders {
		t.Error("the missing headers were not noticed")
	}
}
