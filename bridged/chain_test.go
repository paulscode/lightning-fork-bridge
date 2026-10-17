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

	// headerSkew moves the time Header reads, and not the tip's: a header
	// misread. ahead is a height whose block claims a time three hours on.
	headerSkew time.Duration
	ahead      int32
}

func (f *fakeChain) at(h int32) lnd.Header {
	// Doubling the difficulty halves the target: the exponent byte stays
	// and the mantissa halves, from 0x7fffff down.
	bits := uint32(0x1d000000) | (0x7fffff >> uint(h/2016%16))

	at := f.now.Add(-time.Duration(f.tip-h) * f.spacing)
	if h == f.ahead && h != 0 {
		at = f.now.Add(3 * time.Hour)
	}

	return lnd.Header{Height: h, Time: at, Bits: bits}
}

func (f *fakeChain) Tip(context.Context) (lnd.Tip, error) {
	return lnd.Tip{Height: f.tip, Time: f.at(f.tip).Time, Synced: true}, nil
}

func (f *fakeChain) Header(_ context.Context, h int32) (lnd.Header, error) {
	if f.noHeaders {
		return lnd.Header{}, status.Error(codes.Unimplemented,
			"unknown service chainrpc.ChainKit")
	}

	hdr := f.at(h)
	hdr.Time = hdr.Time.Add(f.headerSkew)

	return hdr, nil
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

// TestAHeaderTheNodeDisagreesWithIsNotSampled: the node's best header time and
// the tip's header, read through ChainKit, are the same block's time decoded
// twice. When they differ one of them is misread, and the tip is not sampled.
func TestAHeaderTheNodeDisagreesWithIsNotSampled(t *testing.T) {
	now := time.Now()
	lf := &fakeChain{tip: 5*2016 + 200, spacing: 5 * time.Minute, now: now,
		headerSkew: 600 * time.Second}
	s := chained(t, lf, lf)

	s.sampleChain(context.Background(), s.chains()[0])
	if n := s.lfChain.Len(); n != 0 {
		t.Errorf("%d blocks observed from a tip whose header disagrees", n)
	}

	lf.headerSkew = 0
	s.sampleChain(context.Background(), s.chains()[0])
	if n := s.lfChain.Len(); n != 1 {
		t.Errorf("%d blocks observed once the two agree, want 1", n)
	}
}

// TestABlockFromTheFutureIsNotSampled: a block time more than two hours ahead
// is past what either chain accepts, so it is misread or made up, and at the
// tip it would make the chain look stalled. It is dropped, from the backfill
// and from the tip alike.
func TestABlockFromTheFutureIsNotSampled(t *testing.T) {
	now := time.Now()
	lf := &fakeChain{tip: 5*2016 + 200, spacing: 5 * time.Minute, now: now,
		ahead: 5*2016 + 150}
	s := chained(t, lf, lf)

	s.backfill(context.Background())
	if n := s.lfChain.Len(); n != backfillBlocks-1 {
		t.Errorf("%d blocks backfilled, want %d without the one ahead", n,
			backfillBlocks-1)
	}

	lf.tip++
	lf.ahead = lf.tip
	s.sampleChain(context.Background(), s.chains()[0])
	if n := s.lfChain.Len(); n != backfillBlocks-1 {
		t.Errorf("a tip three hours ahead was sampled: %d blocks", n)
	}
}
