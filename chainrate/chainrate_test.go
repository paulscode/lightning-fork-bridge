package chainrate

import (
	"encoding/csv"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

// steady builds a run of blocks at an exact spacing, all at one difficulty.
func steady(from int32, n int, start time.Time, spacing time.Duration,
	difficulty float64) []Block {

	out := make([]Block, n)
	for i := range out {
		out[i] = Block{
			Height:     from + int32(i),
			Time:       start.Add(time.Duration(i) * spacing),
			Difficulty: difficulty,
		}
	}

	return out
}

func feed(t *testing.T, o *Observer, blocks []Block) {
	t.Helper()
	for _, b := range blocks {
		if err := o.Add(b); err != nil {
			t.Fatalf("Add(%d): %v", b.Height, err)
		}
	}
}

// last returns the timestamp just after the final block, so Estimate is not
// tripped by staleness.
func last(blocks []Block) time.Time {
	return blocks[len(blocks)-1].Time.Add(time.Second)
}

func TestSteadyChain(t *testing.T) {
	o, err := New(BlakeParams)
	if err != nil {
		t.Fatal(err)
	}
	// One full epoch at exactly 500 s a block.
	blocks := steady(2016, 500, time.Unix(1_700_000_000, 0), 500*time.Second, 1e9)
	feed(t, o, blocks)

	est, err := o.Estimate(last(blocks))
	if err != nil {
		t.Fatal(err)
	}
	if est.Spacing != 500*time.Second {
		t.Errorf("spacing %v, want 500s", est.Spacing)
	}
	// Surge 3, stall 4.
	if want := 500 * time.Second / 3; est.Bounds.Fast != want {
		t.Errorf("fast %v, want %v", est.Bounds.Fast, want)
	}
	if want := 2000 * time.Second; est.Bounds.Slow != want {
		t.Errorf("slow %v, want %v", est.Bounds.Slow, want)
	}
	if est.FromPreviousEpoch {
		t.Error("a full epoch should stand on its own")
	}
	if est.Epoch != 1 {
		t.Errorf("epoch %d, want 1", est.Epoch)
	}
}

// TestDownwardRetargetIsTheDangerousOne is why the estimate is taken from the
// current epoch rather than a trailing window. Difficulty falls, blocks get
// faster, and a mean that still included the slow pre-retarget blocks would
// claim a fast bound the chain has already beaten.
func TestDownwardRetargetIsTheDangerousOne(t *testing.T) {
	p := BlakeParams
	p.SurgeWindow = 0 // the surge guard is tested separately
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)

	// Previous epoch: slow blocks at high difficulty.
	prev := steady(2016, 2016, base, 1000*time.Second, 4e9)
	feed(t, o, prev)

	// New epoch: difficulty quartered, blocks four times faster.
	cur := steady(4032, 300, last(prev), 250*time.Second, 1e9)
	feed(t, o, cur)

	est, err := o.Estimate(last(cur))
	if err != nil {
		t.Fatal(err)
	}
	// The current epoch says 250 s. The scaled previous epoch says
	// 1000 * (1e9/4e9) = 250 s too, so they agree and neither is stale.
	if est.Spacing > 260*time.Second {
		t.Fatalf("spacing %v: a trailing window would have said far more",
			est.Spacing)
	}
	// The fast bound must be below the rate the chain is actually running.
	if est.Bounds.Fast >= 250*time.Second {
		t.Errorf("fast bound %v is not below the observed 250s",
			est.Bounds.Fast)
	}
}

// TestFreshEpochUsesScaledPrevious checks the fallback that keeps the bridge
// quoting in the first hours after a retarget.
func TestFreshEpochUsesScaledPrevious(t *testing.T) {
	p := BlakeParams
	p.SurgeWindow = 0
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)

	// A full previous epoch at 200 s and difficulty 1e9.
	prev := steady(2016, 2016, base, 200*time.Second, 1e9)
	feed(t, o, prev)

	// Difficulty triples. Only ten blocks so far, and by luck they are
	// fast, which the scaled estimate should override.
	cur := steady(4032, 10, last(prev), 100*time.Second, 3e9)
	feed(t, o, cur)

	est, err := o.Estimate(last(cur))
	if err != nil {
		t.Fatal(err)
	}
	if est.Samples != 10 {
		t.Errorf("samples %d, want 10", est.Samples)
	}
	// Scaled previous says 200 * 3 = 600 s; the ten fresh blocks say 100 s.
	// The conservative choice is the smaller, so 100 s stands and the flag
	// stays false.
	if est.Spacing != 100*time.Second {
		t.Errorf("spacing %v, want the more conservative 100s", est.Spacing)
	}
	if est.FromPreviousEpoch {
		t.Error("the current epoch was the conservative one here")
	}

	// Now make the fresh blocks slow, so the scaled previous epoch is the
	// conservative one and gets used.
	o2, _ := New(p)
	feed(t, o2, prev)
	slowCur := steady(4032, 10, last(prev), 5000*time.Second, 3e9)
	feed(t, o2, slowCur)

	est, err = o2.Estimate(last(slowCur))
	if err != nil {
		t.Fatal(err)
	}
	if est.Spacing != 600*time.Second {
		t.Errorf("spacing %v, want the scaled 600s", est.Spacing)
	}
	if !est.FromPreviousEpoch {
		t.Error("FromPreviousEpoch should be set when it supplied the number")
	}
}

// TestSurgeGuard is the runtime net: blocks arriving faster than the fast
// bound mean the surge factor has been exceeded and quoting must stop.
func TestSurgeGuard(t *testing.T) {
	p := BlakeParams
	p.SurgeWindow = 20
	p.MinSamples = 10
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)

	// An epoch at 600 s gives a fast bound of 200 s.
	blocks := steady(2016, 400, base, 600*time.Second, 1e9)
	feed(t, o, blocks)
	if _, err := o.Estimate(last(blocks)); err != nil {
		t.Fatalf("a steady chain should estimate cleanly: %v", err)
	}

	// Now twenty blocks at 60 s, far inside the fast bound.
	surge := steady(2416, 21, last(blocks), 60*time.Second, 1e9)
	feed(t, o, surge)

	_, err = o.Estimate(last(surge))
	if !errors.Is(err, ErrSurge) {
		t.Fatalf("want ErrSurge, got %v", err)
	}
}

func TestStaleness(t *testing.T) {
	o, err := New(BlakeParams)
	if err != nil {
		t.Fatal(err)
	}
	blocks := steady(2016, 200, time.Unix(1_700_000_000, 0), 500*time.Second, 1e9)
	feed(t, o, blocks)

	fresh := last(blocks)
	if _, err := o.Estimate(fresh); err != nil {
		t.Fatalf("fresh: %v", err)
	}
	// Default staleness is ten target spacings, so 100 minutes.
	if _, err := o.Estimate(fresh.Add(101 * time.Minute)); !errors.Is(err, ErrStale) {
		t.Errorf("want ErrStale, got %v", err)
	}
}

func TestInsufficient(t *testing.T) {
	o, err := New(BlakeParams)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	if _, err := o.Estimate(now); !errors.Is(err, ErrInsufficient) {
		t.Errorf("empty: want ErrInsufficient, got %v", err)
	}

	// A single block cannot give a spacing and there is no earlier epoch.
	one := steady(2016, 1, now, time.Minute, 1e9)
	feed(t, o, one)
	if _, err := o.Estimate(last(one)); !errors.Is(err, ErrInsufficient) {
		t.Errorf("one block: want ErrInsufficient, got %v", err)
	}
}

func TestIncoherentTimestamps(t *testing.T) {
	p := BlakeParams
	p.MinSamples = 2
	p.SurgeWindow = 0
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)

	// Every timestamp identical: no span, so no spacing.
	for i := 0; i < 10; i++ {
		if err := o.Add(Block{
			Height: 2016 + int32(i), Time: base, Difficulty: 1e9,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o.Estimate(base.Add(time.Second)); !errors.Is(err, ErrIncoherent) {
		t.Errorf("want ErrIncoherent, got %v", err)
	}
}

// TestBackwardsNeighbourIsTolerated: header timestamps may step backwards
// between neighbours without the window as a whole being wrong. Sixteen such
// steps appear in the real data.
func TestBackwardsNeighbourIsTolerated(t *testing.T) {
	p := BlakeParams
	p.MinSamples = 10
	p.SurgeWindow = 0
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	blocks := steady(2016, 200, base, 600*time.Second, 1e9)
	// Push one block back behind its predecessor.
	blocks[100].Time = blocks[99].Time.Add(-500 * time.Second)
	feed(t, o, blocks)

	est, err := o.Estimate(last(blocks))
	if err != nil {
		t.Fatalf("one backwards step must not break the estimate: %v", err)
	}
	if est.Spacing != 600*time.Second {
		t.Errorf("spacing %v, want 600s: the ends set the mean, not the "+
			"individual steps", est.Spacing)
	}
}

func TestAddOrderAndDuplicates(t *testing.T) {
	p := BlakeParams
	p.MinSamples = 2
	p.SurgeWindow = 0
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	blocks := steady(2016, 50, base, 600*time.Second, 1e9)

	// Insert backwards.
	for i := len(blocks) - 1; i >= 0; i-- {
		if err := o.Add(blocks[i]); err != nil {
			t.Fatal(err)
		}
	}
	if o.Len() != 50 {
		t.Fatalf("retained %d, want 50", o.Len())
	}
	est, err := o.Estimate(last(blocks))
	if err != nil {
		t.Fatal(err)
	}
	if est.Spacing != 600*time.Second {
		t.Errorf("out of order gave %v", est.Spacing)
	}

	// A repeat at a known height replaces it, as a reorg would.
	replaced := blocks[10]
	replaced.Time = replaced.Time.Add(time.Hour)
	if err := o.Add(replaced); err != nil {
		t.Fatal(err)
	}
	if o.Len() != 50 {
		t.Errorf("a replacement changed the count to %d", o.Len())
	}

	// Rejections.
	if err := o.Add(Block{Height: -1, Time: base}); !errors.Is(err, ErrParams) {
		t.Errorf("negative height: %v", err)
	}
	if err := o.Add(Block{Height: 10}); !errors.Is(err, ErrParams) {
		t.Errorf("zero timestamp: %v", err)
	}
}

func TestTrimKeepsTwoEpochs(t *testing.T) {
	p := BlakeParams
	p.RetargetInterval = 100
	p.MinSamples = 2
	p.SurgeWindow = 0
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)
	// Five epochs' worth.
	feed(t, o, steady(0, 500, base, 600*time.Second, 1e9))

	// The tip is at height 499, epoch 4, so everything below epoch 3
	// (height 300) is dropped.
	if o.Len() != 200 {
		t.Errorf("retained %d, want 200", o.Len())
	}
	if o.blocks[0].Height != 300 {
		t.Errorf("oldest retained height %d, want 300", o.blocks[0].Height)
	}
}

func TestBadParams(t *testing.T) {
	for _, p := range []Params{
		{},
		{RetargetInterval: 0, TargetSpacing: time.Minute, SurgeFactor: 2, StallFactor: 2, MinSamples: 2},
		{RetargetInterval: 2016, TargetSpacing: 0, SurgeFactor: 2, StallFactor: 2, MinSamples: 2},
		{RetargetInterval: 2016, TargetSpacing: time.Minute, SurgeFactor: 0.5, StallFactor: 2, MinSamples: 2},
		{RetargetInterval: 2016, TargetSpacing: time.Minute, SurgeFactor: 2, StallFactor: 0.5, MinSamples: 2},
		{RetargetInterval: 2016, TargetSpacing: time.Minute, SurgeFactor: 2, StallFactor: 2, MinSamples: 1},
		{RetargetInterval: 2016, TargetSpacing: time.Minute, SurgeFactor: 2, StallFactor: 2, MinSamples: 2, SurgeWindow: -1},
	} {
		if _, err := New(p); !errors.Is(err, ErrParams) {
			t.Errorf("New(%+v) = %v, want ErrParams", p, err)
		}
	}
}

// TestAgainstRealChain replays the last two epochs of the BLAKE2b chain and
// checks the bounds land where S11 said they should.
func TestAgainstRealChain(t *testing.T) {
	f, err := os.Open("testdata/btcb2-tail.csv")
	if err != nil {
		t.Skipf("no recorded chain data: %v", err)
	}
	defer f.Close()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	o, err := New(BlakeParams)
	if err != nil {
		t.Fatal(err)
	}
	var newest time.Time
	for _, r := range rows[1:] {
		h, _ := strconv.Atoi(r[0])
		ts, _ := strconv.ParseInt(r[1], 10, 64)
		d, _ := strconv.ParseFloat(r[2], 64)
		at := time.Unix(ts, 0)
		if at.After(newest) {
			newest = at
		}
		if err := o.Add(Block{
			Height: int32(h), Time: at, Difficulty: d,
		}); err != nil {
			t.Fatal(err)
		}
	}

	est, err := o.Estimate(newest.Add(time.Minute))
	if err != nil {
		t.Fatalf("real chain data should estimate cleanly: %v", err)
	}
	t.Logf("epoch %d, %d samples, spacing %v, fast %v, slow %v, "+
		"from previous epoch %v", est.Epoch, est.Samples, est.Spacing,
		est.Bounds.Fast, est.Bounds.Slow, est.FromPreviousEpoch)

	// S11 measured the final epoch at about 464 s a block, giving a fast
	// bound near 155 s. Allow a wide band: the point is the order of
	// magnitude and that it is nowhere near the ten minute target.
	if est.Spacing < 300*time.Second || est.Spacing > 700*time.Second {
		t.Errorf("spacing %v is outside the measured range", est.Spacing)
	}
	if est.Bounds.Fast < 100*time.Second || est.Bounds.Fast > 240*time.Second {
		t.Errorf("fast bound %v is outside the expected range", est.Bounds.Fast)
	}
	if !est.Bounds.Valid() {
		t.Errorf("bounds %+v are not usable", est.Bounds)
	}

	// The whole point: a BLAKE2b block is worth much less wall-clock than
	// the ten minute target would suggest.
	if est.Bounds.Fast >= 10*time.Minute {
		t.Errorf("fast bound %v: the chain is not running at target",
			est.Bounds.Fast)
	}
}

// TestUpwardRetargetDoesNotFakeASurge covers the defect found in review. After
// difficulty rises, the previous epoch's faster blocks used to sit inside the
// surge window and drag its mean below the new fast bound, reporting a surge
// that was not happening and stopping quoting for a whole window after every
// retarget.
func TestUpwardRetargetDoesNotFakeASurge(t *testing.T) {
	p := BlakeParams
	p.SurgeWindow = 144
	p.MinSamples = 10
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)

	// A full epoch at 200 s, then difficulty triples and blocks settle at
	// 600 s, exactly as the real chain did at height 971,712.
	prev := steady(2016, 2016, base, 200*time.Second, 1e9)
	feed(t, o, prev)
	cur := steady(4032, 200, last(prev), 600*time.Second, 3e9)
	feed(t, o, cur)

	est, err := o.Estimate(last(cur))
	if err != nil {
		t.Fatalf("an ordinary upward retarget must not report a surge: %v", err)
	}
	if est.Spacing != 600*time.Second {
		t.Errorf("spacing %v, want 600s", est.Spacing)
	}

	// The guard must still fire on a real surge inside the new epoch.
	surge := steady(4232, 200, last(cur), 100*time.Second, 3e9)
	feed(t, o, surge)
	if _, err := o.Estimate(last(surge)); !errors.Is(err, ErrSurge) {
		t.Errorf("a genuine surge must still be caught, got %v", err)
	}
}

// TestTheSlowBoundTakesTheSlowerEstimate: a chain that has slowed this epoch,
// as SHA256 did in 2021, is read at its slower current rate for the slow
// bound, not at the target the scaled previous epoch implies; the fast bound
// still takes the faster.
func TestTheSlowBoundTakesTheSlowerEstimate(t *testing.T) {
	p := BitcoinParams
	p.SurgeWindow = 0
	o, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(1_700_000_000, 0)

	// Previous epoch at the target; this one, same difficulty, at 1.26x.
	prev := steady(2016, 2016, base, 600*time.Second, 1e12)
	feed(t, o, prev)
	cur := steady(4032, 1500, last(prev), 756*time.Second, 1e12)
	feed(t, o, cur)

	est, err := o.Estimate(last(cur))
	if err != nil {
		t.Fatal(err)
	}
	if est.Spacing != 600*time.Second {
		t.Errorf("fast side's spacing %v, want the faster 600s", est.Spacing)
	}
	if est.SlowSpacing != 756*time.Second {
		t.Errorf("slow side's spacing %v, want the slower 756s", est.SlowSpacing)
	}
	// 756s x 1.5: above the 1.75x (1050s) the worst four days actually ran.
	if est.Bounds.Slow < 1050*time.Second {
		t.Errorf("slow bound %v would not have covered 2021", est.Bounds.Slow)
	}
}

// TestAnEpochsDifficultyIsItsGreatest: the previous epoch is scaled by the
// difficulty change, and each epoch's difficulty is the greatest its blocks
// show. A test network mines a block at difficulty one when its chain is
// slow, and a block read without its header has none; read as its epoch's,
// either scales the previous epoch by orders of magnitude, a fast bound of
// days or of microseconds, or none at all.
func TestAnEpochsDifficultyIsItsGreatest(t *testing.T) {
	p := BlakeParams
	p.SurgeWindow = 0
	base := time.Unix(1_700_000_000, 0)

	for _, c := range []struct {
		name    string
		prevEnd float64 // the previous epoch's last block's difficulty
		curFrom float64 // the current epoch's first block's
	}{
		{"the previous epoch ends at difficulty one", 1, 3e9},
		{"the current epoch starts at difficulty one", 1e9, 1},
		{"the current epoch's first difficulty is unknown", 1e9, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			o, err := New(p)
			if err != nil {
				t.Fatal(err)
			}
			prev := steady(2016, 2016, base, 200*time.Second, 1e9)
			prev[len(prev)-1].Difficulty = c.prevEnd
			feed(t, o, prev)
			cur := steady(4032, 10, last(prev), 5000*time.Second, 3e9)
			cur[0].Difficulty = c.curFrom
			feed(t, o, cur)

			// 200 s at a third of the difficulty is 600 s, which is
			// faster than the ten blocks so far, so it is the spacing.
			est, err := o.Estimate(last(cur))
			if err != nil {
				t.Fatal(err)
			}
			if est.Spacing != 600*time.Second || !est.FromPreviousEpoch {
				t.Errorf("spacing %v from the previous epoch %v, want "+
					"600s from it", est.Spacing, est.FromPreviousEpoch)
			}
		})
	}
}
