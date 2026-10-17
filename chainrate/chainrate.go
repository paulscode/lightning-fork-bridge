// Package chainrate turns observed blocks into the spacing bounds a swap may
// rely on.
//
// Difficulty is fixed inside a retarget epoch, so any variation in block rate
// within one is hashrate moving, which is the thing that can happen without
// warning. Measured over the BLAKE2b chain's first 10,514 blocks (S11,
// 2026-09-14), no window of 144 blocks or more ever ran more than about 2.3x
// faster than its own epoch's mean, and 2.84x at the short and noisy end. The
// surge factor is that observation with margin.
//
// The bounds deliberately come from the current epoch rather than from a
// trailing window of fixed length. A window spanning a retarget mixes two
// difficulties, and while that errs safe across an upward retarget it errs
// dangerously across a downward one, where blocks get faster and a stale mean
// would say they cannot.
package chainrate

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/paulscode/lightning-fork-bridge/margin"
)

var (
	// ErrInsufficient is returned when too few blocks have been seen in the
	// current epoch to say anything, and no previous epoch is available to
	// fall back on.
	ErrInsufficient = errors.New("not enough blocks to establish bounds")

	// ErrStale is returned when the newest block is too old for the
	// observation to describe the present.
	ErrStale = errors.New("block observations are stale")

	// ErrIncoherent is returned when observed timestamps cannot yield a
	// positive spacing, which means the data is wrong rather than the chain
	// being unusual.
	ErrIncoherent = errors.New("observed timestamps do not yield a spacing")

	// ErrSurge is returned when blocks have recently arrived faster than the
	// fast bound itself, meaning the surge factor has been exceeded and the
	// assumptions behind every outstanding quote no longer hold. Stop
	// quoting and re-examine.
	ErrSurge = errors.New("recent blocks are faster than the fast bound")

	// ErrParams is returned for an unusable configuration.
	ErrParams = errors.New("chain parameters are not usable")
)

// Params describes a chain's retarget behaviour and how much slack to allow.
type Params struct {
	// RetargetInterval is how many blocks share one difficulty. 2016 on
	// both chains.
	RetargetInterval int32

	// TargetSpacing is what the chain aims for, used only for staleness.
	TargetSpacing time.Duration

	// SurgeFactor is how much faster than the epoch mean a window may run.
	// The fast bound is the observed spacing divided by this.
	SurgeFactor float64

	// StallFactor is how much slower. The slow bound is the observed
	// spacing multiplied by this. It is larger than SurgeFactor because
	// hashrate can leave instantly while difficulty falls at most fourfold
	// per retarget, so a stall persists for epochs.
	StallFactor float64

	// MinSamples is the fewest blocks in the current epoch before its own
	// mean is trusted on its own.
	MinSamples int

	// StaleAfter is how old the newest block may be. Zero means ten times
	// TargetSpacing.
	StaleAfter time.Duration

	// SurgeWindow is the number of recent blocks checked against the fast
	// bound to detect a breach. Zero disables the check.
	SurgeWindow int
}

// BlakeParams matches the BLAKE2b chain as measured on 2026-09-14: 2016 block
// epochs, a ten minute target it has not yet settled at, and a surge factor
// above the 2.84x worst case seen in its first two weeks.
var BlakeParams = Params{
	RetargetInterval: 2016,
	TargetSpacing:    10 * time.Minute,
	SurgeFactor:      3,
	StallFactor:      4,
	MinSamples:       100,
	SurgeWindow:      144,
}

// BitcoinParams is the same shape for the SHA256 chain, which sits close to
// its target and varies far less, so the factors are tighter.
//
// The stall factor is measured: the slowest stretch in the chain's history,
// the 2021 mining ban (heights 688824-689208, about four days), ran 1.38x the
// spacing the observer would have measured when it began (1.26x the target,
// running 1.75x). 1.5 covers it. It is that low only because the slow bound
// takes the slower of the current and previous epochs (SlowSpacing); taken
// from the faster, as it once was, 2 was needed.
var BitcoinParams = Params{
	RetargetInterval: 2016,
	TargetSpacing:    10 * time.Minute,
	SurgeFactor:      2,
	StallFactor:      1.5,
	MinSamples:       100,
	SurgeWindow:      144,
}

// Valid reports whether the parameters can be used.
func (p Params) Valid() bool {
	return p.RetargetInterval > 0 && p.TargetSpacing > 0 &&
		p.SurgeFactor >= 1 && p.StallFactor >= 1 &&
		p.MinSamples >= 2 && p.StaleAfter >= 0 && p.SurgeWindow >= 0
}

func (p Params) staleAfter() time.Duration {
	if p.StaleAfter > 0 {
		return p.StaleAfter
	}

	return 10 * p.TargetSpacing
}

// Block is one observation: a height, the header's timestamp, and the
// difficulty that height was mined at.
type Block struct {
	Height     int32
	Time       time.Time
	Difficulty float64
}

// Observer accumulates blocks and derives bounds from them. It is not safe for
// concurrent use; callers that need that should guard it.
type Observer struct {
	params Params

	// blocks is kept sorted by height, with duplicates collapsed, and
	// trimmed to the current and previous epoch.
	blocks []Block
}

// New returns an Observer for a chain.
func New(p Params) (*Observer, error) {
	if !p.Valid() {
		return nil, fmt.Errorf("%w: %+v", ErrParams, p)
	}

	return &Observer{params: p}, nil
}

// epoch returns the retarget epoch a height belongs to.
func (o *Observer) epoch(height int32) int32 {
	return height / o.params.RetargetInterval
}

// Add records a block. Blocks may arrive in any order and may repeat; a repeat
// of a known height replaces the earlier observation, since a reorg can change
// what is at a height.
func (o *Observer) Add(b Block) error {
	if b.Height < 0 {
		return fmt.Errorf("%w: height %d", ErrParams, b.Height)
	}
	if b.Time.IsZero() {
		return fmt.Errorf("%w: height %d has no timestamp", ErrParams, b.Height)
	}

	i := sort.Search(len(o.blocks), func(i int) bool {
		return o.blocks[i].Height >= b.Height
	})
	switch {
	case i < len(o.blocks) && o.blocks[i].Height == b.Height:
		o.blocks[i] = b
	default:
		o.blocks = append(o.blocks, Block{})
		copy(o.blocks[i+1:], o.blocks[i:])
		o.blocks[i] = b
	}

	o.trim()

	return nil
}

// trim drops blocks older than the previous epoch, which is all the history
// any estimate uses.
func (o *Observer) trim() {
	if len(o.blocks) == 0 {
		return
	}
	tipEpoch := o.epoch(o.blocks[len(o.blocks)-1].Height)
	keepFrom := (tipEpoch - 1) * o.params.RetargetInterval
	if keepFrom < 0 {
		return
	}
	i := sort.Search(len(o.blocks), func(i int) bool {
		return o.blocks[i].Height >= keepFrom
	})
	if i > 0 {
		o.blocks = append(o.blocks[:0:0], o.blocks[i:]...)
	}
}

// Len reports how many observations are retained.
func (o *Observer) Len() int { return len(o.blocks) }

// Estimate is what the observer concluded, with the working shown so an
// operator can see why a swap was sized the way it was.
type Estimate struct {
	// Bounds is what margin consumes.
	Bounds margin.Bounds

	// Spacing is the mean seconds per block the fast bound derives from:
	// the current epoch's own mean, or the shorter of that and the previous
	// epoch's mean scaled by the difficulty change.
	Spacing time.Duration

	// SlowSpacing is the one the slow bound derives from: the longer of the
	// two. Each bound takes the more cautious estimate for its own side. A
	// chain that has slowed this epoch reads near its target from the
	// previous one, scaled; the slow bound taken from that read SHA256's
	// 2021 slowdown (blocks 1.75x the target over four days) as 1.0x.
	SlowSpacing time.Duration

	// Epoch is the retarget epoch the tip is in.
	Epoch int32

	// Samples is how many blocks of the current epoch were used.
	Samples int

	// FromPreviousEpoch is true when the previous epoch, scaled by the
	// difficulty change, was the more conservative estimate and so set the
	// spacing. Early in an epoch this is expected.
	FromPreviousEpoch bool
}

// meanSpacing returns the mean time per block across a run of observations.
func meanSpacing(blocks []Block) (time.Duration, error) {
	if len(blocks) < 2 {
		return 0, ErrInsufficient
	}
	first, last := blocks[0], blocks[len(blocks)-1]
	span := last.Time.Sub(first.Time)
	steps := int64(last.Height - first.Height)
	if steps <= 0 {
		return 0, fmt.Errorf("%w: heights %d..%d", ErrIncoherent,
			first.Height, last.Height)
	}
	if span <= 0 {
		// Header timestamps may step backwards between neighbours, but
		// not across a whole window. If they do, the feed is wrong.
		return 0, fmt.Errorf("%w: %d blocks span %v", ErrIncoherent,
			steps, span)
	}

	return span / time.Duration(steps), nil
}

// maxDifficulty is the greatest difficulty among blocks, zero when none is
// known.
func maxDifficulty(blocks []Block) float64 {
	var d float64
	for _, b := range blocks {
		if b.Difficulty > d {
			d = b.Difficulty
		}
	}

	return d
}

// Estimate derives the bounds from what has been observed.
//
// It returns an error rather than a guess whenever the observations cannot
// support a conclusion: too few of them, too old, incoherent, or contradicted
// by blocks arriving faster than the fast bound. Every one of those means the
// caller should stop quoting, not carry on with a default.
func (o *Observer) Estimate(now time.Time) (Estimate, error) {
	var est Estimate
	if len(o.blocks) == 0 {
		return est, ErrInsufficient
	}
	tip := o.blocks[len(o.blocks)-1]
	if age := now.Sub(tip.Time); age > o.params.staleAfter() {
		return est, fmt.Errorf("%w: newest block is %v old", ErrStale, age)
	}

	tipEpoch := o.epoch(tip.Height)
	est.Epoch = tipEpoch

	// Split the retained blocks at the epoch boundary.
	boundary := tipEpoch * o.params.RetargetInterval
	i := sort.Search(len(o.blocks), func(i int) bool {
		return o.blocks[i].Height >= boundary
	})
	current, previous := o.blocks[i:], o.blocks[:i]
	est.Samples = len(current)

	currentMean, currentErr := meanSpacing(current)

	// The previous epoch's mean, scaled by how much difficulty changed. If
	// hashrate is unchanged, spacing moves in proportion to difficulty, so
	// this is what the current epoch should look like.
	var scaled time.Duration
	var haveScaled bool
	if prevMean, err := meanSpacing(previous); err == nil {
		// Each epoch's difficulty is the greatest any of its blocks
		// shows, not one block's: a test network mines blocks at
		// difficulty one when its chain is slow, and a block read without
		// its header has none. Either, read as the epoch's, would scale
		// the previous epoch by orders of magnitude.
		prevDiff := maxDifficulty(previous)
		curDiff := maxDifficulty(current)
		if prevDiff > 0 && curDiff > 0 {
			scaled = time.Duration(float64(prevMean) * curDiff / prevDiff)
			haveScaled = scaled > 0
		}
	}

	// The slow bound's spacing: the longer of the two, where both exist.
	slow := currentMean
	switch {
	case currentErr != nil && haveScaled:
		slow = scaled
	case currentErr == nil && haveScaled && scaled > currentMean:
		slow = scaled
	}
	est.SlowSpacing = slow

	switch {
	case currentErr == nil && len(current) >= o.params.MinSamples:
		// Enough of the current epoch to stand on its own. Still take the
		// scaled previous epoch if it is more conservative, which guards
		// the case where the epoch has only just turned faster.
		est.Spacing = currentMean
		if haveScaled && scaled < currentMean {
			est.Spacing = scaled
			est.FromPreviousEpoch = true
		}

	case currentErr == nil && haveScaled:
		// Too few blocks this epoch to trust alone; take whichever of the
		// two is more conservative.
		est.Spacing = currentMean
		if scaled < currentMean {
			est.Spacing = scaled
			est.FromPreviousEpoch = true
		}

	case haveScaled:
		est.Spacing = scaled
		est.FromPreviousEpoch = true

	case currentErr == nil:
		return est, fmt.Errorf("%w: %d blocks this epoch, none before",
			ErrInsufficient, len(current))

	default:
		// Propagate what meanSpacing actually said. Too few blocks is
		// ErrInsufficient and means wait; timestamps that cannot yield a
		// spacing is ErrIncoherent and means the feed is wrong. Telling an
		// operator to wait for blocks when their data source is broken
		// would be the worse of the two mistakes.
		return est, fmt.Errorf("current epoch: %w", currentErr)
	}

	if est.Spacing <= 0 {
		return est, fmt.Errorf("%w: spacing %v", ErrIncoherent, est.Spacing)
	}

	if est.SlowSpacing < est.Spacing {
		est.SlowSpacing = est.Spacing
	}
	est.Bounds = margin.Bounds{
		Fast: time.Duration(float64(est.Spacing) / o.params.SurgeFactor),
		Slow: time.Duration(float64(est.SlowSpacing) * o.params.StallFactor),
	}
	if est.Bounds.Fast <= 0 {
		return est, fmt.Errorf("%w: fast bound %v", ErrIncoherent,
			est.Bounds.Fast)
	}
	if !est.Bounds.Valid() {
		return est, fmt.Errorf("%w: %+v", ErrIncoherent, est.Bounds)
	}

	// The runtime guard. If blocks have recently come in faster than the
	// fast bound, the surge factor has already been exceeded and every
	// outstanding quote was sized on an assumption that no longer holds.
	if err := checkSurge(current, o.params.SurgeWindow, est.Bounds.Fast); err != nil {
		return est, err
	}

	return est, nil
}

// checkSurge compares the most recent window against the fast bound.
//
// The window is taken from the current epoch only. A window spanning a
// retarget mixes two difficulties, and after an upward retarget the older,
// faster blocks in it would drag the mean below the new fast bound and report
// a surge that is not happening, stopping quoting for a whole window after
// every retarget.
func checkSurge(current []Block, n int, fast time.Duration) error {
	if n <= 0 || len(current) < n+1 {
		return nil
	}
	recent := current[len(current)-(n+1):]
	got, err := meanSpacing(recent)
	if err != nil {
		// An incoherent tail is not evidence of a surge; the caller will
		// see incoherence elsewhere if it matters.
		return nil
	}
	if got < fast {
		return fmt.Errorf("%w: last %d blocks averaged %v against a fast "+
			"bound of %v", ErrSurge, n, got, fast)
	}

	return nil
}
