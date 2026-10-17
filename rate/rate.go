// Package rate turns price observations into a rate a swap may be quoted at,
// or into a refusal.
//
// There is one thin market for BTCB2, so the usual defence of comparing
// venues is not available. What is available is that the same venue quotes
// BTCB2 against BTC directly and against dollars, and a mainstream BTC/USD
// price is easy to come by. Dividing the second pair by that price should
// reproduce the first, and on 2026-09-14 it did to under one percent. Two
// readings that are supposed to agree and do not is the strongest signal this
// bridge can get for free, and it is treated as a reason to stop rather than
// as noise to average away.
//
// Everything else here exists because the asset moved 31% peak to trough in an
// ordinary day: a hard staleness limit, a breaker that trips on an abnormal
// move and stays tripped through a cooldown, and a spread that widens with
// what the market has actually been doing rather than with a number somebody
// chose once.
package rate

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

var (
	// ErrStale is returned when nothing recent enough to quote on is
	// available.
	ErrStale = errors.New("no price observation is recent enough")

	// ErrDisagreement is returned when sources that should agree do not.
	// It means one of them is wrong and there is no way to tell which.
	ErrDisagreement = errors.New("price sources disagree")

	// ErrBroken is returned while the circuit breaker is tripped.
	ErrBroken = errors.New("price circuit breaker is tripped")

	// ErrObservation is returned for an unusable reading.
	ErrObservation = errors.New("price observation is not usable")

	// ErrPolicy is returned for an incoherent policy.
	ErrPolicy = errors.New("price policy is not usable")
)

// Observation is one source's reading of the pair, in the direction the swap
// runs.
type Observation struct {
	// Source names where it came from, for logs and for saying which pair
	// disagreed.
	Source string

	// OutgoingPerIncoming is how many outgoing units one incoming unit buys,
	// which is what the quote divides by. Named for the legs rather than for
	// the assets because the bridge runs in both directions and the two are
	// reciprocals: a bridge taking BTCB2 and paying BTC uses the venue's own
	// BTCB2_BTC price, 0.00308078 on 2026-09-14, and one taking BTC and
	// paying BTCB2 uses its inverse.
	//
	// The old name said BTCPerBTCB2, which was true of one direction and a
	// lie in the other, and the formula it feeds does not care which asset
	// is which.
	OutgoingPerIncoming float64

	// At is when the reading was taken, not when it was received.
	At time.Time

	// Venue is the market the reading comes from. Two readings from one
	// venue are one venue's say, however many there are, and Reading.Venues
	// counts each venue once. Empty means the reading's Source.
	Venue string
}

// venue is the market an observation counts as.
func (o Observation) venue() string {
	if o.Venue != "" {
		return o.Venue
	}

	return o.Source
}

// Valid reports whether an observation can be used.
func (o Observation) Valid() bool {
	return o.OutgoingPerIncoming > 0 && !math.IsInf(o.OutgoingPerIncoming, 0) &&
		!math.IsNaN(o.OutgoingPerIncoming) && !o.At.IsZero()
}

// Policy is how careful to be.
type Policy struct {
	// MaxAge is how old an observation may be and still count.
	MaxAge time.Duration

	// MaxDisagreement is the relative gap two sources may show before the
	// pair is refused, as a fraction: 0.05 is five percent.
	MaxDisagreement float64

	// BreakerMove is the relative move within BreakerWindow that trips the
	// breaker.
	BreakerMove float64

	// BreakerWindow is how far back the breaker looks.
	BreakerWindow time.Duration

	// Cooldown is how long the breaker stays tripped once it has fired,
	// measured from the observation that tripped it. It must be positive,
	// or Trip would set an expiry already in the past and an operator's
	// manual stop would silently do nothing.
	//
	// Setting it shorter than BreakerWindow lets the breaker re-trip on the
	// same move as soon as it clears, which is a flapping quote feed rather
	// than a safety feature.
	Cooldown time.Duration

	// BaseSpread is the spread charged in a calm market, as a fraction.
	BaseSpread float64

	// VolatilityMultiple scales the observed move into extra spread.
	VolatilityMultiple float64

	// MaxSpread caps the result, because past some width a quote is not a
	// quote.
	MaxSpread float64
}

// DefaultPolicy is a starting point. The numbers come from the design
// document's measurements and S10 exists to replace them with observed ones.
var DefaultPolicy = Policy{
	MaxAge:             90 * time.Second,
	MaxDisagreement:    0.05,
	BreakerMove:        0.20,
	BreakerWindow:      10 * time.Minute,
	Cooldown:           30 * time.Minute,
	BaseSpread:         0.01,
	VolatilityMultiple: 1.0,
	MaxSpread:          0.10,
}

// Valid reports whether the policy is usable.
func (p Policy) Valid() bool {
	return p.MaxAge > 0 && p.MaxDisagreement > 0 && p.BreakerMove > 0 &&
		p.BreakerWindow > 0 && p.Cooldown > 0 && p.BaseSpread >= 0 &&
		p.VolatilityMultiple >= 0 && p.MaxSpread >= p.BaseSpread
}

// Reading is what the oracle concluded.
type Reading struct {
	// OutgoingPerIncoming is the price to quote from.
	OutgoingPerIncoming float64

	// At is the moment it describes.
	At time.Time

	// Sources is how many independent readings agreed. One is allowed but
	// is worth knowing about, since the cross-check did not happen.
	Sources int

	// Venues is how many distinct venues those readings came from.
	Venues int

	// Lower and Upper are the two middle readings, the same reading when
	// their number is odd. With an even number the median falls between
	// two readings that both agreed, and each direction prices from the one
	// that is worse for the bridge: Lower when OutgoingPerIncoming is used
	// as it is, Upper when it is inverted. OutgoingPerIncoming is the
	// midpoint of the two.
	Lower, Upper float64

	// Spread is the fraction to charge, widened by what the market has
	// recently done.
	Spread float64

	// Volatility is the largest relative move seen inside the breaker
	// window, which is what widened the spread.
	Volatility float64
}

// Oracle combines observations over time, keeping enough history to measure
// volatility and to trip a breaker. It is not safe for concurrent use.
type Oracle struct {
	policy  Policy
	history []Reading
	broken  time.Time // the breaker stays tripped until this moment
	reason  string
}

// New returns an Oracle.
func New(p Policy) (*Oracle, error) {
	if !p.Valid() {
		return nil, fmt.Errorf("%w: %+v", ErrPolicy, p)
	}

	return &Oracle{policy: p}, nil
}

// Broken reports whether the breaker is tripped, and why.
func (o *Oracle) Broken(now time.Time) (bool, string) {
	if now.Before(o.broken) {
		return true, o.reason
	}

	return false, ""
}

// Trip fires the breaker by hand. An operator who sees something the numbers
// do not should be able to stop quoting without editing configuration.
func (o *Oracle) Trip(now time.Time, reason string) {
	o.broken = now.Add(o.policy.Cooldown)
	o.reason = reason
}

// combined is what combine concluded: the two middle readings, how many fresh
// readings there were, and from how many venues.
type combined struct {
	lower, upper    float64
	sources, venues int
}

// combine reduces observations to one price, refusing when those that should
// agree do not.
func combine(obs []Observation, p Policy, now time.Time) (combined, error) {
	var fresh []Observation
	venues := map[string]bool{}
	for _, o := range obs {
		if !o.Valid() {
			return combined{}, fmt.Errorf("%w: %+v", ErrObservation, o)
		}
		if now.Sub(o.At) > p.MaxAge || o.At.After(now.Add(p.MaxAge)) {
			continue
		}
		fresh = append(fresh, o)
		venues[o.venue()] = true
	}
	if len(fresh) == 0 {
		return combined{}, fmt.Errorf("%w: %d observations, none within %v",
			ErrStale, len(obs), p.MaxAge)
	}

	// Every pair that should agree must. With one source there is nothing
	// to check, which is itself worth reporting upward.
	for i := 0; i < len(fresh); i++ {
		for j := i + 1; j < len(fresh); j++ {
			a, b := fresh[i], fresh[j]
			gap := math.Abs(a.OutgoingPerIncoming-b.OutgoingPerIncoming) /
				math.Min(a.OutgoingPerIncoming, b.OutgoingPerIncoming)
			if gap > p.MaxDisagreement {
				return combined{}, fmt.Errorf("%w: %s says %g, %s says %g, "+
					"a gap of %.1f%% against a limit of %.1f%%",
					ErrDisagreement, a.Source, a.OutgoingPerIncoming,
					b.Source, b.OutgoingPerIncoming, gap*100,
					p.MaxDisagreement*100)
			}
		}
	}

	// They agree, so the median is as good as any and is robust to a third
	// source drifting. With an even number there are two middle readings,
	// and which one is cautious depends on the direction (see Reading), so
	// both are kept.
	prices := make([]float64, len(fresh))
	for i, o := range fresh {
		prices[i] = o.OutgoingPerIncoming
	}
	sort.Float64s(prices)

	return combined{
		lower:   prices[(len(prices)-1)/2],
		upper:   prices[len(prices)/2],
		sources: len(fresh),
		venues:  len(venues),
	}, nil
}

// volatility is the largest relative move inside the breaker window.
func (o *Oracle) volatility(price float64, now time.Time, p Policy) float64 {
	lo, hi := price, price
	for _, r := range o.history {
		if now.Sub(r.At) > p.BreakerWindow {
			continue
		}
		lo = math.Min(lo, r.OutgoingPerIncoming)
		hi = math.Max(hi, r.OutgoingPerIncoming)
	}
	if lo <= 0 {
		return 0
	}

	return (hi - lo) / lo
}

// Update feeds observations and returns a rate to quote from, or a refusal.
func (o *Oracle) Update(now time.Time, obs []Observation) (Reading, error) {
	if broken, why := o.Broken(now); broken {
		return Reading{}, fmt.Errorf("%w until %v: %s", ErrBroken,
			o.broken.Format(time.RFC3339), why)
	}

	c, err := combine(obs, o.policy, now)
	if err != nil {
		return Reading{}, err
	}
	price := (c.lower + c.upper) / 2

	vol := o.volatility(price, now, o.policy)
	if vol > o.policy.BreakerMove {
		o.Trip(now, fmt.Sprintf("price moved %.1f%% within %v, limit %.1f%%",
			vol*100, o.policy.BreakerWindow, o.policy.BreakerMove*100))

		return Reading{}, fmt.Errorf("%w: %s", ErrBroken, o.reason)
	}

	spread := o.policy.BaseSpread + vol*o.policy.VolatilityMultiple
	if spread > o.policy.MaxSpread {
		spread = o.policy.MaxSpread
	}

	r := Reading{
		OutgoingPerIncoming: price,
		At:                  now,
		Sources:             c.sources,
		Venues:              c.venues,
		Lower:               c.lower,
		Upper:               c.upper,
		Spread:              spread,
		Volatility:          vol,
	}

	// Keep only what the breaker window needs, in place: this runs on every
	// poll for as long as the bridge is up.
	o.history = append(o.history, r)
	cut := 0
	for cut < len(o.history) &&
		now.Sub(o.history[cut].At) > o.policy.BreakerWindow {

		cut++
	}
	if cut > 0 {
		o.history = o.history[:copy(o.history, o.history[cut:])]
	}

	return r, nil
}

// ForSide is the price a direction quotes from: the lower middle reading
// as it is, or the reciprocal of the upper one for a direction that inverts
// the rate. Either is the middle reading worse for the bridge. A reading
// without its middle readings, one built by hand, gives OutgoingPerIncoming.
func (r Reading) ForSide(invert bool) (float64, error) {
	price := r.OutgoingPerIncoming
	switch {
	case invert && r.Upper > 0:
		price = r.Upper
	case !invert && r.Lower > 0:
		price = r.Lower
	}
	if !(price > 0) || math.IsInf(price, 0) {
		return 0, fmt.Errorf("%w: a rate of %g", ErrObservation, price)
	}
	if invert {
		return 1 / price, nil
	}

	return price, nil
}

// Derive turns a dollar pair and a BTC price into the cross-check reading.
//
// The venue's deepest dollar market is the anchor for what a BTCB2 is worth
// (the plan's D12), and dividing by a mainstream BTC/USD gives a second
// opinion on the direct BTCB2/BTC pair that costs nothing and comes from a
// different part of the book.
func Derive(source string, btcb2USD, btcUSD float64, at time.Time) (Observation,
	error) {

	if btcb2USD <= 0 || btcUSD <= 0 ||
		math.IsNaN(btcb2USD) || math.IsNaN(btcUSD) ||
		math.IsInf(btcb2USD, 0) || math.IsInf(btcUSD, 0) {

		return Observation{}, fmt.Errorf("%w: BTCB2 %g USD, BTC %g USD",
			ErrObservation, btcb2USD, btcUSD)
	}

	return Observation{
		Source:              source,
		OutgoingPerIncoming: btcb2USD / btcUSD,
		At:                  at,
	}, nil
}
