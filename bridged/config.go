// Package bridged is the daemon: the thing that holds the two node
// connections, the journal and the policies for as long as the process runs,
// and lets a user ask for a swap.
//
// Everything below it decides; this wires. The decisions were written and
// tested first, deliberately, because they are the parts that lose money when
// they are wrong. What this adds is the two things a library cannot have: a
// process that outlives one swap, so an interrupted swap has something to come
// back to, and a surface a user can reach.
package bridged

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/paulscode/lightning-fork-bridge/chainrate"
	"github.com/paulscode/lightning-fork-bridge/inventory"
	"github.com/paulscode/lightning-fork-bridge/lnd"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/paulscode/lightning-fork-bridge/rate"
)

// ErrConfig is returned for a configuration this daemon will not run on.
var ErrConfig = errors.New("configuration is not usable")

// Node is one Lightning node's connection details.
type Node struct {
	Address      string        `json:"address"`
	TLSCertPath  string        `json:"tlsCert"`
	MacaroonPath string        `json:"macaroon"`
	ServerName   string        `json:"serverName,omitempty"`
	Timeout      time.Duration `json:"timeout,omitempty"`
}

func (n Node) toLND() lnd.Config {
	return lnd.Config{
		Address: n.Address, TLSCertPath: n.TLSCertPath,
		MacaroonPath: n.MacaroonPath, ServerName: n.ServerName,
		Timeout: n.Timeout,
	}
}

func (n Node) valid(which string) error {
	switch {
	case n.Address == "":
		return fmt.Errorf("%w: %s has no address", ErrConfig, which)
	case n.TLSCertPath == "":
		return fmt.Errorf("%w: %s has no TLS certificate", ErrConfig, which)
	case n.MacaroonPath == "":
		return fmt.Errorf("%w: %s has no macaroon", ErrConfig, which)
	}

	return nil
}

// Chain is one chain's block-timing parameters, which is how a CLTV in blocks
// is turned into an amount of time. The two chains need separate ones: the
// BLAKE2b chain has run between 1.3x and 19x its ten minute target, so a
// number that describes one describes neither.
type Chain struct {
	RetargetInterval int32         `json:"retargetInterval"`
	TargetSpacing    time.Duration `json:"targetSpacing"`
	SurgeFactor      float64       `json:"surgeFactor"`
	StallFactor      float64       `json:"stallFactor"`
	MinSamples       int           `json:"minSamples"`
	StaleAfter       time.Duration `json:"staleAfter"`
	SurgeWindow      int           `json:"surgeWindow"`
}

func (c Chain) toParams() chainrate.Params {
	return chainrate.Params{
		RetargetInterval: c.RetargetInterval,
		TargetSpacing:    c.TargetSpacing,
		SurgeFactor:      c.SurgeFactor,
		StallFactor:      c.StallFactor,
		MinSamples:       c.MinSamples,
		StaleAfter:       c.StaleAfter,
		SurgeWindow:      c.SurgeWindow,
	}
}

// Direction is one side of the bridge: which node receives and which pays.
//
// Both are optional and a daemon may run either alone. Running only one is a
// real configuration rather than a half-finished one: a bridge with no BTCB2
// to pay out cannot serve the reverse direction, and saying so in the config
// is better than quoting and then failing on headroom.
type Direction struct {
	// Enabled runs this direction. One that is configured and not enabled
	// is not wired at all, and an invoice for it is refused by name rather
	// than as though the daemon had never heard of that chain.
	Enabled bool `json:"enabled"`

	// Policy is the terms offered in this direction.
	Policy quote.Policy `json:"policy"`

	// Inventory prices the spread from what the outgoing side is holding.
	Inventory inventory.Policy `json:"inventory"`
}

// Config is what the daemon needs to run. There are no defaults for anything
// that decides what a swap costs or how long it may take: a bridge quoting on
// numbers nobody chose is worse than a bridge that will not start.
type Config struct {
	// Listen is the address the HTTP surface binds to. Loopback unless
	// something in front of it is doing authentication, because a quote
	// commits the bridge's money.
	Listen string `json:"listen"`

	// Journal is the path to the swap journal. It must survive restarts:
	// it is the only thing that says whether an outgoing payment might be
	// in flight.
	Journal string `json:"journal"`

	// Blake2b and Bitcoin are the two nodes. Which one receives and which
	// pays depends on the direction of the swap, not on the configuration.
	Blake2b Node `json:"blake2b"`
	Bitcoin Node `json:"bitcoin"`

	// Blake2bChain and BitcoinChain are their block-timing parameters.
	Blake2bChain Chain `json:"blake2bChain"`
	BitcoinChain Chain `json:"bitcoinChain"`

	// ToBitcoin is a user on the BLAKE2b chain paying a Bitcoin invoice.
	// ToBlake2b is the reverse.
	ToBitcoin Direction `json:"toBitcoin"`
	ToBlake2b Direction `json:"toBlake2b"`

	// Margin is the HTLC timing policy both directions are sized against.
	Margin margin.Policy `json:"margin"`

	// Limits bounds each caller of POST /quote on their own, a caller
	// being the address the request came from, an IPv6 one by its /64: how
	// many unpaid quotes and unfinished swaps it may hold, and how many
	// quotes it may ask for in a minute. Absent, quote.DefaultLimits; a
	// zero field means no limit.
	//
	// Behind Tor or a reverse proxy every request comes from one address,
	// so every caller shares one set of limits and they are global. The
	// README says how to give each user an address of their own there.
	Limits quote.Limits `json:"limits"`

	// Rate is how the price is combined and when to stop trusting it. Its
	// volatilityMultiple and maxSpread size the spread added for the
	// market's move; its baseSpread does not price swaps, since the
	// calm-market spread is each direction's inventory baseSpread.
	Rate rate.Policy `json:"rate"`

	// RateSources are where the price comes from. There is no fallback: a
	// bridge that invents a rate when its sources are down is a bridge that
	// pays out at a price nobody quoted.
	//
	// Readings from two venues are required, counting CrossCheck as one,
	// and two venues have to answer for a price to be quoted at all: the
	// oracle's one defence on a thin market is readings that should agree
	// being checked against each other, and one venue's readings agreeing
	// with each other say nothing about that venue being moved.
	RateSources []RateSource `json:"rateSources"`

	// CrossCheck, when set, is one more reading of the price, derived from
	// two dollar prices with rate.Derive: a second opinion on a direct
	// BTCB2/BTC price from a different part of the book.
	CrossCheck *CrossCheck `json:"crossCheck,omitempty"`

	// PollInterval is how often the chain and price observers refresh.
	// Zero means thirty seconds.
	PollInterval time.Duration `json:"pollInterval,omitempty"`

	// CompactInterval is how often the journal is rewritten down to each
	// swap's latest record. Zero means an hour. The journal is
	// append-only, so without this it grows for as long as the daemon runs
	// and is replayed in full at every start.
	CompactInterval time.Duration `json:"compactInterval,omitempty"`
}

// RateSource is one place to read the price from.
type RateSource struct {
	// Name is what to call it in logs and in a disagreement. Names are
	// unique across the rate sources, the cross-check and its two prices.
	Name string `json:"name"`

	// URL is fetched and expected to return JSON.
	URL string `json:"url"`

	// Venue is the market the price comes from. Prices from one venue,
	// however many, are that venue's say once: two of its books cannot
	// check each other against the venue itself being moved. Empty means
	// the URL's host, so two hosts of one venue need naming as one.
	Venue string `json:"venue,omitempty"`

	// Field is the JSON field holding the price, which is how many outgoing
	// units one incoming unit buys in the BLAKE2b-to-Bitcoin direction. The
	// reverse direction inverts it. Dotted paths index nested objects.
	Field string `json:"field"`

	// TimeField is the JSON field holding when the source worked the price
	// out, where it says: seconds or milliseconds since 1970, or an RFC
	// 3339 time. The reading is then as old as the source says, not as old
	// as the fetch, and the rate policy's maxAge applies to that. A reply
	// without it is refused, since a source that stopped saying when is
	// one that may have stopped computing.
	TimeField string `json:"timeField,omitempty"`

	// MaxUnchanged applies to a source with no TimeField: how long it may
	// answer the very same price before that price is taken for a static
	// one, cached or frozen, and refused until it moves. Zero means
	// DefaultMaxUnchanged.
	MaxUnchanged time.Duration `json:"maxUnchanged,omitempty"`
}

// DefaultMaxUnchanged is RateSource.MaxUnchanged when none is set. A venue's
// own clock is the better evidence, where it gives one; without it, an hour
// of a price that has not moved by a single digit is long for a market and
// short for a cache.
const DefaultMaxUnchanged = time.Hour

func (s RateSource) maxUnchanged() time.Duration {
	if s.MaxUnchanged > 0 {
		return s.MaxUnchanged
	}

	return DefaultMaxUnchanged
}

// venue is the market a source counts as: its Venue, or its URL's host.
func (s RateSource) venue() string {
	if s.Venue != "" {
		return strings.ToLower(s.Venue)
	}
	if u, err := url.Parse(s.URL); err == nil && u.Hostname() != "" {
		return strings.ToLower(u.Hostname())
	}

	return s.URL
}

func (s RateSource) valid() error {
	switch {
	case s.Name == "":
		return fmt.Errorf("%w: a rate source has no name", ErrConfig)
	case s.URL == "":
		return fmt.Errorf("%w: rate source %q has no URL", ErrConfig, s.Name)
	case s.Field == "":
		return fmt.Errorf("%w: rate source %q names no field", ErrConfig,
			s.Name)
	case s.MaxUnchanged < 0:
		return fmt.Errorf("%w: rate source %q has a negative maxUnchanged",
			ErrConfig, s.Name)
	}

	return nil
}

// CrossCheck is a reading of the price derived from two dollar prices: the
// incoming asset's and the outgoing asset's, BTCB2 and BTC, whose ratio is the
// BTCB2/BTC price the sources quote.
type CrossCheck struct {
	// Name is what to call it in logs and in a disagreement.
	Name string `json:"name"`

	// IncomingUSD is dollars per BTCB2, OutgoingUSD dollars per BTC. Each
	// is read as a rate source, timestamps and all. The reading counts as
	// IncomingUSD's venue, the BTCB2 market it reads.
	IncomingUSD RateSource `json:"incomingUSD"`
	OutgoingUSD RateSource `json:"outgoingUSD"`
}

// venue is the market the cross-check's reading counts as.
func (c CrossCheck) venue() string {
	return c.IncomingUSD.venue()
}

func (c CrossCheck) valid() error {
	if c.Name == "" {
		return fmt.Errorf("%w: the cross-check has no name", ErrConfig)
	}
	if err := c.IncomingUSD.valid(); err != nil {
		return err
	}
	if err := c.OutgoingUSD.valid(); err != nil {
		return err
	}
	if c.IncomingUSD.URL == c.OutgoingUSD.URL &&
		c.IncomingUSD.Field == c.OutgoingUSD.Field {

		return fmt.Errorf("%w: the cross-check reads both of its prices "+
			"from the same place", ErrConfig)
	}

	return nil
}

// sources checks the price sources: readings from at least MinPriceVenues
// venues, counting the cross-check as one, each named once and fetched from
// its own URL.
func (c Config) sources() error {
	var errs []error
	n := len(c.RateSources)
	if c.CrossCheck != nil {
		n++
	}
	if n < 2 {
		errs = append(errs, fmt.Errorf("%w: %d price sources, and two are "+
			"needed, counting crossCheck as one. There is no default: a "+
			"bridge that invents a price pays out at one nobody quoted, "+
			"and one that trusts a single source has nothing to check it "+
			"against", ErrConfig, n))
	}

	names, urls := map[string]bool{}, map[string]bool{}
	for _, s := range c.RateSources {
		if err := s.valid(); err != nil {
			errs = append(errs, err)
		}
		if names[s.Name] {
			errs = append(errs, fmt.Errorf("%w: two price sources are "+
				"called %q", ErrConfig, s.Name))
		}
		if urls[s.URL] {
			errs = append(errs, fmt.Errorf("%w: two price sources read "+
				"%s, which is one source and not two", ErrConfig, s.URL))
		}
		names[s.Name], urls[s.URL] = true, true
	}
	if c.CrossCheck != nil {
		if err := c.CrossCheck.valid(); err != nil {
			errs = append(errs, err)
		}
		// The unchanged-price check keys on these names, so the
		// cross-check's two prices need names of their own too.
		for _, n := range []string{c.CrossCheck.Name,
			c.CrossCheck.IncomingUSD.Name, c.CrossCheck.OutgoingUSD.Name} {

			if names[n] {
				errs = append(errs, fmt.Errorf("%w: two price sources, "+
					"counting the cross-check and its two prices, are "+
					"called %q", ErrConfig, n))
			}
			names[n] = true
		}
	}
	if v := c.venues(); n >= 2 && len(v) < MinPriceVenues {
		errs = append(errs, fmt.Errorf("%w: every price source reads %s, "+
			"and %d venues are needed: one venue's prices cannot check "+
			"each other against that venue being moved. Name another "+
			"venue, or set each source's venue where one venue has "+
			"several hosts", ErrConfig, strings.Join(v, " and "),
			MinPriceVenues))
	}

	return errors.Join(errs...)
}

// venues is the distinct venues the price sources read, the cross-check as
// its incoming price's.
func (c Config) venues() []string {
	seen := map[string]bool{}
	var out []string
	add := func(v string) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	for _, s := range c.RateSources {
		add(s.venue())
	}
	if c.CrossCheck != nil {
		add(c.CrossCheck.venue())
	}

	return out
}

// Load reads a configuration and checks it. Unknown fields are an error: a
// misspelled policy key that is silently ignored leaves the daemon running on
// a default the operator thought they had overridden.
//
// The margin policy starts from margin.DefaultPolicy, and the per-caller limits
// from quote.DefaultLimits, so a configuration that leaves a field out runs on
// the package's default rather than on zero.
// HoldExpiryDelta is the one that matters: zero would count the blocks the
// incoming node has already given up in as margin, and the policy refuses it.
func Load(r io.Reader) (Config, error) {
	c := Config{Margin: margin.DefaultPolicy, Limits: quote.DefaultLimits}

	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	if err := c.Valid(); err != nil {
		return Config{}, err
	}

	return c, nil
}

// LoadFile reads a configuration from a path.
func LoadFile(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	defer f.Close()

	return Load(f)
}

// Valid reports whether the daemon will run on this configuration, with
// every reason it will not rather than the first: each one found at a restart
// costs another.
func (c Config) Valid() error {
	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if c.Listen == "" {
		add(fmt.Errorf("%w: no listen address", ErrConfig))
	}
	if c.Journal == "" {
		add(fmt.Errorf("%w: no journal path", ErrConfig))
	}
	add(c.Blake2b.valid("the BLAKE2b node"))
	add(c.Bitcoin.valid("the Bitcoin node"))
	if !c.ToBitcoin.Enabled && !c.ToBlake2b.Enabled {
		add(fmt.Errorf("%w: neither direction is enabled, so this "+
			"daemon could not quote anything", ErrConfig))
	}

	// reachable sizes swaps with the chains' parameters and the margin
	// policy, so it runs only when those are usable.
	timing := true
	if !c.Blake2bChain.toParams().Valid() {
		timing = false
		add(fmt.Errorf("%w: the BLAKE2b chain's timing parameters are "+
			"not usable", ErrConfig))
	}
	if !c.BitcoinChain.toParams().Valid() {
		timing = false
		add(fmt.Errorf("%w: the Bitcoin chain's timing parameters are "+
			"not usable", ErrConfig))
	}
	add(c.Blake2bChain.measured("blake2bChain"))
	add(c.BitcoinChain.measured("bitcoinChain"))
	if !c.Margin.Valid() {
		timing = false
		add(fmt.Errorf("%w: the margin policy is not usable (a hold "+
			"expiry delta of %d blocks must be at least lnd's default of "+
			"%d and under maxIncomingBlocks %d)", ErrConfig,
			c.Margin.HoldExpiryDelta, margin.DefaultHoldExpiryDelta,
			c.Margin.MaxIncomingBlocks))
	}
	if !c.Rate.Valid() {
		add(fmt.Errorf("%w: the rate policy is not usable", ErrConfig))
	}
	add(c.sources())
	if c.Limits.MaxUnpaid < 0 || c.Limits.MaxInProgress < 0 ||
		c.Limits.PerMinute < 0 {

		add(fmt.Errorf("%w: a negative per-caller limit", ErrConfig))
	}
	add(c.ToBitcoin.valid("toBitcoin"))
	add(c.ToBlake2b.valid("toBlake2b"))
	if timing {
		add(c.reachable())
	}
	if c.Blake2b.Address != "" && c.Blake2b.Address == c.Bitcoin.Address {
		add(fmt.Errorf("%w: both nodes are %s, so the bridge would hold "+
			"an invoice and pay one on the same node. The two chains need "+
			"two nodes", ErrConfig, c.Blake2b.Address))
	}
	// The oracle refuses an observation older than MaxAge, so polling more
	// slowly than that guarantees every reading is stale by the daemon's own
	// policy for most of its life, and quotes would be refused constantly
	// for a reason the operator never chose.
	if c.Rate.Valid() && c.pollInterval() >= c.Rate.MaxAge {
		add(fmt.Errorf("%w: prices are polled every %v but the policy "+
			"calls an observation stale after %v, so most quotes would be "+
			"refused on a price that had simply not been refreshed",
			ErrConfig, c.pollInterval(), c.Rate.MaxAge))
	}

	return errors.Join(errs...)
}

// MinSamplesFloor and SurgeWindowFloor are the least a chain's minSamples and
// surgeWindow may be: chainrate's own defaults, and the window its surge
// factor was measured over. Fewer samples let a mean of a handful of blocks,
// which Poisson arrivals scatter widely, set every swap's margin; a shorter
// surge window trips on that same scatter, or with the window much shorter,
// misses a real surge for it.
const (
	MinSamplesFloor  = 100
	SurgeWindowFloor = 144
)

// measured checks the two numbers that decide how much of a chain an estimate
// rests on.
func (c Chain) measured(which string) error {
	if c.MinSamples < MinSamplesFloor {
		return fmt.Errorf("%w: %s.minSamples is %d; an estimate needs at "+
			"least %d blocks of the current epoch to stand on its own",
			ErrConfig, which, c.MinSamples, MinSamplesFloor)
	}
	if c.SurgeWindow < SurgeWindowFloor {
		return fmt.Errorf("%w: %s.surgeWindow is %d; the surge check "+
			"needs a window of at least %d blocks", ErrConfig, which,
			c.SurgeWindow, SurgeWindowFloor)
	}

	return nil
}

// worstBounds is the block spacing a chain's own parameters say to plan for:
// as fast as the surge factor allows, as slow as the stall factor allows.
func (c Chain) worstBounds() margin.Bounds {
	b := margin.Bounds{Fast: c.TargetSpacing, Slow: c.TargetSpacing}
	if c.SurgeFactor >= 1 {
		b.Fast = time.Duration(float64(c.TargetSpacing) / c.SurgeFactor)
	}
	if c.StallFactor >= 1 {
		b.Slow = time.Duration(float64(c.TargetSpacing) * c.StallFactor)
	}

	return b
}

// reachable checks that each enabled direction could quote at all.
//
// The incoming CLTV a swap needs is the outgoing leg's worst-case duration
// divided by how fast the incoming chain can produce blocks, so a conservative
// surge factor, a long outgoing budget and a low block cap can combine into a
// bridge that refuses every swap it is asked for. Each of the three looks
// reasonable alone, which is why this is worth checking at startup rather than
// discovering at the first quote.
func (c Config) reachable() error {
	check := func(which string, d Direction, in, out Chain) error {
		if !d.Enabled {
			return nil
		}
		_, err := margin.RequiredIncomingBlocks(in.worstBounds(), margin.Leg{
			Bounds: out.worstBounds(), Blocks: d.Policy.OutgoingCLTVLimit,
		}, c.Margin)
		if err != nil {
			return fmt.Errorf("%w: %s could not quote any swap: %w. The "+
				"outgoing CLTV budget, the settlement allowance and the "+
				"incoming chain's surge factor have to fit inside "+
				"margin.maxIncomingBlocks, and here they do not",
				ErrConfig, which, err)
		}

		return nil
	}

	return errors.Join(
		check("toBitcoin", c.ToBitcoin, c.Blake2bChain, c.BitcoinChain),
		check("toBlake2b", c.ToBlake2b, c.BitcoinChain, c.Blake2bChain),
	)
}

func (d Direction) valid(which string) error {
	if !d.Enabled {
		return nil
	}
	if err := d.Inventory.Valid(); err != nil {
		return fmt.Errorf("%w: %s inventory policy: %w", ErrConfig, which,
			err)
	}
	// quote.Policy has no Valid of its own; the Quoter checks what it needs
	// when it quotes. Check the two that would otherwise fail every swap
	// rather than one, since those are worth catching at startup.
	if d.Policy.MaxSwapMsat == 0 {
		return fmt.Errorf("%w: %s has no maximum swap size", ErrConfig,
			which)
	}
	if d.Policy.InvoiceExpiry < d.Policy.Validity {
		return fmt.Errorf("%w: %s quotes are valid for %v but its hold "+
			"invoices live %v, so a quote can outlive the invoice it is "+
			"for", ErrConfig, which, d.Policy.Validity,
			d.Policy.InvoiceExpiry)
	}

	return nil
}

// compactInterval is how often to rewrite the journal.
func (c Config) compactInterval() time.Duration {
	if c.CompactInterval <= 0 {
		return DefaultCompactInterval
	}

	return c.CompactInterval
}

// pollInterval is how often to refresh the observers.
func (c Config) pollInterval() time.Duration {
	if c.PollInterval <= 0 {
		return 30 * time.Second
	}

	return c.PollInterval
}
