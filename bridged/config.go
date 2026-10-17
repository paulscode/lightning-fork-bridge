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
	"os"
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

	// Rate is how the price is combined and when to stop trusting it.
	Rate rate.Policy `json:"rate"`

	// RateSources are where the price comes from. At least one is required
	// and there is no fallback: a bridge that invents a rate when its
	// sources are down is a bridge that pays out at a price nobody quoted.
	RateSources []RateSource `json:"rateSources"`

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
	// Name is what to call it in logs and in a disagreement.
	Name string `json:"name"`

	// URL is fetched and expected to return JSON.
	URL string `json:"url"`

	// Field is the JSON field holding the price, which is how many outgoing
	// units one incoming unit buys in the BLAKE2b-to-Bitcoin direction. The
	// reverse direction inverts it. Dotted paths index nested objects.
	Field string `json:"field"`
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
	}

	return nil
}

// Load reads a configuration and checks it. Unknown fields are an error: a
// misspelled policy key that is silently ignored leaves the daemon running on
// a default the operator thought they had overridden.
func Load(r io.Reader) (Config, error) {
	var c Config

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

// Valid reports whether the daemon will run on this configuration.
func (c Config) Valid() error {
	if c.Listen == "" {
		return fmt.Errorf("%w: no listen address", ErrConfig)
	}
	if c.Journal == "" {
		return fmt.Errorf("%w: no journal path", ErrConfig)
	}
	if err := c.Blake2b.valid("the BLAKE2b node"); err != nil {
		return err
	}
	if err := c.Bitcoin.valid("the Bitcoin node"); err != nil {
		return err
	}
	if !c.ToBitcoin.Enabled && !c.ToBlake2b.Enabled {
		return fmt.Errorf("%w: neither direction is enabled, so this "+
			"daemon could not quote anything", ErrConfig)
	}
	if !c.Blake2bChain.toParams().Valid() {
		return fmt.Errorf("%w: the BLAKE2b chain's timing parameters are "+
			"not usable", ErrConfig)
	}
	if !c.BitcoinChain.toParams().Valid() {
		return fmt.Errorf("%w: the Bitcoin chain's timing parameters are "+
			"not usable", ErrConfig)
	}
	if !c.Margin.Valid() {
		return fmt.Errorf("%w: the margin policy is not usable", ErrConfig)
	}
	if !c.Rate.Valid() {
		return fmt.Errorf("%w: the rate policy is not usable", ErrConfig)
	}
	if len(c.RateSources) == 0 {
		return fmt.Errorf("%w: no rate sources, and there is no default: a "+
			"bridge that invents a price pays out at one nobody quoted",
			ErrConfig)
	}
	for _, s := range c.RateSources {
		if err := s.valid(); err != nil {
			return err
		}
	}
	if err := c.ToBitcoin.valid("toBitcoin"); err != nil {
		return err
	}
	if err := c.ToBlake2b.valid("toBlake2b"); err != nil {
		return err
	}
	if err := c.reachable(); err != nil {
		return err
	}
	if c.Blake2b.Address == c.Bitcoin.Address {
		return fmt.Errorf("%w: both nodes are %s, so the bridge would hold "+
			"an invoice and pay one on the same node. The two chains need "+
			"two nodes", ErrConfig, c.Blake2b.Address)
	}
	// The oracle refuses an observation older than MaxAge, so polling more
	// slowly than that guarantees every reading is stale by the daemon's own
	// policy for most of its life, and quotes would be refused constantly
	// for a reason the operator never chose.
	if c.pollInterval() >= c.Rate.MaxAge {
		return fmt.Errorf("%w: prices are polled every %v but the policy "+
			"calls an observation stale after %v, so most quotes would be "+
			"refused on a price that had simply not been refreshed",
			ErrConfig, c.pollInterval(), c.Rate.MaxAge)
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

	if err := check("toBitcoin", c.ToBitcoin, c.Blake2bChain,
		c.BitcoinChain); err != nil {

		return err
	}

	return check("toBlake2b", c.ToBlake2b, c.BitcoinChain, c.Blake2bChain)
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
