package bridged

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/quote"
)

// good is a configuration that loads, so each test can break exactly one
// thing and the failure names that thing rather than the first missing field.
const good = `{
  "listen": "127.0.0.1:9737",
  "journal": "/tmp/swaps.journal",
  "blake2b": {"address": "lf:10009", "tlsCert": "/t.cert", "macaroon": "/m"},
  "bitcoin": {"address": "btc:10009", "tlsCert": "/t.cert", "macaroon": "/m"},
  "blake2bChain": {
    "retargetInterval": 2016, "targetSpacing": 600000000000,
    "surgeFactor": 4, "stallFactor": 4, "minSamples": 100,
    "staleAfter": 3600000000000, "surgeWindow": 144
  },
  "bitcoinChain": {
    "retargetInterval": 2016, "targetSpacing": 600000000000,
    "surgeFactor": 2, "stallFactor": 2, "minSamples": 100,
    "staleAfter": 7200000000000, "surgeWindow": 144
  },
  "margin": {"Settlement": 1800000000000, "MaxIncomingBlocks": 2016},
  "rate": {
    "maxAge": 300000000000, "maxDisagreement": 0.02, "breakerMove": 0.1,
    "breakerWindow": 600000000000, "cooldown": 1800000000000,
    "baseSpread": 0.01, "volatilityMultiple": 2, "maxSpread": 0.05
  },
  "rateSources": [
    {"name": "venue", "url": "https://x/p", "field": "price"},
    {"name": "other", "url": "https://y/p", "field": "price"}
  ],
  "toBitcoin": {
    "enabled": true,
    "policy": {
      "minSwapMsat": 100000, "maxSwapMsat": 500000000,
      "validity": 120000000000, "invoiceExpiry": 600000000000,
      "outgoingCLTVLimit": 80, "feeFraction": 0.003,
      "minRemainingLife": 120000000000
    },
    "inventory": {
      "TargetOutgoingMsat": 100000000, "FloorOutgoingMsat": 10000000,
      "BaseSpread": 0.01, "MaxSpread": 0.03, "MaxDiscount": 0.003,
      "RebalanceCost": 0.004, "Curve": 1, "MaxAge": 300000000000
    }
  }
}`

func load(t *testing.T, s string) (Config, error) {
	t.Helper()

	return Load(strings.NewReader(s))
}

func TestGoodConfigLoads(t *testing.T) {
	t.Parallel()

	cfg, err := load(t, good)
	if err != nil {
		t.Fatalf("the reference configuration should load: %v", err)
	}
	if !cfg.ToBitcoin.Enabled || cfg.ToBlake2b.Enabled {
		t.Errorf("expected only toBitcoin enabled, got toBitcoin=%v "+
			"toBlake2b=%v", cfg.ToBitcoin.Enabled, cfg.ToBlake2b.Enabled)
	}
	if cfg.pollInterval() != 30*time.Second {
		t.Errorf("poll interval defaulted to %v, wanted 30s",
			cfg.pollInterval())
	}
}

// A misspelled key that is ignored leaves the daemon running on a default the
// operator believes they overrode, which is the worst of both.
func TestUnknownFieldsAreRefused(t *testing.T) {
	t.Parallel()

	_, err := load(t, strings.Replace(good, `"listen"`, `"lissen"`, 1))
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("a misspelled key should be refused, got %v", err)
	}
	if !strings.Contains(err.Error(), "lissen") {
		t.Errorf("the error should name the key it did not know: %v", err)
	}
}

func TestRefusals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(string) string
		want string
	}{
		{
			name: "no rate source",
			edit: func(s string) string {
				return strings.Replace(s, `"rateSources": [`,
					`"unused": [`, 1)
			},
			want: "unknown field",
		},
		{
			// One source has nothing to be checked against.
			name: "one rate source",
			edit: func(s string) string {
				return strings.Replace(s,
					`{"name": "venue", "url": "https://x/p", "field": "price"},`,
					``, 1)
			},
			want: "two are needed",
		},
		{
			name: "one source twice",
			edit: func(s string) string {
				return strings.Replace(s, `"https://y/p"`, `"https://x/p"`, 1)
			},
			want: "one source and not two",
		},
		{
			name: "one name twice",
			edit: func(s string) string {
				return strings.Replace(s, `"name": "other"`,
					`"name": "venue"`, 1)
			},
			want: "two price sources are called",
		},
		{
			name: "no direction enabled",
			edit: func(s string) string {
				return strings.Replace(s, `"enabled": true`,
					`"enabled": false`, 1)
			},
			want: "neither direction is enabled",
		},
		{
			name: "no journal",
			edit: func(s string) string {
				return strings.Replace(s, `"journal": "/tmp/swaps.journal"`,
					`"journal": ""`, 1)
			},
			want: "no journal path",
		},
		{
			name: "node without a macaroon",
			edit: func(s string) string {
				return strings.Replace(s, `"macaroon": "/m"}`,
					`"macaroon": ""}`, 1)
			},
			want: "macaroon",
		},
		{
			// A quote that outlives the invoice it is for is a promise the
			// bridge cannot keep, and it is quiet until someone tries.
			name: "quote outlives its hold invoice",
			edit: func(s string) string {
				return strings.Replace(s, `"invoiceExpiry": 600000000000`,
					`"invoiceExpiry": 60000000000`, 1)
			},
			want: "outlive",
		},
		{
			// Each of the three looks reasonable alone. Planning for the
			// full surge the chain has shown, with an outgoing budget
			// that suits ordinary destinations, needs more incoming
			// blocks than anyone will route.
			name: "conservative surge against a low block cap",
			edit: func(s string) string {
				return strings.Replace(s, `"surgeFactor": 4, "stallFactor": 4`,
					`"surgeFactor": 19, "stallFactor": 4`, 1)
			},
			want: "could not quote any swap",
		},
		{
			// A mean of six blocks is what Poisson arrivals scatter
			// widely, and it would set every swap's margin.
			name: "too few samples",
			edit: func(s string) string {
				return strings.Replace(s, `"minSamples": 100,`,
					`"minSamples": 99,`, 1)
			},
			want: "minSamples",
		},
		{
			name: "too short a surge window",
			edit: func(s string) string {
				return strings.Replace(s, `"surgeWindow": 144`,
					`"surgeWindow": 143`, 1)
			},
			want: "surgeWindow",
		},
		{
			name: "no maximum swap size",
			edit: func(s string) string {
				return strings.Replace(s, `"maxSwapMsat": 500000000`,
					`"maxSwapMsat": 0`, 1)
			},
			want: "maximum swap size",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := load(t, test.edit(good))
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("wanted ErrConfig, got %v", err)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("the error should say %q, said: %v", test.want, err)
			}
		})
	}
}

func TestExtractReadsNumbersAndStrings(t *testing.T) {
	t.Parallel()

	doc := map[string]any{
		"price":  0.00308078,
		"quoted": "0.00308078",
		"data":   map[string]any{"last": 1.5},
		"wrong":  "not a number",
	}

	tests := []struct {
		path string
		want float64
		ok   bool
	}{
		{path: "price", want: 0.00308078, ok: true},
		// Venues quote numbers as strings often enough that refusing them
		// would mean refusing the venue.
		{path: "quoted", want: 0.00308078, ok: true},
		{path: "data.last", want: 1.5, ok: true},
		{path: "wrong"},
		{path: "missing"},
		{path: "price.deeper"},
	}

	for _, test := range tests {
		got, err := extract(doc, test.path)
		if test.ok {
			if err != nil {
				t.Errorf("%s: unexpected error %v", test.path, err)
			} else if got != test.want {
				t.Errorf("%s: got %v, wanted %v", test.path, got, test.want)
			}

			continue
		}
		if err == nil {
			t.Errorf("%s: wanted an error, got %v", test.path, got)
		}
	}
}

func TestLoadFileReadsAConfiguration(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("the reference configuration should load from a file: %v",
			err)
	}
	if cfg.Listen != "127.0.0.1:9737" {
		t.Errorf("listen %q", cfg.Listen)
	}

	if _, err := LoadFile(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Error("a missing file should be an error")
	} else if !errors.Is(err, ErrConfig) {
		t.Errorf("wanted ErrConfig, got %v", err)
	}
}

func TestIntervalsDefaultWhenUnset(t *testing.T) {
	t.Parallel()

	var c Config
	if c.pollInterval() != 30*time.Second {
		t.Errorf("poll interval %v", c.pollInterval())
	}
	if c.compactInterval() != DefaultCompactInterval {
		t.Errorf("compact interval %v", c.compactInterval())
	}

	c.PollInterval = time.Minute
	c.CompactInterval = 2 * time.Hour
	if c.pollInterval() != time.Minute {
		t.Errorf("configured poll interval was ignored: %v", c.pollInterval())
	}
	if c.compactInterval() != 2*time.Hour {
		t.Errorf("configured compact interval was ignored: %v",
			c.compactInterval())
	}
}

// The node details have to reach the dialler intact: a macaroon that did not
// arrive is a connection that fails at the worst moment rather than at start.
func TestNodeDetailsReachTheDialler(t *testing.T) {
	t.Parallel()

	n := Node{
		Address: "host:10009", TLSCertPath: "/tls.cert",
		MacaroonPath: "/admin.macaroon", ServerName: "lf1",
		Timeout: 5 * time.Second,
	}
	got := n.toLND()

	if got.Address != n.Address || got.TLSCertPath != n.TLSCertPath ||
		got.MacaroonPath != n.MacaroonPath ||
		got.ServerName != n.ServerName || got.Timeout != n.Timeout {

		t.Errorf("the dialler config does not match: %+v against %+v", got, n)
	}
}

// Two misconfigurations that each look reasonable in isolation.
func TestConfigCatchesIncoherentPairs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		edit func(string) string
		want string
	}{
		{
			// A copy-paste that would have the bridge hold an invoice and
			// pay one on the same node.
			name: "both nodes are the same",
			edit: func(s string) string {
				return strings.Replace(s, `"address": "btc:10009"`,
					`"address": "lf:10009"`, 1)
			},
			want: "two nodes",
		},
		{
			// Polling more slowly than the price may age guarantees the
			// daemon refuses most quotes for a reason nobody chose.
			name: "polled more slowly than the price may age",
			edit: func(s string) string {
				return strings.Replace(s, `"rateSources"`,
					`"pollInterval": 600000000000, "rateSources"`, 1)
			},
			want: "not been refreshed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := load(t, test.edit(good))
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("wanted ErrConfig, got %v", err)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Errorf("the error should say %q, said: %v", test.want, err)
			}
		})
	}
}

// TestTheHoldExpiryDeltaDefaultsAndIsChecked: a configuration that leaves the
// margin's hold expiry delta out runs on lnd's default, starting from
// margin.DefaultPolicy, and one that sets it under the default is refused.
// Zero would count the blocks the incoming node has already given up in as
// margin.
func TestTheHoldExpiryDeltaDefaultsAndIsChecked(t *testing.T) {
	t.Parallel()

	cfg, err := load(t, good)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Margin.HoldExpiryDelta != margin.DefaultHoldExpiryDelta {
		t.Errorf("hold expiry delta %d, want %d when not set",
			cfg.Margin.HoldExpiryDelta, margin.DefaultHoldExpiryDelta)
	}
	if cfg.Margin.Settlement != 30*time.Minute {
		t.Errorf("the configured settlement became %v",
			cfg.Margin.Settlement)
	}

	for _, delta := range []string{"0", "17"} {
		_, err := load(t, strings.Replace(good, `"MaxIncomingBlocks": 2016}`,
			`"MaxIncomingBlocks": 2016, "HoldExpiryDelta": `+delta+`}`, 1))
		if !errors.Is(err, ErrConfig) {
			t.Errorf("a hold expiry delta of %s: want ErrConfig, got %v",
				delta, err)
		}
	}
}

// TestTheExampleConfigurationLoads: config.example.json is what an operator
// starts from, and one that the daemon refuses teaches the wrong numbers.
func TestTheExampleConfigurationLoads(t *testing.T) {
	t.Parallel()

	cfg, err := LoadFile(filepath.Join("..", "config.example.json"))
	if err != nil {
		t.Fatalf("config.example.json does not load: %v", err)
	}
	if cfg.Margin.HoldExpiryDelta != margin.DefaultHoldExpiryDelta {
		t.Errorf("the example's hold expiry delta is %d",
			cfg.Margin.HoldExpiryDelta)
	}
}

// TestACrossCheckCountsAsASource: one direct source and a cross-check derived
// from two dollar prices are two independent readings.
func TestACrossCheckCountsAsASource(t *testing.T) {
	t.Parallel()

	one := strings.Replace(good,
		`{"name": "venue", "url": "https://x/p", "field": "price"},`, ``, 1)
	withCross := strings.Replace(one, `"rateSources"`, `"crossCheck": {
    "name": "usd",
    "incomingUSD": {"name": "btcb2-usd", "url": "https://z/b", "field": "p"},
    "outgoingUSD": {"name": "btc-usd", "url": "https://z/c", "field": "p",
      "timeField": "at"}
  },
  "rateSources"`, 1)

	cfg, err := load(t, withCross)
	if err != nil {
		t.Fatalf("a source and a cross-check should load: %v", err)
	}
	if cfg.CrossCheck == nil ||
		cfg.CrossCheck.OutgoingUSD.TimeField != "at" {

		t.Errorf("the cross-check did not load: %+v", cfg.CrossCheck)
	}

	// Both of its prices from the same place is no cross-check.
	same := strings.Replace(withCross, `"https://z/c", "field": "p"`,
		`"https://z/b", "field": "p"`, 1)
	if _, err := load(t, same); !errors.Is(err, ErrConfig) {
		t.Errorf("a cross-check reading one price twice loaded: %v", err)
	}
}

// TestPerCallerLimitsDefault: a configuration that sets no limits gets
// quote.DefaultLimits rather than none, and a negative one is refused.
func TestPerCallerLimitsDefault(t *testing.T) {
	t.Parallel()

	cfg, err := load(t, good)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Limits != quote.DefaultLimits {
		t.Errorf("limits %+v, want %+v", cfg.Limits, quote.DefaultLimits)
	}

	_, err = load(t, strings.Replace(good, `"rateSources"`,
		`"limits": {"PerMinute": -1}, "rateSources"`, 1))
	if !errors.Is(err, ErrConfig) {
		t.Errorf("a negative limit: want ErrConfig, got %v", err)
	}
}
