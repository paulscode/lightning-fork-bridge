package bridged

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/rate"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// serving is a source that answers with a fixed body and status.
func serving(t *testing.T, status int, body string) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
	t.Cleanup(srv.Close)

	return srv.URL
}

func TestFetchPrice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		body   string
		field  string
		want   float64
		ok     bool
	}{
		{
			name: "a number", status: 200, body: `{"price": 0.00308078}`,
			field: "price", want: 0.00308078, ok: true,
		},
		{
			name: "a nested number", status: 200,
			body:  `{"data": {"last": 0.0031}}`,
			field: "data.last", want: 0.0031, ok: true,
		},
		{
			// Venues quote numbers as strings often enough that refusing
			// them would mean refusing the venue.
			name: "a quoted number", status: 200, body: `{"price": "0.0031"}`,
			field: "price", want: 0.0031, ok: true,
		},
		{
			name: "not found", status: 404, body: `nope`, field: "price",
		},
		{
			name: "not JSON", status: 200, body: `<html>`, field: "price",
		},
		{
			name: "missing field", status: 200, body: `{"other": 1}`,
			field: "price",
		},
		{
			name: "not a number", status: 200, body: `{"price": "abc"}`,
			field: "price",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, _, err := fetchPrice(context.Background(), RateSource{
				Name: "test", URL: serving(t, test.status, test.body),
				Field: test.field,
			})
			if !test.ok {
				if err == nil {
					t.Fatalf("wanted an error, got %v", got)
				}

				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Errorf("got %v, wanted %v", got, test.want)
			}
		})
	}
}

// A body that never ends must not take the daemon with it.
func TestFetchPriceIsBounded(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			// More than the reader allows, and not valid JSON either way.
			for i := 0; i < 64; i++ {
				_, _ = w.Write([]byte(strings.Repeat("x", 1<<16)))
			}
		}))
	defer srv.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = fetchPrice(context.Background(), RateSource{
			Name: "flood", URL: srv.URL, Field: "price",
		})
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("fetchPrice did not give up on an endless body")
	}
}

// A source that fails is left out rather than defaulted. The oracle's whole
// job is to notice when sources disagree, and a made-up number that agrees
// with everything defeats it.
func TestSamplePriceDropsAFailedSourceRatherThanInventingOne(t *testing.T) {
	t.Parallel()

	good := serving(t, 200, `{"price": 0.0031}`)
	alsoGood := serving(t, 200, `{"price": 0.0031}`)
	bad := serving(t, 500, `down`)

	oracle, err := rate.New(rate.DefaultPolicy)
	if err != nil {
		t.Fatal(err)
	}

	s := &Service{log: quiet(), oracle: oracle, cfg: Config{RateSources: []RateSource{
		{Name: "good", URL: good, Field: "price", Venue: "a"},
		{Name: "also good", URL: alsoGood, Field: "price", Venue: "b"},
		{Name: "bad", URL: bad, Field: "price", Venue: "c"},
	}}}
	s.priceErr = fmt.Errorf("none yet")

	s.samplePrice(context.Background())

	s.priceMu.RLock()
	got, gotErr := s.price, s.priceErr
	s.priceMu.RUnlock()

	if gotErr != nil {
		t.Fatalf("two good sources should still price: %v", gotErr)
	}
	if got.Sources != 2 {
		t.Errorf("counted %d sources, wanted only the two that answered",
			got.Sources)
	}
	if got.OutgoingPerIncoming != 0.0031 {
		t.Errorf("priced at %v, wanted the good source's %v",
			got.OutgoingPerIncoming, 0.0031)
	}
}

// With every source down the daemon must refuse to quote, not carry on with
// the last price it happened to see.
func TestSamplePriceRefusesWhenEverySourceFails(t *testing.T) {
	t.Parallel()

	oracle, err := rate.New(rate.DefaultPolicy)
	if err != nil {
		t.Fatal(err)
	}

	s := &Service{log: quiet(), oracle: oracle, cfg: Config{RateSources: []RateSource{
		{Name: "bad", URL: serving(t, 500, `down`), Field: "price"},
	}}}
	// A price from before, which must not be served once the sources are
	// gone.
	s.price = rate.Reading{OutgoingPerIncoming: 0.0031, At: time.Now()}
	s.priceErr = nil

	s.samplePrice(context.Background())

	s.priceMu.RLock()
	gotErr := s.priceErr
	s.priceMu.RUnlock()

	if gotErr == nil {
		t.Fatal("no usable source should leave the daemon refusing to price")
	}

	// And the refusal has to reach the quote, not just the field.
	sd := &side{name: "toBitcoin", balance: func(context.Context) (uint64, error) {
		return 1, nil
	}}
	if _, err := s.pricer(sd, inventoryForTest(), sd.balance)(context.Background()); err == nil {
		t.Error("a quote should be refused while there is no usable price")
	}
}

// resume has to send each swap to the direction whose paying node can read
// its invoice. Running both directions' Resume over one journal would have
// each drive every swap, including the one whose nodes are reversed.
func TestResumeRoutesEachSwapToItsOwnDirection(t *testing.T) {
	t.Parallel()

	j := openJournal(t, t.TempDir())
	defer j.Close()

	btc := &fakeOut{accepts: "ln"}
	lf := &fakeOut{accepts: "ln"}
	s := testService(t, btc, lf, nil)
	s.journal = j

	// Two unfinished swaps, one for each direction.
	now := time.Now()
	for i, invoice := range []string{"lnbcrt1payme", "lnbcrt1" + b2bMarker + "payme"} {
		err := j.Put(context.Background(), store.Record{
			Hash: node8(byte(i + 1)), State: swap.Offered,
			OutgoingCLTVLimit: 40, Invoice: invoice,
			IncomingMsat: 1_000_000, OutgoingMsat: 3_080,
			Rate: 0.00308078, Spread: 0.01, Created: now, Updated: now,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Runners are nil here on purpose: this is about routing, and driving
	// is covered by the live tests. Count the decode calls instead.
	btcBefore, lfBefore := btc.calls, lf.calls

	pending, err := j.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var routed []string
	for _, rec := range pending {
		sd, _, err := s.route(context.Background(), rec.Invoice)
		if err != nil {
			t.Fatalf("%s: %v", rec.Invoice, err)
		}
		routed = append(routed, sd.name)
	}

	if len(routed) != 2 {
		t.Fatalf("routed %d swaps, wanted 2", len(routed))
	}
	seen := map[string]bool{routed[0]: true, routed[1]: true}
	if !seen["toBitcoin"] || !seen["toBlake2b"] {
		t.Errorf("both directions should have been used, got %v", routed)
	}
	if btc.calls == btcBefore && lf.calls == lfBefore {
		t.Error("routing did not ask either node to decode anything")
	}
}

// A swap whose direction is no longer enabled must be reported loudly: it is
// an HTLC nobody is watching, which is the worst way to be quiet.
func TestResumeReportsASwapItCannotRoute(t *testing.T) {
	t.Parallel()

	j := openJournal(t, t.TempDir())
	defer j.Close()

	// Only the Bitcoin direction is enabled, and the journal holds a swap
	// for the other one.
	s := testService(t, &fakeOut{accepts: "lnbc"}, nil, nil)
	s.journal = j

	now := time.Now()
	err := j.Put(context.Background(), store.Record{
		Hash: node8(9), State: swap.Funded, OutgoingCLTVLimit: 40,
		Invoice: "lnblakert1orphan", IncomingMsat: 1_000_000,
		OutgoingMsat: 3_080, Rate: 0.00308078, Spread: 0.01,
		Created: now, Updated: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.resume(context.Background())
	if err == nil {
		t.Fatal("a swap that cannot be routed should be reported")
	}
	if !strings.Contains(err.Error(), "lnblakert1orphan") &&
		!strings.Contains(err.Error(), fmt.Sprintf("%x", node8(9))) {

		t.Errorf("the error should identify the stuck swap: %v", err)
	}
}

func TestResumeWithNothingPendingIsQuiet(t *testing.T) {
	t.Parallel()

	j := openJournal(t, t.TempDir())
	defer j.Close()

	s := testService(t, &fakeOut{accepts: "lnbc"}, nil, nil)
	s.journal = j

	if err := s.resume(context.Background()); err != nil {
		t.Errorf("an empty journal should resume cleanly: %v", err)
	}
}

// samplingWith is a service pricing from the given sources.
func samplingWith(t *testing.T, cc *CrossCheck, srcs ...RateSource) *Service {
	t.Helper()

	oracle, err := rate.New(rate.DefaultPolicy)
	if err != nil {
		t.Fatal(err)
	}
	// Every test server is on one host, which would make every source one
	// venue; each is its own here unless a test says otherwise.
	for i := range srcs {
		if srcs[i].Venue == "" {
			srcs[i].Venue = srcs[i].Name
		}
	}
	if cc != nil && cc.IncomingUSD.Venue == "" {
		cc.IncomingUSD.Venue = cc.Name
	}
	s := &Service{log: quiet(), oracle: oracle, cfg: Config{
		RateSources: srcs, CrossCheck: cc,
	}}
	s.priceErr = fmt.Errorf("none yet")

	return s
}

func sampled(s *Service) (rate.Reading, error) {
	s.priceMu.RLock()
	defer s.priceMu.RUnlock()

	return s.price, s.priceErr
}

// TestOneSourceIsNotAPrice: with one source answering there is nothing to
// check it against, which on a thin market is the oracle's one defence, so
// the price is refused rather than quoted unchecked.
func TestOneSourceIsNotAPrice(t *testing.T) {
	t.Parallel()

	s := samplingWith(t, nil,
		RateSource{Name: "good", URL: serving(t, 200, `{"price": 0.0031}`),
			Field: "price"},
		RateSource{Name: "bad", URL: serving(t, 500, `down`),
			Field: "price"},
	)
	s.samplePrice(context.Background())

	if _, err := sampled(s); !errors.Is(err, rate.ErrStale) {
		t.Fatalf("one answering source priced: %v", err)
	}
}

// TestASourcesOwnTimeIsHowOldItsPriceIs: a source that says when it worked the
// price out is as old as it says, not as old as the fetch, and one that stops
// saying is refused. A frozen ticker served fresh is the failure this exists
// for.
func TestASourcesOwnTimeIsHowOldItsPriceIs(t *testing.T) {
	t.Parallel()

	now := time.Now()
	fresh := fmt.Sprintf(`{"price": 0.0031, "at": %d}`, now.UnixMilli())
	frozen := fmt.Sprintf(`{"price": 0.0031, "at": %d}`,
		now.Add(-time.Hour).Unix())
	rfc := fmt.Sprintf(`{"price": 0.0031, "at": %q}`,
		now.UTC().Format(time.RFC3339))

	for _, c := range []struct {
		name  string
		body  string
		price bool
	}{
		{"fresh, in milliseconds", fresh, true},
		{"fresh, in RFC 3339", rfc, true},
		{"an hour old", frozen, false},
		{"no time at all", `{"price": 0.0031}`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			s := samplingWith(t, nil,
				RateSource{Name: "timed", URL: serving(t, 200, c.body),
					Field: "price", TimeField: "at"},
				RateSource{Name: "other",
					URL:   serving(t, 200, `{"price": 0.0031}`),
					Field: "price"},
			)
			s.samplePrice(context.Background())

			_, err := sampled(s)
			if c.price && err != nil {
				t.Errorf("refused: %v", err)
			}
			if !c.price && !errors.Is(err, rate.ErrStale) {
				t.Errorf("want a stale refusal, got %v", err)
			}
		})
	}
}

// TestAStaticPriceHasAMaximumAge: a source with no time of its own that keeps
// answering the very same price, a cache or a frozen ticker, is refused once
// it has done so for longer than its maxUnchanged, and accepted again when
// the price moves.
func TestAStaticPriceHasAMaximumAge(t *testing.T) {
	t.Parallel()

	var price atomic.Value
	price.Store("0.0031")
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{"price": %s}`, price.Load())
		}))
	t.Cleanup(srv.Close)

	static := RateSource{Name: "static", URL: srv.URL, Field: "price",
		MaxUnchanged: 50 * time.Millisecond}
	s := samplingWith(t, nil, static, RateSource{Name: "other",
		URL: serving(t, 200, `{"price": 0.0031}`), Field: "price"})

	s.samplePrice(context.Background())
	if _, err := sampled(s); err != nil {
		t.Fatalf("a price just read was refused: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	s.samplePrice(context.Background())
	if _, err := sampled(s); !errors.Is(err, rate.ErrStale) {
		t.Fatalf("a price that has not moved past its maxUnchanged: %v",
			err)
	}

	price.Store("0.00311")
	s.samplePrice(context.Background())
	if _, err := sampled(s); err != nil {
		t.Errorf("a price that moved was refused: %v", err)
	}
}

// TestTheCrossCheckIsDerivedFromDollars: BTCB2 in dollars over BTC in dollars
// is a reading of BTCB2 in BTC, checked against the direct one: agreeing, it
// is a second source; disagreeing, the price is refused.
func TestTheCrossCheckIsDerivedFromDollars(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		name   string
		b2bUSD string
		agrees bool
	}{
		// 0.0031 BTC at 100,000 dollars is 310 dollars.
		{"agreeing", "310", true},
		{"ten percent apart", "341", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			cc := &CrossCheck{
				Name: "usd",
				IncomingUSD: RateSource{Name: "btcb2-usd",
					URL:   serving(t, 200, `{"p": "`+c.b2bUSD+`"}`),
					Field: "p"},
				OutgoingUSD: RateSource{Name: "btc-usd",
					URL: serving(t, 200, `{"p": 100000}`), Field: "p"},
			}
			s := samplingWith(t, cc, RateSource{Name: "direct",
				URL: serving(t, 200, `{"price": 0.0031}`), Field: "price"})
			s.samplePrice(context.Background())

			got, err := sampled(s)
			switch {
			case c.agrees && err != nil:
				t.Errorf("refused: %v", err)
			case c.agrees && got.Sources != 2:
				t.Errorf("%d sources, want the direct one and the "+
					"cross-check", got.Sources)
			case !c.agrees && !errors.Is(err, rate.ErrDisagreement):
				t.Errorf("want a disagreement, got %v", err)
			}
		})
	}
}

// TestOneVenueIsNotAPrice: two readings from one venue agreeing with each
// other say nothing about that venue being moved, so they are one venue's
// say, and the price is refused until a second venue answers.
func TestOneVenueIsNotAPrice(t *testing.T) {
	t.Parallel()

	ticker := RateSource{Name: "ticker", Venue: "neoxa.exchange",
		URL: serving(t, 200, `{"price": 0.0031}`), Field: "price"}
	book := RateSource{Name: "book", Venue: "neoxa.exchange",
		URL: serving(t, 200, `{"price": 0.0031}`), Field: "price"}
	other := RateSource{Name: "other", Venue: "api.nonkyc.io",
		URL: serving(t, 500, `down`), Field: "price"}

	s := samplingWith(t, nil, ticker, book, other)
	s.samplePrice(context.Background())
	if _, err := sampled(s); !errors.Is(err, rate.ErrStale) {
		t.Fatalf("one venue's two readings priced: %v", err)
	}

	// The same with the second venue answering.
	other.URL = serving(t, 200, `{"price": 0.0031}`)
	s = samplingWith(t, nil, ticker, book, other)
	s.samplePrice(context.Background())
	got, err := sampled(s)
	if err != nil {
		t.Fatalf("two venues refused: %v", err)
	}
	if got.Sources != 3 || got.Venues != 2 {
		t.Errorf("%d sources from %d venues, want 3 from 2", got.Sources,
			got.Venues)
	}

	// Nor when the second venue answers with a price too old to use,
	// which the oracle leaves out.
	other.URL = serving(t, 200, fmt.Sprintf(`{"price": 0.0031, "at": %d}`,
		time.Now().Add(-time.Hour).Unix()))
	other.TimeField = "at"
	s = samplingWith(t, nil, ticker, book, other)
	s.samplePrice(context.Background())
	if _, err := sampled(s); !errors.Is(err, rate.ErrStale) {
		t.Fatalf("one venue's two readings priced beside a stale one: %v",
			err)
	}

	// A cross-check counts as the venue of the BTCB2 price it reads.
	cc := &CrossCheck{
		Name: "usd",
		IncomingUSD: RateSource{Name: "btcb2-usd", Venue: "neoxa.exchange",
			URL: serving(t, 200, `{"p": 310}`), Field: "p"},
		OutgoingUSD: RateSource{Name: "btc-usd", Venue: "mempool.space",
			URL: serving(t, 200, `{"p": 100000}`), Field: "p"},
	}
	s = samplingWith(t, cc, ticker)
	s.samplePrice(context.Background())
	if _, err := sampled(s); !errors.Is(err, rate.ErrStale) {
		t.Fatalf("a cross-check on the direct source's venue priced: %v",
			err)
	}
}
