package bridged

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/paulscode/lightning-fork-bridge/chainrate"
	"github.com/paulscode/lightning-fork-bridge/lnd"
	"github.com/paulscode/lightning-fork-bridge/rate"
)

// chainReader is what the observers are fed from: a node's tip and its block
// headers by height. lnd.Chain is the real one.
type chainReader interface {
	Tip(ctx context.Context) (lnd.Tip, error)
	Header(ctx context.Context, height int32) (lnd.Header, error)
}

// chainSource is one chain's reader, observer, and what has been read of it.
type chainSource struct {
	name   string
	reader chainReader
	obs    *chainrate.Observer

	// headerAt is the tip height whose header, with its difficulty, is
	// already in the observer. Between blocks there is nothing new to read.
	headerAt int32

	// noHeaders is set once the node has said it serves no headers, so the
	// log says so once rather than every poll.
	noHeaders bool
}

// chains is both chains' sources, in a fixed order.
func (s *Service) chains() []*chainSource {
	s.chainSrcOnce.Do(func() {
		s.chainSrcs = []*chainSource{
			{name: "blake2b", obs: s.lfChain},
			{name: "bitcoin", obs: s.btcChain},
		}
		if s.lfConn != nil {
			s.chainSrcs[0].reader = lnd.NewChain(s.lfConn)
		}
		if s.btcConn != nil {
			s.chainSrcs[1].reader = lnd.NewChain(s.btcConn)
		}
	})

	return s.chainSrcs
}

// pollChains keeps both chain observers fed until ctx ends.
//
// A chain observer with nothing in it refuses to estimate, and a refused
// estimate refuses the quote that needed it. That is the right way round: the
// bridge cannot size an HTLC against a chain it has not looked at.
func (s *Service) pollChains(ctx context.Context) {
	tick := time.NewTicker(s.cfg.pollInterval())
	defer tick.Stop()

	for {
		for _, src := range s.chains() {
			s.sampleChain(ctx, src)
		}

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// sampleChain adds the tip, with its header's difficulty, to an observer.
//
// The difficulty is what lets the observer carry spacing across a retarget,
// by scaling the previous epoch's by the difficulty change. Without it the
// bridge refuses every swap until MinSamples blocks of the new epoch exist,
// which at ten minute blocks is most of a day after every retarget.
func (s *Service) sampleChain(ctx context.Context, src *chainSource) {
	// A poller that panics takes the daemon down with it, including any
	// swap holding an HTLC, and it would do that for a reason unrelated to
	// the swap. New always sets both connections, so this is a programming
	// error rather than an operational one, but it should not be a fatal
	// one from a background goroutine.
	if src.reader == nil {
		s.log.Warn("no connection to sample from", "chain", src.name)

		return
	}

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	tip, err := src.reader.Tip(ctx)
	if err != nil {
		s.log.Warn("could not read the chain tip", "chain", src.name,
			"err", err)

		return
	}
	// A node catching up reports its backend's height beside its best
	// header's time, which belong to different blocks, and that pair would
	// skew the spacing every margin is sized on.
	if tip.Height == 0 || tip.Time.IsZero() || !tip.Synced {
		return
	}
	if src.headerAt == tip.Height {
		// Already in the observer with its difficulty.
		return
	}

	b := chainrate.Block{Height: tip.Height, Time: tip.Time}
	hdr, err := src.reader.Header(ctx, tip.Height)
	read := err == nil && hdr.Height == tip.Height && !hdr.Time.IsZero()
	if read {
		// The node's best header time and the header read here are the
		// same block's, decoded twice. Disagreeing, one of the two is
		// misread, and there is no telling which.
		if !hdr.Time.Equal(tip.Time) {
			s.log.Warn("the tip's header and the node disagree about "+
				"when it was mined; not sampled", "chain", src.name,
				"height", tip.Height, "header", hdr.Time,
				"node", tip.Time)

			return
		}
		b.Time = hdr.Time
		b.Difficulty = hdr.Difficulty()
	} else if err != nil {
		s.noHeader(src, err)
	}
	if s.future(src, b) {
		return
	}

	// Add rejects nothing it should not; a height it already holds is
	// replaced, which is what a reorg needs.
	if err := s.addBlock(src.obs, b); err == nil && read &&
		b.Difficulty > 0 {

		src.headerAt = tip.Height
	}
}

// noHeader logs a header that could not be read: once, at info, for a node
// that serves none, and at debug otherwise, since the tip is read again next
// poll.
func (s *Service) noHeader(src *chainSource, err error) {
	if status.Code(err) == codes.Unimplemented {
		if !src.noHeaders {
			src.noHeaders = true
			s.log.Info("this node serves no block headers (lnd needs "+
				"the chainrpc build tag), so block spacing is learned "+
				"from new blocks only, and quotes wait for enough of "+
				"them after a start and after each retarget",
				"chain", src.name)
		}

		return
	}
	s.log.Debug("could not read a block header", "chain", src.name,
		"err", err)
}

// MaxFutureBlockTime is how far past now a block's time may be and still be
// sampled: the chains' own rule. A later one is misread or made up, and one
// such block at the tip would make the chain look stalled, and the blocks
// before it fast.
const MaxFutureBlockTime = 2 * time.Hour

// future reports, and logs, a block whose time is too far ahead to sample.
func (s *Service) future(src *chainSource, b chainrate.Block) bool {
	if !b.Time.After(time.Now().Add(MaxFutureBlockTime)) {
		return false
	}
	s.log.Warn("a block's time is too far in the future to use; not "+
		"sampled", "chain", src.name, "height", b.Height, "time", b.Time)

	return true
}

// backfillBlocks is how many recent blocks are read into each observer at
// startup: enough for the observer's floor of MinSamples blocks in the current
// epoch, with room for a window straddling a retarget, where the blocks
// before it count towards the previous epoch instead.
const backfillBlocks = 150

// backfill seeds both chain observers from history.
//
// Without it an observer learns only from tips as they arrive, one a block,
// and refuses to estimate until it has MinSamples of them. The headers already
// exist; reading them turns a wait of most of a day into seconds. A node that
// serves no headers is left to the slow way rather than stopping the daemon.
func (s *Service) backfill(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	for _, src := range s.chains() {
		if src.reader == nil {
			continue
		}
		tip, err := src.reader.Tip(ctx)
		if err != nil {
			s.log.Warn("could not read the chain tip to backfill from",
				"chain", src.name, "err", err)

			continue
		}

		from := tip.Height - backfillBlocks + 1
		if from < 0 {
			from = 0
		}
		var added int
		for h := from; h <= tip.Height && ctx.Err() == nil; h++ {
			hdr, err := src.reader.Header(ctx, h)
			if err != nil {
				// The first failure says it: a node without
				// headers fails every height the same way.
				s.noHeader(src, err)

				break
			}
			b := chainrate.Block{
				Height: hdr.Height, Time: hdr.Time,
				Difficulty: hdr.Difficulty(),
			}
			if !s.future(src, b) && s.addBlock(src.obs, b) == nil {
				added++
			}
		}
		if added > 0 {
			s.log.Info("read block history", "chain", src.name,
				"blocks", added)
		}
	}
}

// addBlock and estimate guard the observers, which the pollers write and the
// quotes, the runners and the health check read, and which are not safe for
// concurrent use.
func (s *Service) addBlock(obs *chainrate.Observer, b chainrate.Block) error {
	s.obsMu.Lock()
	defer s.obsMu.Unlock()

	return obs.Add(b)
}

func (s *Service) estimate(obs *chainrate.Observer) (chainrate.Estimate,
	error) {

	s.obsMu.Lock()
	defer s.obsMu.Unlock()

	return obs.Estimate(time.Now())
}

// pollPrice keeps the combined rate reading fresh until ctx ends.
func (s *Service) pollPrice(ctx context.Context) {
	tick := time.NewTicker(s.cfg.pollInterval())
	defer tick.Stop()

	for {
		s.samplePrice(ctx)

		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// MinPriceVenues is how many venues' readings must answer, fresh, for a
// price to be quoted: one to be checked against the other. Readings from one
// venue count once (see RateSource.Venue).
const MinPriceVenues = 2

// samplePrice reads every source and combines them.
//
// A source that fails is left out rather than defaulted: the oracle's whole
// job is to notice when sources disagree, and a made-up number that agrees
// with everything defeats it. If that leaves readings from fewer than
// MinPriceVenues venues, the price is refused, and the refusal is what quotes
// see.
func (s *Service) samplePrice(ctx context.Context) {
	// Same reasoning as sampleChain: New always builds the oracle, so a
	// missing one is a programming error, and a background goroutine is
	// the wrong place to turn one into a dead daemon.
	if s.oracle == nil {
		s.log.Warn("no rate oracle to price with")

		return
	}

	obs := make([]rate.Observation, 0, len(s.cfg.RateSources)+1)
	for _, src := range s.cfg.RateSources {
		price, at, err := s.readSource(ctx, src)
		if err != nil {
			s.log.Warn("a rate source failed", "source", src.Name,
				"err", err)

			continue
		}
		obs = append(obs, rate.Observation{
			Source: src.Name, OutgoingPerIncoming: price, At: at,
			Venue: src.venue(),
		})
	}
	if cc := s.cfg.CrossCheck; cc != nil {
		o, err := s.crossCheck(ctx, *cc)
		if err != nil {
			s.log.Warn("the cross-check failed", "source", cc.Name,
				"err", err)
		} else {
			o.Venue = cc.venue()
			obs = append(obs, o)
		}
	}

	venues := map[string]bool{}
	for _, o := range obs {
		venues[o.Venue] = true
	}

	var reading rate.Reading
	var err error
	if len(venues) < MinPriceVenues {
		err = fmt.Errorf("%w: %d of the price sources answered, from %d "+
			"venues, and %d venues are needed to check one against "+
			"another", rate.ErrStale, len(obs), len(venues),
			MinPriceVenues)
	} else {
		reading, err = s.oracle.Update(time.Now(), obs)
		if err == nil && reading.Venues < MinPriceVenues {
			// The oracle drops a reading older than its maxAge, by
			// the source's own clock where it gives one.
			err = fmt.Errorf("%w: %d of the price sources are recent "+
				"enough, from %d venues, and %d venues are needed to "+
				"check one against another", rate.ErrStale,
				reading.Sources, reading.Venues, MinPriceVenues)
		}
	}

	s.priceMu.Lock()
	if err != nil {
		s.priceErr = err
	} else {
		s.price, s.priceErr = reading, nil
	}
	s.priceMu.Unlock()

	if err != nil {
		s.log.Warn("no usable price", "sources", len(obs), "err", err)

		return
	}
	s.log.Info("price", "outgoingPerIncoming", reading.OutgoingPerIncoming,
		"sources", reading.Sources, "venues", reading.Venues,
		"spread", reading.Spread)
}

// readSource reads one source and says when its price is from: the source's
// own time where it gives one, and otherwise now, unless it has answered the
// very same price for longer than its MaxUnchanged, which is refused as a
// static price.
func (s *Service) readSource(ctx context.Context, src RateSource) (float64,
	time.Time, error) {

	price, at, err := fetchPrice(ctx, src)
	if err != nil {
		return 0, time.Time{}, err
	}
	if src.TimeField != "" {
		return price, at, nil
	}

	now := time.Now()
	s.staticMu.Lock()
	defer s.staticMu.Unlock()
	if s.unchanged == nil {
		s.unchanged = map[string]unchanged{}
	}
	u, ok := s.unchanged[src.Name]
	if !ok || u.price != price {
		u = unchanged{price: price, since: now}
		s.unchanged[src.Name] = u
	}
	if held := now.Sub(u.since); held > src.maxUnchanged() {
		return 0, time.Time{}, fmt.Errorf("%w: %s has answered %g for %v, "+
			"longer than the %v a price may stand still before it is "+
			"taken for a static one", rate.ErrStale, src.Name, price,
			held.Round(time.Second), src.maxUnchanged())
	}

	return price, now, nil
}

// unchanged is a source's last price and since when it has been answering it.
type unchanged struct {
	price float64
	since time.Time
}

// crossCheck derives a reading from the cross-check's two dollar prices. It is
// as old as the older of the two.
func (s *Service) crossCheck(ctx context.Context, cc CrossCheck) (
	rate.Observation, error) {

	in, inAt, err := s.readSource(ctx, cc.IncomingUSD)
	if err != nil {
		return rate.Observation{}, fmt.Errorf("%s: %w", cc.IncomingUSD.Name,
			err)
	}
	out, outAt, err := s.readSource(ctx, cc.OutgoingUSD)
	if err != nil {
		return rate.Observation{}, fmt.Errorf("%s: %w", cc.OutgoingUSD.Name,
			err)
	}
	at := inAt
	if outAt.Before(at) {
		at = outAt
	}

	return rate.Derive(cc.Name, in, out, at)
}

// priceClient fetches prices. Redirects are refused rather than followed: the
// URL is the operator's choice, and a source that answers with a redirect has
// stopped being the thing they configured.
var priceClient = &http.Client{
	Timeout: 30 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// fetchPrice reads one source: the price, and when the source says it worked
// it out, which is zero for a source with no TimeField.
func fetchPrice(ctx context.Context, src RateSource) (float64, time.Time,
	error) {

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return 0, time.Time{}, err
	}
	resp, err := priceClient.Do(req)
	if err != nil {
		return 0, time.Time{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, time.Time{}, fmt.Errorf("%s answered %s", src.URL,
			resp.Status)
	}

	// Bounded: a source that streams forever must not take the daemon with
	// it, and a price is a few hundred bytes.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, time.Time{}, err
	}

	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return 0, time.Time{}, fmt.Errorf("%s did not return JSON: %w",
			src.URL, err)
	}

	price, err := extract(doc, src.Field)
	if err != nil {
		return 0, time.Time{}, err
	}
	// Checked here, so that the source is left out like one that did not
	// answer: the oracle refuses every reading when any one is unusable.
	if !(price > 0) || math.IsInf(price, 0) {
		return 0, time.Time{}, fmt.Errorf("%w: %s answered %g",
			rate.ErrObservation, src.Name, price)
	}
	if src.TimeField == "" {
		return price, time.Time{}, nil
	}

	at, err := extractTime(doc, src.TimeField)
	if err != nil {
		// Read as stale rather than as unreadable: a source that stops
		// saying when may have stopped computing.
		return 0, time.Time{}, fmt.Errorf("%w: %s gave no usable time: %w",
			rate.ErrStale, src.Name, err)
	}

	return price, at, nil
}

// extractTime reads a time: seconds or milliseconds since 1970, as a number
// or a string, or an RFC 3339 string. A count past 10^12 is milliseconds,
// which as seconds would be tens of thousands of years from now.
func extractTime(doc any, path string) (time.Time, error) {
	cur, err := walk(doc, path)
	if err != nil {
		return time.Time{}, err
	}
	if str, ok := cur.(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
			return t, nil
		}
	}

	n, err := number(cur, path)
	if err != nil {
		return time.Time{}, err
	}
	if n <= 0 || math.IsInf(n, 0) || math.IsNaN(n) {
		return time.Time{}, fmt.Errorf("%q holds %v, which is not a time",
			path, cur)
	}
	if n > 1e12 {
		return time.UnixMilli(int64(n)), nil
	}

	return time.Unix(int64(n), 0), nil
}

// walk follows a dotted path through nested objects.
func walk(doc any, path string) (any, error) {
	cur := doc
	for _, part := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%q: %s is not an object", path, part)
		}
		cur, ok = obj[part]
		if !ok {
			return nil, fmt.Errorf("%q: no field %s", path, part)
		}
	}

	return cur, nil
}

// extract walks a dotted path and reads a number, which JSON may have encoded
// as a string.
func extract(doc any, path string) (float64, error) {
	cur, err := walk(doc, path)
	if err != nil {
		return 0, err
	}

	return number(cur, path)
}

// number reads a JSON number, or a string holding one.
func number(cur any, path string) (float64, error) {
	switch v := cur.(type) {
	case float64:
		return v, nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("%q holds %q, which is not a number", path, v)
		}

		return f, nil
	default:
		return 0, fmt.Errorf("%q holds %T, which is not a number", path, cur)
	}
}
