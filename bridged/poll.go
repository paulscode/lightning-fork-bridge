package bridged

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
		b.Time = hdr.Time
		b.Difficulty = hdr.Difficulty()
	} else if err != nil {
		s.noHeader(src, err)
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
			if s.addBlock(src.obs, chainrate.Block{
				Height: hdr.Height, Time: hdr.Time,
				Difficulty: hdr.Difficulty(),
			}) == nil {
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

// samplePrice reads every source and combines them.
//
// A source that fails is left out rather than defaulted: the oracle's whole
// job is to notice when sources disagree, and a made-up number that agrees
// with everything defeats it. If that leaves too few, the oracle refuses and
// the refusal is what quotes see.
func (s *Service) samplePrice(ctx context.Context) {
	// Same reasoning as sampleChain: New always builds the oracle, so a
	// missing one is a programming error, and a background goroutine is
	// the wrong place to turn one into a dead daemon.
	if s.oracle == nil {
		s.log.Warn("no rate oracle to price with")

		return
	}

	obs := make([]rate.Observation, 0, len(s.cfg.RateSources))
	for _, src := range s.cfg.RateSources {
		price, err := fetchPrice(ctx, src)
		if err != nil {
			s.log.Warn("a rate source failed", "source", src.Name,
				"err", err)

			continue
		}
		obs = append(obs, rate.Observation{
			Source: src.Name, OutgoingPerIncoming: price, At: time.Now(),
		})
	}

	reading, err := s.oracle.Update(time.Now(), obs)

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
		"sources", reading.Sources, "spread", reading.Spread)
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

// fetchPrice reads one source.
func fetchPrice(ctx context.Context, src RateSource) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := priceClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s answered %s", src.URL, resp.Status)
	}

	// Bounded: a source that streams forever must not take the daemon with
	// it, and a price is a few hundred bytes.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}

	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return 0, fmt.Errorf("%s did not return JSON: %w", src.URL, err)
	}

	return extract(doc, src.Field)
}

// extract walks a dotted path and reads a number, which JSON may have encoded
// as a string.
func extract(doc any, path string) (float64, error) {
	cur := doc
	for _, part := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return 0, fmt.Errorf("%q: %s is not an object", path, part)
		}
		cur, ok = obj[part]
		if !ok {
			return 0, fmt.Errorf("%q: no field %s", path, part)
		}
	}

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
