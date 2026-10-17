package bridged

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// DefaultCompactInterval is how often the journal is rewritten down to each
// swap's latest record.
const DefaultCompactInterval = time.Hour

// Run starts the daemon and blocks until ctx ends or the listener fails.
//
// The order matters. Pending swaps are resumed before the surface opens, so a
// swap interrupted by the last shutdown is being driven again before any new
// one can be quoted: an HTLC that is already locked in has a deadline, and a
// new quote can wait.
//
// However it ends, the swaps being driven are stopped and waited for before
// this returns. The caller's next move is Close, which shuts the node
// connections those swaps are talking over, and the error paths used to skip
// the wait: resume starts driving before the listener is opened, so a failure
// to listen would have pulled the connections out from under them.
func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctxOrBackground(ctx))
	s.setDriveCtx(ctx)

	err := s.serve(ctx)

	// Unconditional, and deliberately not a defer: the cancel has to happen
	// before the wait, or the swaps would be waited on while still running,
	// and defer ordering is an easy thing to get backwards later.
	cancel()
	if n := s.active(); n > 0 {
		s.log.Info("waiting for swaps in flight", "swaps", n)
	}
	s.wg.Wait()

	return err
}

// serve is everything between the observers starting and the surface closing.
func (s *Service) serve(ctx context.Context) error {
	// Observers first, and a moment to fill: a resumed swap is sized
	// against block spacing, and an observer with nothing in it refuses.
	go s.pollChains(ctx)
	go s.pollPrice(ctx)
	go s.compact(ctx)

	if err := s.waitForChains(ctx); err != nil {
		return err
	}

	if err := s.resume(ctx); err != nil {
		// Not fatal. A swap that cannot be resumed is a swap that needs an
		// operator, and refusing to start would take the others down with
		// it and close the surface that reports them.
		s.log.Error("some swaps could not be resumed", "err", err)
	}

	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", s.cfg.Listen, err)
	}

	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// A quote waits on two Lightning nodes, so the read and write
		// timeouts are left off deliberately; the handler bounds itself.
		// Idle connections are not left to accumulate.
		IdleTimeout: 2 * time.Minute,
	}

	s.log.Info("bridge is up", "listen", ln.Addr().String(),
		"directions", len(s.sides))

	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()

	select {
	case err := <-served:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	case <-ctx.Done():
	}

	// Stop accepting before the caller stops driving, so no new swap is
	// quoted into a daemon on its way out. Shutdown waits for the handlers
	// already running, which is what makes the wait afterwards complete.
	shutdown, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()
	_ = srv.Shutdown(shutdown)

	return nil
}

// active is how many swaps are being driven, across directions.
func (s *Service) active() int {
	var n int
	for _, sd := range s.sides {
		if sd.runner == nil {
			continue
		}
		n += sd.runner.Active()
	}

	return n
}

// compact rewrites the journal down to each swap's latest record, until ctx
// ends. Finished swaps keep theirs: a new quote for the same hash is judged by
// how the last attempt ended.
//
// The journal is append-only, so every state a swap passes through is another
// line, and a daemon that runs for months replays all of them at startup.
// Nothing else calls this, which is why it belongs here rather than in an
// operator's memory.
func (s *Service) compact(ctx context.Context) {
	if s.journal == nil {
		return
	}

	tick := time.NewTicker(s.cfg.compactInterval())
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}

		// Not while swaps are being driven. Compaction takes the journal's
		// lock, and a swap blocked on writing its own state is a swap that
		// is not watching an HTLC.
		if n := s.active(); n > 0 {
			s.log.Debug("not compacting yet", "swaps", n)

			continue
		}
		if err := s.journal.Compact(ctx); err != nil {
			s.log.Warn("could not compact the journal", "err", err)

			continue
		}
		s.log.Info("compacted the journal")
	}
}

// waitForChains blocks until both observers can estimate, or ctx ends.
//
// Without this the daemon would open its surface and refuse every quote until
// the observers filled, which looks like a broken bridge rather than a
// starting one.
func (s *Service) waitForChains(ctx context.Context) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	// Waiting in silence looks the same as being hung, and this runs before
	// the surface opens, so there is nothing to ask. Say so periodically.
	say := time.NewTicker(15 * time.Second)
	defer say.Stop()

	var last error
	for {
		_, lfErr := s.lfChain.Estimate(time.Now())
		_, btcErr := s.btcChain.Estimate(time.Now())
		if lfErr == nil && btcErr == nil {
			return nil
		}
		last = errors.Join(lfErr, btcErr)

		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up waiting for both chains to be "+
				"measurable: %w", last)
		case <-say.C:
			s.log.Info("waiting for both chains to be measurable, which "+
				"takes a few blocks from each", "why", last)
		case <-tick.C:
		}
	}
}

// resume drives every swap the journal says is unfinished.
//
// Each is routed to the direction whose paying node can read its outgoing
// invoice, because that is the node that will pay it. Running every direction's
// Resume over a shared journal would have both of them drive every swap, and
// the one whose nodes are the wrong way round would decide against a chain the
// swap is not on.
func (s *Service) resume(ctx context.Context) error {
	pending, err := s.journal.Pending(ctx)
	if err != nil {
		return fmt.Errorf("reading pending swaps: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	s.log.Info("resuming", "swaps", len(pending))

	var errs []error
	for _, rec := range pending {
		sd, _, err := s.route(ctx, rec.Invoice)
		if err != nil {
			// Nothing will drive this swap, and if its incoming HTLC is
			// locked in it has a deadline. The usual cause is a direction
			// that was enabled when the swap was quoted and is not now.
			errs = append(errs, fmt.Errorf("swap %x (state %v, invoice %q) "+
				"has no enabled direction that can pay it, so nothing will "+
				"drive it: %w", rec.Hash, rec.State, rec.Invoice, err))

			continue
		}
		s.drive(sd, rec.Hash)
	}

	return errors.Join(errs...)
}
