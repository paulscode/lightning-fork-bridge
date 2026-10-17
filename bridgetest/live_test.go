// Package bridgetest drives the whole bridge against real nodes.
//
// Everything below this has been tested in isolation, which is the right way
// to test the parts that lose money. It is not a substitute for watching an
// actual HTLC lock in on one chain and an actual payment leave on another,
// because the assumptions that survive a unit test are exactly the ones nobody
// thought to question.
//
// The lab stands the nodes up:
//
//	cd /mnt/Black/lightning-fork-lab && make bridge-setup
//	cd /mnt/Black/lightning-fork-bridge && BRIDGE_LAB=/tmp/bridge-lab \
//	    go test -run TestLive -v ./bridgetest/
//
// It skips without BRIDGE_LAB, and lives outside a build tag so it cannot
// quietly stop compiling.
package bridgetest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/inventory"
	"github.com/paulscode/lightning-fork-bridge/lnd"
	"github.com/paulscode/lightning-fork-bridge/lndrpc/lnrpc"
	"github.com/paulscode/lightning-fork-bridge/lndrpc/routerrpc"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/quote"
	"github.com/paulscode/lightning-fork-bridge/rate"
	"github.com/paulscode/lightning-fork-bridge/runner"
	"github.com/paulscode/lightning-fork-bridge/store"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// lab is where make bridge-setup left the credentials.
func lab(t *testing.T) string {
	t.Helper()

	dir := os.Getenv("BRIDGE_LAB")
	if dir == "" {
		t.Skip("set BRIDGE_LAB to the directory make bridge-setup wrote")
	}

	return dir
}

// dial connects to one of the lab's nodes by name.
func dial(t *testing.T, dir, name string) *grpc.ClientConn {
	t.Helper()

	addr, err := os.ReadFile(filepath.Join(dir, name+".addr"))
	if err != nil {
		t.Fatalf("no address for %s: %v", name, err)
	}

	conn, err := lnd.Dial(context.Background(), lnd.Config{
		Address:      strings.TrimSpace(string(addr)) + ":10009",
		TLSCertPath:  filepath.Join(dir, name+"-tls.cert"),
		MacaroonPath: filepath.Join(dir, name+"-admin.macaroon"),
		// The lab's certificates cover the container name, not the address
		// the host reaches them on.
		ServerName: name,
	})
	if err != nil {
		t.Fatalf("dialling %s: %v", name, err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// TestLiveSwap runs one swap end to end across two chains.
//
// lf2 is a user on the BLAKE2b chain who wants to pay a Bitcoin invoice.
// lnd-sha2 issues that invoice. lf1 and lnd-sha are the bridge's two nodes.
// Nothing is mocked: real hold invoices, real HTLCs, real payments, and a real
// journal on disk.
func TestLiveSwap(t *testing.T) {
	dir := lab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var (
		lf1Conn  = dial(t, dir, "lf1")      // bridge receives here
		lf2Conn  = dial(t, dir, "lf2")      // the user
		shaConn  = dial(t, dir, "lnd-sha")  // bridge pays from here
		sha2Conn = dial(t, dir, "lnd-sha2") // the destination
	)

	in := lnd.NewIncoming(lf1Conn)
	out := lnd.NewOutgoing(shaConn)
	checkNodes(t, ctx, in, out)

	// The destination issues the invoice the user wants paid. Its final-hop
	// delta is set explicitly: lnd's default of 80 would fill the whole
	// outgoing route budget on its own, and the quote refuses that for good
	// reason rather than as an accident of configuration.
	// Small, because the user's channel bounds this and BTCB2 is worth about
	// 1/325th of a BTC: every satoshi asked for on the Bitcoin side costs the
	// user several hundred on the BLAKE2b side. The lab's lf2 holds a few
	// hundred thousand, so the Bitcoin invoice has to be in the hundreds.
	const payMsat = 150_000 // 150 sats
	sha2 := lnrpc.NewLightningClient(sha2Conn)
	invoice, err := sha2.AddInvoice(ctx, &lnrpc.Invoice{
		ValueMsat: payMsat, Memo: "a coffee, paid from the BLAKE2b chain",
		CltvExpiry: 40, Expiry: 3600,
	})
	if err != nil {
		t.Fatalf("creating the destination invoice: %v", err)
	}
	t.Logf("destination invoice: %s...", invoice.GetPaymentRequest()[:32])

	// A real journal on disk, so the durability path runs too.
	journal, err := store.Open(filepath.Join(t.TempDir(), "swaps.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	q := labQuoter(t, in, out, shaConn, journal)

	terms, err := q.Quote(ctx, invoice.GetPaymentRequest())
	if err != nil {
		t.Fatalf("quoting: %v", err)
	}
	t.Logf("quote: pay %d msat BTCB2 for %d msat BTC, spread %.2f%%, "+
		"incoming CLTV %d blocks, hold invoice %s...",
		terms.IncomingMsat, terms.OutgoingMsat, terms.Spread*100,
		terms.CLTVDelta, terms.HoldInvoice[:24])

	if terms.IncomingMsat <= terms.OutgoingMsat {
		t.Errorf("the user pays %d BTCB2 msat for %d BTC msat, which is not "+
			"the right way round at a rate of %g", terms.IncomingMsat,
			terms.OutgoingMsat, labRate)
	}
	if terms.CLTVDelta < 100 {
		t.Errorf("an incoming CLTV of %d blocks is too small to have come "+
			"from the margin policy", terms.CLTVDelta)
	}

	// The user pays the hold invoice. It blocks: a hold invoice does not
	// settle until the bridge says so, which is the entire mechanism, so
	// this runs alongside the bridge rather than before it.
	paid := make(chan error, 1)
	go func() { paid <- payHoldInvoice(ctx, t, lf2Conn, terms.HoldInvoice) }()

	r := &runner.Runner{
		Driver: &driver.Driver{
			In: in, Out: out, Store: journal, Policy: labMargin,
		},
		Store: journal, Rates: labRates, Poll: 250 * time.Millisecond,
		PayTimeout: 60 * time.Second,
	}

	driven := make(chan error, 1)
	go func() { driven <- r.Drive(ctx, terms.Hash) }()

	select {
	case err := <-driven:
		if err != nil {
			t.Fatalf("driving the swap: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the bridge did not finish the swap in time")
	}

	// What the bridge thinks happened.
	rec, err := journal.Get(context.Background(), terms.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != swap.Settled {
		// Say why before saying what. A swap that ends in Expired because
		// the user could not fund it is a fact about their channel, and
		// reporting only the state sends the reader to the wrong place.
		select {
		case err := <-paid:
			t.Fatalf("the swap ended in %v; the user's payment says: %v",
				rec.State, err)
		default:
			t.Fatalf("the swap ended in %v, not settled", rec.State)
		}
	}
	t.Logf("swap settled, preimage %x", rec.Preimage[:8])

	// What the user's node thinks happened.
	select {
	case err := <-paid:
		if err != nil {
			t.Fatalf("the user's payment did not succeed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the user's payment never resolved after the bridge settled")
	}

	// And what the destination thinks, which is the only party whose opinion
	// the user actually cares about.
	got, err := sha2.LookupInvoice(context.Background(), &lnrpc.PaymentHash{
		RHash: terms.Hash[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetState() != lnrpc.Invoice_SETTLED {
		t.Fatalf("the destination's invoice is %v, not settled", got.GetState())
	}
	if got.GetAmtPaidMsat() != payMsat {
		t.Errorf("the destination was paid %d msat, wanted %d",
			got.GetAmtPaidMsat(), payMsat)
	}

	t.Logf("end to end: %d msat BTCB2 in on the BLAKE2b chain, %d msat BTC "+
		"out on Bitcoin, one preimage settled both",
		rec.IncomingMsat, got.GetAmtPaidMsat())
}

type errUserPaymentFailed struct {
	reason lnrpc.PaymentFailureReason
}

func (e errUserPaymentFailed) Error() string {
	return "user payment failed: " + e.reason.String()
}

// TestLiveRefund is the other half, and for a user it is the half that
// matters: what happens when the bridge cannot deliver.
//
// The destination's invoice is cancelled after the quote is struck but before
// the user funds it, so the outgoing leg fails definitively against a real
// node. The incoming HTLC must come back. Nothing about this is the bridge
// being clever: it is the state machine refusing to let a cancel happen until
// the outgoing leg is definitively resolved, and then insisting on it.
func TestLiveRefund(t *testing.T) {
	dir := lab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var (
		lf1Conn  = dial(t, dir, "lf1")
		lf2Conn  = dial(t, dir, "lf2")
		shaConn  = dial(t, dir, "lnd-sha")
		sha2Conn = dial(t, dir, "lnd-sha2")
	)

	in := lnd.NewIncoming(lf1Conn)
	out := lnd.NewOutgoing(shaConn)
	dest := lnd.NewIncoming(sha2Conn)
	checkNodes(t, ctx, in, out)

	const payMsat = 120_000
	sha2 := lnrpc.NewLightningClient(sha2Conn)
	invoice, err := sha2.AddInvoice(ctx, &lnrpc.Invoice{
		ValueMsat: payMsat, Memo: "a coffee that will not be served",
		CltvExpiry: 40, Expiry: 3600,
	})
	if err != nil {
		t.Fatal(err)
	}

	journal, err := store.Open(filepath.Join(t.TempDir(), "swaps.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	q := labQuoter(t, in, out, shaConn, journal)
	terms, err := q.Quote(ctx, invoice.GetPaymentRequest())
	if err != nil {
		t.Fatalf("quoting: %v", err)
	}
	t.Logf("quote: %d msat BTCB2 for %d msat BTC", terms.IncomingMsat,
		terms.OutgoingMsat)

	// The destination changes its mind. From here the outgoing leg cannot
	// succeed, whatever the bridge does.
	if err := dest.CancelInvoice(ctx, terms.Hash); err != nil {
		t.Fatalf("cancelling the destination invoice: %v", err)
	}
	t.Log("destination invoice cancelled; the outgoing leg can no longer be paid")

	paid := make(chan error, 1)
	go func() { paid <- payHoldInvoice(ctx, t, lf2Conn, terms.HoldInvoice) }()

	r := &runner.Runner{
		Driver: &driver.Driver{
			In: in, Out: out, Store: journal, Policy: labMargin,
		},
		Store: journal, Rates: labRates, Poll: 250 * time.Millisecond,
		PayTimeout: 45 * time.Second,
	}
	if err := r.Drive(ctx, terms.Hash); err != nil {
		t.Fatalf("driving: %v", err)
	}

	rec, err := journal.Get(context.Background(), terms.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != swap.Failed {
		t.Fatalf("the swap ended in %v, want failed", rec.State)
	}
	var zero [32]byte
	if rec.Preimage != zero {
		t.Error("a failed swap is holding a preimage")
	}

	// The user's money came back. This is the assertion the whole design is
	// for: a bridge that cannot deliver must not keep the funds.
	select {
	case err := <-paid:
		if err == nil {
			t.Fatal("the user's payment succeeded against a cancelled invoice")
		}
		t.Logf("the user was refunded: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the user's payment never resolved; their money is still held")
	}

	// And the bridge sent nothing.
	pmt, err := out.LookupPayment(context.Background(), terms.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if pmt.State == node.PaymentSucceeded {
		t.Fatal("the bridge paid out on a swap it then failed")
	}
	t.Logf("outgoing leg: %v, incoming claim returned, nothing lost",
		pmt.State)
}

// labMargin and labRates are the timing policy both live tests run under. The
// bounds are the mainnet-shaped numbers from S11: a regtest where blocks are
// mined on demand has no spacing to measure, and bounds that reflected that
// would wave every swap through the check this exists to make.
var labMargin = margin.Policy{Settlement: time.Hour, MaxIncomingBlocks: 2016,
	HoldExpiryDelta: margin.DefaultHoldExpiryDelta}

// labRate is what a BTCB2 was worth in BTC when the venue was measured. The
// lab has no market, so the number comes from the design's measurement rather
// than from anywhere live.
const labRate = 0.00308078

func labRates(context.Context) (driver.Rates, error) {
	return driver.Rates{
		Incoming: margin.Bounds{
			Fast: 155 * time.Second, Slow: 1200 * time.Second,
		},
		Outgoing: margin.Bounds{
			Fast: 300 * time.Second, Slow: 600 * time.Second,
		},
		As: time.Now(),
	}, nil
}

// labQuoter builds a quoter priced off the bridge's real channel balance.
func labQuoter(t *testing.T, in *lnd.Incoming, out *lnd.Outgoing,
	shaConn *grpc.ClientConn, journal *store.Journal) *quote.Quoter {

	t.Helper()

	balance := func(ctx context.Context) (uint64, error) {
		bal, err := lnrpc.NewLightningClient(shaConn).ChannelBalance(ctx,
			&lnrpc.ChannelBalanceRequest{})
		if err != nil {
			return 0, err
		}

		return uint64(bal.GetLocalBalance().GetMsat()), nil
	}

	return &quote.Quoter{
		In: in, Out: out, Store: journal,
		Price: func(ctx context.Context) (rate.Reading, error) {
			held, err := balance(ctx)
			if err != nil {
				return rate.Reading{}, err
			}
			// Scaled to the lab's channel rather than the design's Stage 1
			// band, so the curve is exercised on the balance that exists.
			p := inventory.DefaultPolicy
			p.TargetOutgoingMsat = held
			p.FloorOutgoingMsat = held / 10

			spread, err := p.Spread(inventory.State{
				OutgoingMsat: held, At: time.Now(),
			}, inventory.Draining, time.Now())
			if err != nil {
				return rate.Reading{}, err
			}

			return rate.Reading{
				OutgoingPerIncoming: labRate, At: time.Now(), Sources: 1,
				Spread: spread.Spread,
			}, nil
		},
		Spacing: labRates,
		Room: func(ctx context.Context) (uint64, error) {
			held, err := balance(ctx)
			if err != nil {
				return 0, err
			}
			committed, err := store.Committed(ctx, journal)
			if err != nil {
				return 0, err
			}
			if held < committed {
				return 0, nil
			}

			return held - committed, nil
		},
		Margin: labMargin,
		Policy: quote.Policy{
			MinSwapMsat: 100_000, MaxSwapMsat: 500_000_000,
			Validity: 2 * time.Minute, InvoiceExpiry: 10 * time.Minute,
			OutgoingCLTVLimit: 80, FeeFraction: 0.003,
			MinRemainingLife: 2 * time.Minute,
		},
	}
}

// payHoldInvoice pays as the user would, and blocks: a hold invoice does not
// settle until the bridge says so, which is the entire mechanism.
func payHoldInvoice(ctx context.Context, t *testing.T, conn *grpc.ClientConn,
	invoice string) error {

	stream, err := routerrpc.NewRouterClient(conn).SendPaymentV2(ctx,
		&routerrpc.SendPaymentRequest{
			PaymentRequest: invoice, TimeoutSeconds: 150,
			FeeLimitMsat: 50_000_000,
		})
	if err != nil {
		return err
	}
	for {
		update, err := stream.Recv()
		if err != nil {
			return err
		}
		t.Logf("user payment: %v", update.GetStatus())
		switch update.GetStatus() {
		case lnrpc.Payment_SUCCEEDED:
			return nil
		case lnrpc.Payment_FAILED:
			return errUserPaymentFailed{update.GetFailureReason()}
		}
	}
}

// checkNodes refuses to start against a node that is not caught up, and says
// what to do about it.
//
// An idle regtest goes unsynced on its own: lnd judges by the age of the best
// block, so a chain nobody is mining drifts out of sync without falling
// behind. That is the lab, not the bridge; on a live chain the same check is
// what stops a swap being sized against a height that is not the present one.
func checkNodes(t *testing.T, ctx context.Context, in *lnd.Incoming,
	out *lnd.Outgoing) {

	t.Helper()

	for name, err := range map[string]error{
		"incoming": in.Check(ctx), "outgoing": out.Check(ctx),
	} {
		if err == nil {
			continue
		}
		if errors.Is(err, lnd.ErrNotSynced) {
			t.Fatalf("the %s node is not caught up, which an idle regtest "+
				"does on its own: run `make bridge-setup` in the lab, which "+
				"mines on both chains, and try again. (%v)", name, err)
		}
		t.Fatalf("the %s node: %v", name, err)
	}
}

// labRatesReverse is labRates with the legs swapped: the incoming chain is
// Bitcoin and the outgoing one is BLAKE2b. Sizing a swap needs each chain's
// own spacing against the leg it is actually on, and this direction is the
// easier of the two for it: the outgoing leg resolves on the fast chain, so
// the incoming CLTV it demands of Bitcoin is small.
func labRatesReverse(context.Context) (driver.Rates, error) {
	return driver.Rates{
		Incoming: margin.Bounds{
			Fast: 300 * time.Second, Slow: 600 * time.Second,
		},
		Outgoing: margin.Bounds{
			Fast: 155 * time.Second, Slow: 1200 * time.Second,
		},
		As: time.Now(),
	}, nil
}

// labQuoterReverse is labQuoter for a bridge taking BTC and paying BTCB2.
//
// Nothing in quote, driver, runner or store changes to run this way. The
// quoter takes its nodes and its rate as collaborators, so the reverse
// direction is the same machinery with the two nodes exchanged and the rate
// inverted. That is the whole of it, and it is why the rate is named for the
// legs rather than for the assets.
func labQuoterReverse(t *testing.T, in *lnd.Incoming, out *lnd.Outgoing,
	lfConn *grpc.ClientConn, journal *store.Journal) *quote.Quoter {

	t.Helper()

	// The outgoing side is now the BLAKE2b node, so that is the balance the
	// bridge is spending and the one the spread must respond to.
	balance := func(ctx context.Context) (uint64, error) {
		bal, err := lnrpc.NewLightningClient(lfConn).ChannelBalance(ctx,
			&lnrpc.ChannelBalanceRequest{})
		if err != nil {
			return 0, err
		}

		return uint64(bal.GetLocalBalance().GetMsat()), nil
	}

	return &quote.Quoter{
		In: in, Out: out, Store: journal,
		Price: func(ctx context.Context) (rate.Reading, error) {
			held, err := balance(ctx)
			if err != nil {
				return rate.Reading{}, err
			}
			p := inventory.DefaultPolicy
			p.TargetOutgoingMsat = held
			p.FloorOutgoingMsat = held / 10

			// Replenishing: this direction brings in the asset the
			// draining direction spends, which is the flow the
			// inventory model wants and does not need a miner to
			// produce.
			spread, err := p.Spread(inventory.State{
				OutgoingMsat: held, At: time.Now(),
			}, inventory.Replenishing, time.Now())
			if err != nil {
				return rate.Reading{}, err
			}

			return rate.Reading{
				OutgoingPerIncoming: 1 / labRate, At: time.Now(),
				Sources: 1, Spread: spread.Spread,
			}, nil
		},
		Spacing: labRatesReverse,
		Room: func(ctx context.Context) (uint64, error) {
			held, err := balance(ctx)
			if err != nil {
				return 0, err
			}
			committed, err := store.Committed(ctx, journal)
			if err != nil {
				return 0, err
			}
			if held < committed {
				return 0, nil
			}

			return held - committed, nil
		},
		Margin: labMargin,
		// This direction pays the user's BLAKE2b invoice.
		PaysBLAKE2b: true,
		Policy: quote.Policy{
			MinSwapMsat: 1_000, MaxSwapMsat: 500_000_000,
			Validity: 2 * time.Minute, InvoiceExpiry: 10 * time.Minute,
			OutgoingCLTVLimit: 80, FeeFraction: 0.003,
			MinRemainingLife: 2 * time.Minute,
		},
	}
}

// TestLiveReverseSwap is the other direction: someone on Bitcoin's Lightning
// network paying a BLAKE2b invoice.
//
// The forward direction has been the one under test because it is the one
// users asked for. This one matters for a second reason: it is the flow that
// puts back what the forward direction spends. The inventory model calls that
// replenishing and its comments reach for mining payouts as the example, which
// made the reverse flow look like it depended on a mining integration. It does
// not. An ordinary payment in the other direction is hash-linked exactly like
// the forward one, and replenishes just as well.
func TestLiveReverseSwap(t *testing.T) {
	dir := lab(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var (
		shaConn  = dial(t, dir, "lnd-sha")  // bridge receives here, on Bitcoin
		sha2Conn = dial(t, dir, "lnd-sha2") // the user, on Bitcoin
		lf1Conn  = dial(t, dir, "lf1")      // bridge pays from here, on BLAKE2b
		lf2Conn  = dial(t, dir, "lf2")      // the destination, on BLAKE2b
	)

	in := lnd.NewIncoming(shaConn)
	out := lnd.NewOutgoing(lf1Conn)
	checkNodes(t, ctx, in, out)

	// The destination is on the BLAKE2b chain and asks in BTCB2. A hundred
	// thousand sats of it is worth a few hundred sats of BTC, which is what
	// the user's channel can carry.
	const payMsat = 100_000_000 // 100k sats of BTCB2
	lf2 := lnrpc.NewLightningClient(lf2Conn)
	invoice, err := lf2.AddInvoice(ctx, &lnrpc.Invoice{
		ValueMsat: payMsat, Memo: "paid from Bitcoin's Lightning network",
		CltvExpiry: 40, Expiry: 3600,
	})
	if err != nil {
		t.Fatalf("creating the destination invoice: %v", err)
	}
	t.Logf("destination invoice: %s...", invoice.GetPaymentRequest()[:32])

	journal, err := store.Open(filepath.Join(t.TempDir(), "swaps.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	q := labQuoterReverse(t, in, out, lf1Conn, journal)

	terms, err := q.Quote(ctx, invoice.GetPaymentRequest())
	if err != nil {
		t.Fatalf("quoting: %v", err)
	}
	t.Logf("quote: pay %d msat BTC for %d msat BTCB2, spread %.2f%%, "+
		"incoming CLTV %d blocks, hold invoice %s...",
		terms.IncomingMsat, terms.OutgoingMsat, terms.Spread*100,
		terms.CLTVDelta, terms.HoldInvoice[:24])

	// The other way round from the forward test, and for the same reason:
	// a BTC msat buys many BTCB2 msat, so the payer brings fewer of them.
	if terms.IncomingMsat >= terms.OutgoingMsat {
		t.Errorf("the user pays %d BTC msat for %d BTCB2 msat, which is not "+
			"the right way round at a rate of %g", terms.IncomingMsat,
			terms.OutgoingMsat, 1/labRate)
	}

	paid := make(chan error, 1)
	go func() { paid <- payHoldInvoice(ctx, t, sha2Conn, terms.HoldInvoice) }()

	r := &runner.Runner{
		Driver: &driver.Driver{
			In: in, Out: out, Store: journal, Policy: labMargin,
		},
		Store: journal, Rates: labRatesReverse, Poll: 250 * time.Millisecond,
		PayTimeout: 60 * time.Second,
	}

	driven := make(chan error, 1)
	go func() { driven <- r.Drive(ctx, terms.Hash) }()

	select {
	case err := <-driven:
		if err != nil {
			t.Fatalf("driving the swap: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the bridge did not finish the swap in time")
	}

	rec, err := journal.Get(context.Background(), terms.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if rec.State != swap.Settled {
		select {
		case err := <-paid:
			t.Fatalf("the swap ended in %v; the user's payment says: %v",
				rec.State, err)
		default:
			t.Fatalf("the swap ended in %v, not settled", rec.State)
		}
	}
	t.Logf("swap settled, preimage %x", rec.Preimage[:8])

	select {
	case err := <-paid:
		if err != nil {
			t.Fatalf("the user's payment did not succeed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the user's payment never resolved after the bridge settled")
	}

	got, err := lf2.LookupInvoice(context.Background(), &lnrpc.PaymentHash{
		RHash: terms.Hash[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetState() != lnrpc.Invoice_SETTLED {
		t.Fatalf("the destination's invoice is %v, not settled", got.GetState())
	}
	if got.GetAmtPaidMsat() != payMsat {
		t.Errorf("the destination was paid %d msat, wanted %d",
			got.GetAmtPaidMsat(), payMsat)
	}

	t.Logf("end to end: %d msat BTC in on Bitcoin, %d msat BTCB2 out on the "+
		"BLAKE2b chain, one preimage settled both",
		rec.IncomingMsat, got.GetAmtPaidMsat())
}
