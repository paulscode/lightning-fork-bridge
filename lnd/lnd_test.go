package lnd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/paulscode/lightning-fork-bridge/lndrpc/invoicesrpc"
	"github.com/paulscode/lightning-fork-bridge/lndrpc/lnrpc"
	"github.com/paulscode/lightning-fork-bridge/lndrpc/routerrpc"
	"github.com/paulscode/lightning-fork-bridge/node"
)

// fakeNode is a scripted LND, served over a real gRPC connection.
//
// Real gRPC rather than a stub client, because the things most likely to be
// wrong in this package are at the boundary: a status code that is not what
// was assumed, a stream that ends rather than answers, a field read from the
// wrong place. None of those show up against a hand-written double.
type fakeNode struct {
	lnrpc.UnimplementedLightningServer
	invoicesrpc.UnimplementedInvoicesServer
	routerrpc.UnimplementedRouterServer

	info    *lnrpc.GetInfoResponse
	infoErr error

	payreq    *lnrpc.PayReq
	payreqErr error

	invoice    *lnrpc.Invoice
	invoiceErr error

	added     *invoicesrpc.AddHoldInvoiceRequest
	addErr    error
	settleErr error
	cancelErr error
	settled   int
	cancelled int

	deleteErr error
	deleted   []string

	// payments is the sequence of updates a Send or Track stream emits, then
	// streamErr if it is set, otherwise a clean end.
	payments  []*lnrpc.Payment
	sendReq   *routerrpc.SendPaymentRequest
	streamErr error
	trackErr  error
}

func (f *fakeNode) GetInfo(context.Context, *lnrpc.GetInfoRequest) (
	*lnrpc.GetInfoResponse, error) {

	return f.info, f.infoErr
}

func (f *fakeNode) DecodePayReq(context.Context, *lnrpc.PayReqString) (
	*lnrpc.PayReq, error) {

	return f.payreq, f.payreqErr
}

func (f *fakeNode) AddHoldInvoice(_ context.Context,
	r *invoicesrpc.AddHoldInvoiceRequest) (*invoicesrpc.AddHoldInvoiceResp,
	error) {

	f.added = r
	if f.addErr != nil {
		return nil, f.addErr
	}

	return &invoicesrpc.AddHoldInvoiceResp{PaymentRequest: "lnblakert1fake"},
		nil
}

func (f *fakeNode) LookupInvoiceV2(context.Context,
	*invoicesrpc.LookupInvoiceMsg) (*lnrpc.Invoice, error) {

	return f.invoice, f.invoiceErr
}

func (f *fakeNode) SettleInvoice(context.Context, *invoicesrpc.SettleInvoiceMsg) (
	*invoicesrpc.SettleInvoiceResp, error) {

	f.settled++
	if f.settleErr != nil {
		return nil, f.settleErr
	}

	return &invoicesrpc.SettleInvoiceResp{}, nil
}

func (f *fakeNode) CancelInvoice(context.Context, *invoicesrpc.CancelInvoiceMsg) (
	*invoicesrpc.CancelInvoiceResp, error) {

	f.cancelled++
	if f.cancelErr != nil {
		return nil, f.cancelErr
	}

	return &invoicesrpc.CancelInvoiceResp{}, nil
}

func (f *fakeNode) DeleteCanceledInvoice(_ context.Context,
	r *lnrpc.DelCanceledInvoiceReq) (*lnrpc.DelCanceledInvoiceResp, error) {

	f.deleted = append(f.deleted, r.GetInvoiceHash())
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	f.invoice, f.invoiceErr = nil, status.Error(codes.NotFound,
		"there are no existing invoices")

	return &lnrpc.DelCanceledInvoiceResp{}, nil
}

func (f *fakeNode) SendPaymentV2(r *routerrpc.SendPaymentRequest,
	stream grpc.ServerStreamingServer[lnrpc.Payment]) error {

	f.sendReq = r

	return f.stream(stream)
}

func (f *fakeNode) TrackPaymentV2(_ *routerrpc.TrackPaymentRequest,
	stream grpc.ServerStreamingServer[lnrpc.Payment]) error {

	if f.trackErr != nil {
		return f.trackErr
	}

	return f.stream(stream)
}

func (f *fakeNode) stream(s grpc.ServerStreamingServer[lnrpc.Payment]) error {
	for _, p := range f.payments {
		if err := s.Send(p); err != nil {
			return err
		}
	}

	return f.streamErr
}

// serve puts a fakeNode behind a real gRPC connection.
func serve(t *testing.T, f *fakeNode) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	lnrpc.RegisterLightningServer(srv, f)
	invoicesrpc.RegisterInvoicesServer(srv, f)
	routerrpc.RegisterRouterServer(srv, f)

	go func() {
		if err := srv.Serve(lis); err != nil {
			return
		}
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn,
			error) {

			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
		lis.Close()
	})

	return conn
}

var synced = &lnrpc.GetInfoResponse{BlockHeight: 500, SyncedToChain: true}

// TestExpiryIsTheEarliestAcceptedHTLC is the translation that decides whether
// a swap is safe to pay out against.
//
// A multi-part payment arrives as several HTLCs down different routes, so they
// expire at different heights, and the claim on the whole invoice dies with
// the earliest. Reporting anything later tells the margin check there is more
// time than there is, which is how a bridge pays out against a claim it can no
// longer collect.
func TestExpiryIsTheEarliestAcceptedHTLC(t *testing.T) {
	ctx := context.Background()

	htlc := func(h int32, st lnrpc.InvoiceHTLCState) *lnrpc.InvoiceHTLC {
		return &lnrpc.InvoiceHTLC{ExpiryHeight: h, State: st}
	}

	for _, c := range []struct {
		name  string
		htlcs []*lnrpc.InvoiceHTLC
		want  int32
	}{
		{"one part", []*lnrpc.InvoiceHTLC{
			htlc(900, lnrpc.InvoiceHTLCState_ACCEPTED),
		}, 900},
		{"three parts, the earliest wins", []*lnrpc.InvoiceHTLC{
			htlc(900, lnrpc.InvoiceHTLCState_ACCEPTED),
			htlc(710, lnrpc.InvoiceHTLCState_ACCEPTED),
			htlc(1200, lnrpc.InvoiceHTLCState_ACCEPTED),
		}, 710},
		{"the earliest arriving last", []*lnrpc.InvoiceHTLC{
			htlc(1200, lnrpc.InvoiceHTLCState_ACCEPTED),
			htlc(900, lnrpc.InvoiceHTLCState_ACCEPTED),
			htlc(600, lnrpc.InvoiceHTLCState_ACCEPTED),
		}, 600},
		{
			// A cancelled part is gone and bounds nothing, so letting it
			// set the answer would refuse swaps that are perfectly safe.
			"a cancelled part is ignored", []*lnrpc.InvoiceHTLC{
				htlc(300, lnrpc.InvoiceHTLCState_CANCELED),
				htlc(900, lnrpc.InvoiceHTLCState_ACCEPTED),
			}, 900,
		},
		{"a settled part is ignored", []*lnrpc.InvoiceHTLC{
			htlc(300, lnrpc.InvoiceHTLCState_SETTLED),
			htlc(900, lnrpc.InvoiceHTLCState_ACCEPTED),
		}, 900},
		{"no HTLCs yet", nil, 0},
		{"only cancelled HTLCs", []*lnrpc.InvoiceHTLC{
			htlc(300, lnrpc.InvoiceHTLCState_CANCELED),
		}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeNode{info: synced, invoice: &lnrpc.Invoice{
				State: lnrpc.Invoice_ACCEPTED, ValueMsat: 1000,
				Htlcs: c.htlcs,
			}}
			in := NewIncoming(serve(t, f))

			got, err := in.LookupInvoice(ctx, node.Hash{1})
			if err != nil {
				t.Fatal(err)
			}
			if got.ExpiryHeight != c.want {
				t.Errorf("expiry %d, want %d", got.ExpiryHeight, c.want)
			}
			if got.State != node.InvoiceAccepted {
				t.Errorf("state %v", got.State)
			}
		})
	}
}

// TestACancelledInvoiceStillShowsItWasFunded: a quote may replace a cancelled
// invoice only if nobody ever paid it, and the state alone does not say: an
// invoice that lapsed unpaid and one funded and then given back are both
// cancelled. lnd keeps the given-back HTLC on the invoice, so every HTLC in
// any state is counted.
func TestACancelledInvoiceStillShowsItWasFunded(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name  string
		htlcs []*lnrpc.InvoiceHTLC
		want  int
	}{
		{"never paid", nil, 0},
		{"funded and given back", []*lnrpc.InvoiceHTLC{
			{ExpiryHeight: 900, State: lnrpc.InvoiceHTLCState_CANCELED},
		}, 1},
		{"two parts given back", []*lnrpc.InvoiceHTLC{
			{ExpiryHeight: 900, State: lnrpc.InvoiceHTLCState_CANCELED},
			{ExpiryHeight: 910, State: lnrpc.InvoiceHTLCState_CANCELED},
		}, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeNode{info: synced, invoice: &lnrpc.Invoice{
				State: lnrpc.Invoice_CANCELED, ValueMsat: 1000,
				Htlcs: c.htlcs,
			}}
			in := NewIncoming(serve(t, f))

			got, err := in.LookupInvoice(ctx, node.Hash{1})
			if err != nil {
				t.Fatal(err)
			}
			if got.HTLCs != c.want {
				t.Errorf("%d HTLCs, want %d", got.HTLCs, c.want)
			}
		})
	}
}

func TestInvoiceStatesTranslate(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		from lnrpc.Invoice_InvoiceState
		want node.InvoiceState
	}{
		{lnrpc.Invoice_OPEN, node.InvoiceOpen},
		{lnrpc.Invoice_ACCEPTED, node.InvoiceAccepted},
		{lnrpc.Invoice_SETTLED, node.InvoiceSettled},
		{lnrpc.Invoice_CANCELED, node.InvoiceCancelled},
		// A state this build does not know must not be claimable: paying
		// out against something the bridge cannot describe is the failure
		// every rule above this one is trying to avoid.
		{lnrpc.Invoice_InvoiceState(99), node.InvoiceCancelled},
	} {
		f := &fakeNode{info: synced,
			invoice: &lnrpc.Invoice{State: c.from, ValueMsat: 7}}
		in := NewIncoming(serve(t, f))

		got, err := in.LookupInvoice(ctx, node.Hash{1})
		if err != nil {
			t.Fatal(err)
		}
		if got.State != c.want {
			t.Errorf("%v translated to %v, want %v", c.from, got.State, c.want)
		}
		if got.AmountMsat != 7 {
			t.Errorf("amount %d", got.AmountMsat)
		}
	}
}

func TestUnknownHashIsItsOwnAnswer(t *testing.T) {
	ctx := context.Background()
	f := &fakeNode{info: synced,
		invoiceErr: status.Error(codes.NotFound, "there is no invoice")}
	in := NewIncoming(serve(t, f))

	if _, err := in.LookupInvoice(ctx, node.Hash{1}); !errors.Is(err,
		node.ErrUnknownHash) {

		t.Errorf("want ErrUnknownHash, got %v", err)
	}

	// A node that has never made an invoice answers this instead, with no
	// code: a new SHA256 node, before its first swap.
	f.invoiceErr = status.Error(codes.Unknown, "there are no existing invoices")
	if _, err := in.LookupInvoice(ctx, node.Hash{1}); !errors.Is(err,
		node.ErrUnknownHash) {

		t.Errorf("no invoices yet: want ErrUnknownHash, got %v", err)
	}

	// Anything else is a real error and must not be flattened into it, or a
	// node that is merely unreachable would read as one that never had the
	// invoice.
	f.invoiceErr = status.Error(codes.Unavailable, "node is restarting")
	if _, err := in.LookupInvoice(ctx, node.Hash{1}); err == nil ||
		errors.Is(err, node.ErrUnknownHash) {

		t.Errorf("an unavailable node gave %v", err)
	}
}

// TestHoldInvoiceCarriesTheMarginsCLTV. The delta is the whole reason the
// incoming leg outlives the outgoing one, so it must reach the node, and a
// request without one must not fall back to a node default.
func TestHoldInvoiceCarriesTheMarginsCLTV(t *testing.T) {
	ctx := context.Background()
	f := &fakeNode{info: synced}
	in := NewIncoming(serve(t, f))

	got, err := in.AddHoldInvoice(ctx, node.HoldInvoice{
		Hash: node.Hash{1, 2, 3}, AmountMsat: 1_000_000,
		CLTVDelta: 581, Expiry: 10 * time.Minute, Memo: "swap",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "lnblakert1fake" {
		t.Errorf("payment request %q", got)
	}
	if f.added.GetCltvExpiry() != 581 {
		t.Errorf("the node was asked for a CLTV of %d, want 581",
			f.added.GetCltvExpiry())
	}
	if f.added.GetValueMsat() != 1_000_000 || f.added.GetExpiry() != 600 {
		t.Errorf("value %d expiry %d", f.added.GetValueMsat(),
			f.added.GetExpiry())
	}

	for _, c := range []struct {
		name string
		req  node.HoldInvoice
	}{
		{"no CLTV delta", node.HoldInvoice{Hash: node.Hash{1},
			AmountMsat: 1000}},
		{"no amount", node.HoldInvoice{Hash: node.Hash{1}, CLTVDelta: 581}},
	} {
		if _, err := in.AddHoldInvoice(ctx, c.req); !errors.Is(err,
			ErrConfig) {

			t.Errorf("%s: want ErrConfig, got %v", c.name, err)
		}
	}
}

// TestSettleAndCancelAreIdempotentByState. After a crash the bridge reissues
// these, and a reissue reaching a node that already did it must read as
// success. Checking the resulting state rather than the error's wording,
// because the wording is not an interface.
func TestSettleAndCancelAreIdempotentByState(t *testing.T) {
	ctx := context.Background()
	pre := node.Preimage{4}

	// Already settled: the call fails, the state says it is done.
	f := &fakeNode{
		info:      synced,
		settleErr: status.Error(codes.Unknown, "invoice already settled"),
		invoice:   &lnrpc.Invoice{State: lnrpc.Invoice_SETTLED},
	}
	in := NewIncoming(serve(t, f))
	if err := in.SettleInvoice(ctx, pre); err != nil {
		t.Errorf("settling an already settled invoice: %v", err)
	}

	// Failing for a reason that is not "already done" must still fail, or a
	// swap whose claim never landed would be marked complete.
	f2 := &fakeNode{
		info:      synced,
		settleErr: status.Error(codes.Unavailable, "node is restarting"),
		invoice:   &lnrpc.Invoice{State: lnrpc.Invoice_ACCEPTED},
	}
	if err := NewIncoming(serve(t, f2)).SettleInvoice(ctx, pre); err == nil {
		t.Error("a failed settle against an unsettled invoice reported success")
	}

	f3 := &fakeNode{
		info:      synced,
		cancelErr: status.Error(codes.Unknown, "invoice already canceled"),
		invoice:   &lnrpc.Invoice{State: lnrpc.Invoice_CANCELED},
	}
	if err := NewIncoming(serve(t, f3)).CancelInvoice(ctx,
		node.Hash{1}); err != nil {

		t.Errorf("cancelling an already cancelled invoice: %v", err)
	}

	f4 := &fakeNode{
		info:      synced,
		cancelErr: status.Error(codes.Unavailable, "node is restarting"),
		invoice:   &lnrpc.Invoice{State: lnrpc.Invoice_ACCEPTED},
	}
	if err := NewIncoming(serve(t, f4)).CancelInvoice(ctx,
		node.Hash{1}); err == nil {

		t.Error("a failed cancel against a live invoice reported success")
	}
}

// TestASyncingNodeHasNoUsableHeight. Every margin decision compares against
// this number, and a node behind the tip reports a height that is not the
// present one, overstating how long an incoming HTLC has left.
func TestASyncingNodeHasNoUsableHeight(t *testing.T) {
	ctx := context.Background()

	f := &fakeNode{info: &lnrpc.GetInfoResponse{
		BlockHeight: 400, SyncedToChain: false,
	}}
	in := NewIncoming(serve(t, f))
	if _, err := in.BlockHeight(ctx); !errors.Is(err, ErrNotSynced) {
		t.Errorf("want ErrNotSynced, got %v", err)
	}

	f2 := &fakeNode{info: synced}
	h, err := NewIncoming(serve(t, f2)).BlockHeight(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h != 500 {
		t.Errorf("height %d, want 500", h)
	}

	// And the outgoing side reads it the same way.
	if _, err := NewOutgoing(serve(t, f)).BlockHeight(ctx); !errors.Is(err,
		ErrNotSynced) {

		t.Errorf("outgoing: want ErrNotSynced, got %v", err)
	}
}

// pmt builds a payment update as the node would send it.
func pmt(st lnrpc.Payment_PaymentStatus, pre node.Preimage) *lnrpc.Payment {
	p := &lnrpc.Payment{Status: st, FeeMsat: 42}
	var zero node.Preimage
	if pre != zero {
		p.PaymentPreimage = hex.EncodeToString(pre[:])
	}

	return p
}

// TestOnlyAFailedNodeMeansFailed is the translation that decides whether the
// bridge may give up its incoming claim.
//
// swap.OutgoingFailed is the one event that leads to cancelling the incoming
// HTLC, so only a status that means "every HTLC has resolved and none
// succeeded" may produce it. Everything else — in flight, initiated, a status
// this build has never heard of — has to come back as in flight, because
// waiting on a payment that is already over costs time while concluding
// failure on one that is not costs the whole swap.
func TestOnlyAFailedNodeMeansFailed(t *testing.T) {
	ctx := context.Background()
	pre := node.Preimage{9, 9, 9}

	for _, c := range []struct {
		name string
		from lnrpc.Payment_PaymentStatus
		want node.PaymentState
	}{
		{"failed", lnrpc.Payment_FAILED, node.PaymentFailed},
		{"succeeded", lnrpc.Payment_SUCCEEDED, node.PaymentSucceeded},
		{"in flight", lnrpc.Payment_IN_FLIGHT, node.PaymentInFlight},
		{"initiated", lnrpc.Payment_INITIATED, node.PaymentInFlight},
		{"unknown", lnrpc.Payment_UNKNOWN, node.PaymentInFlight},
		{"a status from a newer node", lnrpc.Payment_PaymentStatus(77),
			node.PaymentInFlight},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeNode{info: synced,
				payments: []*lnrpc.Payment{pmt(c.from, pre)}}
			out := NewOutgoing(serve(t, f))

			got, err := out.LookupPayment(ctx, node.Hash{1})
			if err != nil {
				t.Fatal(err)
			}
			if got.State != c.want {
				t.Errorf("%v translated to %v, want %v", c.from, got.State,
					c.want)
			}
			if c.want == node.PaymentSucceeded && got.Preimage != pre {
				t.Errorf("preimage %x, want %x", got.Preimage, pre)
			}
			if c.want != node.PaymentFailed &&
				got.State == node.PaymentFailed {

				t.Error("a non-failure was reported as a failure")
			}
		})
	}
}

// TestASucceededPaymentWithoutAPreimageIsRefused. The preimage is the entire
// proceeds of a swap: it is what claims the incoming HTLC. A node reporting
// success without one is contradicting itself, and translating that into
// success would move the swap to a state whose only action cannot be carried
// out, where it would sit retrying forever.
func TestASucceededPaymentWithoutAPreimageIsRefused(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name string
		hex  string
	}{
		{"none at all", ""},
		{"all zeroes", "00000000000000000000000000000000" +
			"00000000000000000000000000000000"},
		{"too short", "aabbcc"},
		{"not hex", "zzzz"},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeNode{info: synced, payments: []*lnrpc.Payment{{
				Status: lnrpc.Payment_SUCCEEDED, PaymentPreimage: c.hex,
			}}}
			out := NewOutgoing(serve(t, f))

			if _, err := out.LookupPayment(ctx, node.Hash{1}); !errors.Is(err,
				ErrNodeContradiction) {

				t.Errorf("want ErrNodeContradiction, got %v", err)
			}
		})
	}
}

// TestAPaymentTheNodeHasNeverSeen is how recovery tells "never sent" apart
// from "sent and lost track of". It has to be a clean answer rather than an
// error, because the driver's response to it is to stop and ask a human, and
// that only happens if the state arrives intact.
func TestAPaymentTheNodeHasNeverSeen(t *testing.T) {
	ctx := context.Background()

	f := &fakeNode{info: synced,
		trackErr: status.Error(codes.NotFound, "payment isn't initiated")}
	out := NewOutgoing(serve(t, f))

	got, err := out.LookupPayment(ctx, node.Hash{1})
	if err != nil {
		t.Fatalf("a never-seen payment errored: %v", err)
	}
	if got.State != node.PaymentNone {
		t.Errorf("state %v, want none", got.State)
	}

	// An unreachable node is not the same thing and must not collapse into
	// it, or a restart during an outage would report every payment as never
	// sent.
	//
	// Note that node.PaymentNone is the zero value, so a Payment returned
	// alongside an error reads as "no record" to anyone who ignores the
	// error. That is the conservative direction — the driver's answer to
	// PaymentNone is to stop and ask a human — but it means the assertion
	// here has to be about the error, not the state.
	f2 := &fakeNode{info: synced,
		trackErr: status.Error(codes.Unavailable, "node is restarting")}
	if _, err := NewOutgoing(serve(t, f2)).LookupPayment(ctx,
		node.Hash{1}); err == nil {

		t.Error("an unavailable node was reported as never having seen it")
	}
}

// TestAStreamThatDiesIsNotAFailure. An HTLC that has left is out there
// whatever this call reports. The only safe reading of "I stopped listening"
// is "I do not know yet", because the alternative lets the caller cancel the
// incoming claim against a payment still in flight.
func TestAStreamThatDiesIsNotAFailure(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name     string
		updates  []*lnrpc.Payment
		streamer error
	}{
		{"nothing at all, then a clean end", nil, nil},
		{"an in-flight update, then the stream breaks",
			[]*lnrpc.Payment{pmt(lnrpc.Payment_IN_FLIGHT, node.Preimage{})},
			status.Error(codes.Unavailable, "connection reset")},
		{"an error before any update", nil,
			status.Error(codes.Internal, "something went wrong")},
		{"initiated, then the node goes away",
			[]*lnrpc.Payment{pmt(lnrpc.Payment_INITIATED, node.Preimage{})},
			status.Error(codes.Canceled, "context canceled")},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeNode{info: synced, payments: c.updates,
				streamErr: c.streamer}
			out := NewOutgoing(serve(t, f))

			got, err := out.Pay(ctx, node.PayRequest{
				Invoice: "lnbc1fake", CLTVLimit: 40,
				MaxFeeMsat: 5000, Timeout: 30 * time.Second,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.State != node.PaymentInFlight {
				t.Errorf("state %v, want in flight", got.State)
			}
		})
	}
}

// TestPayReturnsTheTerminalUpdate and stops there.
func TestPayReturnsTheTerminalUpdate(t *testing.T) {
	ctx := context.Background()
	pre := node.Preimage{3, 1, 4}

	f := &fakeNode{info: synced, payments: []*lnrpc.Payment{
		pmt(lnrpc.Payment_INITIATED, node.Preimage{}),
		pmt(lnrpc.Payment_IN_FLIGHT, node.Preimage{}),
		pmt(lnrpc.Payment_SUCCEEDED, pre),
	}}
	out := NewOutgoing(serve(t, f))

	got, err := out.Pay(ctx, node.PayRequest{
		Invoice: "lnbc1fake", CLTVLimit: 40, MaxFeeMsat: 5000,
		Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.State != node.PaymentSucceeded || got.Preimage != pre {
		t.Errorf("%v %x", got.State, got.Preimage)
	}
	if got.FeeMsat != 42 {
		t.Errorf("fee %d", got.FeeMsat)
	}

	// The limits the margin policy chose must reach the node. A payment sent
	// without a CLTV limit could take a route that outlives the incoming
	// leg, which is the failure the whole policy exists to prevent.
	if f.sendReq.GetCltvLimit() != 40 {
		t.Errorf("CLTV limit %d, want 40", f.sendReq.GetCltvLimit())
	}
	if f.sendReq.GetFeeLimitMsat() != 5000 {
		t.Errorf("fee limit %d", f.sendReq.GetFeeLimitMsat())
	}
	if f.sendReq.GetTimeoutSeconds() != 30 {
		t.Errorf("timeout %d", f.sendReq.GetTimeoutSeconds())
	}
	if f.sendReq.GetPaymentRequest() != "lnbc1fake" {
		t.Errorf("the invoice was rewritten to %q",
			f.sendReq.GetPaymentRequest())
	}
}

// TestPayRefusesAnUnboundedRoute. Without a CLTV limit the route can be as
// long as the network allows, and the incoming leg was sized against a
// specific number.
func TestPayRefusesAnUnboundedRoute(t *testing.T) {
	ctx := context.Background()
	f := &fakeNode{info: synced,
		payments: []*lnrpc.Payment{pmt(lnrpc.Payment_SUCCEEDED,
			node.Preimage{1})}}
	out := NewOutgoing(serve(t, f))

	for _, c := range []struct {
		name string
		req  node.PayRequest
	}{
		{"no CLTV limit", node.PayRequest{Invoice: "lnbc1fake"}},
		{"no invoice", node.PayRequest{CLTVLimit: 40}},
		{"a CLTV limit that does not fit", node.PayRequest{
			Invoice: "lnbc1fake", CLTVLimit: 1 << 31}},
	} {
		if _, err := out.Pay(ctx, c.req); !errors.Is(err, ErrConfig) {
			t.Errorf("%s: want ErrConfig, got %v", c.name, err)
		}
	}
	if f.sendReq != nil {
		t.Error("a refused request still reached the node")
	}
}

// TestRequestsThatWouldOutliveTheirGuards. Each of these reaches the node as a
// silent default if it is not caught here: an invoice that lives a day past
// its quote, a payment attempt with no timeout, a route with no CLTV bound.
// None is a node error, which is why they have to be errors of ours.
func TestRequestsThatWouldOutliveTheirGuards(t *testing.T) {
	ctx := context.Background()
	f := &fakeNode{info: synced}
	in := NewIncoming(serve(t, f))

	if _, err := in.AddHoldInvoice(ctx, node.HoldInvoice{
		Hash: node.Hash{1}, AmountMsat: 1000, CLTVDelta: 581,
	}); !errors.Is(err, ErrConfig) {
		t.Errorf("an invoice with no expiry: %v", err)
	}
	if f.added != nil {
		t.Error("it reached the node anyway")
	}

	out := NewOutgoing(serve(t, f))
	if _, err := out.Pay(ctx, node.PayRequest{
		Invoice: "lnbc1fake", CLTVLimit: 40,
	}); !errors.Is(err, ErrConfig) {
		t.Errorf("a payment with no timeout: %v", err)
	}
}

func TestCheckCatchesAtStartupWhatWouldOtherwiseShowUpMidSwap(t *testing.T) {
	ctx := context.Background()

	good := serve(t, &fakeNode{info: synced})
	if err := NewIncoming(good).Check(ctx); err != nil {
		t.Errorf("a synced node: %v", err)
	}
	if err := NewOutgoing(good).Check(ctx); err != nil {
		t.Errorf("a synced node: %v", err)
	}

	syncing := serve(t, &fakeNode{info: &lnrpc.GetInfoResponse{
		BlockHeight: 400, SyncedToChain: false,
	}})
	if err := NewIncoming(syncing).Check(ctx); !errors.Is(err, ErrNotSynced) {
		t.Errorf("incoming: %v", err)
	}
	if err := NewOutgoing(syncing).Check(ctx); !errors.Is(err, ErrNotSynced) {
		t.Errorf("outgoing: %v", err)
	}
}

// selfSigned writes a certificate and key, standing in for the one LND
// generates at first start.
func selfSigned(t *testing.T, dir string) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "lnd"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl,
		&key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "tls.cert")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: der,
	}), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestDialValidatesWhatItCan(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cert := selfSigned(t, dir)

	mac := filepath.Join(dir, "admin.macaroon")
	if err := os.WriteFile(mac, []byte{1, 2, 3}, 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty.macaroon")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name string
		cfg  Config
	}{
		{"no address", Config{TLSCertPath: cert, MacaroonPath: mac}},
		{"a certificate that is not there", Config{Address: "x:1",
			TLSCertPath: filepath.Join(dir, "nope"), MacaroonPath: mac}},
		{"a macaroon that is not there", Config{Address: "x:1",
			TLSCertPath: cert, MacaroonPath: filepath.Join(dir, "nope")}},
		{
			// An empty macaroon file is what a half-finished install
			// leaves behind. Sending it would mean every call failing
			// with a permission error at the worst possible moment.
			"an empty macaroon", Config{Address: "x:1",
				TLSCertPath: cert, MacaroonPath: empty},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, err := Dial(ctx, c.cfg); !errors.Is(err, ErrConfig) {
				t.Errorf("want ErrConfig, got %v", err)
			}
		})
	}

	// A usable configuration dials. It does not prove the node is there,
	// which is what Check is for.
	conn, err := Dial(ctx, Config{
		Address: "localhost:10009", TLSCertPath: cert, MacaroonPath: mac,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
}

// TestTheMacaroonIsNeverSentInTheClear. It is a bearer token: the whole of the
// node's authority, handed to whoever is listening.
func TestTheMacaroonIsNeverSentInTheClear(t *testing.T) {
	cred := macaroonCredential{hex: "aabb"}
	if !cred.RequireTransportSecurity() {
		t.Fatal("the macaroon would be sent over an unencrypted connection")
	}

	md, err := cred.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if md["macaroon"] != "aabb" {
		t.Errorf("metadata %v", md)
	}
}

// TestDecodeUsesThePayingNodesReading. The bridge builds its hold invoice
// around whatever payment hash comes back here, and the node that pays will
// use its own reading of the same string. A second parser in this process that
// disagreed by one field would have the two legs built on different hashes,
// which is the one thing the swap's security rests on them not doing.
func TestDecodeUsesThePayingNodesReading(t *testing.T) {
	ctx := context.Background()
	created := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	want := node.Hash{1, 2, 3, 4}

	f := &fakeNode{info: synced, payreq: &lnrpc.PayReq{
		Destination: "03abc",
		PaymentHash: hex.EncodeToString(want[:]),
		NumMsat:     100_000_000,
		CltvExpiry:  40,
		Timestamp:   created.Unix(),
		Expiry:      3600,
		Description: "a coffee",
	}}
	out := NewOutgoing(serve(t, f))

	got, err := out.Decode(ctx, "lnbc1fake")
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash != want {
		t.Errorf("hash %x, want %x", got.Hash, want)
	}
	if got.AmountMsat != 100_000_000 {
		t.Errorf("amount %d", got.AmountMsat)
	}
	if got.CLTVDelta != 40 {
		t.Errorf("cltv %d", got.CLTVDelta)
	}
	// Expiry is relative to the invoice's own timestamp, and the bridge
	// needs the wall-clock moment.
	if !got.Expiry.Equal(created.Add(time.Hour)) {
		t.Errorf("expiry %v, want %v", got.Expiry, created.Add(time.Hour))
	}
	if got.Destination != "03abc" || got.Description != "a coffee" {
		t.Errorf("%+v", got)
	}
}

func TestDecodeRefusesWhatItCannotUse(t *testing.T) {
	ctx := context.Background()
	good := func() *lnrpc.PayReq {
		return &lnrpc.PayReq{
			PaymentHash: hex.EncodeToString(make([]byte, 32)),
			NumMsat:     1000, CltvExpiry: 40, Timestamp: 1_700_000_000,
		}
	}

	for _, c := range []struct {
		name string
		with func(p *lnrpc.PayReq)
	}{
		{"a hash that is not hex", func(p *lnrpc.PayReq) {
			p.PaymentHash = "zzzz"
		}},
		{"a hash of the wrong length", func(p *lnrpc.PayReq) {
			p.PaymentHash = "aabb"
		}},
		{"a negative amount", func(p *lnrpc.PayReq) { p.NumMsat = -1 }},
		{"a CLTV delta that does not fit", func(p *lnrpc.PayReq) {
			p.CltvExpiry = 1 << 40
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := good()
			c.with(p)
			out := NewOutgoing(serve(t, &fakeNode{info: synced, payreq: p}))

			if _, err := out.Decode(ctx, "lnbc1x"); !errors.Is(err,
				ErrInvoice) {

				t.Errorf("want ErrInvoice, got %v", err)
			}
		})
	}

	// An empty string never reaches the node.
	f := &fakeNode{info: synced, payreq: good()}
	if _, err := NewOutgoing(serve(t, f)).Decode(ctx, ""); !errors.Is(err,
		ErrInvoice) {

		t.Errorf("empty: %v", err)
	}

	// And a node that refuses to decode is a refusal, not a silent zero.
	bad := &fakeNode{info: synced,
		payreqErr: status.Error(codes.InvalidArgument, "checksum failed")}
	if _, err := NewOutgoing(serve(t, bad)).Decode(ctx,
		"lnbc1garbage"); !errors.Is(err, ErrInvoice) {

		t.Errorf("undecodable: %v", err)
	}
}

// TestDecodeReportsTheChainFromTheFeatureBits: the Bitcoin-side node does not
// know option_blake2b, and a stock lnd still lists the bits an invoice sets,
// marked unknown. That listing is how the bridge tells the chains apart, since
// both nodes read both chains' invoices.
func TestDecodeReportsTheChainFromTheFeatureBits(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name     string
		features map[uint32]*lnrpc.Feature
		want     bool
	}{
		{"none", nil, false},
		{"ordinary bits", map[uint32]*lnrpc.Feature{
			9: {Name: "tlv-onion"}, 15: {Name: "payment-addr"},
		}, false},
		{"required", map[uint32]*lnrpc.Feature{
			512: {Name: "unknown", IsRequired: true},
		}, true},
		{"optional", map[uint32]*lnrpc.Feature{
			513: {Name: "unknown"},
		}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeNode{info: synced, payreq: &lnrpc.PayReq{
				Destination: "03abc",
				PaymentHash: hex.EncodeToString(make([]byte, 32)),
				NumMsat:     1000, CltvExpiry: 40,
				Timestamp: time.Now().Unix(), Expiry: 3600,
				Features: c.features,
			}}
			got, err := NewOutgoing(serve(t, f)).Decode(ctx, "lnbc1fake")
			if err != nil {
				t.Fatal(err)
			}
			if got.BLAKE2b != c.want {
				t.Errorf("BLAKE2b %v, want %v", got.BLAKE2b, c.want)
			}
		})
	}
}

// TestForgetOnlyACancelledInvoice: deleting anything else would erase the
// record of an HTLC that is held or was paid.
func TestForgetOnlyACancelledInvoice(t *testing.T) {
	ctx := context.Background()
	hash := node.Hash{9}

	for _, st := range []lnrpc.Invoice_InvoiceState{
		lnrpc.Invoice_OPEN, lnrpc.Invoice_ACCEPTED, lnrpc.Invoice_SETTLED,
	} {
		f := &fakeNode{info: synced, invoice: &lnrpc.Invoice{State: st}}
		err := NewIncoming(serve(t, f)).ForgetInvoice(ctx, hash)
		if err == nil {
			t.Errorf("%v: forgotten", st)
		}
		if len(f.deleted) != 0 {
			t.Errorf("%v: delete was called", st)
		}
	}

	f := &fakeNode{info: synced, invoice: &lnrpc.Invoice{
		State: lnrpc.Invoice_CANCELED,
	}}
	if err := NewIncoming(serve(t, f)).ForgetInvoice(ctx, hash); err != nil {
		t.Fatal(err)
	}
	if len(f.deleted) != 1 || f.deleted[0] != hex.EncodeToString(hash[:]) {
		t.Errorf("deleted %v", f.deleted)
	}
}

// TestForgettingIsSafeToRepeat: after a crash between forgetting and quoting,
// the next attempt finds nothing there, which is the outcome wanted.
func TestForgettingIsSafeToRepeat(t *testing.T) {
	ctx := context.Background()

	f := &fakeNode{info: synced, invoiceErr: status.Error(codes.NotFound,
		"there are no existing invoices")}
	if err := NewIncoming(serve(t, f)).ForgetInvoice(ctx,
		node.Hash{9}); err != nil {

		t.Fatalf("an unknown hash should already count as forgotten: %v",
			err)
	}

	// A delete that fails because the invoice vanished in between is also
	// the outcome wanted.
	f = &fakeNode{info: synced, invoice: &lnrpc.Invoice{
		State: lnrpc.Invoice_CANCELED,
	}, deleteErr: errors.New("raced")}
	in := NewIncoming(serve(t, f))
	if err := in.ForgetInvoice(ctx, node.Hash{9}); err == nil {
		t.Fatal("a failed delete of an invoice still there must be reported")
	}
}
