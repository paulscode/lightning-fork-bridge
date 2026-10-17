package lnd

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/paulscode/lightning-fork-bridge/lndrpc/invoicesrpc"
	"github.com/paulscode/lightning-fork-bridge/lndrpc/lnrpc"
	"github.com/paulscode/lightning-fork-bridge/node"
)

// ErrNotSynced is returned when a node has not caught up with its chain.
//
// A node behind the tip reports a block height that is not the present one,
// and every margin decision is a comparison against that height. Reading it
// while syncing would overstate how long an incoming HTLC has left, in the one
// direction that loses money.
var ErrNotSynced = errors.New("node is not synced to its chain")

// Incoming drives the node the bridge receives on.
type Incoming struct {
	invoices invoicesrpc.InvoicesClient
	main     lnrpc.LightningClient
}

// NewIncoming wraps a connection.
func NewIncoming(conn grpc.ClientConnInterface) *Incoming {
	return &Incoming{
		invoices: invoicesrpc.NewInvoicesClient(conn),
		main:     lnrpc.NewLightningClient(conn),
	}
}

var _ node.Incoming = (*Incoming)(nil)

// AddHoldInvoice creates an invoice that will not settle until told to.
//
// It sends no route hints. That is correct while the bridge is its users' own
// LSP, which is the topology the design calls for, and it is wrong the moment
// it is not: a payer with no public path to the bridge's node needs a hint to
// find one. Adding them is a change here plus a source of channel information,
// not a change anywhere above.
func (i *Incoming) AddHoldInvoice(ctx context.Context, req node.HoldInvoice) (
	string, error) {

	if req.AmountMsat == 0 {
		return "", fmt.Errorf("%w: a hold invoice for any amount would let "+
			"the payer choose what the bridge is paid", ErrConfig)
	}
	if req.CLTVDelta == 0 {
		return "", fmt.Errorf("%w: no CLTV delta; the margin policy sizes "+
			"this and a node default would not survive the outgoing leg",
			ErrConfig)
	}
	if req.Expiry <= 0 {
		// LND's default is a day. An invoice that outlives its quote can be
		// funded hours later at a rate that has moved, and while the state
		// machine does cancel it, the window is real and there is no reason
		// to leave it open.
		return "", fmt.Errorf("%w: no invoice expiry; the node's default "+
			"would outlive the quote by hours", ErrConfig)
	}

	resp, err := i.invoices.AddHoldInvoice(ctx,
		&invoicesrpc.AddHoldInvoiceRequest{
			Memo:            req.Memo,
			Hash:            req.Hash[:],
			ValueMsat:       int64(req.AmountMsat),
			CltvExpiry:      uint64(req.CLTVDelta),
			Expiry:          int64(req.Expiry.Seconds()),
			PaymentMetadata: req.Metadata,
		})
	if err != nil {
		return "", fmt.Errorf("adding a hold invoice: %w", err)
	}

	return resp.GetPaymentRequest(), nil
}

// LookupInvoice reports what a hold invoice is doing.
func (i *Incoming) LookupInvoice(ctx context.Context, hash node.Hash) (
	node.Invoice, error) {

	inv, err := i.invoices.LookupInvoiceV2(ctx, &invoicesrpc.LookupInvoiceMsg{
		InvoiceRef: &invoicesrpc.LookupInvoiceMsg_PaymentHash{
			PaymentHash: hash[:],
		},
	})
	if err != nil {
		if invoiceAbsent(err) {
			return node.Invoice{}, fmt.Errorf("%w: %x", node.ErrUnknownHash,
				hash)
		}

		return node.Invoice{}, fmt.Errorf("looking up %x: %w", hash, err)
	}

	return translateInvoice(inv), nil
}

// translateInvoice is the part of this package worth reading twice.
func translateInvoice(inv *lnrpc.Invoice) node.Invoice {
	out := node.Invoice{AmountMsat: uint64(inv.GetValueMsat())}

	switch inv.GetState() {
	case lnrpc.Invoice_OPEN:
		out.State = node.InvoiceOpen
	case lnrpc.Invoice_ACCEPTED:
		out.State = node.InvoiceAccepted
	case lnrpc.Invoice_SETTLED:
		out.State = node.InvoiceSettled
	case lnrpc.Invoice_CANCELED:
		out.State = node.InvoiceCancelled
	default:
		// An unrecognised state is not claimable as far as this bridge is
		// concerned. Treating an unknown as accepted would authorise a
		// payment against something that may not be there.
		out.State = node.InvoiceCancelled
	}

	// The expiry height is the minimum across the accepted HTLCs, not the
	// invoice's nominal delta.
	//
	// A multi-part payment arrives as several HTLCs that took different
	// routes, so they expire at different heights, and the claim on the whole
	// invoice dies with the earliest of them: once that one times out its
	// payer's channel force-closes and takes that share back, and settling
	// afterwards claims less than the swap was priced at. Reporting the
	// nominal delta, or the largest, would tell the margin check there is
	// more time than there is, which is exactly the direction that pays out
	// against a claim the bridge can no longer collect.
	//
	// Settled and cancelled HTLCs are skipped: a cancelled one is gone and a
	// settled one is already collected, so neither bounds anything.
	var earliest int32
	for _, htlc := range inv.GetHtlcs() {
		if htlc.GetState() != lnrpc.InvoiceHTLCState_ACCEPTED {
			continue
		}
		h := htlc.GetExpiryHeight()
		if h <= 0 {
			continue
		}
		if earliest == 0 || h < earliest {
			earliest = h
		}
	}
	out.ExpiryHeight = earliest

	// Every HTLC the node recorded, in any state: lnd keeps a cancelled one
	// on the invoice, and that is the evidence an invoice was funded once.
	out.HTLCs = len(inv.GetHtlcs())

	return out
}

// SettleInvoice claims the HTLC. It is idempotent.
func (i *Incoming) SettleInvoice(ctx context.Context, pre node.Preimage) error {
	_, err := i.invoices.SettleInvoice(ctx, &invoicesrpc.SettleInvoiceMsg{
		Preimage: pre[:],
	})
	if err == nil {
		return nil
	}

	// Settling one that is already settled is success, not failure: after a
	// crash the bridge reissues this, and the reissue arriving at a node that
	// already did it must not read as a problem.
	//
	// Asking the node what state the invoice is in, rather than matching the
	// error's text, because the text is not an interface and a node that
	// rephrases it would turn a settled swap into one that retries forever.
	if inv, lookupErr := i.LookupInvoice(ctx, pre.Hash()); lookupErr == nil &&
		inv.State == node.InvoiceSettled {

		return nil
	}

	return fmt.Errorf("settling: %w", err)
}

// CancelInvoice returns the HTLC. It is idempotent.
func (i *Incoming) CancelInvoice(ctx context.Context, hash node.Hash) error {
	_, err := i.invoices.CancelInvoice(ctx, &invoicesrpc.CancelInvoiceMsg{
		PaymentHash: hash[:],
	})
	if err == nil {
		return nil
	}

	if inv, lookupErr := i.LookupInvoice(ctx, hash); lookupErr == nil &&
		inv.State == node.InvoiceCancelled {

		return nil
	}

	return fmt.Errorf("cancelling %x: %w", hash, err)
}

// ForgetInvoice deletes a cancelled invoice so its hash can be used again.
//
// The state is read first rather than left to the node to refuse: deleting
// anything but a cancelled invoice would erase the record of an HTLC that is
// held or was paid, and that is a decision this must never make by accident.
// A hash the node has never seen is already forgotten, which is what makes
// repeating this after a crash safe.
func (i *Incoming) ForgetInvoice(ctx context.Context, hash node.Hash) error {
	inv, err := i.LookupInvoice(ctx, hash)
	switch {
	case errors.Is(err, node.ErrUnknownHash):
		return nil
	case err != nil:
		return fmt.Errorf("reading %x before forgetting it: %w", hash, err)
	case inv.State != node.InvoiceCancelled:
		return fmt.Errorf("%w: %x is %v, and only a cancelled invoice may "+
			"be forgotten", ErrConfig, hash, inv.State)
	}

	_, err = i.main.DeleteCanceledInvoice(ctx, &lnrpc.DelCanceledInvoiceReq{
		InvoiceHash: hex.EncodeToString(hash[:]),
	})
	if err == nil {
		return nil
	}

	// Gone by the time it was asked for, which is the outcome wanted.
	if _, lookupErr := i.LookupInvoice(ctx, hash); errors.Is(lookupErr,
		node.ErrUnknownHash) {

		return nil
	}

	return fmt.Errorf("forgetting %x: %w", hash, err)
}

// BlockHeight is the tip this node sees.
func (i *Incoming) BlockHeight(ctx context.Context) (int32, error) {
	return blockHeight(ctx, i.main)
}

// Check confirms the node answers and is synced to its chain.
//
// Worth calling at startup. Dialling succeeds against a node that is not
// there, so without this the first sign of a wrong address or a node still
// catching up is a swap that has already accepted someone's money.
func (i *Incoming) Check(ctx context.Context) error {
	if _, err := i.BlockHeight(ctx); err != nil {
		return fmt.Errorf("incoming node: %w", err)
	}

	return nil
}

func blockHeight(ctx context.Context, c lnrpc.LightningClient) (int32, error) {
	info, err := c.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return 0, fmt.Errorf("reading the node's height: %w", err)
	}
	if !info.GetSyncedToChain() {
		// Refuse rather than return a height that is not the present one.
		// Every margin decision is a comparison against this number, and a
		// stale one says an HTLC has more time left than it does.
		return 0, fmt.Errorf("%w: at height %d", ErrNotSynced,
			info.GetBlockHeight())
	}

	h := info.GetBlockHeight()
	if h > 1<<31-1 {
		return 0, fmt.Errorf("%w: implausible height %d", ErrNotSynced, h)
	}

	return int32(h), nil
}

// invoiceAbsent reports whether a lookup failed because the node has no such
// invoice. lnd answers NotFound for that, except on a node that has never
// made an invoice at all, which answers "there are no existing invoices" with
// no code; a new SHA256 node is one, until its first swap.
func invoiceAbsent(err error) bool {
	return status.Code(err) == codes.NotFound ||
		strings.Contains(err.Error(), "there are no existing invoices")
}
