// Package node is what the bridge needs from the two Lightning nodes it
// drives, expressed as interfaces so the logic above can be tested without
// either of them.
//
// The incoming side is a Lightning Fork node on the BLAKE2b chain and the
// outgoing side a stock LND on Bitcoin, or the reverse for a swap going the
// other way. Nothing here is specific to either: a swap receives on one chain
// and pays on the other, and which is which is the caller's business.
package node

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

// ErrNotSent says a payment request provably never reached the paying node,
// so nothing can be in flight because of it.
//
// An adapter may return it only when that is certain from how the call
// failed, not merely likely: the connection could not be made at all, or the
// node refused the call before any handler ran (it was still starting, or its
// wallet was locked). A connection lost after the request was written does
// not qualify, nor does a timeout, since either may follow delivery.
//
// The driver reads it as "this attempt never happened" and returns the swap to
// Funded to be decided again, rather than leaving it in Paying with a node
// that has no record of the payment, which is ambiguous and needs an operator.
// Even a wrong reading would not pay twice through a node that still has its
// records, which refuses a second payment for a hash it is paying or has paid;
// but adapters must not lean on that.
var ErrNotSent = errors.New("the payment never reached the paying node")

// ErrUnknownHash is returned when a node has no record of a payment hash.
var ErrUnknownHash = errors.New("node has no record of this payment hash")

// ErrNoRoute is returned by a RouteFinder that has no route to an invoice's
// destination within the limits asked for.
var ErrNoRoute = errors.New("no route to the destination within the limits")

// Route is a route a RouteFinder found: the CLTV it would lock the payment
// for, counted in blocks from now, and what it would pay in routing fees.
type Route struct {
	CLTV    uint32
	FeeMsat uint64
}

// RouteFinder is an Outgoing node that can say, before anything is committed,
// whether it has a route to an invoice's destination within a routing budget
// and a CLTV limit. Optional: a quote asks one when the paying node is one,
// and otherwise sizes the swap to the whole budget as before.
type RouteFinder interface {
	FindRoute(ctx context.Context, invoice string, maxFeeMsat uint64,
		cltvLimit uint32) (Route, error)
}

// Hash is a payment hash. The same one appears on both chains, which is what
// makes a swap atomic.
type Hash [32]byte

// Preimage is what settles both legs.
type Preimage [32]byte

// Hash is the payment hash this preimage settles.
//
// It is SHA256 on both chains. The BLAKE2b fork changed how blocks are hashed,
// not how payments are, and that is the whole reason a swap between the two
// networks needs no new cryptography: the same preimage claims both HTLCs. Any
// change here would silently break that, so it is pinned by a test against a
// vector both chains agree on.
func (p Preimage) Hash() Hash {
	return Hash(sha256.Sum256(p[:]))
}

// InvoiceState is what a hold invoice on the incoming chain is doing.
type InvoiceState uint8

const (
	// InvoiceOpen: created and waiting to be paid.
	InvoiceOpen InvoiceState = iota

	// InvoiceAccepted: the HTLC is locked in and claimable with the
	// preimage. This is the state a hold invoice sits in while the bridge
	// works, and the state that makes the swap safe to pay out against.
	InvoiceAccepted

	// InvoiceSettled: claimed.
	InvoiceSettled

	// InvoiceCancelled: returned to the payer, or expired. Once here the
	// money cannot be claimed, whatever the bridge knows.
	InvoiceCancelled
)

var invoiceStateNames = map[InvoiceState]string{
	InvoiceOpen: "open", InvoiceAccepted: "accepted",
	InvoiceSettled: "settled", InvoiceCancelled: "cancelled",
}

func (s InvoiceState) String() string {
	if n, ok := invoiceStateNames[s]; ok {
		return n
	}

	return fmt.Sprintf("invoice state(%d)", uint8(s))
}

// Claimable reports whether the incoming HTLC can still be settled.
func (s InvoiceState) Claimable() bool {
	return s == InvoiceAccepted
}

// Invoice is what the incoming node says about a hold invoice.
type Invoice struct {
	State      InvoiceState
	AmountMsat uint64

	// ExpiryHeight is the chain height at which the incoming HTLC stops
	// being claimable. Together with the current height it gives the blocks
	// of CLTV remaining, which is what the margin check needs.
	//
	// A multi-part payment arrives as several HTLCs with different expiry
	// heights, and the whole claim dies with the earliest of them, so an
	// implementation MUST report the minimum across the accepted HTLCs and
	// not the invoice's nominal delta. Reporting anything later overstates
	// how long the bridge has to claim, which is the direction that loses
	// money.
	//
	// It is zero while no HTLC is locked in.
	ExpiryHeight int32
}

// PaymentState is what an outgoing payment is doing.
type PaymentState uint8

const (
	// PaymentNone: the node has no record of it.
	PaymentNone PaymentState = iota

	// PaymentInFlight: one or more HTLCs are outstanding and the outcome is
	// not known. Nothing may be concluded from this.
	PaymentInFlight

	// PaymentSucceeded: the preimage is known.
	PaymentSucceeded

	// PaymentFailed: definitively failed, every HTLC resolved. A payment
	// whose outcome is merely unknown must never be reported as this.
	PaymentFailed
)

var paymentStateNames = map[PaymentState]string{
	PaymentNone: "none", PaymentInFlight: "in flight",
	PaymentSucceeded: "succeeded", PaymentFailed: "failed",
}

func (s PaymentState) String() string {
	if n, ok := paymentStateNames[s]; ok {
		return n
	}

	return fmt.Sprintf("payment state(%d)", uint8(s))
}

// Payment is what the outgoing node says about a payment.
type Payment struct {
	State    PaymentState
	Preimage Preimage
	FeeMsat  uint64
}

// HoldInvoice is what the bridge asks the incoming node to create.
type HoldInvoice struct {
	Hash       Hash
	AmountMsat uint64

	// CLTVDelta is the minimum CLTV the payer must leave on the final hop,
	// in blocks of the incoming chain. It comes from margin, which sizes it
	// in wall-clock and converts, so it is usually far larger than a
	// same-chain invoice would ask for.
	CLTVDelta uint32

	// Expiry is how long the invoice may go unpaid.
	Expiry time.Duration

	// Memo is what the payer sees.
	Memo string

	// Metadata is the destination invoice's payment metadata (BOLT 11 field
	// m), carried over opaque so the payer reading the hold invoice sees it
	// too (see InvoiceMetadata). A node that cannot put it on an invoice,
	// stock LND, leaves it off.
	Metadata []byte
}

// PayRequest is what the bridge asks the outgoing node to send.
type PayRequest struct {
	// Invoice is the destination invoice, as the payer supplied it. The
	// bridge never rewrites it: its payment hash is what the incoming hold
	// invoice was built on, and settling requires this exact preimage.
	Invoice string

	// MaxFeeMsat caps routing fees.
	MaxFeeMsat uint64

	// CLTVLimit caps the total CLTV of the route, in blocks of the outgoing
	// chain. Keeping it small is the cheapest way to shrink what the
	// incoming leg has to outlive.
	CLTVLimit uint32

	// Timeout abandons the attempt. It bounds how long the payment can sit
	// in flight, but not how long an HTLC that has already left can take to
	// resolve on chain, which is what CLTVLimit bounds.
	Timeout time.Duration
}

// Incoming is the node the bridge receives on.
type Incoming interface {
	// AddHoldInvoice creates an invoice that will not settle until told to.
	AddHoldInvoice(ctx context.Context, req HoldInvoice) (string, error)

	// LookupInvoice reports what a hold invoice is doing. It returns
	// ErrUnknownHash if the node has never seen it.
	LookupInvoice(ctx context.Context, hash Hash) (Invoice, error)

	// SettleInvoice claims the HTLC. It is idempotent: settling an already
	// settled invoice succeeds.
	SettleInvoice(ctx context.Context, preimage Preimage) error

	// CancelInvoice returns the HTLC. It is idempotent.
	CancelInvoice(ctx context.Context, hash Hash) error

	// ForgetInvoice deletes a cancelled invoice, so that its payment hash can
	// carry a new one.
	//
	// It exists for quoting an invoice again after an attempt ended without
	// paying it: a node keeps cancelled invoices and refuses a second one
	// for the same hash, which would leave that destination unpayable
	// through the bridge for good. It must refuse an invoice that is not
	// cancelled, and succeed for one the node has never seen, so that
	// repeating it after a crash is harmless.
	ForgetInvoice(ctx context.Context, hash Hash) error

	// BlockHeight is the tip this node sees, used to turn an expiry height
	// into blocks remaining.
	BlockHeight(ctx context.Context) (int32, error)
}

// Decoded is a payment request the bridge has been asked to pay, as the node
// that will pay it reads it.
type Decoded struct {
	// Hash is the payment hash. The incoming hold invoice is built on this
	// exact value, which is what makes the two legs one swap.
	Hash Hash

	// AmountMsat is what the invoice asks for. Zero means the invoice names
	// no amount, which the bridge refuses: the outgoing leg would be for
	// whatever it chose and the incoming one priced against a guess.
	AmountMsat uint64

	// CLTVDelta is the final hop's required delta, in blocks of the outgoing
	// chain. It is part of what the outgoing leg will cost in time.
	CLTVDelta uint32

	// Expiry is when the invoice stops being payable.
	Expiry time.Time

	// Destination is who gets paid, for logs and for refusing to pay
	// ourselves.
	Destination string

	// Description is what the payer will see quoted back to them.
	Description string

	// BLAKE2b is whether the invoice sets option_blake2b (feature bit 512 or
	// 513), which every invoice issued on the BLAKE2b chain does.
	//
	// It is how a direction is chosen. Both chains' invoices share the same
	// prefix, so both nodes can read every invoice, and "which node can
	// decode it" no longer says which chain it is for.
	BLAKE2b bool
}

// BLAKE2bFeatureBits are option_blake2b's required and optional bits.
var BLAKE2bFeatureBits = [2]uint32{512, 513}

// Outgoing is the node the bridge pays on.
type Outgoing interface {
	// Decode reads a payment request.
	//
	// This is deliberately the paying node's own decoder rather than a
	// second one in this process. A separate parser that disagreed by one
	// field would let the bridge build a hold invoice around one payment
	// hash while the node paid another, and the swap's entire security is
	// that those are the same value.
	Decode(ctx context.Context, invoice string) (Decoded, error)

	// Pay sends a payment and blocks until it resolves or the request's
	// timeout elapses. A timeout is not a failure: the payment may still be
	// in flight, and LookupPayment is the only way to find out.
	//
	// An error wrapping ErrNotSent says the request never reached the
	// node; see there for how certain that has to be.
	Pay(ctx context.Context, req PayRequest) (Payment, error)

	// LookupPayment reports what a payment is doing. Unlike the incoming
	// side it does not error on an unknown hash, because "never started" is
	// a meaningful answer after a restart.
	LookupPayment(ctx context.Context, hash Hash) (Payment, error)

	// BlockHeight is the tip this node sees.
	BlockHeight(ctx context.Context) (int32, error)
}
