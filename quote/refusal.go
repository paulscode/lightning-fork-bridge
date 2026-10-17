package quote

import (
	"errors"

	"github.com/paulscode/lightning-fork-bridge/chainrate"
	"github.com/paulscode/lightning-fork-bridge/driver"
	"github.com/paulscode/lightning-fork-bridge/inventory"
	"github.com/paulscode/lightning-fork-bridge/margin"
	"github.com/paulscode/lightning-fork-bridge/rate"
)

// The reasons a quote is refused. Each refusal wraps ErrRefused and exactly
// one of these, so a caller can branch on the reason while anything that only
// asks "was it refused" keeps working.
//
// They exist because a refusal travels: a payer's wallet has to tell its user
// whether to try a smaller amount, try later, or give up, and parsing the
// sentence for that would break the first time a sentence was improved.
var (
	// ErrInvalidInvoice: the paying node could not read the invoice.
	ErrInvalidInvoice = errors.New("the invoice cannot be read")

	// ErrWrongChain: the invoice is for the other chain than the one this
	// direction pays on.
	ErrWrongChain = errors.New("the invoice is for the other chain")

	// ErrNoAmount: the invoice names no amount.
	ErrNoAmount = errors.New("the invoice names no amount")

	// ErrTooSmall and ErrTooLarge: outside the policy's swap bounds.
	ErrTooSmall = errors.New("the amount is below the minimum")
	ErrTooLarge = errors.New("the amount is above the maximum")

	// ErrExpiresSoon: too little of the invoice's life is left to pay it.
	ErrExpiresSoon = errors.New("the invoice expires too soon")

	// ErrRouteBudget: the destination's final hop leaves no room for a
	// route.
	ErrRouteBudget = errors.New("the destination's CLTV fills the budget")

	// ErrNoRoute: the paying node has no route to the invoice's destination
	// within the bridge's routing budget and CLTV limit. Asked before the
	// quote, so a payer is not held for a payment that cannot be made.
	ErrNoRoute = errors.New("no route to the destination within the " +
		"bridge's limits")

	// ErrSelfPayment: the invoice pays one of the bridge's own nodes.
	ErrSelfPayment = errors.New("the invoice pays the bridge itself")

	// ErrNoLiquidity: not enough left on the paying side.
	ErrNoLiquidity = errors.New("not enough liquidity on the paying side")

	// ErrInProgress: a swap for this payment hash has not finished.
	ErrInProgress = errors.New("a swap for this payment hash is in progress")

	// ErrAlreadyPaid: a swap for this payment hash settled.
	ErrAlreadyPaid = errors.New("a swap for this payment hash was paid")

	// ErrFundedBefore: an earlier attempt to pay this invoice was funded
	// and ended without paying it. The payer's money came back, but the
	// bridge never holds a second invoice under a hash an earlier one was
	// funded under, so the recipient has to issue a new invoice.
	ErrFundedBefore = errors.New("an earlier attempt to pay this invoice " +
		"was funded; ask the recipient for a new invoice")

	// ErrNeedsOperator: a swap for this payment hash ended in a way only the
	// operator can resolve.
	ErrNeedsOperator = errors.New("a swap for this payment hash needs " +
		"the operator")

	// ErrLimit: this participant has reached a limit of their own.
	ErrLimit = errors.New("participant limit reached")

	// ErrUntracked is returned when the swap was recorded but creating its
	// hold invoice failed in a way that does not say whether the node made
	// it. The caller must drive the swap anyway: the record is in the
	// journal and the invoice may be on the node, and only driving it
	// either catches the record up or lets it expire. Leaving it undriven
	// would hold its amount against the bridge's headroom until the next
	// restart.
	ErrUntracked = errors.New("the swap was recorded and its hold " +
		"invoice may exist")
)

// Code is the stable name of a refusal, for anything that crosses a process
// boundary.
type Code string

// The codes a quote can fail with. The payer API document lists the same set.
const (
	CodeInvalidInvoice   Code = "invalid_invoice"
	CodeNoDirection      Code = "no_direction"
	CodeNoAmount         Code = "no_amount"
	CodeTooSmall         Code = "too_small"
	CodeTooLarge         Code = "too_large"
	CodeExpiresSoon      Code = "expires_soon"
	CodeRouteBudget      Code = "route_budget"
	CodeNoRoute          Code = "no_route"
	CodeSelfPayment      Code = "self_payment"
	CodeNoLiquidity      Code = "no_liquidity"
	CodePriceUnavailable Code = "price_unavailable"
	CodeChainUnmeasured  Code = "chain_unmeasured"
	CodeInProgress       Code = "in_progress"
	CodeAlreadyPaid      Code = "already_paid"
	CodeFundedBefore     Code = "funded_before"
	CodeNeedsOperator    Code = "needs_operator"
	CodeLimit            Code = "limit"
	CodeUnavailable      Code = "unavailable"
	CodeInternal         Code = "internal"
)

// CodeOf names the reason behind an error from Quote.
//
// The order matters where one error could match two: the specific reasons
// are checked before the families they belong to, and anything unrecognised
// is internal rather than guessed at. Unavailable is reserved for errors that
// say nothing about the swap, so a payer can retry those and only those.
func CodeOf(err error) Code {
	switch {
	case err == nil:
		return ""

	case errors.Is(err, ErrInvalidInvoice):
		return CodeInvalidInvoice
	case errors.Is(err, ErrWrongChain):
		return CodeNoDirection
	case errors.Is(err, ErrNoAmount):
		return CodeNoAmount
	case errors.Is(err, ErrTooSmall):
		return CodeTooSmall
	case errors.Is(err, ErrTooLarge):
		return CodeTooLarge
	case errors.Is(err, ErrExpiresSoon):
		return CodeExpiresSoon
	case errors.Is(err, ErrRouteBudget):
		return CodeRouteBudget
	case errors.Is(err, ErrNoRoute):
		return CodeNoRoute
	case errors.Is(err, ErrSelfPayment):
		return CodeSelfPayment
	case errors.Is(err, ErrLimit):
		return CodeLimit
	case errors.Is(err, ErrAlreadyPaid):
		return CodeAlreadyPaid
	case errors.Is(err, ErrNeedsOperator):
		return CodeNeedsOperator
	case errors.Is(err, ErrFundedBefore):
		return CodeFundedBefore
	case errors.Is(err, ErrInProgress), errors.Is(err, ErrDuplicate):
		return CodeInProgress

	// Liquidity: the explicit headroom refusal, and an inventory too low
	// to quote the draining direction at any price.
	case errors.Is(err, ErrNoLiquidity), errors.Is(err, inventory.ErrDrained):
		return CodeNoLiquidity

	// Price: no reading, a stale or contested one, a tripped breaker.
	case errors.Is(err, rate.ErrStale), errors.Is(err, rate.ErrDisagreement),
		errors.Is(err, rate.ErrBroken), errors.Is(err, rate.ErrObservation):
		return CodePriceUnavailable

	// A chain whose block rate cannot be measured, or measured blocks that
	// leave no safe CLTV.
	case errors.Is(err, chainrate.ErrInsufficient),
		errors.Is(err, chainrate.ErrStale),
		errors.Is(err, chainrate.ErrIncoherent),
		errors.Is(err, chainrate.ErrSurge),
		errors.Is(err, driver.ErrStaleRates),
		errors.Is(err, margin.ErrTooLong),
		errors.Is(err, margin.ErrBounds):
		return CodeChainUnmeasured

	case errors.Is(err, ErrUntracked):
		return CodeInternal

	// Refused for a reason with no code of its own: the bridge declined,
	// and the sentence says why. Not retryable as such, but not a fault.
	case errors.Is(err, ErrRefused):
		return CodeUnavailable
	}

	return CodeInternal
}
