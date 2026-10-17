# lightning-fork-bridge

Swaps between the Bitcoin BLAKE2b chain's Lightning network and Bitcoin's.

A user pays a `lnbc...` invoice from a Lightning Fork node, or is paid by someone
on the SHA256 chain, without either side giving custody to anyone: both
networks use the same HTLC construction and the same SHA256 payment hash, so a
hold invoice on the receiving chain and a payment on the sending chain settle
or fail together.

This daemon drives two nodes over gRPC, a Lightning Fork node for the BLAKE2b
chain and a stock LND for Bitcoin. Nothing here runs on both chains.

## Packages

| Package | What it decides |
| --- | --- |
| `chainrate` | How fast a chain is producing blocks, and the bounds a swap may rely on |
| `margin` | Whether an outgoing payment is safe to commit, and how much incoming CLTV a swap needs |
| `confirm` | How many confirmations a value at risk needs, priced in forgone block subsidy |
| `swap` | The state machine for one swap, and the rule that keeps a bridge solvent |
| `node` | What the bridge needs from the two Lightning nodes, as interfaces |
| `driver` | When to pay, and what a swap interrupted mid-flight should do |
| `rate` | What rate to quote, or when to refuse to quote at all |
| `store` | The durable record, which is what recovery reads and therefore what recovery believes |
| `quote` | Turning "pay this invoice" into a swap, or into a refusal |
| `inventory` | What to charge, from what the bridge is holding |
| `runner` | Driving a swap to completion, and what to retry versus what to stop for |
| `lnd` | The two real nodes, over gRPC |
| `lndrpc` | Generated protobuf clients, see its own README |

`margin` and `confirm` exist because block counts mean different things on the
two chains. The BLAKE2b chain has run between 1.3x and 19x faster than its ten
minute target, and a block on it costs about 1/325th as much to produce, so
neither HTLC timing nor reorg depth can be reasoned about in blocks.

`store` is about ordering more than storage. A swap's state has to be durable
*before* the action it authorises is taken, because after a crash the record is
the only thing that says whether an outgoing payment might be in flight. Write
it afterwards and a swap whose payment has already left comes back reading as
merely funded, the bridge re-decides, the margin refuses because the crash cost
it time, and it cancels its claim on the money coming in.
`TestTheOrderingRuleIsWhatPreventsTheLoss` runs exactly those facts twice with
only that ordering changed, and gets opposite answers.

`lnd` holds two translations worth reading twice, because every rule above
them is reasoning about what they report. A multi-part payment's expiry height
is the **minimum** across its accepted HTLCs, not the invoice's nominal delta,
because the claim on the whole invoice dies with the earliest part. And a
payment is reported failed only when the node says failed: a broken stream, a
deadline, a status this build has never seen all come back as in flight,
because `OutgoingFailed` is the single event that lets the bridge give up its
incoming claim.

`runner` holds two rules. One goroutine per payment hash, ever: a swap's
safety is a sequence (read the nodes, decide, record, act) and two goroutines
interleaving it can gate against one reading and pay against another. And a
split between failures that must be retried and failures that must not.
A node that did not answer says nothing about the swap, and abandoning a live
HTLC over a blip is how one expires; an inconsistency, or anything the bridge
has said it must not decide alone, must not be repeated at all.

Not every swap can be hash-linked, and `swap.Binding` says which kind each one
is rather than leaving it to be inferred. A **hash-linked** swap shares one
payment hash, so the bridge physically cannot claim the incoming funds without
having delivered, a fact about what it holds which survives any bug in this
program. A **sequenced** swap cannot: a payout's outgoing invoice is issued by
the recipient's node, which chooses its own preimage, so the legs have
different hashes and the bridge knows the incoming preimage from the start.
There the protection is the *order*, enforced here rather than by arithmetic:
the incoming HTLC is held until the outgoing leg has definitively succeeded, so
the recipient is paid before the bridge collects. What that protects them from
is failure, not malice, and the code says so where it matters rather than
letting the two be mistaken for one another.

`quote` never chooses the payment hash. It comes from the invoice the user
asked to have paid, read by **the node that will pay it** rather than by a
second parser in this process, because a parser that disagreed by one field
would have the bridge hold an invoice for one hash while the node paid another.
The rest of the package is refusals: an invoice that expires before the
incoming leg could be funded, a routing budget wider than the spread it comes
out of, an incoming amount rounded in the payer's favour on every swap, a
second hold invoice for a hash that already has one.

`inventory` exists because the number the design most wanted to know in
advance, how much flow comes back the other way, is not knowable in advance.
It is demand to be created, not a fact to be measured, and a bridge that had to
predict it before it could quote would never start. So it is not predicted. The
spread is a function of the measured position instead: one-way flow drains the
outgoing side and prices itself at what one-way flow costs (3% at the default
policy), balanced flow never drains it and sits at the floor (1%), and nobody
decided which it would be. A swap in the replenishing direction gets a discount
instead, because it is doing the bridge a favour it would otherwise pay a venue
for, capped at what that venue would have charged, and lifted rather than
zeroed when there is no venue at all, since flow is then the only mechanism
there is.

## Running it

    bridged -config /etc/bridged/config.json -check   # connect and exit
    bridged -config /etc/bridged/config.json          # run

`config.example.json` is a starting point. There are no defaults for anything
that decides what a swap costs or how long it may take, and unknown keys are an
error rather than ignored: a misspelled policy key that is quietly dropped
leaves the daemon running on a number the operator believes they overrode.

`-check` loads the configuration, dials both nodes and opens the journal, then
exits. A configuration error found at deploy time costs a restart; the same
error found later costs whatever was in flight.

| Endpoint | What it does |
| --- | --- |
| `POST /quote` | takes an invoice on either chain, returns a hold invoice on the other |
| | `expiresAt` is the deadline for paying it. Miss it and the hold invoice is cancelled; pay it and the CLTV, not the clock, is what bounds the swap |
| `GET /swap/{hash}` | what the bridge believes about one swap, and the preimage once there is one |
| `GET /health` | whether it could quote right now, which is not the same as whether it is running |

There is no authentication. A quote commits the bridge's money, so this belongs
on loopback or behind something that authenticates, and a weak scheme here
would be worse than saying so plainly.

Which direction a request is depends on the invoice, and the daemon works that
out by asking each paying node to decode it rather than by parsing it here. The
node that can read an invoice is the node that will pay it, which is the same
reason `quote` takes the payment hash from the paying node.

Three numbers have to fit together and each looks reasonable alone: the
outgoing CLTV budget, the settlement allowance, and how fast the incoming chain
can produce blocks. The incoming CLTV a swap needs is roughly the first two
divided by the third, and if that exceeds `margin.maxIncomingBlocks` the bridge
refuses every swap it is offered. Planning for the full 19x surge this chain
has shown, while keeping an outgoing budget that suits ordinary destinations,
does exactly that. The daemon checks the combination at startup rather than
letting it surface as a refusal on the first quote.

Block spacing is measured from block headers, read through ChainKit, which a
stock lnd serves only when built with the `chainrpc` tag. With it, the daemon
reads the last 150 headers of each chain at startup and is measurable in
seconds, and the headers' difficulty carries the measurement across a
retarget. Without it, the daemon learns from tips as they arrive and refuses
quotes until it has `minSamples` (at least 100) blocks of the current epoch,
which is most of a day after a start and after every retarget.

The journal is append-only, so the daemon rewrites it down to each swap's
latest record, hourly by default and only while nothing is being driven:
compaction takes the journal's lock, and a swap blocked on writing its own
state is a swap that is not watching an HTLC. Without that it grows for as long
as the daemon runs and is replayed in full at every start. Finished swaps keep
their last record, because a new quote for the same payment hash is decided by
how the earlier attempt ended.

`GET /health` answers 503 when there is no usable price, a chain it cannot
measure, a node it cannot reach, or a journal that has gone. A bridge in any of those states is up and
unable to do anything, and reporting "ok" is how that goes unnoticed.

## Status

Built and tested: the logic that decides, the record it decides against, the
adapters to real nodes, the loop that drives a swap to completion, the quote
surface that creates one, and the pricing that decides what it costs.

**Swaps have run end to end across two chains, in both directions.**
`bridgetest` drives the whole stack against the lab's four nodes, with real
hold invoices, real HTLCs and a real journal on disk:

| Test | What runs |
| --- | --- |
| `TestLiveSwap` | a user on the BLAKE2b chain pays a Bitcoin invoice |
| `TestLiveReverseSwap` | a user on Bitcoin pays a BLAKE2b invoice |
| `TestLiveRefund` | the outgoing leg cannot be delivered, and the user is refunded |

The reverse direction needed no change to `quote`, `driver`, `runner` or
`store`. Those take their nodes and their rate as collaborators, so it is the
same machinery with the two nodes exchanged and the rate inverted. What did
change is that the rate is named `OutgoingPerIncoming` rather than for the two
assets, because the formula it feeds does not care which asset is which and the
old name was true in one direction and false in the other.

That matters for more than symmetry. `inventory` prices the spread off how
drained the outgoing side is, and calls the flow that puts it back
*replenishing*; its comments reach for mining payouts as the example, which made
the replenishing flow look like it depended on a mining integration. It does
not. An ordinary payment in the other direction is hash-linked exactly like the
forward one, needs no payout adapter, and replenishes just as well.

**The daemon runs both of those against the lab.** A swap quoted over
`POST /quote`, paid by a user, driven to settlement and reported by
`GET /swap/{hash}`, in both directions. And the thing a daemon exists for:
a swap quoted before a restart is picked up by the next process, driven when
the user pays it afterwards, and settled. Recovery is not a claim about the
journal here, it is something that happened.

Since then, three things changed what a quote does. A direction is chosen by
whether the invoice sets `option_blake2b`, because both chains' invoices now
share a prefix and both nodes read every one. An invoice whose earlier attempt
expired before anyone paid it can be quoted again, which deletes the cancelled
hold invoice first, since a node refuses a second invoice for the same hash;
one whose earlier attempt was funded is refused as `funded_before`, and the
recipient has to issue a new invoice. And a
funded swap that cannot be paid within a grace period of its quote's expiry is
given back rather than paid late. Refusals carry stable codes, and each
participant can be limited separately. `docs/payer-api.md` is what a wallet
codes against.

Not built: the payout adapter, which is the **sequenced** binding for a payer
that issues its own invoice. Nothing has run against mainnet.

Nothing here has moved money. The order was deliberate: the parts that lose
funds when they are wrong are the parts that can be tested exhaustively
without a node, so they were written first and reviewed adversarially before
anything could call them.
