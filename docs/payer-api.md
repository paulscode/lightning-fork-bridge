# The payer API

How a wallet asks a bridge to pay an invoice on the other chain, version 1.

A bridge is a Lightning Fork node running the bridge sub-server, next to a
stock lnd on the SHA256 chain. A payer is anything holding a **bridge code** the
operator issued: in practice, the dashboard that comes with Lightning Fork. The
payer never trusts the bridge with funds. It pays a hold invoice that carries
the same payment hash as the invoice it wants paid, so the bridge can only
collect by paying that invoice, and the payer's own node ends up holding the
preimage as proof.

This document covers paying a **SHA256 invoice from the BLAKE2b chain**
(direction `toSHA256`). The calls are direction-agnostic, and the reverse
direction uses them unchanged.

## 1. The bridge code

The operator gives each participant one code: the string `lfbridge:` followed
by the unpadded base64url encoding of a JSON object.

```json
{
  "v": 1,
  "label": "Paul's bridge",
  "url": "https://abcdef…xyz.onion:8080",
  "node": "02e9ce78…8dfd",
  "macaroon": "0201036c6e64…",
  "cert": "9F:86:D0:…:0A:08"
}
```

| Field | Meaning |
| --- | --- |
| `v` | Code version. A payer refuses a version it does not know. |
| `label` | A name for people. Not used for anything else. |
| `url` | The bridge node's REST address, scheme, host and port, no path. |
| `node` | The bridge's BLAKE2b node key, 33 bytes hex. Every hold invoice must be payable to exactly this key. |
| `macaroon` | Hex. This participant's credential, scoped to the three calls below and nothing else. |
| `cert` | SHA-256 of the DER of the TLS certificate the endpoint presents, colon-separated hex. Required unless the host is a `.onion`, where Tor authenticates the endpoint and lnd's self-signed certificate is not checked. |

The macaroon is the participant's identity: the bridge limits each one on its
own, and the operator revokes one without touching the others. A code is a
secret in the same way a password is.

## 2. Calls

lnd's REST gateway serves all three. Every request carries the macaroon in the
`Grpc-Metadata-macaroon` header. JSON follows the gateway's conventions:
`bytes` fields are standard base64, 64-bit integers are strings, and field
names are snake_case.

### 2.1 `GET /v2/bridge/info`

What the bridge would charge now. **It creates nothing and reserves nothing**,
so a payer may call it to show a price before the user commits.

```json
{
  "version": 1,
  "node": "02e9ce78…8dfd",
  "directions": [
    {
      "name": "toSHA256",
      "open": true,
      "refusal": "",
      "refusal_code": "",
      "rate": 0.00483,
      "rate_set_at": "1790990000",
      "spread": 0.01,
      "min_msat": "100000",
      "max_msat": "500000000"
    }
  ]
}
```

- `rate`: outgoing units per incoming unit. For `toSHA256` that is BTC (SHA256) per
  BTCB2.
- `rate_set_at`: Unix seconds when the operator set the rate, or the time of
  the last price reading.
- `spread`: the fraction charged on top of the rate at the bridge's current
  position. It can change between this call and a quote.
- `min_msat`, `max_msat`: bounds on the amount of the invoice to be paid, in
  the outgoing chain's millisatoshis.
- `open`: false when the bridge would refuse every quote in this direction, with
  `refusal_code` and `refusal` saying why (codes in §3).

**The estimate** for an invoice of `out` msat is exactly what a quote will
charge if nothing moves in between:

```
incoming_msat = ceil(out / rate * (1 + spread))
```

The bridge's routing fee comes out of the spread. The payer pays nothing on top
of `incoming_msat` except its own route to the bridge.

### 2.2 `POST /v2/bridge/quote`

```json
{ "invoice": "lnbc1500n1p…" }
```

Creates the swap and the hold invoice that pays for it.

```json
{
  "hold_invoice": "lnbc490m1p…",
  "hash": "base64 of the 32-byte payment hash",
  "incoming_msat": "49175859",
  "outgoing_msat": "150000",
  "rate": 0.00308,
  "spread": 0.01,
  "cltv_delta": 333,
  "direction": "toSHA256",
  "expires_at": "1790990120"
}
```

- Pay `hold_invoice` before `expires_at` (Unix seconds), or the bridge cancels
  it.
- `cltv_delta` is the hold invoice's final CLTV in BLAKE2b blocks. It is large
  on purpose: the incoming leg must outlive the outgoing one in wall-clock
  time, and the BLAKE2b chain's blocks can come far faster than ten minutes.

**A payer must check the hold invoice before paying it**, by decoding it on its
own node:

1. its payment hash equals the hash of the invoice it asked to have paid;
2. its destination is the `node` from the bridge code;
3. it sets `option_blake2b` (feature bit 512 or 513);
4. its amount is no more than the amount the user agreed to;
5. it has enough time left before `expires_at` to be paid.

If any check fails, the payer does not pay. A wrong hash or a wrong destination
means the bridge is not behaving as this document says.

**Quote at the moment of paying, then pay at once.** A quote reserves the
bridge's liquidity until it expires, and per-participant limits count it.

### 2.3 `POST /v2/bridge/swap`

```json
{ "hash": "base64 of the payment hash" }
```

```json
{
  "hash": "…",
  "state": "settled",
  "incoming_msat": "49175859",
  "outgoing_msat": "150000",
  "preimage": "base64; empty until there is one"
}
```

The payer's own node is the authority on whether the hold invoice was paid or
returned. This call answers what the bridge is doing about it, and tells a
payer when an attempt has ended so it may try again.

| `state` | For the payer |
| --- | --- |
| `quoted`, `offered` | Quoted, not yet paid. |
| `funded`, `paying`, `paid`, `failing`, `aborting`, `expiring` | In progress. Wait. |
| `settled` | Done. The invoice was paid; `preimage` is the proof. |

`preimage` is always present, as an empty string until the swap has one: test
for an empty value, not for the key. A participant sees only the swaps quoted
under their own bridge code; any other hash is `not_found`.
| `failed`, `aborted` | Ended without paying the invoice after the hold invoice was paid. The payment came back. The invoice cannot be quoted again (`funded_before`); ask the recipient for a new one. |
| `expired` | Ended without paying the invoice, usually before the hold invoice was paid. Any payment came back. The invoice may be quoted again unless the hold invoice had been paid (`funded_before`). |
| `lost` | Ended in a way the operator has to resolve. Not something a payer can act on. |

## 3. Refusals

Errors come back as the gateway's error object, with an HTTP status and a
`message` that starts with a stable code, a colon and a space, followed by a
sentence for a person: `"too_large: 600000000 msat is outside 100000..500000000"`.
A payer branches on the code and may show the sentence.

| Code | HTTP | Meaning |
| --- | --- | --- |
| `disabled` | 400 | The bridge is not enabled on this node. |
| `unavailable` | 503 | Starting, shutting down, or a node it needs did not answer. Try later. |
| `invalid_request` | 400 | The request is malformed: no invoice, a hash of the wrong length. |
| `invalid_invoice` | 400 | Neither node can read the invoice. |
| `no_direction` | 400 | The invoice is for a chain whose direction this bridge does not serve. |
| `no_amount` | 400 | The invoice names no amount. |
| `too_small`, `too_large` | 400 | Outside `min_msat`..`max_msat`. |
| `expires_soon` | 400 | The invoice has too little time left to be paid safely. |
| `route_budget` | 400 | The destination's final-hop CLTV leaves no room for a route. |
| `self_payment` | 400 | The invoice is payable to the bridge's own node. |
| `no_liquidity` | 400 | Not enough left on the paying side right now. |
| `price_unavailable` | 400 | No usable price, or the operator's rate is older than the bridge allows. |
| `chain_unmeasured` | 400 | The bridge cannot currently measure a chain's block rate, so it cannot size the hold invoice safely. Usually minutes after a restart. |
| `in_progress` | 409 | A swap for this payment hash is still in progress. |
| `already_paid` | 409 | A swap for this payment hash settled. `/v2/bridge/swap` has the preimage. |
| `needs_operator` | 409 | A swap for this payment hash ended in a way only the operator can resolve. |
| `funded_before` | 409 | An earlier attempt to pay this invoice was funded and ended without paying it. Ask the recipient for a new invoice. |
| `limit` | 429 | This participant has too many quotes outstanding, or is quoting too fast. |
| `not_found` | 404 | No swap with that hash. |
| `internal` | 500 | Anything else. |

A refusal the bridge has no code for comes back as `unavailable`: the bridge
declined, and the sentence says why.

A credential that is missing, revoked or not allowed this call gets lnd's own
authentication error, not one of these codes.

## 4. Quoting the same invoice again

At most one swap per payment hash is live at a time. Once a swap has expired
before anyone paid its hold invoice, a new quote for the same invoice is
allowed: the bridge deletes the cancelled hold invoice and starts afresh. Once
a hold invoice for the hash has been paid, the hash is never quoted again, even
though the attempt ended and the payment came back (`failed`, `aborted`, or
`expired` with a payment that arrived as it was being cancelled): the answer is
`funded_before`, and the recipient has to issue a new invoice. The same
answer is given when the bridge can no longer show that an expired attempt
went unpaid, because the node has since deleted its cancelled hold invoice.
While one is live the answer is `in_progress`; once one has settled it is
`already_paid`.

## 5. Limits per participant

Each credential is limited on its own, independently of the bridge's overall
liquidity:

- how many quotes it may have unpaid at once (an unpaid quote reserves the
  bridge's liquidity and costs the participant nothing, so this is the limit
  that matters most);
- how many swaps it may have unfinished at once, paid or not, which with the
  per-swap maximum bounds the value it can have outstanding;
- how many quotes it may ask for in any minute.

Going over any of them is `limit`. The operator sets them. The operator's own
administrative macaroon is not limited.
