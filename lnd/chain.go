package lnd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/paulscode/lightning-fork-bridge/lndrpc/lnrpc"
)

// ErrHeader is returned for a block header the node could not give, or gave in
// a form that cannot be read.
var ErrHeader = errors.New("block header is not usable")

// Chain reads a node's view of its chain: the tip, and block headers by
// height, for measuring how fast the chain produces blocks.
type Chain struct {
	conn grpc.ClientConnInterface
	main lnrpc.LightningClient
}

// NewChain wraps a connection.
func NewChain(conn grpc.ClientConnInterface) *Chain {
	return &Chain{conn: conn, main: lnrpc.NewLightningClient(conn)}
}

// Tip is the node's best block as GetInfo reports it.
type Tip struct {
	Height int32

	// Time is when the tip was mined, from the header's own timestamp,
	// not when this node heard of it.
	Time time.Time

	// Synced is whether the node has caught up with its chain. Until it
	// has, its height and its best header's time can belong to different
	// blocks.
	Synced bool
}

// Tip reads the node's best block.
func (c *Chain) Tip(ctx context.Context) (Tip, error) {
	info, err := c.main.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return Tip{}, fmt.Errorf("reading the node's tip: %w", err)
	}
	h := info.GetBlockHeight()
	if h > math.MaxInt32 {
		return Tip{}, fmt.Errorf("%w: implausible height %d", ErrNotSynced, h)
	}

	return Tip{
		Height: int32(h),
		Time:   time.Unix(info.GetBestHeaderTimestamp(), 0),
		Synced: info.GetSyncedToChain(),
	}, nil
}

// Header is what the bridge reads from a block header.
type Header struct {
	Height int32

	// Time is the header's timestamp.
	Time time.Time

	// Bits is the header's compact difficulty target.
	Bits uint32
}

// Difficulty is the header's difficulty in units of the easiest possible
// target, 2^256 divided by the target its bits encode. Only its ratio to
// another header's of the same chain means anything, which is how the chain
// observer uses it. Zero when the bits encode no positive target.
func (h Header) Difficulty() float64 {
	target := compactToBig(h.Bits)
	if target.Sign() <= 0 {
		return 0
	}

	d, _ := new(big.Float).Quo(
		new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 256)),
		new(big.Float).SetInt(target),
	).Float64()

	return d
}

// compactToBig decodes a header's compact target: a one byte exponent, a sign
// bit, and a 23 bit mantissa, as both chains encode it.
func compactToBig(bits uint32) *big.Int {
	mantissa := int64(bits & 0x007fffff)
	exponent := uint(bits >> 24)
	negative := bits&0x00800000 != 0

	var n *big.Int
	if exponent <= 3 {
		n = big.NewInt(mantissa >> (8 * (3 - exponent)))
	} else {
		n = new(big.Int).Lsh(big.NewInt(mantissa), 8*(exponent-3))
	}
	if negative {
		n.Neg(n)
	}

	return n
}

// Header reads the header of the block at a height on the node's best chain.
//
// It goes through ChainKit, which a stock lnd serves only when built with the
// chainrpc tag; one without it answers codes.Unimplemented, which the caller
// should read as "no headers here" and do without. The two calls are made
// with a hand-encoded request rather than generated bindings: they are two
// fields wide, and generating ChainKit would add a proto to keep in step for
// them.
//
// Only the timestamp and the bits are decoded, from their fixed offsets, so
// this reads the header of either chain: the BLAKE2b fork changed how a
// header is hashed, not how it is laid out.
func (c *Chain) Header(ctx context.Context, height int32) (Header, error) {
	if height < 0 {
		return Header{}, fmt.Errorf("%w: height %d", ErrHeader, height)
	}

	req := protowire.AppendTag(nil, 1, protowire.VarintType)
	req = protowire.AppendVarint(req, uint64(height))
	hash, err := c.call(ctx, "/chainrpc.ChainKit/GetBlockHash", req)
	if err != nil {
		return Header{}, fmt.Errorf("the hash at height %d: %w", height, err)
	}
	if len(hash) != 32 {
		return Header{}, fmt.Errorf("%w: a %d byte hash at height %d",
			ErrHeader, len(hash), height)
	}

	req = protowire.AppendTag(nil, 1, protowire.BytesType)
	req = protowire.AppendBytes(req, hash)
	raw, err := c.call(ctx, "/chainrpc.ChainKit/GetBlockHeader", req)
	if err != nil {
		return Header{}, fmt.Errorf("the header at height %d: %w", height,
			err)
	}
	if len(raw) < 80 {
		return Header{}, fmt.Errorf("%w: a %d byte header at height %d",
			ErrHeader, len(raw), height)
	}

	// Bytes 68 to 72 are the timestamp and 72 to 76 the compact target,
	// both little endian.
	return Header{
		Height: height,
		Time:   time.Unix(int64(binary.LittleEndian.Uint32(raw[68:72])), 0),
		Bits:   binary.LittleEndian.Uint32(raw[72:76]),
	}, nil
}

// call makes a unary ChainKit call whose reply's field 1 is bytes, and returns
// that field.
func (c *Chain) call(ctx context.Context, method string, req []byte) ([]byte,
	error) {

	in, out := rawMessage(req), rawMessage(nil)
	if err := c.conn.Invoke(ctx, method, &in, &out,
		grpc.ForceCodec(rawCodec{})); err != nil {

		return nil, err
	}

	b := []byte(out)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("%w: %v", ErrHeader, protowire.ParseError(n))
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return nil, fmt.Errorf("%w: %v", ErrHeader,
					protowire.ParseError(m))
			}

			return v, nil
		}
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return nil, fmt.Errorf("%w: %v", ErrHeader, protowire.ParseError(m))
		}
		b = b[m:]
	}

	// proto3 leaves an empty field out, so an absent field is an empty one.
	return nil, nil
}

// rawMessage is a message already in its wire form.
type rawMessage []byte

// rawCodec is the gRPC codec Header uses: a rawMessage passes through as it
// is, and anything else is a protobuf message encoded as usual. Its name is
// "proto", so a server reads what it sends as an ordinary protobuf request.
type rawCodec struct{}

// Name is the content subtype, the protobuf one.
func (rawCodec) Name() string { return "proto" }

// Marshal passes a rawMessage through and encodes anything else as protobuf.
func (rawCodec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case *rawMessage:
		return []byte(*m), nil
	case proto.Message:
		return proto.Marshal(m)
	}

	return nil, fmt.Errorf("cannot encode %T", v)
}

// Unmarshal keeps a rawMessage's bytes as they are and decodes anything else
// as protobuf.
func (rawCodec) Unmarshal(data []byte, v any) error {
	switch m := v.(type) {
	case *rawMessage:
		*m = append((*m)[:0], data...)

		return nil
	case proto.Message:
		return proto.Unmarshal(data, m)
	}

	return fmt.Errorf("cannot decode into %T", v)
}
