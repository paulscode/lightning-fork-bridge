package lnd

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/paulscode/lightning-fork-bridge/lndrpc/lnrpc"
)

// fakeChainKit answers ChainKit's GetBlockHash and GetBlockHeader from a
// table of headers by height, decoding requests by hand as a server without
// the bindings would.
type fakeChainKit struct {
	headers map[int64][]byte
	err     error
}

func (f *fakeChainKit) hash(req []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	_, _, n := protowire.ConsumeTag(req)
	height, _ := protowire.ConsumeVarint(req[n:])
	if _, ok := f.headers[int64(height)]; !ok {
		return nil, status.Error(codes.NotFound, "no block")
	}
	hash := make([]byte, 32)
	binary.LittleEndian.PutUint64(hash, height)

	return field1(hash), nil
}

func (f *fakeChainKit) header(req []byte) ([]byte, error) {
	_, _, n := protowire.ConsumeTag(req)
	hash, _ := protowire.ConsumeBytes(req[n:])
	h, ok := f.headers[int64(binary.LittleEndian.Uint64(hash))]
	if !ok {
		return nil, status.Error(codes.NotFound, "no header")
	}

	return field1(h), nil
}

func field1(b []byte) []byte {
	out := protowire.AppendTag(nil, 1, protowire.BytesType)

	return protowire.AppendBytes(out, b)
}

func rawHandler(fn func([]byte) ([]byte, error)) grpc.MethodHandler {
	return func(_ any, _ context.Context, dec func(any) error,
		_ grpc.UnaryServerInterceptor) (any, error) {

		var in rawMessage
		if err := dec(&in); err != nil {
			return nil, err
		}
		out, err := fn(in)
		if err != nil {
			return nil, err
		}
		m := rawMessage(out)

		return &m, nil
	}
}

// serveChain puts a Lightning server and a ChainKit one behind one real gRPC
// connection, as lnd built with chainrpc has them.
func serveChain(t *testing.T, f *fakeNode, ck *fakeChainKit) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}))
	lnrpc.RegisterLightningServer(srv, f)
	if ck != nil {
		srv.RegisterService(&grpc.ServiceDesc{
			ServiceName: "chainrpc.ChainKit",
			HandlerType: (*any)(nil),
			Methods: []grpc.MethodDesc{
				{MethodName: "GetBlockHash", Handler: rawHandler(ck.hash)},
				{MethodName: "GetBlockHeader",
					Handler: rawHandler(ck.header)},
			},
		}, ck)
	}
	go func() { _ = srv.Serve(lis) }()

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

// header is an 80 byte block header with a timestamp and bits.
func header(ts, bits uint32) []byte {
	h := make([]byte, 80)
	binary.LittleEndian.PutUint32(h[68:72], ts)
	binary.LittleEndian.PutUint32(h[72:76], bits)

	return h
}

// TestHeaderReadsTimeAndDifficulty: the chain observer carries block spacing
// across a retarget by the difficulty change, which comes from the header's
// bits, read through ChainKit. The genesis bits, 0x1d00ffff, are difficulty
// one in the familiar unit, 2^256 / (0xffff * 2^208) in this one.
func TestHeaderReadsTimeAndDifficulty(t *testing.T) {
	ctx := context.Background()
	ck := &fakeChainKit{headers: map[int64][]byte{
		0:   header(1231006505, 0x1d00ffff),
		100: header(1231006505+600*100, 0x1c7fff80),
	}}
	c := NewChain(serveChain(t, &fakeNode{info: synced}, ck))

	genesis, err := c.Header(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !genesis.Time.Equal(time.Unix(1231006505, 0)) ||
		genesis.Bits != 0x1d00ffff {

		t.Errorf("read %v and %#x", genesis.Time, genesis.Bits)
	}
	want := math.Pow(2, 48) / 0xffff
	if d := genesis.Difficulty(); math.Abs(d-want) > want*1e-12 {
		t.Errorf("difficulty %g, want %g", d, want)
	}

	// Half the target is twice the difficulty, which is all the observer
	// uses.
	later, err := c.Header(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if r := later.Difficulty() / genesis.Difficulty(); math.Abs(r-2) > 1e-9 {
		t.Errorf("half the target is %g times the difficulty, want 2", r)
	}

	// Bits that encode no positive target give no difficulty.
	if d := (Header{Bits: 0x1d80ffff}).Difficulty(); d != 0 {
		t.Errorf("a negative target gave difficulty %g", d)
	}
	if d := (Header{}).Difficulty(); d != 0 {
		t.Errorf("zero bits gave difficulty %g", d)
	}
}

// TestANodeWithoutChainKitSaysSo: a stock lnd built without chainrpc answers
// Unimplemented, which the caller reads as "no headers" and does without.
func TestANodeWithoutChainKitSaysSo(t *testing.T) {
	c := NewChain(serveChain(t, &fakeNode{info: synced}, nil))

	_, err := c.Header(context.Background(), 5)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want Unimplemented, got %v", err)
	}
}

// TestTheTipIsTheHeadersTime: spacing is measured from when blocks were mined,
// so the tip carries the best header's timestamp and whether the node is
// synced, without which the two can belong to different blocks.
func TestTheTipIsTheHeadersTime(t *testing.T) {
	c := NewChain(serveChain(t, &fakeNode{info: &lnrpc.GetInfoResponse{
		BlockHeight: 812, BestHeaderTimestamp: 1_700_000_000,
		SyncedToChain: true,
	}}, nil))

	tip, err := c.Tip(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tip.Height != 812 || !tip.Time.Equal(time.Unix(1_700_000_000, 0)) ||
		!tip.Synced {

		t.Errorf("tip %+v", tip)
	}
}

// btcd-blake2b's v2 header vectors profile_0_time_offset and
// profile_0_time_offset_disabled_selector_255, serialized.
const (
	v2OffsetInUse = "" +
		"000000a01f1e1d1c1b1a191817161514131211100f0e0d0c0b0a090807060504" +
		"0302010000112233445566778899aabbccddeeff00102030405060708090a0b0" +
		"c0d0e0f0a8913577ffff001d0df0ad0b44332211efcdab89ffeeddccbbaa9988" +
		"77665544332211005802000003001c0000000000000000000000000000000000" +
		"40d10c008967452301efcdab8967452301efcdab8967452301efcdab89674523" +
		"01efcdab"

	v2OffsetNotInUse = "" +
		"000000a01f1e1d1c1b1a191817161514131211100f0e0d0c0b0a090807060504" +
		"0302010000112233445566778899aabbccddeeff00102030405060708090a0b0" +
		"c0d0e0f000943577ffff001dffffffff44332211efcdab89ffeeddccbbaa9988" +
		"776655443322110088776655030018ff22222222222222221111111111111111" +
		"40d10c0000000000000000000000000000000000000000000000000000000000" +
		"00000000"
)

// TestAV2HeaderAddsItsTimeOffset: a BLAKE2b chain v2 header with the time
// offset flag carries its block time less the offset, and the block time is
// the two added. The headers are btcd-blake2b's wire test vectors
// profile_0_time_offset (flags 0x1c, offset 600) and
// profile_0_time_offset_disabled_selector_255 (flags 0x18, the offset not in
// use), both mined at 2000000000.
func TestAV2HeaderAddsItsTimeOffset(t *testing.T) {
	vectors := map[string]string{
		"offset in use":     v2OffsetInUse,
		"offset not in use": v2OffsetNotInUse,
	}
	ck := &fakeChainKit{headers: map[int64][]byte{}}
	var height int64
	names := map[int64]string{}
	for name, h := range vectors {
		raw, err := hex.DecodeString(h)
		if err != nil || len(raw) != 164 {
			t.Fatalf("%s: %d bytes, %v", name, len(raw), err)
		}
		ck.headers[height], names[height] = raw, name
		height++
	}
	c := NewChain(serveChain(t, &fakeNode{info: synced}, ck))

	for h, name := range names {
		got, err := c.Header(context.Background(), int32(h))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Time.Unix() != 2000000000 || got.Bits != 0x1d00ffff {
			t.Errorf("%s: read %d and %#x, want 2000000000 and 0x1d00ffff",
				name, got.Time.Unix(), got.Bits)
		}
	}

	// The offset adds as a uint32, wrapping, as the chain's nodes add it.
	raw, _ := hex.DecodeString(vectors["offset in use"])
	binary.LittleEndian.PutUint32(raw[68:72], 0xfffffff0)
	binary.LittleEndian.PutUint32(raw[104:108], 0x20)
	if at, err := headerTime(raw); err != nil || at.Unix() != 0x10 {
		t.Errorf("wrapped to %v, %v; want 16", at.Unix(), err)
	}

	// A header marked v2 that is not v2's length is not read.
	if _, err := headerTime(raw[:100]); !errors.Is(err, ErrHeader) {
		t.Errorf("a short v2 header gave %v", err)
	}
}
