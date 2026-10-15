package node

import (
	"bytes"
	"testing"
)

// BOLT 11's own test vector: "please send 0.01 BTC with payment metadata
// 0x01fafaf0".
const metadataVector = "lnbc10m1pvjluezpp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqdp9wpshjmt9de6zqmt9w3skgct5vysxjmnnd9jx2mq8q8a04uqsp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygs9q2gqqqqqqsgq7hf8he7ecf7n4ffphs6awl9t6676rrclv9ckg3d3ncn7fct63p6s365duk5wrk202cfy3aj5xnnp5gs3vrdvruverwwq7yzhkf5a3xqpd05wjc"

// The same spec's "please make a donation of any amount" vector, which has no
// metadata (and a valid checksum: it is read, and has no m field).
const noMetadataVector = "lnbc1pvjluezpp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqdpl2pkx2ctnv5sxxmmwwd5kgetjypeh2ursdae8g6twvus8g6rfwvs8qun0dfjkxaq8rkx3yf5tcsyz3d73gafnh3cax9rn449d9p5uxz9ezhhypd0elx87sjle52x86fux2ypatgddc6k63n7erqz25le42c4u4ecky03ylcqca784w"

func TestInvoiceMetadata(t *testing.T) {
	if got := InvoiceMetadata(metadataVector); !bytes.Equal(got, []byte{0x01, 0xfa, 0xfa, 0xf0}) {
		t.Fatalf("metadata: got %x, want 01fafaf0", got)
	}
	// Upper case and a lightning: prefix are the same invoice.
	if got := InvoiceMetadata("LIGHTNING:" + toUpper(metadataVector)); !bytes.Equal(got, []byte{0x01, 0xfa, 0xfa, 0xf0}) {
		t.Fatalf("upper case: got %x", got)
	}
	if got := InvoiceMetadata(noMetadataVector); got != nil {
		t.Fatalf("an invoice with no m field gave %x", got)
	}
	if !validChecksum(noMetadataVector) {
		t.Fatal("the vector without metadata should parse; the test would pass for the wrong reason")
	}
}

func TestInvoiceMetadataRefusesDamage(t *testing.T) {
	// One character changed: the checksum fails and nothing is read.
	bad := []byte(metadataVector)
	bad[40] = 'q'
	if bad[40] == metadataVector[40] {
		bad[40] = 'p'
	}
	if got := InvoiceMetadata(string(bad)); got != nil {
		t.Fatalf("a damaged invoice gave %x", got)
	}
	for _, s := range []string{"", "lnbc", "lnbc1qqqq", "not an invoice"} {
		if got := InvoiceMetadata(s); got != nil {
			t.Fatalf("%q gave %x", s, got)
		}
	}
}

func validChecksum(s string) bool {
	sep := len(s) - 1
	for s[sep] != '1' {
		sep--
	}
	g := make([]byte, 0, len(s)-sep-1)
	for i := sep + 1; i < len(s); i++ {
		g = append(g, byte(indexOf(s[i])))
	}
	return bech32Polymod(append(hrpExpand(s[:sep]), g...)) == 1
}

func indexOf(c byte) int {
	for i := 0; i < len(bech32Charset); i++ {
		if bech32Charset[i] == c {
			return i
		}
	}
	return -1
}

func toUpper(s string) string { return string(bytes.ToUpper([]byte(s))) }

// TestRegroupRefusesBadPadding checks that the five-bit-to-byte step refuses
// what zpay32 refuses: padding that is not zero, or five bits or more of it.
func TestRegroupRefusesBadPadding(t *testing.T) {
	// 0x01fafaf0 as BOLT 11 writes it: 7 groups, 3 zero padding bits.
	good := []byte{0, 7, 29, 15, 21, 28, 0}
	if got := regroup(good); !bytes.Equal(got, []byte{0x01, 0xfa, 0xfa, 0xf0}) {
		t.Fatalf("got %x", got)
	}
	nonZero := append([]byte(nil), good...)
	nonZero[6] |= 1 // a padding bit set
	if got := regroup(nonZero); got != nil {
		t.Fatalf("non-zero padding gave %x", got)
	}
	// 0x01 is two groups (2 padding bits); a third group makes 7.
	if got := regroup([]byte{0, 4}); !bytes.Equal(got, []byte{0x01}) {
		t.Fatalf("0x01 gave %x", got)
	}
	if got := regroup([]byte{0, 4, 0}); got != nil {
		t.Fatalf("seven bits of padding gave %x", got)
	}
}
