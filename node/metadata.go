package node

import "strings"

// MaxMetadata is the most payment metadata the bridge carries over: what a
// single BOLT 11 tagged field can hold (1023 five-bit groups), in bytes.
const MaxMetadata = 1023 * 5 / 8

// InvoiceMetadata returns the payment metadata (BOLT 11 field m) of invoice,
// or nil if it has none or does not parse.
//
// The bridge copies it onto the hold invoice so a payer reading the hold
// invoice sees what the destination put there (a recipient can use it to say
// it also takes payments on the other chain). It is read here rather than
// asked of a node because the stock LND on Bitcoin does not return it, and
// because it is carried over opaque: nothing the bridge decides, the hash, the
// amount, the CLTV or the expiry, comes from it. Those are still the paying
// node's own reading of the invoice.
//
// The checksum is verified, so a mangled invoice yields nothing rather than
// garbage, and a field longer than MaxMetadata is refused.
func InvoiceMetadata(invoice string) []byte {
	s := strings.ToLower(strings.TrimSpace(invoice))
	s = strings.TrimPrefix(s, "lightning:")
	sep := strings.LastIndexByte(s, '1')
	if sep < 1 || len(s)-sep-1 < 7+104+6 {
		return nil
	}
	hrp, data := s[:sep], s[sep+1:]
	groups := make([]byte, len(data))
	for i := 0; i < len(data); i++ {
		v := strings.IndexByte(bech32Charset, data[i])
		if v < 0 {
			return nil
		}
		groups[i] = byte(v)
	}
	if bech32Polymod(append(hrpExpand(hrp), groups...)) != 1 {
		return nil
	}

	// After the timestamp (7 groups) come the tagged fields; the signature
	// (104 groups) and the checksum (6) close the invoice.
	fields := groups[7 : len(groups)-104-6]
	for len(fields) >= 3 {
		tag := fields[0]
		n := int(fields[1])<<5 | int(fields[2])
		if 3+n > len(fields) {
			return nil
		}
		if tag == fieldTypeM {
			m := regroup(fields[3 : 3+n])
			if len(m) == 0 || len(m) > MaxMetadata {
				return nil
			}
			return m
		}
		fields = fields[3+n:]
	}
	return nil
}

// fieldTypeM is the BOLT 11 tag of the payment metadata field.
const fieldTypeM = 27

const bech32Charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

func hrpExpand(hrp string) []byte {
	out := make([]byte, 0, len(hrp)*2+1)
	for i := 0; i < len(hrp); i++ {
		out = append(out, hrp[i]>>5)
	}
	out = append(out, 0)
	for i := 0; i < len(hrp); i++ {
		out = append(out, hrp[i]&31)
	}
	return out
}

func bech32Polymod(values []byte) uint32 {
	gen := [5]uint32{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	chk := uint32(1)
	for _, v := range values {
		top := chk >> 25
		chk = (chk&0x1ffffff)<<5 ^ uint32(v)
		for i := 0; i < 5; i++ {
			if (top>>i)&1 == 1 {
				chk ^= gen[i]
			}
		}
	}
	return chk
}

// regroup turns five-bit groups into bytes, dropping the padding bits at the
// end as BOLT 11 does for byte fields. Like zpay32 (bech32.ConvertBits without
// padding), it refuses padding of five bits or more, or padding that is not
// zero, returning nil, so the two parsers agree on what a field holds.
func regroup(groups []byte) []byte {
	var out []byte
	acc, bits := uint32(0), uint(0)
	for _, g := range groups {
		acc = acc<<5 | uint32(g)
		bits += 5
		for bits >= 8 {
			bits -= 8
			out = append(out, byte(acc>>bits))
		}
	}
	if bits >= 5 || acc&(1<<bits-1) != 0 {
		return nil
	}
	return out
}
