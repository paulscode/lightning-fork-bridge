package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

var created = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

func rec(n byte, st swap.State) Record {
	return Record{
		Hash:              node.Hash{n},
		State:             st,
		OutgoingCLTVLimit: 40,
		Invoice:           "lnbc1fake",
		IncomingMsat:      1_000_000,
		OutgoingMsat:      3_080,
		Rate:              0.00308078,
		Spread:            0.01,
		Created:           created.Add(time.Duration(n) * time.Minute),
		Updated:           created.Add(time.Duration(n) * time.Minute),
	}
}

func open(t *testing.T, path string) *Journal {
	t.Helper()

	j, err := Open(path)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	t.Cleanup(func() { j.Close() })

	return j
}

// TestConformance runs the same expectations against every implementation, so
// the one used in tests cannot quietly behave differently from the one that
// holds money.
func TestConformance(t *testing.T) {
	ctx := context.Background()

	impls := map[string]func(t *testing.T) Store{
		"journal": func(t *testing.T) Store {
			return open(t, filepath.Join(t.TempDir(), "swaps.journal"))
		},
		"memory": func(t *testing.T) Store { return NewMemory() },
	}

	for name, make := range impls {
		t.Run(name, func(t *testing.T) {
			s := make(t)

			if _, err := s.Get(ctx, node.Hash{1}); !errors.Is(err, ErrNotFound) {
				t.Errorf("missing hash: want ErrNotFound, got %v", err)
			}

			if err := s.Put(ctx, rec(1, swap.Funded)); err != nil {
				t.Fatal(err)
			}
			got, err := s.Get(ctx, node.Hash{1})
			if err != nil {
				t.Fatal(err)
			}
			if got.State != swap.Funded || got.Invoice != "lnbc1fake" ||
				got.IncomingMsat != 1_000_000 || got.Rate != 0.00308078 {

				t.Errorf("round trip lost something: %+v", got)
			}
			if !got.Created.Equal(rec(1, swap.Funded).Created) {
				t.Errorf("created %v, want %v", got.Created,
					rec(1, swap.Funded).Created)
			}

			// A later record for the same hash replaces the earlier one.
			if err := s.Put(ctx, rec(1, swap.Paying)); err != nil {
				t.Fatal(err)
			}
			if got, _ := s.Get(ctx, node.Hash{1}); got.State != swap.Paying {
				t.Errorf("state is %v, want paying", got.State)
			}

			// Pending is every unfinished swap, oldest first.
			for _, r := range []Record{
				rec(3, swap.Offered), rec(2, swap.Settled), rec(4, swap.Lost),
			} {
				if err := s.Put(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			pending, err := s.Pending(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(pending) != 2 {
				t.Fatalf("pending has %d, want 2 (settled and lost are done)",
					len(pending))
			}
			if pending[0].Hash != (node.Hash{1}) ||
				pending[1].Hash != (node.Hash{3}) {

				t.Errorf("pending out of order: %x then %x", pending[0].Hash,
					pending[1].Hash)
			}
		})
	}
}

// TestSurvivesAReopen is the property the whole package exists for.
func TestSurvivesAReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	j := open(t, path)
	for _, r := range []Record{
		rec(1, swap.Funded), rec(2, swap.Offered), rec(1, swap.Paying),
	} {
		if err := j.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	again := open(t, path)
	if again.Repaired {
		t.Error("a clean close should need no repair")
	}
	got, err := again.Get(ctx, node.Hash{1})
	if err != nil {
		t.Fatal(err)
	}
	if got.State != swap.Paying {
		t.Errorf("replay gave %v, want the latest record, paying", got.State)
	}
	pending, _ := again.Pending(ctx)
	if len(pending) != 2 {
		t.Errorf("pending has %d, want 2", len(pending))
	}
}

// TestATornTailIsDropped covers the crash this format is designed around: the
// process died part way through a write. That record was never acknowledged as
// durable, so nobody acted on it, and dropping it is correct.
func TestATornTailIsDropped(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name string
		tail string
	}{
		{"half a record", `1a2b3c4d {"hash":"aa","st`},
		{"only a checksum", "1a2b3c4d "},
		{"a single byte", "{"},
		{
			// The nastiest case: the bytes are a complete, valid,
			// correctly checksummed record and only the newline is
			// missing. It must still go, because the newline is what
			// says the write finished.
			name: "a whole record without its newline",
			tail: func() string {
				line, err := encode(rec(9, swap.Paid))
				if err != nil {
					t.Fatal(err)
				}

				return strings.TrimSuffix(string(line), "\n")
			}(),
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "swaps.journal")

			j := open(t, path)
			if err := j.Put(ctx, rec(1, swap.Funded)); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}

			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteString(c.tail); err != nil {
				t.Fatal(err)
			}
			f.Close()

			again := open(t, path)
			if !again.Repaired {
				t.Error("a torn tail should be reported, not swallowed")
			}
			if again.RepairedBytes != int64(len(c.tail)) {
				t.Errorf("repaired %d bytes, want %d", again.RepairedBytes,
					len(c.tail))
			}
			if _, err := again.Get(ctx, node.Hash{9}); !errors.Is(err,
				ErrNotFound) {

				t.Error("the torn record was kept")
			}
			if got, err := again.Get(ctx, node.Hash{1}); err != nil ||
				got.State != swap.Funded {

				t.Errorf("the complete record before it was lost: %v %v",
					got.State, err)
			}

			// And the repaired journal must be writable again, landing on
			// a record boundary rather than after the discarded bytes.
			if err := again.Put(ctx, rec(2, swap.Offered)); err != nil {
				t.Fatal(err)
			}
			if err := again.Close(); err != nil {
				t.Fatal(err)
			}
			third := open(t, path)
			if third.Repaired {
				t.Error("the repair did not stick; the tail is still torn")
			}
			if _, err := third.Get(ctx, node.Hash{2}); err != nil {
				t.Errorf("the record written after the repair is gone: %v", err)
			}
		})
	}
}

// TestDamageIsLoud is the other half. A record that is complete but wrong is
// not a crash, it is corruption: the bridge no longer knows what it did, and
// carrying on with a repaired-looking file is how it would act on a state that
// never happened.
func TestDamageIsLoud(t *testing.T) {
	ctx := context.Background()

	for _, c := range []struct {
		name   string
		damage func(data []byte) []byte
	}{
		{
			"a flipped byte in the first record",
			func(d []byte) []byte {
				out := append([]byte(nil), d...)
				// Halfway into the first record's JSON body, computed
				// rather than guessed: a fixed offset would drift into
				// the checksum prefix as the record grows and would then
				// be failing for a different reason than the name says.
				end := bytes.IndexByte(out, '\n')
				out[(9+end)/2] ^= 0x20

				return out
			},
		},
		{
			"a substituted record with a stale checksum",
			func(d []byte) []byte {
				lines := strings.SplitAfter(string(d), "\n")
				sum, body, _ := strings.Cut(lines[0], " ")
				// Valid JSON, valid state, wrong checksum: exactly what
				// the checksum is there to catch and what truncation
				// alone would not.
				swapped := strings.Replace(body, `"funded"`, `"paying"`, 1)
				lines[0] = sum + " " + swapped

				return []byte(strings.Join(lines, ""))
			},
		},
		{
			// A well-formed, correctly checksummed record naming a state
			// this build does not have: what reading a newer journal
			// after a downgrade looks like. Refusing is right, because
			// the states differ in whether recovery may cancel a claim
			// and there is no safe guess.
			"a state name this build does not have",
			func(d []byte) []byte {
				body := `{"hash":"03` + strings.Repeat("00", 31) +
					`","state":"half-paid",` +
					`"created":"2026-09-14T12:00:00Z",` +
					`"updated":"2026-09-14T12:00:00Z"}`

				return append(append([]byte(nil), d...),
					checksummed(body)...)
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "swaps.journal")

			j := open(t, path)
			for _, r := range []Record{
				rec(1, swap.Funded), rec(2, swap.Paying),
			} {
				if err := j.Put(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			j.Close()

			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, c.damage(data), 0o600); err != nil {
				t.Fatal(err)
			}

			if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("want ErrCorrupt, got %v", err)
			}
		})
	}
}

// checksummed renders arbitrary JSON as a journal line, so a test can plant a
// well-formed record rather than a bit flip.
func checksummed(body string) []byte {
	return []byte(fmt.Sprintf("%08x %s\n", crc32.ChecksumIEEE([]byte(body)),
		body))
}

// TestStateIsRecordedByName pins the decision that keeps old journals
// readable. Storing the iota value would mean that inserting a state in the
// middle of the block silently reinterprets every record already on disk, and
// the state a record is misread as decides whether recovery cancels a claim.
func TestStateIsRecordedByName(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	j := open(t, path)
	if err := j.Put(ctx, rec(1, swap.Paying)); err != nil {
		t.Fatal(err)
	}
	j.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"state":"paying"`) {
		t.Errorf("the state is not stored by name:\n%s", data)
	}

	// And every state the machine has must survive the round trip, so that
	// adding one without a name is caught here rather than on disk.
	for s := swap.Quoted; s <= swap.Lost; s++ {
		name := s.String()
		if strings.HasPrefix(name, "state(") {
			t.Errorf("%v has no name, so it cannot be stored", s)

			continue
		}
		back, err := swap.ParseState(name)
		if err != nil || back != s {
			t.Errorf("%v round tripped to %v, %v", s, back, err)
		}
	}
}

func TestValidRefusesWhatWouldMislead(t *testing.T) {
	good := rec(1, swap.Funded)
	pre := node.Preimage{7}

	for _, c := range []struct {
		name string
		r    Record
		want string
	}{
		{"a zero hash", func() Record {
			r := good
			r.Hash = node.Hash{}

			return r
		}(), "zero payment hash"},
		{"no creation time", func() Record {
			r := good
			r.Created = time.Time{}

			return r
		}(), "no creation time"},
		{"a state this build does not have", func() Record {
			r := good
			r.State = swap.State(200)

			return r
		}(), "unknown swap state"},
		{
			// The one that costs money: a record carrying a preimage that
			// settles nothing would send the driver to claim with a value
			// the node rejects, in the one state where a rejection is a
			// realised loss.
			"a preimage for a different hash", func() Record {
				r := good
				r.Preimage = pre

				return r
			}(), "does not hash to",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := c.r.Valid()
			if !errors.Is(err, ErrRecord) {
				t.Fatalf("want ErrRecord, got %v", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}

	// The matching preimage is accepted, or nothing could ever settle.
	r := good
	r.Hash = pre.Hash()
	r.Preimage = pre
	if err := r.Valid(); err != nil {
		t.Errorf("the right preimage was refused: %v", err)
	}
}

// TestStoresRefuseAnInvalidRecord checks that the guard is on the way in, not
// only in a helper callers might skip.
func TestStoresRefuseAnInvalidRecord(t *testing.T) {
	ctx := context.Background()
	bad := rec(1, swap.Funded)
	bad.Preimage = node.Preimage{7}

	j := open(t, filepath.Join(t.TempDir(), "swaps.journal"))
	if err := j.Put(ctx, bad); !errors.Is(err, ErrRecord) {
		t.Errorf("journal: want ErrRecord, got %v", err)
	}
	if err := NewMemory().Put(ctx, bad); !errors.Is(err, ErrRecord) {
		t.Errorf("memory: want ErrRecord, got %v", err)
	}
}

func TestCompact(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	j := open(t, path)
	for _, st := range []swap.State{
		swap.Quoted, swap.Offered, swap.Funded, swap.Paying, swap.Paid,
		swap.Settled,
	} {
		if err := j.Put(ctx, rec(1, st)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Put(ctx, rec(2, swap.Funded)); err != nil {
		t.Fatal(err)
	}

	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := j.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) >= len(before) {
		t.Errorf("compaction did not shrink the journal: %d then %d",
			len(before), len(after))
	}
	if n := strings.Count(string(after), "\n"); n != 2 {
		t.Errorf("compacted journal has %d records, want 2", n)
	}

	// Writes after a compaction must land in the file that will be read
	// back, not in the replaced inode.
	if err := j.Put(ctx, rec(3, swap.Offered)); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	again := open(t, path)
	if again.Repaired {
		t.Error("the compacted journal needed repair")
	}
	for _, c := range []struct {
		n    byte
		want swap.State
	}{{1, swap.Settled}, {2, swap.Funded}, {3, swap.Offered}} {
		got, err := again.Get(ctx, node.Hash{c.n})
		if err != nil {
			t.Fatalf("%x: %v", c.n, err)
		}
		if got.State != c.want {
			t.Errorf("%x is %v, want %v", c.n, got.State, c.want)
		}
	}
}

func TestConcurrentPutsAreSerialised(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")
	j := open(t, path)

	var wg sync.WaitGroup
	for i := 1; i <= 32; i++ {
		wg.Add(1)
		go func(n byte) {
			defer wg.Done()
			if err := j.Put(ctx, rec(n, swap.Funded)); err != nil {
				t.Error(err)
			}
		}(byte(i))
	}
	wg.Wait()
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Interleaved appends would leave records spliced into each other, and
	// replay is where that would show up.
	again := open(t, path)
	if again.Repaired {
		t.Fatal("concurrent writes tore the journal")
	}
	pending, err := again.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 32 {
		t.Errorf("replayed %d records, want 32", len(pending))
	}
}

func TestClosedContextIsRefused(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	j := open(t, filepath.Join(t.TempDir(), "swaps.journal"))
	if err := j.Put(ctx, rec(1, swap.Funded)); !errors.Is(err,
		context.Canceled) {

		t.Errorf("put: %v", err)
	}
	if _, err := j.Get(ctx, node.Hash{1}); !errors.Is(err, context.Canceled) {
		t.Errorf("get: %v", err)
	}
	if _, err := j.Pending(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("pending: %v", err)
	}
}

// TestAFailedWriteIsWoundBack covers the case a torn tail does not: bytes
// reach the file, the write reports failure, and the journal keeps running.
// Left in place they would sit in the middle of the file once the next record
// was appended, and damage in the middle refuses to open at all, turning one
// lost record into an unreadable journal.
func TestAFailedWriteIsWoundBack(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	j := open(t, path)
	if err := j.Put(ctx, rec(1, swap.Funded)); err != nil {
		t.Fatal(err)
	}

	// Half a record lands and the write fails.
	partial, err := encode(rec(2, swap.Paying))
	if err != nil {
		t.Fatal(err)
	}
	n, err := j.file.Write(partial[:len(partial)/2])
	if err != nil {
		t.Fatal(err)
	}
	j.rollback(int64(n))

	// The journal is still usable and still lands on a record boundary.
	if err := j.Put(ctx, rec(3, swap.Offered)); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	again := open(t, path)
	if again.Repaired {
		t.Error("the wind-back left a torn tail behind")
	}
	if _, err := again.Get(ctx, node.Hash{2}); !errors.Is(err, ErrNotFound) {
		t.Error("the half-written record survived")
	}
	for _, n := range []byte{1, 3} {
		if _, err := again.Get(ctx, node.Hash{n}); err != nil {
			t.Errorf("record %d is gone: %v", n, err)
		}
	}
}

// TestTextFieldsCannotBreakTheFormat. The journal is line-oriented and two of
// its fields are free text, so a newline in one would split a record in half
// and the half after it would read as corruption.
func TestTextFieldsCannotBreakTheFormat(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	nasty := "line one\nline two\r\n1a2b3c4d {\"hash\":\"deadbeef\"}\n"
	r := rec(1, swap.Lost)
	r.Note = nasty
	r.Invoice = "lnbc1" + nasty

	j := open(t, path)
	if err := j.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := j.Put(ctx, rec(2, swap.Funded)); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	again := open(t, path)
	if again.Repaired {
		t.Fatal("a newline in a text field tore the journal")
	}
	got, err := again.Get(ctx, node.Hash{1})
	if err != nil {
		t.Fatal(err)
	}
	if got.Note != nasty || got.Invoice != "lnbc1"+nasty {
		t.Errorf("text did not round trip:\n%q\n%q", got.Note, got.Invoice)
	}
	if _, err := again.Get(ctx, node.Hash{2}); err != nil {
		t.Errorf("the record after it was lost: %v", err)
	}
}

// FuzzOpen feeds arbitrary bytes to the replay path.
//
// A journal is the one file an operator might restore from a backup, copy
// between machines, or find after a disk did something creative. Whatever is
// in it, opening must either work or say ErrCorrupt; a panic here takes the
// bridge down while HTLCs it no longer knows about run towards their expiry.
func FuzzOpen(f *testing.F) {
	seeds := [][]byte{
		nil,
		[]byte("\n"),
		[]byte("not a journal at all"),
		[]byte("00000000 {}\n"),
	}
	if line, err := encode(rec(1, swap.Funded)); err == nil {
		seeds = append(seeds, line, append(append([]byte(nil), line...),
			line...))
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "swaps.journal")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}

		j, err := Open(path)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("opening gave %v, want ErrCorrupt or success", err)
			}

			return
		}
		defer j.Close()

		ctx := context.Background()
		pending, err := j.Pending(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range pending {
			// Anything replay accepted must be a record the store would
			// have accepted on the way in, or the bridge would come back
			// up holding a swap it could never write again.
			if err := r.Valid(); err != nil {
				t.Fatalf("replayed an invalid record: %v", err)
			}
		}

		// A repaired journal must be usable, not merely openable.
		if err := j.Put(ctx, rec(200, swap.Offered)); err != nil {
			t.Fatalf("put after replay: %v", err)
		}
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		again, err := Open(path)
		if err != nil {
			t.Fatalf("reopening what we just wrote: %v", err)
		}
		defer again.Close()
		if again.Repaired {
			t.Fatal("a journal written by this package needed repair")
		}
		if _, err := again.Get(ctx, node.Hash{200}); err != nil {
			t.Fatalf("the record written after replay is gone: %v", err)
		}
	})
}

// TestDecodeRejectsMalformedRecords covers the replay paths a bit flip inside
// a hex field reaches: the checksum catches most damage, but a record restored
// from a backup taken mid-write, or hand-edited, can be well-checksummed and
// still not describe a swap.
func TestDecodeRejectsMalformedRecords(t *testing.T) {
	for _, c := range []struct {
		name string
		body string
	}{
		{"a hash that is not hex", `{"hash":"zz","state":"funded",` +
			`"created":"2026-09-14T12:00:00Z"}`},
		{"a hash of the wrong length", `{"hash":"aabb","state":"funded",` +
			`"created":"2026-09-14T12:00:00Z"}`},
		{"a preimage of the wrong length", `{"hash":"` +
			strings.Repeat("11", 32) + `","state":"paid","preimage":"aabb",` +
			`"created":"2026-09-14T12:00:00Z"}`},
		{"not an object at all", `["funded"]`},
	} {
		t.Run(c.name, func(t *testing.T) {
			// decode reads a line without its newline, which is how
			// replay hands them over.
			line := bytes.TrimSuffix(checksummed(c.body), []byte("\n"))
			if _, err := decode(line); !errors.Is(err, ErrCorrupt) {

				t.Errorf("want ErrCorrupt, got %v", err)
			}
		})
	}

	// And a line with no checksum prefix at all.
	if _, err := decode([]byte(`{"hash":"aa"}`)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("a line with no checksum gave %v", err)
	}
}

func TestMemoryCloses(t *testing.T) {
	if err := NewMemory().Close(); err != nil {
		t.Error(err)
	}
}

// TestCommittedCountsEverythingStillOwed. A swap merely quoted has no HTLC
// yet, but a payer can fund it at any moment and there is no chance to
// re-decide in between, so it counts. Paid does not: by then the payment has
// left and the balance already reflects it.
func TestCommittedCountsEverythingStillOwed(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	owed := []swap.State{swap.Quoted, swap.Offered, swap.Funded, swap.Paying}
	notOwed := []swap.State{
		swap.Paid, swap.Failing, swap.Aborting, swap.Expiring,
		swap.Settled, swap.Failed, swap.Aborted, swap.Expired, swap.Lost,
	}

	var n byte
	for _, st := range append(append([]swap.State{}, owed...), notOwed...) {
		n++
		r := rec(n, st)
		r.OutgoingMsat = 1000
		if err := s.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	got, err := Committed(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(len(owed)) * 1000; got != want {
		t.Errorf("committed %d, want %d", got, want)
	}

	// Every state is accounted for one way or the other, so adding one to
	// the machine without deciding which it is shows up here.
	if len(owed)+len(notOwed) != len(allStates()) {
		t.Errorf("%d states classified, the machine has %d",
			len(owed)+len(notOwed), len(allStates()))
	}
}

func allStates() []swap.State {
	var out []swap.State
	for st := swap.Quoted; st <= swap.Lost; st++ {
		out = append(out, st)
	}

	return out
}

// TestCommittedRefusesToUnderreportOnOverflow. Wrapping would report a tiny
// commitment for an enormous one, in the direction that oversells.
func TestCommittedRefusesToUnderreportOnOverflow(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	for _, n := range []byte{1, 2} {
		r := rec(n, swap.Funded)
		r.OutgoingMsat = ^uint64(0) - 10
		if err := s.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := Committed(ctx, s); !errors.Is(err, ErrRecord) {
		t.Errorf("want ErrRecord, got %v", err)
	}
}

// TestBindingsThatWouldOverstateTheAssurance. A record that claims the
// stronger arrangement while describing the weaker one is the most expensive
// mistake available: everything downstream would trust it more than it earned.
func TestBindingsThatWouldOverstateTheAssurance(t *testing.T) {
	pre := node.Preimage{0x11}
	other := node.Hash{0x99}

	base := func() Record {
		r := rec(1, swap.Funded)
		r.Hash = pre.Hash()

		return r
	}

	for _, c := range []struct {
		name string
		with func(r *Record)
		want string
	}{
		{
			// Sharing a hash is the whole of the hash-linked security. A
			// record naming a different outgoing hash has no binding at all.
			"hash-linked while paying a different hash",
			func(r *Record) {
				r.Binding = swap.HashLinked
				r.OutgoingHash = other
			},
			"cannot pay",
		},
		{
			"sequenced with no outgoing hash",
			func(r *Record) {
				r.Binding = swap.Sequenced
				r.Preimage = pre
			},
			"needs the outgoing hash",
		},
		{
			// Recording the stronger arrangement as the weaker one is
			// harmless. The reverse is not, so it is refused rather than
			// quietly corrected.
			"sequenced whose legs actually share a hash",
			func(r *Record) {
				r.Binding = swap.Sequenced
				r.Preimage = pre
				r.OutgoingHash = r.Hash
			},
			"is hash-linked and must say so",
		},
		{
			// There is no later moment at which it arrives.
			"sequenced with no preimage to settle with",
			func(r *Record) {
				r.Binding = swap.Sequenced
				r.OutgoingHash = other
			},
			"before it accepts money",
		},
		{"a binding this build does not have",
			func(r *Record) { r.Binding = swap.Binding(77) },
			"unknown swap binding"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := base()
			c.with(&r)
			err := r.Valid()
			if !errors.Is(err, ErrRecord) {
				t.Fatalf("want ErrRecord, got %v", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q does not mention %q", err, c.want)
			}
		})
	}

	// Both well-formed shapes are accepted.
	linked := base()
	if err := linked.Valid(); err != nil {
		t.Errorf("a hash-linked swap was refused: %v", err)
	}
	if got := linked.Outgoing(); got != linked.Hash {
		t.Errorf("a hash-linked swap pays %x, want its own hash %x", got,
			linked.Hash)
	}

	seq := base()
	seq.Binding = swap.Sequenced
	seq.Preimage = pre
	seq.OutgoingHash = other
	if err := seq.Valid(); err != nil {
		t.Errorf("a sequenced swap was refused: %v", err)
	}
	if got := seq.Outgoing(); got != other {
		t.Errorf("a sequenced swap pays %x, want %x", got, other)
	}
}

// TestTheNewFieldsSurviveTheJournal, and a record written before bindings
// existed still reads. Old records named no binding, and they are all
// hash-linked, which is the arrangement that was possible at the time.
func TestTheNewFieldsSurviveTheJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")
	pre := node.Preimage{0x11}
	outPre := node.Preimage{0x22}
	// The outgoing hash is the one that preimage settles, because the record
	// now checks that and a fixture that disagreed was describing a payment
	// nobody made.
	outHash := outPre.Hash()

	r := rec(1, swap.Paid)
	r.Hash = pre.Hash()
	r.Binding = swap.Sequenced
	r.Preimage = pre
	r.OutgoingHash = outHash
	r.OutgoingPreimage = outPre

	j := open(t, path)
	if err := j.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	again := open(t, path)
	got, err := again.Get(ctx, r.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if got.Binding != swap.Sequenced || got.OutgoingHash != outHash ||
		got.OutgoingPreimage != outPre || got.Preimage != pre {

		t.Errorf("round trip lost something: %+v", got)
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}

	// A record from before bindings: no binding field at all.
	old := `{"hash":"` + strings.Repeat("aa", 32) + `","state":"funded",` +
		`"created":"2026-09-14T12:00:00Z","updated":"2026-09-14T12:00:00Z"}`
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(checksummed(old)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	third, err := Open(path)
	if err != nil {
		t.Fatalf("a record from before bindings would not replay: %v", err)
	}
	defer third.Close()

	var oldHash node.Hash
	for i := range oldHash {
		oldHash[i] = 0xaa
	}
	back, err := third.Get(ctx, oldHash)
	if err != nil {
		t.Fatal(err)
	}
	if back.Binding != swap.HashLinked {
		t.Errorf("an old record read as %v, want hash-linked", back.Binding)
	}
}

// TestAnOutgoingPreimageForSomeOtherPayment is refused. Evidence nobody checks
// is not evidence: for a hash-linked swap the incoming check already covers it,
// but for a sequenced one nothing else would ever catch a node handing back a
// preimage belonging to a different payment.
func TestAnOutgoingPreimageForSomeOtherPayment(t *testing.T) {
	ours := node.Preimage{0x11}
	theirs := node.Preimage{0x22}
	stranger := node.Preimage{0x33}

	seq := rec(1, swap.Paid)
	seq.Hash = ours.Hash()
	seq.Binding = swap.Sequenced
	seq.Preimage = ours
	seq.OutgoingHash = theirs.Hash()

	// The right one is accepted.
	good := seq
	good.OutgoingPreimage = theirs
	if err := good.Valid(); err != nil {
		t.Errorf("the correct outgoing preimage was refused: %v", err)
	}

	// One for a different payment is not.
	bad := seq
	bad.OutgoingPreimage = stranger
	err := bad.Valid()
	if !errors.Is(err, ErrRecord) {
		t.Fatalf("want ErrRecord, got %v", err)
	}
	if !strings.Contains(err.Error(), "not the") {
		t.Errorf("error %q does not say which payment it settles", err)
	}

	// And the same for a hash-linked swap, where the outgoing preimage is
	// the incoming one.
	linked := rec(2, swap.Paid)
	linked.Hash = ours.Hash()
	linked.Preimage = ours
	linked.OutgoingPreimage = stranger
	if err := linked.Valid(); !errors.Is(err, ErrRecord) {
		t.Errorf("hash-linked: want ErrRecord, got %v", err)
	}
}

// TestTheParticipantSurvivesTheJournal: per-participant limits are counted
// from the journal, so a restart, or a compaction, must not lose who asked.
func TestTheParticipantSurvivesTheJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	j := open(t, path)
	r := rec(1, swap.Offered)
	r.Participant = "1a2b3c"
	if err := j.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	operator := rec(2, swap.Offered)
	if err := j.Put(ctx, operator); err != nil {
		t.Fatal(err)
	}
	if err := j.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j = open(t, path)
	defer j.Close()
	got, err := j.Get(ctx, r.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if got.Participant != "1a2b3c" {
		t.Errorf("participant %q after a reopen", got.Participant)
	}
	got, err = j.Get(ctx, operator.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if got.Participant != "" {
		t.Errorf("the operator's swap came back as %q", got.Participant)
	}
}

// TestCommittedWhereCountsOnlyWhatItIsAsked: per direction, the two chains'
// amounts are never summed into one number.
func TestCommittedWhereCountsOnlyWhatItIsAsked(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()

	a := rec(1, swap.Offered)
	a.OutgoingMsat = 100
	a.Note = "bitcoin"
	b := rec(2, swap.Funded)
	b.OutgoingMsat = 7_000
	b.Note = "blake2b"
	for _, r := range []Record{a, b} {
		if err := m.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	only := func(note string) func(Record) bool {
		return func(r Record) bool { return r.Note == note }
	}
	for note, want := range map[string]uint64{"bitcoin": 100,
		"blake2b": 7_000} {

		got, err := CommittedWhere(ctx, m, only(note))
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: %d, want %d", note, got, want)
		}
	}
	all, err := Committed(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if all != 7_100 {
		t.Errorf("unfiltered %d, want 7100", all)
	}
}

// TestWhereFindsFinishedSwapsToo: a swap that ended lost is terminal, so
// Pending never shows it, and it is exactly the one an operator must see. It
// survives compaction too, which keeps the latest record of every swap.
func TestWhereFindsFinishedSwapsToo(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")
	j := open(t, path)

	for i, st := range []swap.State{swap.Lost, swap.Settled, swap.Funded} {
		if err := j.Put(ctx, rec(byte(i+1), st)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Compact(ctx); err != nil {
		t.Fatal(err)
	}

	lost, err := j.Where(ctx, func(r Record) bool {
		return r.State == swap.Lost
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 1 || lost[0].Hash != (node.Hash{1}) {
		t.Errorf("found %v, wanted the one lost swap", lost)
	}
}

// TestAJournalHasOneWriter: a second Open of a journal that is open is
// refused, in this process as in another, and allowed once the first is
// closed. Two writers would each replay the file once and append without
// seeing the other's records. The lock is on a file beside the journal, so it
// survives Compact renaming a new journal over the old one.
func TestAJournalHasOneWriter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("a second open while the first is open: want ErrLocked, "+
			"got %v", err)
	}

	// Compaction installs a new file under the journal's name, and the
	// journal stays locked.
	if err := first.Put(ctx, rec(1, swap.Quoted)); err != nil {
		t.Fatal(err)
	}
	if err := first.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("a second open after compacting: want ErrLocked, got %v",
			err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatalf("the journal could not be opened after the first closed: "+
			"%v", err)
	}
	defer second.Close()
	if _, err := second.Get(ctx, rec(1, swap.Quoted).Hash); err != nil {
		t.Errorf("the record did not survive: %v", err)
	}
}

// TestCommittedCountsTheRoutingBudget: a payment leaves with its fees, so a
// swap owes its payout and its routing budget, and headroom that covered only
// the payouts would leave the last swap before the floor unroutable.
func TestCommittedCountsTheRoutingBudget(t *testing.T) {
	ctx := context.Background()
	s := NewMemory()

	for n, st := range []swap.State{swap.Offered, swap.Paying, swap.Settled} {
		r := rec(byte(n+1), st)
		r.OutgoingMsat = 1_000
		r.MaxFeeMsat = 30
		if err := s.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	got, err := Committed(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if got != 2*1_030 {
		t.Errorf("committed %d, want %d", got, 2*1_030)
	}

	// And a budget that overflows the sum is refused like a payout that
	// does.
	r := rec(9, swap.Funded)
	r.OutgoingMsat = 10
	r.MaxFeeMsat = ^uint64(0) - 5
	if err := s.Put(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := Committed(ctx, s); !errors.Is(err, ErrRecord) {
		t.Errorf("want ErrRecord, got %v", err)
	}
}

// TestNoPayAttemptSurvivesTheJournal: a swap that has never recorded Paying
// says so across a restart, and a record that does not say, one written
// before the flag existed, reads as one that may have paid.
func TestNoPayAttemptSurvivesTheJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	j := open(t, path)
	never := rec(1, swap.Aborting)
	never.NoPayAttempt = true
	maybe := rec(2, swap.Aborting)
	for _, r := range []Record{never, maybe} {
		if err := j.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if n := bytes.Count(raw, []byte(`"no_pay_attempt":true`)); n != 1 {
		t.Errorf("the flag is written %d times, want once", n)
	}

	j = open(t, path)
	defer j.Close()
	for _, c := range []struct {
		r    Record
		want bool
	}{{never, true}, {maybe, false}} {
		got, err := j.Get(ctx, c.r.Hash)
		if err != nil {
			t.Fatal(err)
		}
		if got.NoPayAttempt != c.want {
			t.Errorf("%x: no pay attempt %v after a reopen, want %v",
				c.r.Hash[:2], got.NoPayAttempt, c.want)
		}
	}
}

// TestTheInvoiceFlagsSurviveTheJournal: whether a swap ever had an invoice,
// and whether it was seen funded, decide a later quote for its hash, so both
// survive a restart.
func TestTheInvoiceFlagsSurviveTheJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "swaps.journal")

	j := open(t, path)
	none := rec(1, swap.Expired)
	none.NoInvoice = true
	seen := rec(2, swap.Expired)
	seen.HTLCSeen = true
	for _, r := range []Record{none, seen} {
		if err := j.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j = open(t, path)
	defer j.Close()
	if got, _ := j.Get(ctx, none.Hash); !got.NoInvoice || got.HTLCSeen {
		t.Errorf("no invoice %v, seen %v; want true, false", got.NoInvoice,
			got.HTLCSeen)
	}
	if got, _ := j.Get(ctx, seen.Hash); got.NoInvoice || !got.HTLCSeen {
		t.Errorf("no invoice %v, seen %v; want false, true", got.NoInvoice,
			got.HTLCSeen)
	}
}
