package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/paulscode/lightning-fork-bridge/node"
	"github.com/paulscode/lightning-fork-bridge/swap"
)

// wire is the on-disk form, written out by hand rather than inherited from
// Record's field names.
//
// The file is a compatibility surface and an audit trail, so it is worth
// spelling out: hashes in hex because that is how every other tool prints
// them, the state by name because its number is an implementation detail, and
// times in RFC 3339 because an operator reading the file at three in the
// morning should not have to decode anything.
type wire struct {
	Hash      string    `json:"hash"`
	State     string    `json:"state"`
	Preimage  string    `json:"preimage,omitempty"`
	Binding   string    `json:"binding"`
	OutHash   string    `json:"outgoing_hash,omitempty"`
	OutPre    string    `json:"outgoing_preimage,omitempty"`
	CLTVLimit uint32    `json:"outgoing_cltv_limit,omitempty"`
	Invoice   string    `json:"invoice,omitempty"`
	InMsat    uint64    `json:"incoming_msat,omitempty"`
	OutMsat   uint64    `json:"outgoing_msat,omitempty"`
	MaxFee    uint64    `json:"max_fee_msat,omitempty"`
	Expires   time.Time `json:"quote_expiry,omitempty"`
	Rate      float64   `json:"rate,omitempty"`
	Spread    float64   `json:"spread,omitempty"`
	Created   time.Time `json:"created"`
	Updated   time.Time `json:"updated"`
	Note      string    `json:"note,omitempty"`
	Who       string    `json:"participant,omitempty"`
}

func toWire(r Record) wire {
	w := wire{
		Hash:      hex.EncodeToString(r.Hash[:]),
		State:     r.State.String(),
		Binding:   r.Binding.String(),
		CLTVLimit: r.OutgoingCLTVLimit,
		Invoice:   r.Invoice,
		InMsat:    r.IncomingMsat,
		OutMsat:   r.OutgoingMsat,
		MaxFee:    r.MaxFeeMsat,
		Expires:   r.QuoteExpiry,
		Rate:      r.Rate,
		Spread:    r.Spread,
		Created:   r.Created,
		Updated:   r.Updated,
		Note:      r.Note,
		Who:       r.Participant,
	}
	var zeroPre node.Preimage
	if r.Preimage != zeroPre {
		w.Preimage = hex.EncodeToString(r.Preimage[:])
	}
	if r.OutgoingPreimage != zeroPre {
		w.OutPre = hex.EncodeToString(r.OutgoingPreimage[:])
	}
	var zeroHash node.Hash
	if r.OutgoingHash != zeroHash {
		w.OutHash = hex.EncodeToString(r.OutgoingHash[:])
	}

	return w
}

func (w wire) record() (Record, error) {
	var r Record

	h, err := hex.DecodeString(w.Hash)
	if err != nil || len(h) != len(r.Hash) {
		return r, fmt.Errorf("%w: bad hash %q", ErrCorrupt, w.Hash)
	}
	copy(r.Hash[:], h)

	st, err := swap.ParseState(w.State)
	if err != nil {
		return r, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	r.State = st

	if w.Preimage != "" {
		p, err := hex.DecodeString(w.Preimage)
		if err != nil || len(p) != len(r.Preimage) {
			return r, fmt.Errorf("%w: bad preimage for %s", ErrCorrupt, w.Hash)
		}
		copy(r.Preimage[:], p)
	}
	if w.OutPre != "" {
		p, err := hex.DecodeString(w.OutPre)
		if err != nil || len(p) != len(r.OutgoingPreimage) {
			return r, fmt.Errorf("%w: bad outgoing preimage for %s",
				ErrCorrupt, w.Hash)
		}
		copy(r.OutgoingPreimage[:], p)
	}
	if w.OutHash != "" {
		h, err := hex.DecodeString(w.OutHash)
		if err != nil || len(h) != len(r.OutgoingHash) {
			return r, fmt.Errorf("%w: bad outgoing hash for %s", ErrCorrupt,
				w.Hash)
		}
		copy(r.OutgoingHash[:], h)
	}

	// Records written before bindings existed named no binding, and the
	// empty string is not a binding. They are all hash-linked, which is the
	// arrangement that was possible at the time, and reading them as the
	// weaker one would trust them less rather than more.
	if w.Binding == "" {
		r.Binding = swap.HashLinked
	} else {
		b, err := swap.ParseBinding(w.Binding)
		if err != nil {
			return r, fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		r.Binding = b
	}

	r.OutgoingCLTVLimit = w.CLTVLimit
	r.Invoice = w.Invoice
	r.IncomingMsat = w.InMsat
	r.OutgoingMsat = w.OutMsat
	r.MaxFeeMsat = w.MaxFee
	r.QuoteExpiry = w.Expires
	r.Rate = w.Rate
	r.Spread = w.Spread
	r.Created = w.Created
	r.Updated = w.Updated
	r.Note = w.Note
	r.Participant = w.Who

	return r, nil
}

// Journal is an append-only record of every swap, fsynced per write.
//
// Append-only rather than update-in-place because the history is worth having:
// when a swap ends in swap.Lost, the sequence of states it went through is the
// evidence for which assumption failed, and an overwritten row would have
// destroyed it. Compact discards superseded records, keeping each swap's
// latest.
type Journal struct {
	mu    sync.Mutex
	path  string
	file  *os.File
	index map[node.Hash]Record

	// lock holds the exclusive lock on path+".lock" for as long as the
	// journal is open. Nil where the platform has no flock.
	lock *os.File

	// size is the length of the file up to the last record known good. A
	// write that fails part way through is rolled back to it.
	size int64

	// Repaired records that a torn record was dropped from the tail at open,
	// which means the process died mid-write, and RepairedBytes how much was
	// dropped. Nothing was lost that had been reported durable, but it is
	// worth an operator knowing it happened: a caller that opens a journal
	// should log a warning when it is set, since this package does not log.
	Repaired      bool
	RepairedBytes int64
}

// LockSuffix names the file next to a journal that Open locks: the journal's
// path with this appended.
//
// A separate file, because Compact installs a new journal by renaming over
// the old one, and a lock on the journal's own descriptor would stay with the
// replaced inode while a second process opened the new one unopposed.
const LockSuffix = ".lock"

// Open opens or creates a journal and replays it.
//
// It refuses with ErrLocked while another Journal has the same path open, in
// this process or any other, until that one is closed. Two writers would each
// replay the file once and then append without seeing the other's records.
func Open(path string) (*Journal, error) {
	lock, err := lockFile(path + LockSuffix)
	if err != nil {
		return nil, err
	}

	// O_APPEND so that every write lands at the end whatever the offset is.
	// Replay seeks around to read, and an append-only descriptor means a
	// missed seek can corrupt a record rather than merely misread one.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		if lock != nil {
			lock.Close()
		}

		return nil, fmt.Errorf("opening journal: %w", err)
	}

	j := &Journal{path: path, file: f, index: map[node.Hash]Record{},
		lock: lock}
	if err := j.replay(); err != nil {
		f.Close()
		if lock != nil {
			lock.Close()
		}

		return nil, err
	}

	return j, nil
}

// replay rebuilds the index and repairs a torn tail.
func (j *Journal) replay() error {
	if _, err := j.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding journal: %w", err)
	}
	data, err := io.ReadAll(j.file)
	if err != nil {
		return fmt.Errorf("reading journal: %w", err)
	}

	// Every complete record ends in a newline. Whatever follows the last one
	// is a write that did not finish, and it is the only damage tolerated
	// here: it was never acknowledged as durable, so nobody acted on it.
	lines := bytes.Split(data, []byte{'\n'})
	tail := lines[len(lines)-1]
	lines = lines[:len(lines)-1]

	for i, ln := range lines {
		if len(ln) == 0 {
			continue
		}
		r, err := decode(ln)
		if err != nil {
			// A damaged record with good records after it is not a crash,
			// it is corruption, and the bridge no longer knows what it did.
			return fmt.Errorf("%w: line %d of %s: %w", ErrCorrupt, i+1,
				j.path, err)
		}
		j.index[r.Hash] = r
	}

	j.size = int64(len(data) - len(tail))
	if len(tail) > 0 {
		if err := j.file.Truncate(j.size); err != nil {
			return fmt.Errorf("truncating a torn journal tail: %w", err)
		}
		if err := j.file.Sync(); err != nil {
			return fmt.Errorf("syncing a truncated journal: %w", err)
		}
		j.Repaired = true
		j.RepairedBytes = int64(len(tail))
	}

	return nil
}

// encode renders a record as one checksummed line.
func encode(r Record) ([]byte, error) {
	body, err := json.Marshal(toWire(r))
	if err != nil {
		return nil, fmt.Errorf("encoding record: %w", err)
	}
	// A checksum catches the damage a truncated write does not: a record
	// whose bytes were replaced rather than lost still parses as JSON.
	sum := crc32.ChecksumIEEE(body)
	line := make([]byte, 0, 9+len(body)+1)
	line = append(line, fmt.Sprintf("%08x ", sum)...)
	line = append(line, body...)
	line = append(line, '\n')

	return line, nil
}

// decode reads one line, without its newline. Every failure is ErrCorrupt:
// from here on there is no distinction worth drawing between damage and
// nonsense, because neither says what the bridge did.
func decode(line []byte) (Record, error) {
	sp := bytes.IndexByte(line, ' ')
	if sp != 8 {
		return Record{}, fmt.Errorf("%w: no checksum prefix", ErrCorrupt)
	}
	body := line[sp+1:]
	if want := fmt.Sprintf("%08x", crc32.ChecksumIEEE(body)); want !=
		string(line[:8]) {

		return Record{}, fmt.Errorf("%w: checksum mismatch, line says %s "+
			"and its contents give %s", ErrCorrupt, line[:8], want)
	}

	var w wire
	if err := json.Unmarshal(body, &w); err != nil {
		return Record{}, fmt.Errorf("%w: decoding record: %w", ErrCorrupt, err)
	}

	return w.record()
}

// Put appends a record and returns once it is on the platter.
func (j *Journal) Put(ctx context.Context, r Record) error {
	if err := r.Valid(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	line, err := encode(r)
	if err != nil {
		return err
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	n, err := j.file.Write(line)
	if err != nil {
		// A short write leaves a partial record in the file and reports a
		// failure, so the next append would land after it and put damage
		// in the middle, which refuses to open at all. Wind it back: this
		// record was never acknowledged, so losing it is right, but the
		// ones before it must stay readable.
		j.rollback(int64(n))

		return fmt.Errorf("appending to the journal: %w", err)
	}
	// Without this the write sits in the page cache and a power cut loses a
	// record the caller was told was durable, which is the whole failure this
	// package exists to prevent.
	if err := j.file.Sync(); err != nil {
		j.rollback(int64(n))

		return fmt.Errorf("syncing the journal: %w", err)
	}

	j.size += int64(n)
	j.index[r.Hash] = r

	return nil
}

// rollback discards bytes written for a record that failed, leaving the file
// ending on a record boundary. It is best effort: if the truncate itself
// fails there is nothing further to try, and replay's torn-tail handling is
// the remaining defence.
func (j *Journal) rollback(wrote int64) {
	if wrote <= 0 {
		return
	}
	if err := j.file.Truncate(j.size); err == nil {
		j.file.Sync()
	}
}

// Get returns the current record for a hash.
func (j *Journal) Get(ctx context.Context, hash node.Hash) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	r, ok := j.index[hash]
	if !ok {
		return Record{}, fmt.Errorf("%w: %x", ErrNotFound, hash)
	}

	return r, nil
}

// Pending returns every non-terminal record, oldest first.
func (j *Journal) Pending(ctx context.Context) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	var out []Record
	for _, r := range j.index {
		if !r.State.Terminal() {
			out = append(out, r)
		}
	}
	// Oldest first, so recovery works through the swaps closest to their
	// expiry before the ones with time left. Hash breaks ties because map
	// iteration order is random and a recovery that runs in a different order
	// each time is hard to reason about after the fact.
	sort.Slice(out, func(a, b int) bool {
		if !out[a].Created.Equal(out[b].Created) {
			return out[a].Created.Before(out[b].Created)
		}

		return bytes.Compare(out[a].Hash[:], out[b].Hash[:]) < 0
	})

	return out, nil
}

// Where returns every current record keep accepts, finished or not, oldest
// first. It is for the questions Pending cannot answer, such as which swaps
// ended in a state only an operator can resolve.
func (j *Journal) Where(ctx context.Context, keep func(Record) bool) ([]Record,
	error) {

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	var out []Record
	for _, r := range j.index {
		if keep(r) {
			out = append(out, r)
		}
	}
	sortRecords(out)

	return out, nil
}

// sortRecords orders records oldest first, by hash where they tie.
func sortRecords(out []Record) {
	sort.Slice(out, func(a, b int) bool {
		if !out[a].Created.Equal(out[b].Created) {
			return out[a].Created.Before(out[b].Created)
		}

		return bytes.Compare(out[a].Hash[:], out[b].Hash[:]) < 0
	})
}

// Compact rewrites the journal with one record per hash: each swap's latest,
// finished or not, without the states it passed through on the way.
//
// Finished swaps are kept on purpose. A quote for a hash that was swapped
// before is decided by how that swap ended, and a record dropped here would
// leave only the node's word for it.
//
// It writes a new file and renames it over the old one, so a crash part way
// through leaves the original intact: the journal is either wholly the old
// file or wholly the new one.
func (j *Journal) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	tmp, err := os.CreateTemp(filepath.Dir(j.path),
		filepath.Base(j.path)+".compact-*")
	if err != nil {
		return fmt.Errorf("creating a compaction file: %w", err)
	}
	defer os.Remove(tmp.Name())

	records := make([]Record, 0, len(j.index))
	for _, r := range j.index {
		records = append(records, r)
	}
	sort.Slice(records, func(a, b int) bool {
		if !records[a].Created.Equal(records[b].Created) {
			return records[a].Created.Before(records[b].Created)
		}

		return bytes.Compare(records[a].Hash[:], records[b].Hash[:]) < 0
	})

	for _, r := range records {
		line, err := encode(r)
		if err != nil {
			tmp.Close()

			return err
		}
		if _, err := tmp.Write(line); err != nil {
			tmp.Close()

			return fmt.Errorf("writing the compaction file: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()

		return fmt.Errorf("syncing the compaction file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing the compaction file: %w", err)
	}
	written, err := os.Stat(tmp.Name())
	if err != nil {
		return fmt.Errorf("sizing the compaction file: %w", err)
	}
	if err := os.Rename(tmp.Name(), j.path); err != nil {
		return fmt.Errorf("installing the compacted journal: %w", err)
	}
	j.size = written.Size()
	// The rename itself has to reach the platter, or a power cut can leave
	// the directory pointing at neither file.
	if dir, err := os.Open(filepath.Dir(j.path)); err == nil {
		dir.Sync()
		dir.Close()
	}

	// Reopen: the old descriptor still refers to the replaced inode, so
	// appending to it would write to a file nothing will ever read.
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("closing the old journal: %w", err)
	}
	f, err := os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopening the compacted journal: %w", err)
	}
	j.file = f

	return nil
}

// Close closes the journal and releases its lock.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	err := j.file.Close()
	if j.lock != nil {
		// After the journal itself, so that nothing else can open it
		// while this one still might write.
		if lerr := j.lock.Close(); err == nil {
			err = lerr
		}
		j.lock = nil
	}

	return err
}
