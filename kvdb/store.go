// Package kvdb is a tiny append-only key-value store (Bitcask-style).
//
// On-disk record layout (little endian):
//
//	| crc32 (4) | flag (1) | keyLen (4) | valLen (4) | key | value |
//
// The CRC covers everything after the CRC field. flag is:
//
//	0  put
//	1  delete (tombstone)
//	2  put with expiry: the "value" field starts with an 8-byte unix
//	   millisecond deadline, followed by the real value
//
// The whole database is one file; every write is appended to the end. An
// in-memory map remembers where each key's latest record lives, so Get is a
// single disk read. Overwritten, deleted and expired records stay in the file
// until Compact rewrites it with only the live ones.
//
// Files written by older versions (only flags 0 and 1) open unchanged. A file
// that contains flag 2 records cannot be read by those older versions.
package kvdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	headerSize   = 13
	flagPut      = 0
	flagDelete   = 1
	flagPutExp   = 2
	expPrefix    = 8
	MaxKeySize   = 64 << 10 // 64 KB
	MaxValueSize = 64 << 20 // 64 MB
	maxBodyVal   = MaxValueSize + expPrefix

	// MaxBatchBytes bounds the total size of one PutMany call.
	MaxBatchBytes = 256 << 20

	scanOffsetBits = 44        // a SCAN cursor holds a file offset (up to 16 TiB)...
	scanEpochMask  = 1<<19 - 1 // ...and which compaction generation it belongs to
)

var (
	ErrNotFound      = errors.New("kvdb: key not found")
	ErrEmptyKey      = errors.New("kvdb: empty key")
	ErrKeyTooLarge   = errors.New("kvdb: key too large")
	ErrValueTooLarge = errors.New("kvdb: value too large")
	ErrClosed        = errors.New("kvdb: store is closed")
	ErrCorrupt       = errors.New("kvdb: record failed its checksum")
	ErrNotInteger    = errors.New("kvdb: value is not an integer")
	ErrOverflow      = errors.New("kvdb: integer overflow")
	ErrBadOptions    = errors.New("kvdb: IfNotExists and IfExists cannot both be set")
	ErrBatchTooLarge = errors.New("kvdb: batch too large")
	ErrBadBatch      = errors.New("kvdb: keys and values must have the same length")
)

// entry says where a key's current record sits in the file.
type entry struct {
	recPos   int64
	keyLen   uint32
	valLen   uint32 // length of the value itself (not counting any expiry prefix)
	expireAt int64  // unix milliseconds; 0 = never expires
}

func (e entry) prefix() int64 {
	if e.expireAt != 0 {
		return expPrefix
	}
	return 0
}
func (e entry) size() int64        { return headerSize + int64(e.keyLen) + e.prefix() + int64(e.valLen) }
func (e entry) valueOffset() int64 { return headerSize + int64(e.keyLen) + e.prefix() }
func (e entry) expiredAt(nowMs int64) bool {
	return e.expireAt != 0 && nowMs >= e.expireAt
}

// Options configures a Store.
type Options struct {
	// SyncWrites calls fsync after every write. Slow, but an acknowledged
	// write survives power loss. Without it, writes survive a process
	// crash but the last few may be lost if the OS/machine dies.
	SyncWrites bool
	// Now supplies the clock used for expiry. Defaults to time.Now; tests
	// replace it to control time.
	Now func() time.Time
}

// SetOptions controls Set.
type SetOptions struct {
	ExpireAt    time.Time // zero = never expires
	IfNotExists bool      // only write if the key does not exist (NX)
	IfExists    bool      // only write if the key already exists (XX)
}

// Stats describes the size and health of the file.
type Stats struct {
	Keys      int   // live keys
	Expiring  int   // live keys that have an expiry set
	FileSize  int64 // bytes on disk
	LiveBytes int64 // bytes used by the current record of each key
	DeadBytes int64 // bytes Compact would reclaim
}

// CompactStats reports what a compaction did.
type CompactStats struct {
	Keys        int
	BeforeBytes int64
	AfterBytes  int64
}

// RecordInfo describes one record in the file, for inspection tools.
type RecordInfo struct {
	Offset   int64
	Size     int64
	Key      string
	ValueLen uint32
	Delete   bool // a tombstone
	Live     bool // still the current, unexpired record for its key
}

// Store is safe for concurrent use.
type Store struct {
	mu        sync.RWMutex
	path      string
	f         *os.File
	index     map[string]entry
	nExpiring int   // entries in index with an expiry (expired or not)
	end       int64 // offset where the next record will be written
	liveBytes int64
	version   uint64 // bumped by every successful write; lets callers detect change
	epoch     uint64 // bumped by every compaction; part of SCAN cursors
	opts      Options
	closed    bool
}

// Open opens (or creates) the database file and rebuilds the index by
// replaying the log from the start.
func Open(path string, opts Options) (*Store, error) {
	// A leftover temp file means a compaction was interrupted before its
	// atomic rename. The original file is still intact, so just discard it.
	_ = os.Remove(path + ".compact")

	if opts.Now == nil {
		opts.Now = time.Now
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Store{path: path, f: f, index: make(map[string]entry), opts: opts}
	if err := s.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// Now returns the store's clock.
func (s *Store) Now() time.Time { return s.opts.Now() }

func (s *Store) nowMs() int64 { return s.opts.Now().UnixMilli() }

// scan reads records from r until it hits the end, the first record that is
// incomplete or fails its checksum, or until fn returns false. It calls fn for
// each good record and returns the offset just past the last record fn accepted
// (when fn returns false, the offset of the record it rejected).
func scan(r io.Reader, fn func(off int64, flag byte, key string, val []byte) bool) (goodEnd int64) {
	br := bufio.NewReader(r)
	hdr := make([]byte, headerSize)
	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			return // clean EOF or partial header
		}
		flag := hdr[4]
		kl := binary.LittleEndian.Uint32(hdr[5:])
		vl := binary.LittleEndian.Uint32(hdr[9:])
		if (flag != flagPut && flag != flagDelete && flag != flagPutExp) ||
			kl > MaxKeySize || vl > maxBodyVal || (flag == flagPutExp && vl < expPrefix) {
			return // garbage header
		}
		body := make([]byte, int(kl)+int(vl))
		if _, err := io.ReadFull(br, body); err != nil {
			return // partial body
		}
		h := crc32.NewIEEE()
		h.Write(hdr[4:])
		h.Write(body)
		if h.Sum32() != binary.LittleEndian.Uint32(hdr[0:]) {
			return // checksum mismatch
		}
		if !fn(goodEnd, flag, string(body[:kl]), body[kl:]) {
			return
		}
		goodEnd += headerSize + int64(kl) + int64(vl)
	}
}

// replay rebuilds the index and truncates any torn tail left by a crash.
func (s *Store) replay() error {
	st, err := s.f.Stat()
	if err != nil {
		return err
	}
	end := scan(io.NewSectionReader(s.f, 0, st.Size()), func(off int64, flag byte, key string, val []byte) bool {
		s.apply(off, flag, key, val)
		return true
	})
	if end != st.Size() {
		if err := s.f.Truncate(end); err != nil {
			return err
		}
	}
	s.end = end
	return nil
}

// apply updates the index and counters for a record at off. val is the raw
// value field of the record (including the expiry prefix for flagPutExp).
func (s *Store) apply(off int64, flag byte, key string, val []byte) {
	if old, ok := s.index[key]; ok {
		s.liveBytes -= old.size()
		if old.expireAt != 0 {
			s.nExpiring--
		}
		delete(s.index, key)
	}
	e := entry{recPos: off, keyLen: uint32(len(key)), valLen: uint32(len(val))}
	switch flag {
	case flagPut:
	case flagPutExp:
		e.expireAt = int64(binary.LittleEndian.Uint64(val[:expPrefix]))
		e.valLen -= expPrefix
	default: // flagDelete
		return
	}
	s.index[key] = e
	s.liveBytes += e.size()
	if e.expireAt != 0 {
		s.nExpiring++
	}
}

func encode(flag byte, key, val []byte) []byte {
	buf := make([]byte, headerSize+len(key)+len(val))
	buf[4] = flag
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[9:], uint32(len(val)))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], val)
	binary.LittleEndian.PutUint32(buf[0:], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

// readRecord reads a whole record and verifies its checksum.
func (s *Store) readRecord(e entry) ([]byte, error) {
	buf := make([]byte, e.size())
	n, err := s.f.ReadAt(buf, e.recPos)
	if n < len(buf) {
		if err == nil || err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if crc32.ChecksumIEEE(buf[4:]) != binary.LittleEndian.Uint32(buf) {
		return nil, ErrCorrupt
	}
	return buf, nil
}

// value extracts the value bytes from a record returned by readRecord.
func (e entry) value(rec []byte) []byte {
	start := e.valueOffset()
	end := start + int64(e.valLen)
	return rec[start:end:end]
}

func (s *Store) append(flag byte, key string, val []byte) (recordStart int64, err error) {
	buf := encode(flag, []byte(key), val)
	if _, err := s.f.WriteAt(buf, s.end); err != nil {
		return 0, err // s.end not advanced; next write overwrites the debris
	}
	if s.opts.SyncWrites {
		if err := s.f.Sync(); err != nil {
			return 0, err
		}
	}
	start := s.end
	s.end += int64(len(buf))
	s.version++
	return start, nil
}

// putLocked appends a put record (with an expiry if expMs is non-zero) and
// updates the index. The caller holds the write lock.
func (s *Store) putLocked(key string, value []byte, expMs int64) error {
	flag, body := byte(flagPut), value
	if expMs != 0 {
		flag = flagPutExp
		body = make([]byte, expPrefix+len(value))
		binary.LittleEndian.PutUint64(body, uint64(expMs))
		copy(body[expPrefix:], value)
	}
	start, err := s.append(flag, key, body)
	if err != nil {
		return err
	}
	s.apply(start, flag, key, body)
	return nil
}

// deleteLocked appends a tombstone and updates the index.
func (s *Store) deleteLocked(key string) error {
	start, err := s.append(flagDelete, key, nil)
	if err != nil {
		return err
	}
	s.apply(start, flagDelete, key, nil)
	return nil
}

func validate(key string, value []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > MaxKeySize:
		return ErrKeyTooLarge
	case len(value) > MaxValueSize:
		return ErrValueTooLarge
	}
	return nil
}

// Put stores value under key, replacing any previous value and expiry.
func (s *Store) Put(key string, value []byte) error {
	_, err := s.Set(key, value, SetOptions{})
	return err
}

// Set stores value under key, subject to the conditions in o. It reports
// whether the write happened (false means an IfNotExists/IfExists condition
// was not met). A deadline that has already passed deletes the key instead.
func (s *Store) Set(key string, value []byte, o SetOptions) (bool, error) {
	if err := validate(key, value); err != nil {
		return false, err
	}
	if o.IfNotExists && o.IfExists {
		return false, ErrBadOptions
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	now := s.nowMs()
	e, present := s.index[key]
	exists := present && !e.expiredAt(now)
	if (o.IfNotExists && exists) || (o.IfExists && !exists) {
		return false, nil
	}
	var expMs int64
	if !o.ExpireAt.IsZero() {
		expMs = o.ExpireAt.UnixMilli()
		if expMs <= now {
			if present {
				return true, s.deleteLocked(key)
			}
			return true, nil
		}
	}
	if err := s.putLocked(key, value, expMs); err != nil {
		return false, err
	}
	return true, nil
}

// PutMany writes several keys in one step. No other operation can see a
// partial batch, and everything is validated before anything is written, so
// an invalid entry rejects the whole call. Later entries for the same key win.
//
// The records go to disk in a single write, but a crash in the middle of that
// write can leave the first part of the batch on disk and lose the rest.
func (s *Store) PutMany(keys []string, values [][]byte) error {
	if len(keys) != len(values) {
		return ErrBadBatch
	}
	total := 0
	for i := range keys {
		if err := validate(keys[i], values[i]); err != nil {
			return err
		}
		total += headerSize + len(keys[i]) + len(values[i])
		if total > MaxBatchBytes {
			return ErrBatchTooLarge
		}
	}
	if len(keys) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	buf := make([]byte, 0, total)
	starts := make([]int64, len(keys))
	for i := range keys {
		starts[i] = s.end + int64(len(buf))
		buf = append(buf, encode(flagPut, []byte(keys[i]), values[i])...)
	}
	if _, err := s.f.WriteAt(buf, s.end); err != nil {
		return err // s.end not advanced; the debris is overwritten by the next write
	}
	if s.opts.SyncWrites {
		if err := s.f.Sync(); err != nil {
			return err
		}
	}
	s.end += int64(len(buf))
	s.version++
	for i := range keys {
		s.apply(starts[i], flagPut, keys[i], values[i])
	}
	return nil
}

// Scan returns up to about count live keys, walking the log file from the
// position encoded in cursor. Start with cursor 0; keep calling with the
// returned cursor until it comes back as 0.
//
// Guarantee: a key that exists for the whole iteration is returned at least
// once, even if other keys are written, deleted or compacted meanwhile. A key
// that is overwritten during the iteration may be returned twice; keys created
// or deleted during it may or may not appear. A page can be shorter than count
// (or empty) when it had to skip a lot of old records.
func (s *Store) Scan(cursor int64, count int) (keys []string, next int64, err error) {
	if count < 1 {
		count = 10
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	epoch := int64(s.epoch & scanEpochMask)
	var start int64
	// A cursor from before a compaction points into a file that no longer
	// exists, so begin again (duplicates are allowed, misses are not).
	if cursor > 0 && cursor>>scanOffsetBits == epoch {
		start = cursor & (1<<scanOffsetBits - 1)
	}
	if start > s.end {
		start = 0
	}
	now := s.nowMs()
	maxExamine := max(count*10, 1000)
	examined := 0
	stopAt := int64(-1)
	scan(io.NewSectionReader(s.f, start, s.end-start), func(off int64, flag byte, key string, val []byte) bool {
		abs := start + off
		if len(keys) >= count || examined >= maxExamine {
			stopAt = abs
			return false
		}
		examined++
		if flag != flagDelete {
			if e, ok := s.index[key]; ok && e.recPos == abs && !e.expiredAt(now) {
				keys = append(keys, key)
			}
		}
		return true
	})
	if stopAt < 0 {
		return keys, 0, nil
	}
	if stopAt >= 1<<scanOffsetBits {
		return keys, 0, errors.New("kvdb: file too large for SCAN")
	}
	return keys, epoch<<scanOffsetBits | stopAt, nil
}

// Get returns the value for key, or ErrNotFound. The record's checksum is
// verified on every read, so silent disk corruption surfaces as ErrCorrupt.
func (s *Store) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	e, ok := s.index[key]
	if !ok || e.expiredAt(s.nowMs()) {
		return nil, ErrNotFound
	}
	rec, err := s.readRecord(e)
	if err != nil {
		return nil, err
	}
	return e.value(rec), nil
}

// Delete removes key by appending a tombstone. Deleting a missing key is a no-op.
func (s *Store) Delete(key string) error {
	_, err := s.Remove(key)
	return err
}

// Remove deletes key and reports whether it existed (an expired key counts
// as not existing).
func (s *Store) Remove(key string) (existed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	e, ok := s.index[key]
	if !ok {
		return false, nil
	}
	existed = !e.expiredAt(s.nowMs())
	if err := s.deleteLocked(key); err != nil {
		return false, err
	}
	return existed, nil
}

// Has reports whether key exists and has not expired. It never touches the disk.
func (s *Store) Has(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.index[key]
	return ok && !e.expiredAt(s.nowMs())
}

// ExpiresAt reports when key expires. exists is false for a missing or
// expired key; hasExpiry is false for a key that never expires.
func (s *Store) ExpiresAt(key string) (at time.Time, hasExpiry, exists bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.index[key]
	if !ok || e.expiredAt(s.nowMs()) {
		return time.Time{}, false, false
	}
	if e.expireAt != 0 {
		return time.UnixMilli(e.expireAt), true, true
	}
	return time.Time{}, false, true
}

// Expire sets key to expire at the given time, keeping its value. It reports
// false if the key does not exist. A time that has already passed deletes the key.
func (s *Store) Expire(key string, at time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	now := s.nowMs()
	e, ok := s.index[key]
	if !ok || e.expiredAt(now) {
		return false, nil
	}
	expMs := at.UnixMilli()
	if expMs <= now {
		return true, s.deleteLocked(key)
	}
	rec, err := s.readRecord(e)
	if err != nil {
		return false, err
	}
	if err := s.putLocked(key, e.value(rec), expMs); err != nil {
		return false, err
	}
	return true, nil
}

// Persist removes the expiry from key. It reports false if the key does not
// exist or had no expiry.
func (s *Store) Persist(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	e, ok := s.index[key]
	if !ok || e.expireAt == 0 || e.expiredAt(s.nowMs()) {
		return false, nil
	}
	rec, err := s.readRecord(e)
	if err != nil {
		return false, err
	}
	if err := s.putLocked(key, e.value(rec), 0); err != nil {
		return false, err
	}
	return true, nil
}

// Incr adds delta to the integer stored at key (a missing key counts as 0)
// and returns the new value. It is atomic, and keeps the key's expiry.
func (s *Store) Incr(key string, delta int64) (int64, error) {
	if len(key) == 0 {
		return 0, ErrEmptyKey
	}
	if len(key) > MaxKeySize {
		return 0, ErrKeyTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	var cur, expMs int64
	if e, ok := s.index[key]; ok && !e.expiredAt(s.nowMs()) {
		rec, err := s.readRecord(e)
		if err != nil {
			return 0, err
		}
		raw := e.value(rec)
		n, perr := strconv.ParseInt(string(raw), 10, 64)
		// Reject forms like "+5", "007" or " 5" that ParseInt tolerates.
		if perr != nil || strconv.FormatInt(n, 10) != string(raw) {
			return 0, ErrNotInteger
		}
		cur, expMs = n, e.expireAt
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	cur += delta
	if err := s.putLocked(key, []byte(strconv.FormatInt(cur, 10)), expMs); err != nil {
		return 0, err
	}
	return cur, nil
}

// PurgeExpired deletes every expired key (writing tombstones) and returns how
// many there were. Expired keys are already invisible to readers; purging
// frees their memory and lets Compact reclaim their disk space.
func (s *Store) PurgeExpired() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, ErrClosed
	}
	if s.nExpiring == 0 {
		return 0, nil
	}
	now := s.nowMs()
	var dead []string
	for k, e := range s.index {
		if e.expiredAt(now) {
			dead = append(dead, k)
		}
	}
	for i, k := range dead {
		if err := s.deleteLocked(k); err != nil {
			return i, err
		}
	}
	return len(dead), nil
}

// Keys returns all live keys (unordered).
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.nowMs()
	keys := make([]string, 0, len(s.index))
	for k, e := range s.index {
		if !e.expiredAt(now) {
			keys = append(keys, k)
		}
	}
	return keys
}

// liveCounts returns the number of unexpired keys and how many of those have
// an expiry. The caller holds a lock.
func (s *Store) liveCounts() (keys, expiring int) {
	if s.nExpiring == 0 {
		return len(s.index), 0
	}
	now := s.nowMs()
	for _, e := range s.index {
		if e.expiredAt(now) {
			continue
		}
		keys++
		if e.expireAt != 0 {
			expiring++
		}
	}
	return keys, expiring
}

// Len returns the number of live keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, _ := s.liveCounts()
	return n
}

// Stats reports file size and how much of it is wasted on old records.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys, expiring := s.liveCounts()
	return Stats{
		Keys:      keys,
		Expiring:  expiring,
		FileSize:  s.end,
		LiveBytes: s.liveBytes,
		DeadBytes: s.end - s.liveBytes,
	}
}

// Inspect lists records in file order. If limit is positive only the first
// limit records are returned; total is always the full record count.
func (s *Store) Inspect(limit int) (recs []RecordInfo, total int, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, 0, ErrClosed
	}
	now := s.nowMs()
	var prevEnd int64
	scan(io.NewSectionReader(s.f, 0, s.end), func(off int64, flag byte, key string, val []byte) bool {
		total++
		size := headerSize + int64(len(key)) + int64(len(val))
		prevEnd = off + size
		if limit > 0 && len(recs) >= limit {
			return true
		}
		valueLen := uint32(len(val))
		if flag == flagPutExp {
			valueLen -= expPrefix
		}
		e, ok := s.index[key]
		recs = append(recs, RecordInfo{
			Offset:   off,
			Size:     size,
			Key:      key,
			ValueLen: valueLen,
			Delete:   flag == flagDelete,
			Live:     flag != flagDelete && ok && e.recPos == off && !e.expiredAt(now),
		})
		return true
	})
	if prevEnd != s.end {
		return recs, total, fmt.Errorf("kvdb: file changed underneath the store (scan ended at %d, expected %d)", prevEnd, s.end)
	}
	return recs, total, nil
}

// copyLive streams the current record of every live, unexpired key to w, in
// file order (so the reads are sequential), verifying each checksum on the
// way. It returns where each key lands in the output and the number of bytes
// written. The caller must hold s.mu.
func (s *Store) copyLive(w io.Writer) (map[string]entry, int64, error) {
	type item struct {
		key string
		e   entry
	}
	now := s.nowMs()
	live := make([]item, 0, len(s.index))
	for k, e := range s.index {
		if !e.expiredAt(now) {
			live = append(live, item{k, e})
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].e.recPos < live[j].e.recPos })

	newIndex := make(map[string]entry, len(live))
	var off int64
	for _, it := range live {
		rec, err := s.readRecord(it.e)
		if err != nil {
			return nil, 0, fmt.Errorf("copy: key %q: %w", it.key, err)
		}
		if _, err := w.Write(rec); err != nil {
			return nil, 0, err
		}
		ne := it.e
		ne.recPos = off
		newIndex[it.key] = ne
		off += int64(len(rec))
	}
	return newIndex, off, nil
}

// Version changes whenever data is written. Two equal values mean nothing
// was written in between.
func (s *Store) Version() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// Backup writes a consistent snapshot of the live data to dst while the
// store stays open. Readers are not blocked; writers wait until the copy is
// done. The snapshot is a normal database file: open it with Open, or copy it
// over the data file (with the server stopped) to restore.
//
// The file is written to dst+".tmp", fsynced, then renamed into place, so dst
// is either complete or absent.
func (s *Store) Backup(dst string) (bytes int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, ErrClosed
	}
	if same, err := sameFile(dst, s.path); err != nil || same {
		return 0, errors.New("kvdb: backup destination is the live database file")
	}
	tmpPath := dst + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}
	fail := func(err error) (int64, error) {
		tmp.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	w := bufio.NewWriterSize(tmp, 1<<20)
	_, n, err := s.copyLive(w)
	if err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return 0, err
	}
	if err := os.Rename(tmpPath, dst); err != nil {
		os.Remove(tmpPath)
		return 0, err
	}
	syncDir(filepath.Dir(dst))
	return n, nil
}

// sameFile reports whether a and b are the same path or the same file.
func sameFile(a, b string) (bool, error) {
	absA, err := filepath.Abs(a)
	if err != nil {
		return false, err
	}
	absB, err := filepath.Abs(b)
	if err != nil {
		return false, err
	}
	if absA == absB {
		return true, nil
	}
	ia, errA := os.Stat(absA)
	ib, errB := os.Stat(absB)
	if errA == nil && errB == nil {
		return os.SameFile(ia, ib), nil
	}
	return false, nil
}

// Compact rewrites the file keeping only the current record of each live,
// unexpired key, then atomically swaps it in. Writes and reads are blocked
// while it runs.
//
// The new file is written next to the old one and fsynced, then renamed over
// it. Rename is the commit point: a crash before it leaves the old file
// untouched, a crash after it leaves the new one. Each record's checksum is
// verified as it is copied, so compaction never launders corrupt data.
func (s *Store) Compact() (CompactStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return CompactStats{}, ErrClosed
	}
	stats := CompactStats{BeforeBytes: s.end, AfterBytes: s.end}
	stats.Keys, _ = s.liveCounts()

	tmpPath := s.path + ".compact"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return stats, err
	}
	fail := func(err error) (CompactStats, error) {
		tmp.Close()
		os.Remove(tmpPath)
		return stats, err
	}

	w := bufio.NewWriterSize(tmp, 1<<20)
	newIndex, off, err := s.copyLive(w)
	if err != nil {
		return fail(err)
	}
	if err := w.Flush(); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return stats, err
	}

	// Close the old handle first so the rename also works on Windows.
	s.f.Close()
	if err := os.Rename(tmpPath, s.path); err != nil {
		os.Remove(tmpPath)
		f, e2 := os.OpenFile(s.path, os.O_RDWR, 0o644)
		if e2 != nil {
			s.closed = true
			return stats, errors.Join(err, e2)
		}
		s.f = f
		return stats, err
	}
	syncDir(filepath.Dir(s.path))

	f, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
	if err != nil {
		s.closed = true
		return stats, err
	}
	s.f = f
	s.index = newIndex
	s.nExpiring = 0
	for _, e := range newIndex {
		if e.expireAt != 0 {
			s.nExpiring++
		}
	}
	s.end = off
	s.liveBytes = off
	s.epoch++
	stats.Keys = len(newIndex)
	stats.AfterBytes = off
	return stats, nil
}

// syncDir flushes a directory entry (best effort; unsupported on Windows).
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// Sync flushes pending writes to disk.
func (s *Store) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.f.Sync()
}

// Close syncs and closes the file.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.f.Sync(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}
