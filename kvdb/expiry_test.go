package kvdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// clock is a controllable time source.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }
func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func openClock(t *testing.T, path string, c *clock) *Store {
	t.Helper()
	s, err := Open(path, Options{Now: c.Now})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestExpiryHidesKeyEverywhere(t *testing.T) {
	c := newClock()
	s := openClock(t, filepath.Join(t.TempDir(), "db"), c)
	defer s.Close()

	s.Set("session", []byte("abc"), SetOptions{ExpireAt: c.Now().Add(10 * time.Second)})
	s.Put("forever", []byte("x"))

	if v, err := s.Get("session"); err != nil || string(v) != "abc" {
		t.Fatalf("before expiry: %q %v", v, err)
	}
	if st := s.Stats(); st.Keys != 2 || st.Expiring != 1 {
		t.Fatalf("stats: %+v", st)
	}

	c.Advance(9999 * time.Millisecond)
	if !s.Has("session") {
		t.Fatal("still valid 1ms before the deadline")
	}
	c.Advance(time.Millisecond) // exactly at the deadline: expired
	if _, err := s.Get("session"); err != ErrNotFound {
		t.Fatalf("Get after expiry: %v", err)
	}
	if s.Has("session") || s.Len() != 1 {
		t.Fatalf("Has/Len after expiry: %v %d", s.Has("session"), s.Len())
	}
	if ks := s.Keys(); len(ks) != 1 || ks[0] != "forever" {
		t.Fatalf("Keys: %v", ks)
	}
	if _, has, exists := s.ExpiresAt("session"); has || exists {
		t.Fatal("ExpiresAt on expired key")
	}
	if ok, _ := s.Remove("session"); ok {
		t.Fatal("removing an expired key reports it did not exist")
	}
	if st := s.Stats(); st.Keys != 1 || st.Expiring != 0 {
		t.Fatalf("stats after expiry: %+v", st)
	}
}

func TestExpiryPersistsAcrossReopen(t *testing.T) {
	c := newClock()
	path := filepath.Join(t.TempDir(), "db")
	s := openClock(t, path, c)
	s.Set("short", []byte("1"), SetOptions{ExpireAt: c.Now().Add(5 * time.Second)})
	s.Set("long", []byte("2"), SetOptions{ExpireAt: c.Now().Add(time.Hour)})
	s.Close()

	s = openClock(t, path, c)
	defer s.Close()
	at, has, exists := s.ExpiresAt("long")
	if !exists || !has || !at.Equal(time.UnixMilli(c.Now().Add(time.Hour).UnixMilli())) {
		t.Fatalf("deadline lost: %v %v %v", at, has, exists)
	}
	c.Advance(10 * time.Second)
	if s.Has("short") || !s.Has("long") {
		t.Fatal("expiry not honoured after reopen")
	}
	if v, _ := s.Get("long"); string(v) != "2" {
		t.Fatalf("value damaged: %q", v)
	}
}

func TestSetConditionsAndPastDeadline(t *testing.T) {
	c := newClock()
	s := openClock(t, filepath.Join(t.TempDir(), "db"), c)
	defer s.Close()

	if ok, _ := s.Set("k", []byte("1"), SetOptions{IfExists: true}); ok {
		t.Fatal("XX must not create")
	}
	if ok, _ := s.Set("k", []byte("1"), SetOptions{IfNotExists: true}); !ok {
		t.Fatal("NX must create")
	}
	if ok, _ := s.Set("k", []byte("2"), SetOptions{IfNotExists: true}); ok {
		t.Fatal("NX must not overwrite")
	}
	if v, _ := s.Get("k"); string(v) != "1" {
		t.Fatalf("NX overwrote: %q", v)
	}
	if ok, _ := s.Set("k", []byte("3"), SetOptions{IfExists: true}); !ok {
		t.Fatal("XX must overwrite")
	}
	if _, err := s.Set("k", nil, SetOptions{IfExists: true, IfNotExists: true}); err != ErrBadOptions {
		t.Fatalf("conflicting options: %v", err)
	}

	// NX succeeds once the old value has expired.
	s.Set("lock", []byte("a"), SetOptions{ExpireAt: c.Now().Add(time.Second)})
	if ok, _ := s.Set("lock", []byte("b"), SetOptions{IfNotExists: true}); ok {
		t.Fatal("lock is held")
	}
	c.Advance(2 * time.Second)
	if ok, _ := s.Set("lock", []byte("b"), SetOptions{IfNotExists: true}); !ok {
		t.Fatal("lock expired, NX should succeed")
	}

	// A deadline in the past deletes.
	s.Put("gone", []byte("x"))
	if ok, err := s.Set("gone", []byte("y"), SetOptions{ExpireAt: c.Now().Add(-time.Second)}); !ok || err != nil {
		t.Fatalf("past deadline: %v %v", ok, err)
	}
	if s.Has("gone") {
		t.Fatal("past deadline must delete")
	}

	// A plain overwrite clears an earlier expiry.
	s.Set("t", []byte("1"), SetOptions{ExpireAt: c.Now().Add(time.Second)})
	s.Put("t", []byte("2"))
	c.Advance(time.Hour)
	if v, err := s.Get("t"); err != nil || string(v) != "2" {
		t.Fatalf("overwrite should clear expiry: %q %v", v, err)
	}
}

func TestExpireAndPersist(t *testing.T) {
	c := newClock()
	s := openClock(t, filepath.Join(t.TempDir(), "db"), c)
	defer s.Close()

	if ok, _ := s.Expire("nope", c.Now().Add(time.Hour)); ok {
		t.Fatal("Expire on a missing key")
	}
	s.Put("k", []byte("value"))
	if ok, err := s.Expire("k", c.Now().Add(5*time.Second)); !ok || err != nil {
		t.Fatalf("Expire: %v %v", ok, err)
	}
	if v, _ := s.Get("k"); string(v) != "value" {
		t.Fatalf("Expire damaged the value: %q", v)
	}
	if ok, _ := s.Persist("k"); !ok {
		t.Fatal("Persist should clear the expiry")
	}
	if ok, _ := s.Persist("k"); ok {
		t.Fatal("second Persist has nothing to clear")
	}
	if ok, _ := s.Persist("nope"); ok {
		t.Fatal("Persist on a missing key")
	}
	c.Advance(time.Hour)
	if v, err := s.Get("k"); err != nil || string(v) != "value" {
		t.Fatalf("persisted key vanished: %q %v", v, err)
	}
	// An expiry in the past deletes immediately.
	if ok, _ := s.Expire("k", c.Now().Add(-time.Second)); !ok || s.Has("k") {
		t.Fatal("Expire in the past should delete")
	}
}

func TestIncrKeepsTTLAndValidates(t *testing.T) {
	c := newClock()
	s := openClock(t, filepath.Join(t.TempDir(), "db"), c)
	defer s.Close()

	s.Set("hits", []byte("41"), SetOptions{ExpireAt: c.Now().Add(10 * time.Second)})
	if n, err := s.Incr("hits", 1); n != 42 || err != nil {
		t.Fatalf("Incr: %d %v", n, err)
	}
	c.Advance(9 * time.Second)
	if !s.Has("hits") {
		t.Fatal("Incr must keep the TTL, not clear it")
	}
	c.Advance(2 * time.Second)
	if s.Has("hits") {
		t.Fatal("key should have expired on schedule")
	}
	// An expired counter restarts from zero.
	if n, _ := s.Incr("hits", 1); n != 1 {
		t.Fatalf("expired counter should restart: %d", n)
	}

	for _, bad := range []string{"abc", "+5", "007", " 5", "5 ", "1.5", ""} {
		s.Put("x", []byte(bad))
		if _, err := s.Incr("x", 1); err != ErrNotInteger {
			t.Errorf("Incr on %q: want ErrNotInteger, got %v", bad, err)
		}
	}
	s.Put("max", []byte("9223372036854775807"))
	if _, err := s.Incr("max", 1); err != ErrOverflow {
		t.Fatalf("overflow: %v", err)
	}
	s.Put("min", []byte("-9223372036854775808"))
	if _, err := s.Incr("min", -1); err != ErrOverflow {
		t.Fatalf("underflow: %v", err)
	}
	if v, _ := s.Get("max"); string(v) != "9223372036854775807" {
		t.Fatal("failed Incr must not change the value")
	}
}

func TestPurgeWritesTombstonesSoNothingResurrects(t *testing.T) {
	c := newClock()
	path := filepath.Join(t.TempDir(), "db")
	s := openClock(t, path, c)
	s.Put("a", []byte("old, no expiry"))
	s.Set("a", []byte("new"), SetOptions{ExpireAt: c.Now().Add(time.Second)})
	s.Set("b", []byte("2"), SetOptions{ExpireAt: c.Now().Add(time.Second)})
	s.Put("keep", []byte("k"))

	if n, _ := s.PurgeExpired(); n != 0 {
		t.Fatalf("nothing expired yet, purged %d", n)
	}
	c.Advance(2 * time.Second)
	n, err := s.PurgeExpired()
	if n != 2 || err != nil {
		t.Fatalf("purged %d, %v", n, err)
	}
	if s.Len() != 1 {
		t.Fatalf("len %d", s.Len())
	}
	s.Close()

	// Even with the clock rolled back, purged keys stay deleted.
	back := newClock()
	s = openClock(t, path, back)
	defer s.Close()
	if s.Has("a") || s.Has("b") || !s.Has("keep") {
		t.Fatalf("keys after reopen: %v", s.Keys())
	}
}

func TestCompactDropsExpiredAndKeepsDeadlines(t *testing.T) {
	c := newClock()
	path := filepath.Join(t.TempDir(), "db")
	s := openClock(t, path, c)
	for i := 0; i < 50; i++ {
		s.Set(fmt.Sprintf("tmp%d", i), []byte("x"), SetOptions{ExpireAt: c.Now().Add(time.Second)})
	}
	s.Set("later", []byte("L"), SetOptions{ExpireAt: c.Now().Add(time.Hour)})
	s.Put("forever", []byte("F"))
	c.Advance(2 * time.Second)

	before := s.Stats()
	cs, err := s.Compact()
	if err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	if cs.Keys != 2 || after.Keys != 2 || after.Expiring != 1 {
		t.Fatalf("compact stats %+v / %+v", cs, after)
	}
	if after.FileSize >= before.FileSize/5 || after.DeadBytes != 0 {
		t.Fatalf("expired data should be reclaimed: %d -> %d", before.FileSize, after.FileSize)
	}
	if v, _ := s.Get("later"); string(v) != "L" {
		t.Fatalf("later: %q", v)
	}
	// The surviving deadline still works after the records moved.
	c.Advance(2 * time.Hour)
	if s.Has("later") || !s.Has("forever") {
		t.Fatal("deadline lost in compaction")
	}
	s.Close()

	s = openClock(t, path, c)
	defer s.Close()
	if s.Len() != 1 || !s.Has("forever") {
		t.Fatalf("after reopen: %v", s.Keys())
	}
}

func TestBackupSkipsExpired(t *testing.T) {
	c := newClock()
	dir := t.TempDir()
	s := openClock(t, filepath.Join(dir, "db"), c)
	defer s.Close()
	s.Set("gone", []byte("x"), SetOptions{ExpireAt: c.Now().Add(time.Second)})
	s.Set("later", []byte("y"), SetOptions{ExpireAt: c.Now().Add(time.Hour)})
	c.Advance(2 * time.Second)

	if _, err := s.Backup(filepath.Join(dir, "bak")); err != nil {
		t.Fatal(err)
	}
	b := openClock(t, filepath.Join(dir, "bak"), c)
	defer b.Close()
	ks := b.Keys()
	sort.Strings(ks)
	if len(ks) != 1 || ks[0] != "later" {
		t.Fatalf("backup keys: %v", ks)
	}
	if _, has, _ := b.ExpiresAt("later"); !has {
		t.Fatal("backup lost the deadline")
	}
}

func TestInspectMarksExpiredAsNotLive(t *testing.T) {
	c := newClock()
	s := openClock(t, filepath.Join(t.TempDir(), "db"), c)
	defer s.Close()
	s.Set("k", []byte("12345"), SetOptions{ExpireAt: c.Now().Add(time.Second)})

	recs, _, _ := s.Inspect(0)
	if len(recs) != 1 || !recs[0].Live || recs[0].ValueLen != 5 {
		t.Fatalf("before expiry: %+v", recs)
	}
	c.Advance(2 * time.Second)
	recs, _, _ = s.Inspect(0)
	if recs[0].Live {
		t.Fatal("expired record should not be live")
	}
}

// Files from before expiry existed (only put/delete records) must open.
func TestOldFormatStillOpens(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(encode(flagPut, []byte("name"), []byte("vijay")))
	buf.Write(encode(flagPut, []byte("city"), []byte("chennai")))
	buf.Write(encode(flagDelete, []byte("city"), nil))
	path := filepath.Join(t.TempDir(), "old.db")
	os.WriteFile(path, buf.Bytes(), 0o644)

	s := open(t, path)
	defer s.Close()
	if v, _ := s.Get("name"); string(v) != "vijay" || s.Len() != 1 || s.Stats().Expiring != 0 {
		t.Fatalf("old file misread: %q len=%d", v, s.Len())
	}
	if st := s.Stats(); st.DeadBytes == 0 {
		t.Fatalf("stats: %+v", st)
	}
}

// A torn expiry record is dropped like any other torn record.
func TestTornExpiryRecordIsDropped(t *testing.T) {
	c := newClock()
	path := filepath.Join(t.TempDir(), "db")
	s := openClock(t, path, c)
	s.Put("good", []byte("1"))
	s.Set("bad", []byte("2"), SetOptions{ExpireAt: c.Now().Add(time.Hour)})
	s.Close()
	st, _ := os.Stat(path)
	os.Truncate(path, st.Size()-4)

	s = openClock(t, path, c)
	defer s.Close()
	if !s.Has("good") || s.Has("bad") {
		t.Fatalf("keys: %v", s.Keys())
	}
}
