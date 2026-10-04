package kvdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func open(t *testing.T, path string) *Store {
	t.Helper()
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()

	if _, err := s.Get("a"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	s.Put("a", []byte("1"))
	s.Put("a", []byte("two")) // overwrite
	s.Put("empty", nil)
	if v, _ := s.Get("a"); string(v) != "two" {
		t.Fatalf("got %q", v)
	}
	if v, err := s.Get("empty"); err != nil || len(v) != 0 {
		t.Fatalf("empty value: %q %v", v, err)
	}
	s.Delete("a")
	if _, err := s.Get("a"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
	if err := s.Put("", []byte("x")); err != ErrEmptyKey {
		t.Fatalf("want ErrEmptyKey, got %v", err)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := open(t, path)
	for i := 0; i < 1000; i++ {
		s.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("v%d", i)))
	}
	s.Put("k5", []byte("changed"))
	s.Delete("k7")
	s.Close()

	s = open(t, path)
	defer s.Close()
	if s.Len() != 999 {
		t.Fatalf("len = %d, want 999", s.Len())
	}
	if v, _ := s.Get("k5"); string(v) != "changed" {
		t.Fatalf("k5 = %q", v)
	}
	if _, err := s.Get("k7"); err != ErrNotFound {
		t.Fatalf("k7 should be deleted")
	}
	if v, _ := s.Get("k999"); string(v) != "v999" {
		t.Fatalf("k999 = %q", v)
	}
}

// Simulate a crash mid-write: chop bytes off the end of the file.
func TestRecoversFromTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := open(t, path)
	s.Put("good", []byte("keep me"))
	s.Put("bad", []byte("this record will be torn"))
	s.Close()

	st, _ := os.Stat(path)
	os.Truncate(path, st.Size()-5)

	s = open(t, path)
	defer s.Close()
	if v, err := s.Get("good"); err != nil || string(v) != "keep me" {
		t.Fatalf("good record lost: %q %v", v, err)
	}
	if _, err := s.Get("bad"); err != ErrNotFound {
		t.Fatalf("torn record should be dropped")
	}
	// The store must be writable again right after recovery.
	if err := s.Put("after", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(t, path)
	defer s.Close()
	if v, _ := s.Get("after"); string(v) != "ok" {
		t.Fatalf("write after recovery lost")
	}
}

// Flip a byte inside the last record: the checksum must catch it.
func TestDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := open(t, path)
	s.Put("a", []byte("aaaa"))
	s.Put("b", []byte("bbbb"))
	s.Close()

	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xFF
	os.WriteFile(path, data, 0o644)

	s = open(t, path)
	defer s.Close()
	if _, err := s.Get("a"); err != nil {
		t.Fatalf("a should survive: %v", err)
	}
	if _, err := s.Get("b"); err != ErrNotFound {
		t.Fatalf("corrupt record b should be dropped")
	}
}

func TestLargeValueAndConcurrency(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()

	big := bytes.Repeat([]byte("x"), 5<<20)
	s.Put("big", big)
	if v, _ := s.Get("big"); !bytes.Equal(v, big) {
		t.Fatal("big value mismatch")
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("g%d-%d", g, i)
				s.Put(k, []byte(k))
				if v, err := s.Get(k); err != nil || string(v) != k {
					t.Errorf("concurrent get %s: %q %v", k, v, err)
				}
			}
		}(g)
	}
	wg.Wait()
	if s.Len() != 1601 {
		t.Fatalf("len = %d", s.Len())
	}
}

func TestCompactReclaimsSpaceAndKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := open(t, path)

	for round := 0; round < 50; round++ {
		for i := 0; i < 10; i++ {
			s.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("round-%d", round)))
		}
	}
	s.Delete("k3")
	s.Delete("k4")

	before := s.Stats()
	if before.DeadBytes <= before.LiveBytes {
		t.Fatalf("expected mostly dead data, got %+v", before)
	}

	cs, err := s.Compact()
	if err != nil {
		t.Fatal(err)
	}
	after := s.Stats()
	if after.FileSize >= before.FileSize/10 {
		t.Fatalf("file barely shrank: %d -> %d", before.FileSize, after.FileSize)
	}
	if after.DeadBytes != 0 || after.FileSize != after.LiveBytes || cs.AfterBytes != after.FileSize {
		t.Fatalf("stats after compact: %+v / %+v", after, cs)
	}
	if st, _ := os.Stat(path); st.Size() != after.FileSize {
		t.Fatalf("on-disk size %d != reported %d", st.Size(), after.FileSize)
	}
	if _, err := os.Stat(path + ".compact"); !os.IsNotExist(err) {
		t.Fatalf("temp file should be gone")
	}

	check := func(s *Store) {
		t.Helper()
		if s.Len() != 8 {
			t.Fatalf("len = %d, want 8", s.Len())
		}
		for i := 0; i < 10; i++ {
			v, err := s.Get(fmt.Sprintf("k%d", i))
			if i == 3 || i == 4 {
				if err != ErrNotFound {
					t.Fatalf("k%d should stay deleted", i)
				}
				continue
			}
			if err != nil || string(v) != "round-49" {
				t.Fatalf("k%d = %q, %v", i, v, err)
			}
		}
	}
	check(s)

	// Writes after compaction land in the new file and survive a reopen.
	s.Put("fresh", []byte("after"))
	s.Close()
	s = open(t, path)
	defer s.Close()
	if v, _ := s.Get("fresh"); string(v) != "after" {
		t.Fatalf("write after compaction lost")
	}
	s.Delete("fresh")
	check(s)
}

func TestCompactEmptyAndIdempotent(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	if _, err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Put("a", []byte("1"))
	s.Compact()
	size := s.Stats().FileSize
	s.Compact()
	if s.Stats().FileSize != size {
		t.Fatal("second compaction changed the file")
	}
}

func TestOpenDiscardsInterruptedCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := open(t, path)
	s.Put("a", []byte("1"))
	s.Close()
	os.WriteFile(path+".compact", []byte("half-written junk"), 0o644)

	s = open(t, path)
	defer s.Close()
	if v, _ := s.Get("a"); string(v) != "1" {
		t.Fatal("original data lost")
	}
	if _, err := os.Stat(path + ".compact"); !os.IsNotExist(err) {
		t.Fatal("stale temp file should be removed")
	}
}

func TestGetDetectsCorruptionAfterOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := open(t, path)
	defer s.Close()
	s.Put("a", []byte("hello"))

	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xFF // flip a bit in the value, behind the store's back
	os.WriteFile(path, data, 0o644)

	if _, err := s.Get("a"); err != ErrCorrupt {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
	if _, err := s.Compact(); err == nil {
		t.Fatal("compaction must refuse to copy a corrupt record")
	}
	if _, err := s.Get("a"); err != ErrCorrupt {
		t.Fatal("failed compaction must leave the store usable and unchanged")
	}
}

func TestInspect(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	s.Put("a", []byte("1"))
	s.Put("a", []byte("22"))
	s.Put("b", []byte("x"))
	s.Delete("b")

	recs, total, err := s.Inspect(0)
	if err != nil || total != 4 || len(recs) != 4 {
		t.Fatalf("recs=%d total=%d err=%v", len(recs), total, err)
	}
	wantLive := []bool{false, true, false, false}
	wantDel := []bool{false, false, false, true}
	for i, r := range recs {
		if r.Live != wantLive[i] || r.Delete != wantDel[i] {
			t.Fatalf("record %d: %+v", i, r)
		}
	}
	var sum int64
	for _, r := range recs {
		sum += r.Size
	}
	if sum != s.Stats().FileSize {
		t.Fatalf("sizes add up to %d, file is %d", sum, s.Stats().FileSize)
	}
	if first, total, _ := s.Inspect(2); len(first) != 2 || total != 4 {
		t.Fatalf("limit not honored: %d of %d", len(first), total)
	}
}

func TestHasAndRemove(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	s.Put("a", []byte("1"))
	if !s.Has("a") || s.Has("b") {
		t.Fatal("Has wrong")
	}
	if ok, err := s.Remove("a"); !ok || err != nil {
		t.Fatalf("Remove existing: %v %v", ok, err)
	}
	if ok, err := s.Remove("a"); ok || err != nil {
		t.Fatalf("Remove missing: %v %v", ok, err)
	}
	if s.Has("a") {
		t.Fatal("still there")
	}
}

func TestBackupIsAConsistentOpenableCopy(t *testing.T) {
	dir := t.TempDir()
	path, bak := filepath.Join(dir, "live.db"), filepath.Join(dir, "backup.db")
	s := open(t, path)
	defer s.Close()
	for i := 0; i < 200; i++ {
		s.Put(fmt.Sprintf("k%d", i%20), []byte(fmt.Sprintf("round-%d", i)))
	}
	s.Delete("k3")
	liveBefore := s.Stats()

	n, err := s.Backup(bak)
	if err != nil {
		t.Fatal(err)
	}
	if n != liveBefore.LiveBytes {
		t.Fatalf("backup wrote %d bytes, expected exactly the live data (%d)", n, liveBefore.LiveBytes)
	}
	if _, err := os.Stat(bak + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
	if s.Stats() != liveBefore {
		t.Fatal("backup must not change the live database")
	}

	// The copy opens as a normal database with the same data, and no waste.
	b := open(t, bak)
	defer b.Close()
	if b.Len() != 19 || b.Stats().DeadBytes != 0 {
		t.Fatalf("backup has %d keys, stats %+v", b.Len(), b.Stats())
	}
	for _, k := range s.Keys() {
		want, _ := s.Get(k)
		if got, err := b.Get(k); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: %q vs %q (%v)", k, got, want, err)
		}
	}
	if _, err := b.Get("k3"); err != ErrNotFound {
		t.Fatal("deleted key must not be in the backup")
	}

	// Later writes to the live store do not touch the backup.
	s.Put("new", []byte("x"))
	if b.Has("new") {
		t.Fatal("backup changed")
	}
}

func TestBackupRefusesToOverwriteTheLiveFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live.db")
	s := open(t, path)
	defer s.Close()
	s.Put("a", []byte("1"))
	if _, err := s.Backup(path); err == nil {
		t.Fatal("backing up onto the live file would destroy it")
	}
	if _, err := s.Backup(filepath.Join(filepath.Dir(path), ".", "live.db")); err == nil {
		t.Fatal("same file via a different spelling must also be refused")
	}
	if v, _ := s.Get("a"); string(v) != "1" {
		t.Fatal("live data damaged")
	}
}

func TestBackupWhileWriting(t *testing.T) {
	dir := t.TempDir()
	s := open(t, filepath.Join(dir, "live.db"))
	defer s.Close()
	for i := 0; i < 500; i++ {
		s.Put(fmt.Sprintf("seed%d", i), []byte("v"))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				s.Put(fmt.Sprintf("w%d", i%300), []byte(fmt.Sprint(i)))
			}
		}
	}()
	for i := 0; i < 5; i++ {
		bak := filepath.Join(dir, fmt.Sprintf("b%d.db", i))
		if _, err := s.Backup(bak); err != nil {
			t.Fatal(err)
		}
		b := open(t, bak)
		for _, k := range b.Keys() {
			if _, err := b.Get(k); err != nil {
				t.Fatalf("backup %d unreadable at %s: %v", i, k, err)
			}
		}
		if b.Len() < 500 {
			t.Fatalf("backup %d lost seed keys: %d", i, b.Len())
		}
		b.Close()
	}
	close(stop)
	wg.Wait()
}

func TestVersionChangesOnlyOnWrites(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	v0 := s.Version()
	s.Get("nothing")
	s.Keys()
	if s.Version() != v0 {
		t.Fatal("reads must not change the version")
	}
	s.Put("a", []byte("1"))
	v1 := s.Version()
	if v1 == v0 {
		t.Fatal("write must change the version")
	}
	s.Delete("missing")
	if s.Version() != v1 {
		t.Fatal("no-op delete must not change the version")
	}
	s.Compact()
	if s.Version() != v1 {
		t.Fatal("compaction does not change the logical data")
	}
}
