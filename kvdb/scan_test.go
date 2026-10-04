package kvdb

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func TestPutMany(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s := open(t, path)
	if err := s.PutMany([]string{"a", "b", "a"}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Get("a"); string(v) != "3" {
		t.Fatalf("later entry for the same key must win, got %q", v)
	}
	if s.Len() != 2 {
		t.Fatalf("len %d", s.Len())
	}
	// byte accounting stays exact even with a duplicate inside one batch
	st := s.Stats()
	if st.LiveBytes+st.DeadBytes != st.FileSize || st.DeadBytes != 15 {
		t.Fatalf("stats %+v (the first record for a is 13+1+1 = 15 bytes of waste)", st)
	}
	v0 := s.Version()
	s.PutMany(nil, nil)
	if s.Version() != v0 {
		t.Fatal("empty batch must not write")
	}
	s.Close()

	s = open(t, path)
	defer s.Close()
	if v, _ := s.Get("a"); string(v) != "3" || s.Len() != 2 {
		t.Fatal("batch lost on reopen")
	}
}

func TestPutManyIsAllOrNothing(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	s.Put("keep", []byte("v"))
	before := s.Stats()

	err := s.PutMany([]string{"ok", "", "alsoOK"}, [][]byte{[]byte("1"), []byte("2"), []byte("3")})
	if err != ErrEmptyKey {
		t.Fatalf("want ErrEmptyKey, got %v", err)
	}
	if s.Has("ok") || s.Has("alsoOK") || s.Stats() != before {
		t.Fatal("a rejected batch must write nothing at all")
	}
	if err := s.PutMany([]string{"a"}, nil); err != ErrBadBatch {
		t.Fatalf("mismatched lengths: %v", err)
	}
}

func collect(t *testing.T, s *Store, count int) (all []string, pages int) {
	t.Helper()
	var cursor int64
	for {
		keys, next, err := s.Scan(cursor, count)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, keys...)
		pages++
		if next == 0 {
			return
		}
		if pages > 100000 {
			t.Fatal("scan never finished")
		}
		cursor = next
	}
}

func TestScanReturnsEveryLiveKeyOnce(t *testing.T) {
	c := newClock()
	s := openClock(t, filepath.Join(t.TempDir(), "db"), c)
	defer s.Close()
	for i := 0; i < 300; i++ {
		s.Put(fmt.Sprintf("k%03d", i), []byte("v"))
	}
	for i := 0; i < 300; i += 3 { // overwrite: leaves dead records in the file
		s.Put(fmt.Sprintf("k%03d", i), []byte("v2"))
	}
	for i := 1; i < 300; i += 7 { // delete some
		s.Delete(fmt.Sprintf("k%03d", i))
	}
	s.Set("tmp", []byte("x"), SetOptions{ExpireAt: c.Now().Add(time.Second)})
	c.Advance(2 * time.Second) // expired but not purged

	want := s.Keys()
	sort.Strings(want)
	for _, count := range []int{1, 7, 100, 1000} {
		got, pages := collect(t, s, count)
		sort.Strings(got)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("count=%d: got %d keys, want %d (no concurrent changes, so no duplicates either)", count, len(got), len(want))
		}
		if count == 1000 && pages != 1 {
			t.Fatalf("one page should be enough, took %d", pages)
		}
	}
}

func TestScanOnEmptyStore(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	keys, next, err := s.Scan(0, 10)
	if err != nil || len(keys) != 0 || next != 0 {
		t.Fatalf("%v %d %v", keys, next, err)
	}
}

// The promise: a key that exists for the whole iteration is never missed,
// whatever else happens in between.
func TestScanSurvivesConcurrentChangesAndCompaction(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	const n = 400
	for i := 0; i < n; i++ {
		s.Put(fmt.Sprintf("k%03d", i), []byte("v"))
	}
	doomed := map[string]bool{}
	for i := 100; i < 130; i++ {
		doomed[fmt.Sprintf("k%03d", i)] = true
	}

	seen := map[string]bool{}
	var cursor int64
	for step := 0; ; step++ {
		keys, next, err := s.Scan(cursor, 15)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			seen[k] = true
		}
		if next == 0 {
			break
		}
		cursor = next
		switch step {
		case 3: // overwrite keys both behind and ahead of the cursor
			for i := 0; i < 400; i += 5 {
				s.Put(fmt.Sprintf("k%03d", i), []byte("rewritten"))
			}
		case 6:
			for k := range doomed {
				s.Delete(k)
			}
		case 9: // the file is rewritten and every offset changes
			if _, err := s.Compact(); err != nil {
				t.Fatal(err)
			}
		case 14:
			s.Put("brand-new", []byte("x"))
		}
		if step > 5000 {
			t.Fatal("scan did not terminate")
		}
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%03d", i)
		if !doomed[k] && !seen[k] {
			t.Errorf("key %s existed throughout but was never returned", k)
		}
	}
}

func TestScanToleratesForgedCursors(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	for i := 0; i < 20; i++ {
		s.Put(fmt.Sprintf("k%d", i), []byte("v"))
	}
	for _, cur := range []int64{-5, 1, 7, 12345, 1 << 40, 1<<62 + 99, 1<<63 - 1} {
		if _, _, err := s.Scan(cur, 5); err != nil {
			t.Errorf("cursor %d: %v", cur, err)
		}
	}
	if keys, _, _ := s.Scan(-1, 100); len(keys) != 20 {
		t.Fatalf("negative cursor should start over, got %d keys", len(keys))
	}
}

func TestScanSkipsLongRunsOfDeadRecords(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	for i := 0; i < 5000; i++ {
		s.Put("hot", []byte(fmt.Sprint(i))) // 4999 dead records
	}
	s.Put("last", []byte("x"))
	got, pages := collect(t, s, 10)
	sort.Strings(got)
	if fmt.Sprint(got) != "[hot last]" {
		t.Fatalf("got %v", got)
	}
	if pages < 2 {
		t.Fatalf("a long dead run should be split across pages (bounded work per call), took %d", pages)
	}
}
