package kvdb

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

// FuzzReplay feeds arbitrary bytes to Open as if they were a database file.
// Whatever the bytes are, Open must not panic, must leave a valid file behind
// (a second Open sees exactly the same keys), and the store must stay usable.
func FuzzReplay(f *testing.F) {
	dir := f.TempDir()
	seedPath := filepath.Join(dir, "seed")
	s, _ := Open(seedPath, Options{})
	s.Set("session", []byte("abc"), SetOptions{ExpireAt: time.Unix(4_000_000_000, 0)})
	s.Put("name", []byte("vijay"))
	s.Put("name", []byte("vijay anbu"))
	s.Put("city", []byte("chennai"))
	s.Delete("city")
	s.Close()
	seed, _ := os.ReadFile(seedPath)
	f.Add(seed)
	f.Add(seed[:len(seed)-3])
	f.Add([]byte{})
	f.Add([]byte("not a database at all"))
	corrupt := append([]byte(nil), seed...)
	corrupt[20] ^= 0xFF
	f.Add(corrupt)

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "db")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		fixed := func() time.Time { return time.Unix(2_000_000_000, 0) }
		s, err := Open(path, Options{Now: fixed})
		if err != nil {
			t.Fatalf("Open must tolerate any file contents: %v", err)
		}
		keys := s.Keys()
		sort.Strings(keys)
		for _, k := range keys {
			if _, err := s.Get(k); err != nil {
				t.Fatalf("key %q in the index but unreadable: %v", k, err)
			}
		}
		st := s.Stats()
		if st.FileSize < 0 || st.LiveBytes < 0 || st.LiveBytes > st.FileSize || st.Expiring > st.Keys {
			t.Fatalf("inconsistent stats: %+v", st)
		}
		if err := s.Put("fuzz-probe", []byte("ok")); err != nil {
			t.Fatal(err)
		}
		s.Delete("fuzz-probe")
		s.Close()

		s2, err := Open(path, Options{Now: fixed})
		if err != nil {
			t.Fatal(err)
		}
		defer s2.Close()
		keys2 := s2.Keys()
		sort.Strings(keys2)
		if !reflect.DeepEqual(keys, keys2) && !(len(keys) == 0 && len(keys2) == 0) {
			t.Fatalf("reopen changed the data: %q vs %q", keys, keys2)
		}
	})
}
