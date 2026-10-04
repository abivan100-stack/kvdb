package kvdb

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkPut(b *testing.B) {
	s, _ := Open(filepath.Join(b.TempDir(), "db"), Options{})
	defer s.Close()
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(fmt.Sprintf("key-%d", i%100000), val)
	}
}

func BenchmarkGet(b *testing.B) {
	s, _ := Open(filepath.Join(b.TempDir(), "db"), Options{})
	defer s.Close()
	val := make([]byte, 100)
	for i := 0; i < 100000; i++ {
		s.Put(fmt.Sprintf("key-%d", i), val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key-%d", i%100000))
	}
}
