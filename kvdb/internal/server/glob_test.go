package server

import (
	"strings"
	"testing"
	"time"
)

func TestGlob(t *testing.T) {
	cases := []struct {
		p, s string
		want bool
	}{
		{"*", "", true}, {"*", "anything", true}, {"", "", true}, {"", "a", false},
		{"user:*", "user:ada", true}, {"user:*", "users", false},
		{"h?llo", "hello", true}, {"h?llo", "hllo", false},
		{"h[ae]llo", "hallo", true}, {"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true}, {"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true}, {"h[a-c]llo", "hdllo", false},
		{`a\*b`, "a*b", true}, {`a\*b`, "axb", false},
		{"*a*b*c", "xxaxxbxxc", true}, {"*a*b*c", "xxaxxcxxb", false},
		{"a/b/*", "a/b/c", true},
		{"[", "[", true}, {"[abc", "[abc", true},
	}
	for _, c := range cases {
		if got := globMatch(c.p, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.p, c.s, got, c.want)
		}
	}
}

// A pattern designed to blow up a naive recursive matcher must still be fast.
func TestGlobIsNotExponential(t *testing.T) {
	done := make(chan bool)
	go func() {
		globMatch(strings.Repeat("*a", 40)+"b", strings.Repeat("a", 5000))
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pattern matching took too long")
	}
}

func FuzzGlob(f *testing.F) {
	f.Add("user:*", "user:ada")
	f.Add("h[a-c]?llo", "hbxllo")
	f.Add("[^", "[")
	f.Add(`\`, `\`)
	f.Fuzz(func(t *testing.T, pattern, s string) {
		globMatch(pattern, s) // must terminate without panicking
		if !globMatch("*", s) {
			t.Fatal(`"*" must match everything`)
		}
		if !containsMeta(s) && !globMatch(s, s) {
			t.Fatalf("a literal string must match itself: %q", s)
		}
	})
}

func containsMeta(s string) bool { return strings.ContainsAny(s, `*?[]\`) }
