package resp

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
)

func rd(s string) *bufio.Reader { return bufio.NewReader(strings.NewReader(s)) }

func TestReadCommandArray(t *testing.T) {
	args, err := ReadCommand(rd("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhe\r\no\r\n"))
	if err != nil || len(args) != 3 || string(args[2]) != "he\r\no" {
		t.Fatalf("%q %v", args, err)
	}
}

func TestReadCommandInline(t *testing.T) {
	args, err := ReadCommand(rd("\r\nPING  hello\r\n"))
	if err != nil || len(args) != 2 || string(args[0]) != "PING" || string(args[1]) != "hello" {
		t.Fatalf("%q %v", args, err)
	}
}

func TestReadCommandRejectsGarbage(t *testing.T) {
	for _, in := range []string{
		"*x\r\n",
		"*1\r\nfoo\r\n",
		"*1\r\n$-5\r\n",
		"*1\r\n$3\r\nabcXY",
		"*999999999\r\n",
		"*1\r\n$999999999999\r\n",
	} {
		if _, err := ReadCommand(rd(in)); !errors.Is(err, ErrProtocol) {
			t.Errorf("%q: want protocol error, got %v", in, err)
		}
	}
	// A line longer than the buffer must be refused, not buffered forever.
	long := strings.Repeat("a", 100000) + "\r\n"
	if _, err := ReadCommand(bufio.NewReaderSize(strings.NewReader(long), 4096)); !errors.Is(err, ErrProtocol) {
		t.Errorf("long line: %v", err)
	}
}

func TestReplyRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	WriteSimple(w, "OK")
	WriteError(w, "ERR bad\r\nthing")
	WriteInt(w, -42)
	WriteNull(w)
	WriteBulk(w, []byte("a\r\nb"))
	WriteArrayHeader(w, 2)
	WriteBulk(w, []byte("x"))
	WriteNull(w)
	w.Flush()

	r := bufio.NewReader(&buf)
	want := []string{"OK", "(error) ERR bad  thing", "(integer) -42", "(nil)", `"a\r\nb"`, "1) \"x\"\n2) (nil)"}
	for _, wnt := range want {
		v, err := ReadReply(r)
		if err != nil {
			t.Fatal(err)
		}
		if got := Format(v); got != wnt {
			t.Errorf("got %q want %q", got, wnt)
		}
	}
}

func TestWriteCommandMatchesReadCommand(t *testing.T) {
	var buf bytes.Buffer
	WriteCommand(&buf, "SET", "key", "va lue\r\n")
	args, err := ReadCommand(bufio.NewReader(&buf))
	if err != nil || len(args) != 3 || string(args[2]) != "va lue\r\n" {
		t.Fatalf("%q %v", args, err)
	}
}

func TestDeeplyNestedReplyRejected(t *testing.T) {
	if _, err := ReadReply(rd(strings.Repeat("*1\r\n", 50))); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v", err)
	}
}

// Neither parser may panic, hang, or allocate wildly on arbitrary input.
func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"))
	f.Add([]byte("PING\r\n"))
	f.Add([]byte("*2\r\n$-1\r\n"))
	f.Add([]byte("*99999999\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReaderSize(bytes.NewReader(data), 4096)
		for i := 0; i < 100; i++ {
			if _, err := ReadCommand(r); err != nil {
				return
			}
		}
	})
}

func FuzzReadReply(f *testing.F) {
	f.Add([]byte("+OK\r\n"))
	f.Add([]byte("*2\r\n$1\r\na\r\n:5\r\n"))
	f.Add([]byte("%1\r\n$1\r\nk\r\n_\r\n"))
	f.Add([]byte("*1\r\n*1\r\n*1\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReaderSize(bytes.NewReader(data), 4096)
		for i := 0; i < 100; i++ {
			v, err := ReadReply(r)
			if err != nil {
				return
			}
			_ = Format(v)
		}
	})
}
