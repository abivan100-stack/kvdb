// Package resp implements the small part of the Redis wire protocol (RESP2)
// that kvdb needs, for both the server side and the client side.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	MaxBulk  = 70 << 20 // a bit more than kvdb.MaxValueSize
	MaxArgs  = 100000
	maxDepth = 8
)

// ErrProtocol marks malformed input from the peer.
var ErrProtocol = errors.New("protocol error")

func protoErr(msg string) error { return fmt.Errorf("%w: %s", ErrProtocol, msg) }

// readLine returns one line without its trailing CRLF (or LF).
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, protoErr("line too long")
	}
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return append([]byte(nil), line...), nil
}

// ReadCommand reads one request: either a RESP array of bulk strings, or an
// "inline" command typed by hand over telnet/netcat.
func ReadCommand(r *bufio.Reader) ([][]byte, error) {
	for {
		line, err := readLine(r)
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			continue
		}
		if line[0] != '*' {
			if f := strings.Fields(string(line)); len(f) > 0 {
				args := make([][]byte, len(f))
				for i := range f {
					args[i] = []byte(f[i])
				}
				return args, nil
			}
			continue
		}
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil || n > MaxArgs {
			return nil, protoErr("invalid multibulk length")
		}
		if n <= 0 {
			continue
		}
		args := make([][]byte, 0, min(n, 1024))
		for i := 0; i < n; i++ {
			h, err := readLine(r)
			if err != nil {
				return nil, err
			}
			if len(h) == 0 || h[0] != '$' {
				return nil, protoErr("expected '$'")
			}
			l, err := strconv.Atoi(string(h[1:]))
			if err != nil || l < 0 || l > MaxBulk {
				return nil, protoErr("invalid bulk length")
			}
			buf := make([]byte, l+2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			if buf[l] != '\r' || buf[l+1] != '\n' {
				return nil, protoErr("bulk string not terminated by CRLF")
			}
			args = append(args, buf[:l])
		}
		return args, nil
	}
}

// ---- writing replies (server side) ----

func WriteSimple(w *bufio.Writer, s string) { w.WriteString("+" + clean(s) + "\r\n") }
func WriteError(w *bufio.Writer, s string)  { w.WriteString("-" + clean(s) + "\r\n") }
func WriteInt(w *bufio.Writer, n int64)     { w.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n") }
func WriteNull(w *bufio.Writer)             { w.WriteString("$-1\r\n") }

// WriteNull3 is the RESP3 null, used after a client switches with HELLO 3.
func WriteNull3(w *bufio.Writer) { w.WriteString("_\r\n") }

// WriteMapHeader starts a RESP3 map of n key/value pairs.
func WriteMapHeader(w *bufio.Writer, n int) { w.WriteString("%" + strconv.Itoa(n) + "\r\n") }
func WriteArrayHeader(w *bufio.Writer, n int) {
	w.WriteString("*" + strconv.Itoa(n) + "\r\n")
}
func WriteBulk(w *bufio.Writer, b []byte) {
	w.WriteString("$" + strconv.Itoa(len(b)) + "\r\n")
	w.Write(b)
	w.WriteString("\r\n")
}

// clean keeps a stray CR/LF from breaking the framing of a status line.
func clean(s string) string {
	if len(s) > 200 {
		s = s[:200]
	}
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

// ---- client side ----

// WriteCommand sends a command as a RESP array of bulk strings.
func WriteCommand(w io.Writer, args ...string) error {
	var b strings.Builder
	b.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		b.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// Value is a decoded reply.
type Value struct {
	Kind byte // '+' simple, '-' error, ':' integer, '$' bulk, '*' array
	Str  string
	Int  int64
	Arr  []Value
	Null bool
}

// ReadReply decodes one reply.
func ReadReply(r *bufio.Reader) (Value, error) { return readReply(r, 0) }

func readReply(r *bufio.Reader, depth int) (Value, error) {
	if depth > maxDepth {
		return Value{}, protoErr("reply nested too deeply")
	}
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, protoErr("empty reply line")
	}
	body := string(line[1:])
	switch line[0] {
	case '+', '-':
		return Value{Kind: line[0], Str: body}, nil
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return Value{}, protoErr("bad integer reply")
		}
		return Value{Kind: ':', Int: n}, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n > MaxBulk {
			return Value{}, protoErr("bad bulk length")
		}
		if n < 0 {
			return Value{Kind: '$', Null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return Value{}, err
		}
		return Value{Kind: '$', Str: string(buf[:n])}, nil
	case '_':
		return Value{Kind: '_', Null: true}, nil
	case '%':
		n, err := strconv.Atoi(body)
		if err != nil || n < 0 || n > MaxArgs {
			return Value{}, protoErr("bad map length")
		}
		arr := make([]Value, 0, min(2*n, 1024))
		for i := 0; i < 2*n; i++ {
			v, err := readReply(r, depth+1)
			if err != nil {
				return Value{}, err
			}
			arr = append(arr, v)
		}
		return Value{Kind: '%', Arr: arr}, nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n > MaxArgs*10 {
			return Value{}, protoErr("bad array length")
		}
		if n < 0 {
			return Value{Kind: '*', Null: true}, nil
		}
		arr := make([]Value, 0, min(n, 1024))
		for i := 0; i < n; i++ {
			v, err := readReply(r, depth+1)
			if err != nil {
				return Value{}, err
			}
			arr = append(arr, v)
		}
		return Value{Kind: '*', Arr: arr}, nil
	}
	return Value{}, protoErr("unknown reply type")
}

// Format renders a reply the way redis-cli does.
func Format(v Value) string { return format(v, 0) }

func format(v Value, indent int) string {
	switch v.Kind {
	case '+':
		return v.Str
	case '-':
		return "(error) " + v.Str
	case ':':
		return "(integer) " + strconv.FormatInt(v.Int, 10)
	case '$':
		if v.Null {
			return "(nil)"
		}
		return strconv.Quote(v.Str)
	case '_':
		return "(nil)"
	case '*', '%':
		if v.Null {
			return "(nil)"
		}
		if len(v.Arr) == 0 {
			return "(empty array)"
		}
		var b strings.Builder
		pad := strings.Repeat(" ", indent)
		for i, e := range v.Arr {
			if i > 0 {
				b.WriteString("\n" + pad)
			}
			fmt.Fprintf(&b, "%d) %s", i+1, format(e, indent+3))
		}
		return b.String()
	}
	return "(unknown reply)"
}
