// kvsh is an interactive shell for a running kvserver (like redis-cli).
//
//	go run ./cmd/kvsh                          # connects to 127.0.0.1:6380
//	go run ./cmd/kvsh -addr 192.168.1.20:6380  # another machine
//
// If the server needs a password, set KVDB_PASSWORD or pass -password.
package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"kvdb/internal/resp"
)

// splitArgs splits a line on spaces, honouring "double" and 'single' quotes
// and backslash escapes inside double quotes.
func splitArgs(line string) ([]string, error) {
	var args []string
	var cur strings.Builder
	inArg := false
	var quote rune
	esc := false
	for _, r := range line {
		switch {
		case esc:
			switch r {
			case 'n':
				cur.WriteRune('\n')
			case 't':
				cur.WriteRune('\t')
			default:
				cur.WriteRune(r)
			}
			esc = false
		case quote == '"' && r == '\\':
			esc = true
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '\'':
			quote, inArg = r, true
		case r == ' ' || r == '\t':
			if inArg {
				args = append(args, cur.String())
				cur.Reset()
				inArg = false
			}
		default:
			cur.WriteRune(r)
			inArg = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unbalanced quotes")
	}
	if inArg {
		args = append(args, cur.String())
	}
	return args, nil
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "server address")
	password := flag.String("password", "", "password (or set KVDB_PASSWORD)")
	useTLS := flag.Bool("tls", false, "connect with TLS encryption")
	caFile := flag.String("ca", "", "with -tls: trust this certificate file (needed for self-signed servers)")
	flag.Parse()
	pw := *password
	if pw == "" {
		pw = os.Getenv("KVDB_PASSWORD")
	}

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	var conn net.Conn
	var err error
	if *useTLS || *caFile != "" {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if *caFile != "" {
			pem, rerr := os.ReadFile(*caFile)
			pool := x509.NewCertPool()
			if rerr != nil || !pool.AppendCertsFromPEM(pem) {
				fmt.Printf("Could not load the certificate %q.\n", *caFile)
				os.Exit(1)
			}
			cfg.RootCAs = pool
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", *addr, cfg)
	} else {
		conn, err = dialer.Dial("tcp", *addr)
	}
	if err != nil {
		fmt.Printf("Could not connect to %s: %v\nIs the server running? Start it with: go run ./cmd/kvserver\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()
	r := bufio.NewReader(conn)

	roundTrip := func(args ...string) (resp.Value, error) {
		if err := resp.WriteCommand(conn, args...); err != nil {
			return resp.Value{}, err
		}
		return resp.ReadReply(r)
	}

	if pw != "" {
		v, err := roundTrip("AUTH", pw)
		if err != nil || v.Kind == '-' {
			fmt.Println("Authentication failed.")
			os.Exit(1)
		}
	}
	fmt.Printf("Connected to %s. Try: set name vijay | get name | keys * | info | compact | quit\n", *addr)

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 64<<20)
	for fmt.Print("kvdb> "); in.Scan(); fmt.Print("kvdb> ") {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		args, err := splitArgs(line)
		if err != nil {
			fmt.Println("(error)", err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		if c := strings.ToLower(args[0]); c == "exit" {
			args = []string{"QUIT"}
		}
		v, err := roundTrip(args...)
		if err != nil {
			fmt.Println("connection lost:", err)
			os.Exit(1)
		}
		fmt.Println(resp.Format(v))
		if strings.EqualFold(args[0], "quit") {
			return
		}
	}
}
