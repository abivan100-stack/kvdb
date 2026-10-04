package server

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"kvdb"
	"kvdb/internal/resp"
)

func quiet(string, ...any) {}

// fakeClock lets tests move time forward without sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type env struct {
	srv  *Server
	db   *kvdb.Store
	addr string
	path string
	clk  *fakeClock
}

func startAt(t *testing.T, path string, cfg Config) *env {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	db, err := kvdb.Open(path, kvdb.Options{Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Logf = quiet
	srv := New(db, cfg)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	return &env{srv: srv, db: db, addr: ln.Addr().String(), path: path, clk: clk}
}

func start(t *testing.T, cfg Config) *env {
	t.Helper()
	e := startAt(t, filepath.Join(t.TempDir(), "s.db"), cfg)
	t.Cleanup(e.stop)
	return e
}

func (e *env) stop() { e.srv.Close(); e.db.Close() }

type client struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func (e *env) dial(t *testing.T) *client {
	t.Helper()
	c, err := net.Dial("tcp", e.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &client{t: t, c: c, r: bufio.NewReader(c)}
}

func (c *client) do(args ...string) resp.Value {
	c.t.Helper()
	c.c.SetDeadline(time.Now().Add(5 * time.Second))
	if err := resp.WriteCommand(c.c, args...); err != nil {
		c.t.Fatal(err)
	}
	return c.read()
}

func (c *client) read() resp.Value {
	c.t.Helper()
	v, err := resp.ReadReply(c.r)
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	return v
}

func expect(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if got := resp.Format(v); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestBasicCommands(t *testing.T) {
	c := start(t, Config{}).dial(t)

	expect(t, c.do("PING"), "PONG")
	expect(t, c.do("PING", "hi"), `"hi"`)
	expect(t, c.do("ECHO", "yo"), `"yo"`)
	expect(t, c.do("GET", "name"), "(nil)")
	expect(t, c.do("SET", "name", "vijay"), "OK")
	expect(t, c.do("GET", "name"), `"vijay"`)
	expect(t, c.do("SET", "name", "vijay anbu"), "OK")
	expect(t, c.do("GET", "name"), `"vijay anbu"`)
	expect(t, c.do("SET", "city", "chennai"), "OK")
	expect(t, c.do("EXISTS", "name", "city", "nope", "name"), "(integer) 3")
	expect(t, c.do("DBSIZE"), "(integer) 2")
	expect(t, c.do("MGET", "name", "nope", "city"), "1) \"vijay anbu\"\n2) (nil)\n3) \"chennai\"")
	expect(t, c.do("KEYS", "*"), "1) \"city\"\n2) \"name\"")
	expect(t, c.do("KEYS", "n*"), "1) \"name\"")
	expect(t, c.do("KEYS", "zzz*"), "(empty array)")
	expect(t, c.do("DEL", "name", "nope"), "(integer) 1")
	expect(t, c.do("GET", "name"), "(nil)")
	expect(t, c.do("DBSIZE"), "(integer) 1")

	// Binary-safe values, including CRLF and empty.
	expect(t, c.do("SET", "bin", "a\r\nb\x00c"), "OK")
	if v := c.do("GET", "bin"); v.Str != "a\r\nb\x00c" {
		t.Fatalf("binary value mangled: %q", v.Str)
	}
	expect(t, c.do("SET", "empty", ""), "OK")
	expect(t, c.do("GET", "empty"), `""`)
}

func TestErrors(t *testing.T) {
	c := start(t, Config{}).dial(t)
	expect(t, c.do("GET"), "(error) ERR wrong number of arguments for 'get' command")
	expect(t, c.do("SET", "a"), "(error) ERR wrong number of arguments for 'set' command")
	expect(t, c.do("SET", "a", "b", "FLY", "10"), "(error) ERR syntax error (supported: EX, PX, NX, XX)")
	expect(t, c.do("SET", "", "x"), "(error) ERR empty keys are not supported")
	expect(t, c.do("FLY"), "(error) ERR unknown command 'fly'")
	expect(t, c.do("FLY\r\nINJECT"), "(error) ERR unknown command 'fly  inject'")
	expect(t, c.do("AUTH", "x"), "(error) ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	expect(t, c.do("SELECT", "3"), "(error) ERR DB index is out of range (kvdb has a single database)")
	expect(t, c.do("SELECT", "0"), "OK")
	expect(t, c.do("COMMAND"), "(empty array)")
	// The connection survives every error above.
	expect(t, c.do("PING"), "PONG")
}

func TestIncr(t *testing.T) {
	c := start(t, Config{}).dial(t)
	expect(t, c.do("INCR", "n"), "(integer) 1")
	expect(t, c.do("INCR", "n"), "(integer) 2")
	expect(t, c.do("INCRBY", "n", "10"), "(integer) 12")
	expect(t, c.do("DECR", "n"), "(integer) 11")
	expect(t, c.do("DECRBY", "n", "20"), "(integer) -9")
	expect(t, c.do("GET", "n"), `"-9"`)
	expect(t, c.do("INCRBY", "n", "abc"), "(error) ERR value is not an integer or out of range")
	c.do("SET", "s", "hello")
	expect(t, c.do("INCR", "s"), "(error) ERR value is not an integer or out of range")
	c.do("SET", "big", "9223372036854775807")
	expect(t, c.do("INCR", "big"), "(error) ERR increment or decrement would overflow")
	expect(t, c.do("DECRBY", "big", "-9223372036854775808"), "(error) ERR increment or decrement would overflow")
}

func TestIncrIsAtomicAcrossClients(t *testing.T) {
	e := start(t, Config{})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.dial(t)
			for j := 0; j < 50; j++ {
				c.do("INCR", "counter")
			}
		}()
	}
	wg.Wait()
	expect(t, e.dial(t).do("GET", "counter"), `"1000"`)
}

func TestPipelining(t *testing.T) {
	c := start(t, Config{}).dial(t)
	var b strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&b, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(fmt.Sprint("k", i)), fmt.Sprint("k", i))
	}
	b.WriteString("*1\r\n$6\r\nDBSIZE\r\n")
	c.c.Write([]byte(b.String()))
	for i := 0; i < 500; i++ {
		if v := c.read(); v.Str != "OK" {
			t.Fatalf("reply %d: %v", i, v)
		}
	}
	expect(t, c.read(), "(integer) 500")
}

func TestInlineCommandsWorkOverRawTCP(t *testing.T) {
	e := start(t, Config{})
	c, _ := net.Dial("tcp", e.addr)
	defer c.Close()
	fmt.Fprint(c, "SET hello world\r\nGET hello\r\n")
	r := bufio.NewReader(c)
	c.SetDeadline(time.Now().Add(3 * time.Second))
	a, _ := resp.ReadReply(r)
	b, _ := resp.ReadReply(r)
	if a.Str != "OK" || b.Str != "world" {
		t.Fatalf("%v %v", a, b)
	}
}

func TestAuth(t *testing.T) {
	e := start(t, Config{Password: "s3cret"})
	c := e.dial(t)
	expect(t, c.do("GET", "a"), "(error) NOAUTH Authentication required.")
	expect(t, c.do("PING"), "(error) NOAUTH Authentication required.")
	expect(t, c.do("AUTH", "wrong"), "(error) WRONGPASS invalid username-password pair or user is disabled.")
	expect(t, c.do("GET", "a"), "(error) NOAUTH Authentication required.")
	expect(t, c.do("AUTH", "s3cret"), "OK")
	expect(t, c.do("SET", "a", "1"), "OK")
	// Redis 6 style: AUTH username password
	c2 := e.dial(t)
	expect(t, c2.do("AUTH", "default", "s3cret"), "OK")
	expect(t, c2.do("GET", "a"), `"1"`)
}

func TestQuitClosesConnection(t *testing.T) {
	c := start(t, Config{}).dial(t)
	expect(t, c.do("QUIT"), "OK")
	c.c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := resp.ReadReply(c.r); err == nil {
		t.Fatal("connection should be closed after QUIT")
	}
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	e := start(t, Config{})
	c := e.dial(t)
	c.c.Write([]byte("*abc\r\n"))
	c.c.SetDeadline(time.Now().Add(2 * time.Second))
	v := c.read()
	if v.Kind != '-' || !strings.Contains(v.Str, "Protocol error") {
		t.Fatalf("got %v", v)
	}
	if _, err := resp.ReadReply(c.r); err == nil {
		t.Fatal("connection should be closed")
	}
	// The server itself is unaffected.
	expect(t, e.dial(t).do("PING"), "PONG")
}

func TestMaxClients(t *testing.T) {
	e := start(t, Config{MaxClients: 1})
	first := e.dial(t)
	expect(t, first.do("PING"), "PONG")
	second := e.dial(t)
	second.c.SetDeadline(time.Now().Add(2 * time.Second))
	v, err := resp.ReadReply(second.r)
	if err != nil || v.Kind != '-' || !strings.Contains(v.Str, "max number of clients") {
		t.Fatalf("got %v, %v", v, err)
	}
	expect(t, first.do("PING"), "PONG") // first client unaffected
}

func TestIdleTimeout(t *testing.T) {
	c := start(t, Config{IdleTimeout: 150 * time.Millisecond}).dial(t)
	expect(t, c.do("PING"), "PONG")
	time.Sleep(400 * time.Millisecond)
	c.c.SetDeadline(time.Now().Add(2 * time.Second))
	resp.WriteCommand(c.c, "PING")
	if _, err := resp.ReadReply(c.r); err == nil {
		t.Fatal("idle client should have been disconnected")
	}
}

func TestDataSurvivesServerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.db")
	e := startAt(t, path, Config{})
	c := e.dial(t)
	c.do("SET", "name", "vijay")
	c.do("SET", "gone", "x")
	c.do("DEL", "gone")
	c.do("INCR", "visits")
	e.stop()

	e2 := startAt(t, path, Config{})
	defer e2.stop()
	c2 := e2.dial(t)
	expect(t, c2.do("GET", "name"), `"vijay"`)
	expect(t, c2.do("GET", "gone"), "(nil)")
	expect(t, c2.do("GET", "visits"), `"1"`)
	expect(t, c2.do("DBSIZE"), "(integer) 2")
}

func TestShutdownDisconnectsClientsAndKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shut.db")
	e := startAt(t, path, Config{})
	c := e.dial(t)
	c.do("SET", "k", "v")
	done := make(chan struct{})
	go func() { e.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung with a client still connected")
	}
	db, _ := kvdb.Open(path, kvdb.Options{})
	defer db.Close()
	if v, _ := db.Get("k"); string(v) != "v" {
		t.Fatal("data lost on shutdown")
	}
}

func TestCompactCommandAndInfo(t *testing.T) {
	c := start(t, Config{}).dial(t)
	for i := 0; i < 300; i++ {
		c.do("SET", "same", fmt.Sprint("value-", i))
	}
	before := c.do("INFO").Str
	if !strings.Contains(before, "kvdb_version:") || !strings.Contains(before, "keys:1\r\n") ||
		strings.Contains(before, "dead_bytes:0\r\n") {
		t.Fatalf("unexpected INFO before compact:\n%s", before)
	}
	v := c.do("COMPACT")
	if v.Kind != ':' || v.Int <= 0 {
		t.Fatalf("COMPACT should report reclaimed bytes, got %v", v)
	}
	after := c.do("INFO").Str
	if !strings.Contains(after, "dead_bytes:0\r\n") {
		t.Fatalf("dead bytes should be zero after compaction:\n%s", after)
	}
	expect(t, c.do("GET", "same"), `"value-299"`)
}

func TestAutoCompaction(t *testing.T) {
	e := start(t, Config{CompactMinDead: 100})
	c := e.dial(t)
	for i := 0; i < 200; i++ {
		c.do("SET", "k", fmt.Sprint("v", i))
	}
	if e.db.Stats().DeadBytes == 0 {
		t.Fatal("setup: expected dead bytes")
	}
	e.srv.maybeCompact()
	if st := e.db.Stats(); st.DeadBytes != 0 {
		t.Fatalf("auto-compaction did not run: %+v", st)
	}
	expect(t, c.do("GET", "k"), `"v199"`)

	// Below the threshold it must leave the file alone.
	e.srv.cfg.CompactMinDead = 1 << 30
	for i := 0; i < 50; i++ {
		c.do("SET", "k", "again")
	}
	before := e.db.Stats().FileSize
	e.srv.maybeCompact()
	if e.db.Stats().FileSize != before {
		t.Fatal("compacted below threshold")
	}
}

func TestReadsAndWritesDuringCompaction(t *testing.T) {
	e := start(t, Config{})
	seed := e.dial(t)
	for i := 0; i < 2000; i++ {
		seed.do("SET", fmt.Sprint("k", i%50), fmt.Sprint("v", i))
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			c := e.dial(t)
			for i := 0; i < 200; i++ {
				k := fmt.Sprint("w", g, "-", i)
				c.do("SET", k, k)
				if v := c.do("GET", k); v.Str != k {
					t.Errorf("lost write %s: %v", k, v)
					return
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := e.dial(t)
		for i := 0; i < 10; i++ {
			c.do("COMPACT")
		}
	}()
	wg.Wait()
	expect(t, e.dial(t).do("DBSIZE"), "(integer) 850")
}

func TestHelloHandshake(t *testing.T) {
	e := start(t, Config{})

	// HELLO 2 / no args: classic array reply, $-1 nulls.
	c := e.dial(t)
	v := c.do("HELLO", "2")
	if v.Kind != '*' || len(v.Arr) != 14 || v.Arr[0].Str != "server" || v.Arr[1].Str != "kvdb" || v.Arr[5].Int != 2 {
		t.Fatalf("HELLO 2: %v", v)
	}
	c.c.Write([]byte("*2\r\n$3\r\nGET\r\n$4\r\nnope\r\n"))
	raw := make([]byte, 5)
	c.c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.Read(raw); err != nil || string(raw) != "$-1\r\n" {
		t.Fatalf("RESP2 null = %q %v", raw, err)
	}

	// HELLO 3: map reply and RESP3 nulls.
	c3 := e.dial(t)
	v = c3.do("HELLO", "3")
	if v.Kind != '%' || len(v.Arr) != 14 || v.Arr[5].Int != 3 {
		t.Fatalf("HELLO 3: %v", v)
	}
	r := c3.do("GET", "nope")
	if r.Kind != '_' || !r.Null {
		t.Fatalf("RESP3 null: %+v", r)
	}
	if r := c3.do("MGET", "nope"); len(r.Arr) != 1 || r.Arr[0].Kind != '_' {
		t.Fatalf("MGET RESP3: %+v", r)
	}
	// Other connections are unaffected by one client's choice.
	if r := c.do("GET", "nope"); r.Kind != '$' || !r.Null {
		t.Fatalf("connection state leaked: %+v", r)
	}

	expect(t, e.dial(t).do("HELLO", "4"), "(error) NOPROTO unsupported protocol version")
	expect(t, e.dial(t).do("HELLO", "abc"), "(error) ERR Protocol version is not an integer or out of range")
}

func TestHelloWithAuth(t *testing.T) {
	e := start(t, Config{Password: "pw"})
	c := e.dial(t)
	if v := c.do("HELLO", "3"); v.Kind != '-' || !strings.HasPrefix(v.Str, "NOAUTH") {
		t.Fatalf("HELLO without auth: %v", v)
	}
	if v := c.do("HELLO", "3", "AUTH", "default", "nope"); v.Kind != '-' || !strings.HasPrefix(v.Str, "WRONGPASS") {
		t.Fatalf("HELLO wrong auth: %v", v)
	}
	if v := c.do("GET", "x"); !strings.HasPrefix(v.Str, "NOAUTH") {
		t.Fatalf("still unauthenticated expected: %v", v)
	}
	if v := c.do("HELLO", "3", "AUTH", "default", "pw", "SETNAME", "app"); v.Kind != '%' {
		t.Fatalf("HELLO with auth: %v", v)
	}
	expect(t, c.do("SET", "k", "v"), "OK")
}

// ---- TLS ----

func startTLS(t *testing.T, cfg Config) (addr string, caPool *x509.CertPool) {
	t.Helper()
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "c.pem"), filepath.Join(dir, "k.pem")
	if err := GenerateSelfSigned(certPath, keyPath, []string{"localhost", "127.0.0.1"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	tlsCfg, err := LoadTLS(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	db, err := kvdb.Open(filepath.Join(dir, "t.db"), kvdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Logf = quiet
	srv := New(db, cfg)
	raw, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.Serve(tls.NewListener(raw, tlsCfg))
	t.Cleanup(func() { srv.Close(); db.Close() })

	pem, _ := os.ReadFile(certPath)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("could not parse generated certificate")
	}
	return raw.Addr().String(), pool
}

func TestTLS(t *testing.T) {
	addr, pool := startTLS(t, Config{Password: "pw"})

	// A client that trusts the certificate works end to end.
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := &client{t: t, c: conn, r: bufio.NewReader(conn)}
	expect(t, c.do("AUTH", "pw"), "OK")
	expect(t, c.do("SET", "k", "v"), "OK")
	expect(t, c.do("GET", "k"), `"v"`)
	if conn.ConnectionState().Version < tls.VersionTLS12 {
		t.Fatal("TLS version too old")
	}

	// A client that does not trust it is refused.
	if _, err := tls.Dial("tcp", addr, &tls.Config{ServerName: "localhost"}); err == nil {
		t.Fatal("untrusted self-signed certificate must be rejected")
	}
	// Connecting by a name that is not in the certificate is refused.
	if _, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, ServerName: "evil.example"}); err == nil {
		t.Fatal("hostname mismatch must be rejected")
	}
	// Plain text to a TLS port gets nothing useful and does not hang the server.
	plain, _ := net.Dial("tcp", addr)
	plain.Write([]byte("PING\r\n"))
	plain.SetReadDeadline(time.Now().Add(2 * time.Second))
	plain.Read(make([]byte, 64)) // whatever comes back (an alert or nothing), it must not wedge the server
	plain.Close()
	conn2, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"})
	if err != nil {
		t.Fatalf("server should still accept good clients: %v", err)
	}
	conn2.Close()
}

func TestGeneratedKeyIsPrivate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes are not meaningful on Windows")
	}
	dir := t.TempDir()
	if err := GenerateSelfSigned(filepath.Join(dir, "c"), filepath.Join(dir, "k"), []string{"localhost"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(filepath.Join(dir, "k"))
	if st.Mode().Perm()&0o077 != 0 {
		t.Fatalf("private key must not be readable by others, mode %v", st.Mode().Perm())
	}
}

// ---- backups ----

func backupFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestBackupCommandAndPruning(t *testing.T) {
	bdir := filepath.Join(t.TempDir(), "backups")
	e := start(t, Config{BackupDir: bdir, BackupKeep: 3})
	c := e.dial(t)
	c.do("SET", "name", "vijay")

	var last string
	for i := 0; i < 5; i++ {
		c.do("SET", "n", fmt.Sprint(i))
		v := c.do("BACKUP")
		if v.Kind != '$' || !strings.HasPrefix(v.Str, "kvdb-") || !strings.HasSuffix(v.Str, ".db") {
			t.Fatalf("BACKUP reply: %+v", v)
		}
		last = v.Str
		time.Sleep(3 * time.Millisecond) // names carry milliseconds
	}
	names := backupFiles(t, bdir)
	if len(names) != 3 {
		t.Fatalf("expected the 3 newest snapshots, got %v", names)
	}
	if names[len(names)-1] != last {
		t.Fatalf("newest snapshot should be kept: %v vs %s", names, last)
	}

	// The newest snapshot is a real database holding the data.
	snap, err := kvdb.Open(filepath.Join(bdir, last), kvdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if v, _ := snap.Get("name"); string(v) != "vijay" {
		t.Fatalf("snapshot content: %q", v)
	}
	if v, _ := snap.Get("n"); string(v) != "4" {
		t.Fatalf("snapshot should hold the latest value: %q", v)
	}
}

func TestBackupDisabledByDefault(t *testing.T) {
	c := start(t, Config{}).dial(t)
	v := c.do("BACKUP")
	if v.Kind != '-' || !strings.Contains(v.Str, "backups are not enabled") {
		t.Fatalf("got %+v", v)
	}
}

func TestScheduledBackupSkipsWhenUnchanged(t *testing.T) {
	bdir := filepath.Join(t.TempDir(), "b")
	e := start(t, Config{BackupDir: bdir})
	c := e.dial(t)
	c.do("SET", "a", "1")

	e.srv.backupIfChanged()
	if n := len(backupFiles(t, bdir)); n != 1 {
		t.Fatalf("first run should write a snapshot, have %d", n)
	}
	time.Sleep(3 * time.Millisecond)
	e.srv.backupIfChanged() // nothing changed
	if n := len(backupFiles(t, bdir)); n != 1 {
		t.Fatalf("unchanged data must not create another snapshot, have %d", n)
	}
	c.do("GET", "a") // reads are not changes
	e.srv.backupIfChanged()
	if n := len(backupFiles(t, bdir)); n != 1 {
		t.Fatalf("reads must not trigger snapshots, have %d", n)
	}
	c.do("SET", "a", "2")
	time.Sleep(3 * time.Millisecond)
	e.srv.backupIfChanged()
	if n := len(backupFiles(t, bdir)); n != 2 {
		t.Fatalf("a write should trigger a new snapshot, have %d", n)
	}
}

// ---- expiry ----

func TestSetOptionsAndTTL(t *testing.T) {
	e := start(t, Config{})
	c := e.dial(t)

	expect(t, c.do("SET", "k", "v", "EX", "10"), "OK")
	expect(t, c.do("TTL", "k"), "(integer) 10")
	expect(t, c.do("PTTL", "k"), "(integer) 10000")
	e.clk.Advance(4 * time.Second)
	expect(t, c.do("TTL", "k"), "(integer) 6")
	expect(t, c.do("GET", "k"), `"v"`)
	e.clk.Advance(6 * time.Second) // exactly the deadline
	expect(t, c.do("GET", "k"), "(nil)")
	expect(t, c.do("TTL", "k"), "(integer) -2")
	expect(t, c.do("EXISTS", "k"), "(integer) 0")
	expect(t, c.do("DBSIZE"), "(integer) 0")

	expect(t, c.do("SET", "ms", "v", "px", "1500"), "OK") // options are case-insensitive
	expect(t, c.do("PTTL", "ms"), "(integer) 1500")
	expect(t, c.do("TTL", "ms"), "(integer) 2") // rounded like Redis

	expect(t, c.do("SET", "plain", "v"), "OK")
	expect(t, c.do("TTL", "plain"), "(integer) -1")
	c.do("SET", "plain", "v", "EX", "5")
	c.do("SET", "plain", "v2") // overwriting clears the expiry
	expect(t, c.do("TTL", "plain"), "(integer) -1")

	// NX / XX
	expect(t, c.do("SET", "nx", "1", "NX"), "OK")
	expect(t, c.do("SET", "nx", "2", "NX"), "(nil)")
	expect(t, c.do("GET", "nx"), `"1"`)
	expect(t, c.do("SET", "missing", "1", "XX"), "(nil)")
	expect(t, c.do("EXISTS", "missing"), "(integer) 0")
	expect(t, c.do("SET", "nx", "3", "XX"), "OK")
	expect(t, c.do("SET", "lock", "me", "NX", "EX", "30"), "OK")
	expect(t, c.do("SET", "lock", "you", "NX", "EX", "30"), "(nil)")
	e.clk.Advance(31 * time.Second)
	expect(t, c.do("SET", "lock", "you", "NX", "EX", "30"), "OK") // old lock expired

	// bad arguments leave the connection healthy
	expect(t, c.do("SET", "k", "v", "EX", "0"), "(error) ERR invalid expire time in 'set' command")
	expect(t, c.do("SET", "k", "v", "EX", "-5"), "(error) ERR invalid expire time in 'set' command")
	expect(t, c.do("SET", "k", "v", "EX", "abc"), "(error) ERR value is not an integer or out of range")
	expect(t, c.do("SET", "k", "v", "EX", "99999999999999"), "(error) ERR invalid expire time in 'set' command")
	expect(t, c.do("SET", "k", "v", "EX"), "(error) ERR syntax error")
	expect(t, c.do("SET", "k", "v", "EX", "5", "PX", "5"), "(error) ERR syntax error")
	expect(t, c.do("SET", "k", "v", "NX", "XX"), "(error) ERR syntax error")
	expect(t, c.do("EXISTS", "k"), "(integer) 0")
	expect(t, c.do("PING"), "PONG")
}

func TestSetexSetnx(t *testing.T) {
	e := start(t, Config{})
	c := e.dial(t)
	expect(t, c.do("SETEX", "a", "10", "v"), "OK")
	expect(t, c.do("TTL", "a"), "(integer) 10")
	expect(t, c.do("PSETEX", "b", "2500", "v"), "OK")
	expect(t, c.do("PTTL", "b"), "(integer) 2500")
	expect(t, c.do("SETEX", "a", "0", "v"), "(error) ERR invalid expire time in 'setex' command")
	expect(t, c.do("SETEX", "a", "10"), "(error) ERR wrong number of arguments for 'setex' command")
	expect(t, c.do("SETNX", "n", "1"), "(integer) 1")
	expect(t, c.do("SETNX", "n", "2"), "(integer) 0")
	expect(t, c.do("GET", "n"), `"1"`)
}

func TestExpireFamily(t *testing.T) {
	e := start(t, Config{})
	c := e.dial(t)
	now := e.clk.Now()

	expect(t, c.do("EXPIRE", "missing", "10"), "(integer) 0")
	c.do("SET", "k", "v")
	expect(t, c.do("EXPIRE", "k", "10"), "(integer) 1")
	expect(t, c.do("TTL", "k"), "(integer) 10")
	expect(t, c.do("GET", "k"), `"v"`)
	expect(t, c.do("PEXPIRE", "k", "2500"), "(integer) 1")
	expect(t, c.do("PTTL", "k"), "(integer) 2500")
	expect(t, c.do("EXPIREAT", "k", fmt.Sprint(now.Unix()+100)), "(integer) 1")
	expect(t, c.do("TTL", "k"), "(integer) 100")
	expect(t, c.do("PEXPIREAT", "k", fmt.Sprint(now.UnixMilli()+4000)), "(integer) 1")
	expect(t, c.do("PTTL", "k"), "(integer) 4000")

	expect(t, c.do("PERSIST", "k"), "(integer) 1")
	expect(t, c.do("TTL", "k"), "(integer) -1")
	expect(t, c.do("PERSIST", "k"), "(integer) 0")
	expect(t, c.do("PERSIST", "missing"), "(integer) 0")
	expect(t, c.do("GET", "k"), `"v"`)

	// A time in the past deletes the key, as in Redis.
	expect(t, c.do("EXPIRE", "k", "-1"), "(integer) 1")
	expect(t, c.do("EXISTS", "k"), "(integer) 0")
	c.do("SET", "k2", "v")
	expect(t, c.do("EXPIREAT", "k2", "1"), "(integer) 1")
	expect(t, c.do("EXISTS", "k2"), "(integer) 0")

	c.do("SET", "k3", "v")
	expect(t, c.do("EXPIRE", "k3", "abc"), "(error) ERR value is not an integer or out of range")
	expect(t, c.do("EXPIRE", "k3", "99999999999999"), "(error) ERR invalid expire time in 'expire' command")
	expect(t, c.do("EXPIREAT", "k3", "9223372036854775807"), "(error) ERR invalid expire time in 'expireat' command")
	expect(t, c.do("EXPIRE", "k3"), "(error) ERR wrong number of arguments for 'expire' command")
	expect(t, c.do("EXPIRE", "k3", "10", "NX"), "(error) ERR syntax error (EXPIRE options NX/XX/GT/LT are not supported)")
	expect(t, c.do("GET", "k3"), `"v"`)
}

func TestIncrKeepsTTLOverTheWire(t *testing.T) {
	e := start(t, Config{})
	c := e.dial(t)
	c.do("SET", "hits", "5", "EX", "10")
	expect(t, c.do("INCR", "hits"), "(integer) 6")
	expect(t, c.do("INCRBY", "hits", "4"), "(integer) 10")
	expect(t, c.do("TTL", "hits"), "(integer) 10")
	e.clk.Advance(11 * time.Second)
	expect(t, c.do("INCR", "hits"), "(integer) 1") // expired, so it restarts
	expect(t, c.do("TTL", "hits"), "(integer) -1")
}

func TestPurgeAndInfo(t *testing.T) {
	e := start(t, Config{})
	c := e.dial(t)
	for i := 0; i < 20; i++ {
		c.do("SET", fmt.Sprint("t", i), "x", "EX", "5")
	}
	c.do("SET", "keep", "x")
	c.do("SET", "later", "x", "EX", "3600")
	if info := c.do("INFO").Str; !strings.Contains(info, "keys:22\r\n") || !strings.Contains(info, "expiring_keys:21\r\n") {
		t.Fatalf("INFO:\n%s", info)
	}
	e.clk.Advance(10 * time.Second)
	// Expired keys are already hidden even before they are purged.
	expect(t, c.do("DBSIZE"), "(integer) 2")
	if info := c.do("INFO").Str; !strings.Contains(info, "keys:2\r\n") || !strings.Contains(info, "expiring_keys:1\r\n") {
		t.Fatalf("INFO after expiry:\n%s", info)
	}
	e.srv.purgeExpired() // writes tombstones for the 20 expired keys
	expect(t, c.do("DBSIZE"), "(integer) 2")
	expect(t, c.do("KEYS", "*"), "1) \"keep\"\n2) \"later\"")
	// Their records and tombstones are now pure waste, and COMPACT reclaims it.
	v := c.do("COMPACT")
	if v.Kind != ':' || v.Int <= 0 {
		t.Fatalf("COMPACT should reclaim the expired records, got %+v", v)
	}
	if d := e.db.Stats().DeadBytes; d != 0 {
		t.Fatalf("dead bytes after compact: %d", d)
	}
	expect(t, c.do("TTL", "later"), "(integer) 3590") // 3600s minus the 10s we advanced
}

// ---- MSET and SCAN ----

func TestMset(t *testing.T) {
	c := start(t, Config{}).dial(t)
	expect(t, c.do("MSET", "a", "1", "b", "2", "a", "3"), "OK")
	expect(t, c.do("MGET", "a", "b", "c"), "1) \"3\"\n2) \"2\"\n3) (nil)")
	expect(t, c.do("MSET", "a", "1", "b"), "(error) ERR wrong number of arguments for 'mset' command")
	expect(t, c.do("MSET"), "(error) ERR wrong number of arguments for 'mset' command")
	expect(t, c.do("MSET", "x", "1", "", "2"), "(error) ERR empty keys are not supported")
	expect(t, c.do("EXISTS", "x"), "(integer) 0") // the whole batch was rejected
	expect(t, c.do("DBSIZE"), "(integer) 2")
}

// scanAll walks SCAN pages, returning every key and the number of pages.
func scanAll(t *testing.T, c *client, extra ...string) ([]string, int) {
	t.Helper()
	cursor, pages := "0", 0
	var all []string
	for {
		v := c.do(append([]string{"SCAN", cursor}, extra...)...)
		if v.Kind != '*' || len(v.Arr) != 2 {
			t.Fatalf("SCAN reply: %+v", v)
		}
		cursor = v.Arr[0].Str
		for _, k := range v.Arr[1].Arr {
			all = append(all, k.Str)
		}
		pages++
		if cursor == "0" {
			return all, pages
		}
		if pages > 10000 {
			t.Fatal("SCAN never finished")
		}
	}
}

func TestScanCommand(t *testing.T) {
	e := start(t, Config{})
	c := e.dial(t)
	for i := 0; i < 100; i++ {
		c.do("SET", fmt.Sprintf("user:%03d", i), "x")
		c.do("SET", fmt.Sprintf("item:%03d", i), "y")
	}
	c.do("SET", "temp", "t", "EX", "5")
	e.clk.Advance(10 * time.Second)

	all, pages := scanAll(t, c, "COUNT", "25")
	if len(all) != 200 || pages < 8 {
		t.Fatalf("got %d keys in %d pages", len(all), pages)
	}
	seen := map[string]bool{}
	for _, k := range all {
		if seen[k] {
			t.Fatalf("duplicate %s with no concurrent writes", k)
		}
		seen[k] = true
	}
	if seen["temp"] {
		t.Fatal("expired key returned")
	}

	users, _ := scanAll(t, c, "MATCH", "user:*", "COUNT", "30")
	if len(users) != 100 {
		t.Fatalf("MATCH returned %d keys", len(users))
	}
	for _, k := range users {
		if !strings.HasPrefix(k, "user:") {
			t.Fatalf("MATCH leaked %s", k)
		}
	}
	none, _ := scanAll(t, c, "MATCH", "nomatch*")
	if len(none) != 0 {
		t.Fatalf("expected none, got %v", none)
	}

	// default page, lowercase options, argument errors
	v := c.do("scan", "0")
	if v.Kind != '*' || len(v.Arr[1].Arr) != 10 {
		t.Fatalf("default COUNT should be 10: %+v", v)
	}
	expect(t, c.do("SCAN"), "(error) ERR wrong number of arguments for 'scan' command")
	expect(t, c.do("SCAN", "abc"), "(error) ERR invalid cursor")
	expect(t, c.do("SCAN", "-1"), "(error) ERR invalid cursor")
	expect(t, c.do("SCAN", "0", "COUNT", "0"), "(error) ERR syntax error")
	expect(t, c.do("SCAN", "0", "COUNT", "x"), "(error) ERR value is not an integer or out of range")
	expect(t, c.do("SCAN", "0", "COUNT"), "(error) ERR syntax error")
	expect(t, c.do("SCAN", "0", "TYPE", "string"), "(error) ERR syntax error (supported: MATCH, COUNT)")
	// A made-up cursor must not break anything.
	if v := c.do("SCAN", "123456789", "COUNT", "5"); v.Kind == '-' {
		t.Fatalf("forged cursor: %+v", v)
	}
	expect(t, c.do("PING"), "PONG")
}
