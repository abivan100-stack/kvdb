// Package server exposes a kvdb.Store over TCP using the Redis protocol, so
// any Redis client library (and redis-cli) can talk to it.
package server

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kvdb"
	"kvdb/internal/resp"
)

const Version = "0.4.0"

// Config controls the server. The zero value is usable: no password, no
// client limit, no idle timeout, no background compaction.
type Config struct {
	Password       string        // if set, clients must AUTH first
	MaxClients     int           // 0 = unlimited
	IdleTimeout    time.Duration // 0 = never disconnect idle clients
	CompactEvery   time.Duration // 0 = no background compaction
	CompactMinDead int64         // only compact when at least this many bytes are wasted
	PurgeEvery     time.Duration // 0 = never purge expired keys in the background
	BackupDir      string        // where snapshots go; empty = backups disabled
	BackupEvery    time.Duration // 0 = only on demand (BACKUP command and shutdown)
	BackupKeep     int           // newest snapshots to keep; 0 = keep all
	Logf           func(format string, args ...any)
}

type Server struct {
	db    *kvdb.Store
	cfg   Config
	start time.Time

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup // connection handlers
	bg     sync.WaitGroup // background compactor
	stop   chan struct{}
	once   sync.Once

	cmds   atomic.Int64
	nextID atomic.Int64

	backupMu      sync.Mutex
	backedUp      bool
	lastBackupVer uint64
}

// connState is what the server remembers about one client connection.
type connState struct {
	id     int64
	authed bool
	proto  int // 2 (default) or 3, chosen with HELLO
}

func (st *connState) null(w *bufio.Writer) {
	if st.proto == 3 {
		resp.WriteNull3(w)
	} else {
		resp.WriteNull(w)
	}
}

func New(db *kvdb.Store, cfg Config) *Server {
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	return &Server{
		db:    db,
		cfg:   cfg,
		start: time.Now(),
		conns: make(map[net.Conn]struct{}),
		stop:  make(chan struct{}),
	}
}

// Serve accepts connections until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return nil
	}
	s.ln = ln
	s.mu.Unlock()

	s.once.Do(func() {
		if s.cfg.CompactEvery > 0 {
			s.every(s.cfg.CompactEvery, s.maybeCompact)
		}
		if s.cfg.PurgeEvery > 0 {
			s.every(s.cfg.PurgeEvery, s.purgeExpired)
		}
		if s.cfg.BackupDir != "" && s.cfg.BackupEvery > 0 {
			s.every(s.cfg.BackupEvery, s.backupIfChanged)
		}
	})

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.cfg.Logf("accept: %v", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return nil
		}
		if s.cfg.MaxClients > 0 && len(s.conns) >= s.cfg.MaxClients {
			s.mu.Unlock()
			conn.SetWriteDeadline(time.Now().Add(time.Second))
			conn.Write([]byte("-ERR max number of clients reached\r\n"))
			conn.Close()
			continue
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(conn)
	}
}

// Close stops accepting, disconnects clients and waits for them to finish.
// It does not close the database; the caller owns that.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.ln != nil {
		s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	close(s.stop)
	s.wg.Wait()
	s.bg.Wait()
	return nil
}

func (s *Server) clients() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// every runs fn on a timer in the background until the server closes.
func (s *Server) every(d time.Duration, fn func()) {
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		t := time.NewTicker(d)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				fn()
			}
		}
	}()
}

// purgeExpired removes expired keys from memory and writes tombstones for them.
// Expired keys are already invisible to clients; this just frees their space.
func (s *Server) purgeExpired() {
	if _, err := s.db.PurgeExpired(); err != nil {
		s.cfg.Logf("purging expired keys failed: %v", err)
	}
}

// maybeCompact compacts when most of the file is wasted space.
func (s *Server) maybeCompact() {
	st := s.db.Stats()
	if st.DeadBytes < s.cfg.CompactMinDead || st.DeadBytes <= st.LiveBytes {
		return
	}
	cs, err := s.db.Compact()
	if err != nil {
		s.cfg.Logf("auto-compaction failed: %v", err)
		return
	}
	s.cfg.Logf("auto-compaction: %d -> %d bytes", cs.BeforeBytes, cs.AfterBytes)
}

var errBackupsOff = errors.New("backups are not enabled (start the server with -backup-dir)")

// BackupNow writes a snapshot of the database into BackupDir and removes the
// oldest snapshots beyond BackupKeep. It returns the new file's path.
func (s *Server) BackupNow() (string, error) {
	if s.cfg.BackupDir == "" {
		return "", errBackupsOff
	}
	s.backupMu.Lock()
	defer s.backupMu.Unlock()

	ver := s.db.Version()
	if err := os.MkdirAll(s.cfg.BackupDir, 0o755); err != nil {
		return "", err
	}
	name := "kvdb-" + time.Now().UTC().Format("20060102T150405.000") + "Z.db"
	path := filepath.Join(s.cfg.BackupDir, name)
	if _, err := s.db.Backup(path); err != nil {
		return "", err
	}
	s.backedUp, s.lastBackupVer = true, ver
	s.pruneBackups()
	return path, nil
}

// backupIfChanged is the scheduled job: it skips the snapshot when nothing
// has been written since the last one.
func (s *Server) backupIfChanged() {
	s.backupMu.Lock()
	unchanged := s.backedUp && s.db.Version() == s.lastBackupVer
	s.backupMu.Unlock()
	if unchanged {
		return
	}
	if path, err := s.BackupNow(); err != nil {
		s.cfg.Logf("backup failed: %v", err)
	} else {
		s.cfg.Logf("backup written: %s", path)
	}
}

// pruneBackups deletes the oldest snapshots. File names sort by time.
func (s *Server) pruneBackups() {
	if s.cfg.BackupKeep <= 0 {
		return
	}
	entries, err := os.ReadDir(s.cfg.BackupDir)
	if err != nil {
		return
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() && strings.HasPrefix(n, "kvdb-") && strings.HasSuffix(n, ".db") {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for len(names) > s.cfg.BackupKeep {
		os.Remove(filepath.Join(s.cfg.BackupDir, names[0]))
		names = names[1:]
	}
}

func (s *Server) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()
	r := bufio.NewReaderSize(conn, 64<<10)
	w := bufio.NewWriterSize(conn, 64<<10)
	st := &connState{id: s.nextID.Add(1), authed: s.cfg.Password == "", proto: 2}

	for {
		if s.cfg.IdleTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		}
		args, err := resp.ReadCommand(r)
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				resp.WriteError(w, "ERR Protocol error: "+err.Error())
				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				w.Flush()
			}
			return
		}
		quit := s.exec(w, args, st)
		// Pipelined requests share one flush.
		if quit || r.Buffered() == 0 {
			conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			if err := w.Flush(); err != nil || quit {
				return
			}
		}
	}
}

var (
	errNotInt   = "ERR value is not an integer or out of range"
	errOverflow = "ERR increment or decrement would overflow"
)

func (s *Server) dbErr(w *bufio.Writer, err error) {
	switch {
	case errors.Is(err, kvdb.ErrEmptyKey):
		resp.WriteError(w, "ERR empty keys are not supported")
	case errors.Is(err, kvdb.ErrKeyTooLarge):
		resp.WriteError(w, "ERR key too large")
	case errors.Is(err, kvdb.ErrValueTooLarge):
		resp.WriteError(w, "ERR value too large")
	case errors.Is(err, kvdb.ErrCorrupt):
		s.cfg.Logf("corrupt record: %v", err)
		resp.WriteError(w, "ERR data corruption detected")
	default:
		s.cfg.Logf("database error: %v", err)
		resp.WriteError(w, "ERR internal error")
	}
}

func (s *Server) checkPassword(given []byte) bool {
	a := sha256.Sum256(given)
	b := sha256.Sum256([]byte(s.cfg.Password))
	return subtle.ConstantTimeCompare(a[:], b[:]) == 1
}

// exec runs one command and writes its reply. It returns true if the client
// asked to disconnect.
func (s *Server) exec(w *bufio.Writer, args [][]byte, st *connState) (quit bool) {
	s.cmds.Add(1)
	name := strings.ToLower(string(args[0]))
	if len(name) > 64 {
		name = name[:64]
	}

	if !st.authed && name != "auth" && name != "quit" && name != "hello" {
		resp.WriteError(w, "NOAUTH Authentication required.")
		return false
	}
	wrong := func() {
		resp.WriteError(w, fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
	}
	n := len(args)

	switch name {
	case "auth":
		if n != 2 && n != 3 {
			wrong()
			return
		}
		if s.cfg.Password == "" {
			resp.WriteError(w, "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
			return
		}
		if s.checkPassword(args[n-1]) {
			st.authed = true
			resp.WriteSimple(w, "OK")
		} else {
			time.Sleep(100 * time.Millisecond) // slow down guessing
			resp.WriteError(w, "WRONGPASS invalid username-password pair or user is disabled.")
		}

	case "quit":
		resp.WriteSimple(w, "OK")
		return true

	case "ping":
		switch n {
		case 1:
			resp.WriteSimple(w, "PONG")
		case 2:
			resp.WriteBulk(w, args[1])
		default:
			wrong()
		}

	case "echo":
		if n != 2 {
			wrong()
			return
		}
		resp.WriteBulk(w, args[1])

	case "set":
		if n < 3 {
			wrong()
			return
		}
		s.set(w, args, st)

	case "setnx":
		if n != 3 {
			wrong()
			return
		}
		ok, err := s.db.Set(string(args[1]), args[2], kvdb.SetOptions{IfNotExists: true})
		if err != nil {
			s.dbErr(w, err)
			return
		}
		resp.WriteInt(w, b2i(ok))

	case "setex", "psetex":
		if n != 4 {
			wrong()
			return
		}
		unit := time.Second
		if name == "psetex" {
			unit = time.Millisecond
		}
		d, ok := s.ttlArg(w, name, args[2], unit)
		if !ok {
			return
		}
		if _, err := s.db.Set(string(args[1]), args[3], kvdb.SetOptions{ExpireAt: s.db.Now().Add(d)}); err != nil {
			s.dbErr(w, err)
			return
		}
		resp.WriteSimple(w, "OK")

	case "expire", "pexpire", "expireat", "pexpireat":
		if n != 3 {
			if n > 3 {
				resp.WriteError(w, "ERR syntax error (EXPIRE options NX/XX/GT/LT are not supported)")
			} else {
				wrong()
			}
			return
		}
		unit := time.Second
		if strings.HasPrefix(name, "p") {
			unit = time.Millisecond
		}
		s.expire(w, name, string(args[1]), args[2], unit, strings.HasSuffix(name, "at"))

	case "ttl", "pttl":
		if n != 2 {
			wrong()
			return
		}
		at, has, exists := s.db.ExpiresAt(string(args[1]))
		switch {
		case !exists:
			resp.WriteInt(w, -2)
		case !has:
			resp.WriteInt(w, -1)
		default:
			ms := at.Sub(s.db.Now()).Milliseconds()
			if ms < 0 {
				ms = 0
			}
			if name == "ttl" {
				ms = (ms + 500) / 1000 // seconds, rounded like Redis
			}
			resp.WriteInt(w, ms)
		}

	case "persist":
		if n != 2 {
			wrong()
			return
		}
		ok, err := s.db.Persist(string(args[1]))
		if err != nil {
			s.dbErr(w, err)
			return
		}
		resp.WriteInt(w, b2i(ok))

	case "get":
		if n != 2 {
			wrong()
			return
		}
		v, err := s.db.Get(string(args[1]))
		switch {
		case errors.Is(err, kvdb.ErrNotFound):
			st.null(w)
		case err != nil:
			s.dbErr(w, err)
		default:
			resp.WriteBulk(w, v)
		}

	case "mset":
		if n < 3 || n%2 != 1 {
			wrong()
			return
		}
		keys := make([]string, 0, (n-1)/2)
		vals := make([][]byte, 0, (n-1)/2)
		for i := 1; i < n; i += 2 {
			keys = append(keys, string(args[i]))
			vals = append(vals, args[i+1])
		}
		if err := s.db.PutMany(keys, vals); err != nil {
			if errors.Is(err, kvdb.ErrBatchTooLarge) {
				resp.WriteError(w, "ERR batch too large")
			} else {
				s.dbErr(w, err)
			}
			return
		}
		resp.WriteSimple(w, "OK")

	case "scan":
		s.scan(w, args)

	case "mget":
		if n < 2 {
			wrong()
			return
		}
		out := make([][]byte, n-1)
		present := make([]bool, n-1)
		for i, k := range args[1:] {
			v, err := s.db.Get(string(k))
			if err == nil {
				out[i], present[i] = v, true
			} else if !errors.Is(err, kvdb.ErrNotFound) {
				s.dbErr(w, err)
				return
			}
		}
		resp.WriteArrayHeader(w, n-1)
		for i := range out {
			if present[i] {
				resp.WriteBulk(w, out[i])
			} else {
				st.null(w)
			}
		}

	case "del":
		if n < 2 {
			wrong()
			return
		}
		var count int64
		for _, k := range args[1:] {
			existed, err := s.db.Remove(string(k))
			if err != nil {
				s.dbErr(w, err)
				return
			}
			if existed {
				count++
			}
		}
		resp.WriteInt(w, count)

	case "exists":
		if n < 2 {
			wrong()
			return
		}
		var count int64
		for _, k := range args[1:] {
			if s.db.Has(string(k)) {
				count++
			}
		}
		resp.WriteInt(w, count)

	case "incr", "decr":
		if n != 2 {
			wrong()
			return
		}
		d := int64(1)
		if name == "decr" {
			d = -1
		}
		s.incr(w, string(args[1]), d)

	case "incrby", "decrby":
		if n != 3 {
			wrong()
			return
		}
		d, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			resp.WriteError(w, errNotInt)
			return
		}
		if name == "decrby" {
			if d == math.MinInt64 {
				resp.WriteError(w, errOverflow)
				return
			}
			d = -d
		}
		s.incr(w, string(args[1]), d)

	case "keys":
		if n != 2 {
			wrong()
			return
		}
		pat := string(args[1])
		var matches []string
		for _, k := range s.db.Keys() {
			if pat == "*" || globMatch(pat, k) {
				matches = append(matches, k)
			}
		}
		sort.Strings(matches)
		resp.WriteArrayHeader(w, len(matches))
		for _, k := range matches {
			resp.WriteBulk(w, []byte(k))
		}

	case "dbsize":
		resp.WriteInt(w, int64(s.db.Len()))

	case "backup":
		path, err := s.BackupNow()
		switch {
		case errors.Is(err, errBackupsOff):
			resp.WriteError(w, "ERR "+err.Error())
		case err != nil:
			s.dbErr(w, err)
		default:
			resp.WriteBulk(w, []byte(filepath.Base(path)))
		}

	case "compact":
		cs, err := s.db.Compact()
		if err != nil {
			s.dbErr(w, err)
			return
		}
		resp.WriteInt(w, cs.BeforeBytes-cs.AfterBytes) // bytes reclaimed

	case "info":
		ds := s.db.Stats()
		info := fmt.Sprintf("# Server\r\nkvdb_version:%s\r\nuptime_in_seconds:%d\r\n"+
			"# Clients\r\nconnected_clients:%d\r\n"+
			"# Stats\r\ntotal_commands_processed:%d\r\n"+
			"# Keyspace\r\nkeys:%d\r\nexpiring_keys:%d\r\nfile_size_bytes:%d\r\nlive_bytes:%d\r\ndead_bytes:%d\r\n",
			Version, int(time.Since(s.start).Seconds()), s.clients(), s.cmds.Load(),
			ds.Keys, ds.Expiring, ds.FileSize, ds.LiveBytes, ds.DeadBytes)
		resp.WriteBulk(w, []byte(info))

	// Commands client libraries send during connection setup.
	case "client":
		resp.WriteSimple(w, "OK")
	case "command":
		resp.WriteArrayHeader(w, 0)
	case "select":
		if n == 2 && string(args[1]) == "0" {
			resp.WriteSimple(w, "OK")
		} else {
			resp.WriteError(w, "ERR DB index is out of range (kvdb has a single database)")
		}
	case "hello":
		s.hello(w, args, st)

	default:
		resp.WriteError(w, fmt.Sprintf("ERR unknown command '%s'", name))
	}
	return false
}

func (s *Server) incr(w *bufio.Writer, key string, delta int64) {
	v, err := s.db.Incr(key, delta)
	switch {
	case errors.Is(err, kvdb.ErrNotInteger):
		resp.WriteError(w, errNotInt)
	case errors.Is(err, kvdb.ErrOverflow):
		resp.WriteError(w, errOverflow)
	case err != nil:
		s.dbErr(w, err)
	default:
		resp.WriteInt(w, v)
	}
}

// scan handles SCAN cursor [MATCH pattern] [COUNT n].
func (s *Server) scan(w *bufio.Writer, args [][]byte) {
	if len(args) < 2 {
		resp.WriteError(w, "ERR wrong number of arguments for 'scan' command")
		return
	}
	cursor, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || cursor < 0 {
		resp.WriteError(w, "ERR invalid cursor")
		return
	}
	pattern, count := "", 10
	for i := 2; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "match":
			if i+1 >= len(args) {
				resp.WriteError(w, "ERR syntax error")
				return
			}
			pattern = string(args[i+1])
			i++
		case "count":
			if i+1 >= len(args) {
				resp.WriteError(w, "ERR syntax error")
				return
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				resp.WriteError(w, errNotInt)
				return
			}
			if n < 1 {
				resp.WriteError(w, "ERR syntax error")
				return
			}
			count = min(n, 10000)
			i++
		default:
			resp.WriteError(w, "ERR syntax error (supported: MATCH, COUNT)")
			return
		}
	}
	keys, next, err := s.db.Scan(cursor, count)
	if err != nil {
		s.dbErr(w, err)
		return
	}
	resp.WriteArrayHeader(w, 2)
	resp.WriteBulk(w, []byte(strconv.FormatInt(next, 10)))
	matched := keys[:0]
	for _, k := range keys {
		if pattern == "" || pattern == "*" || globMatch(pattern, k) {
			matched = append(matched, k)
		}
	}
	resp.WriteArrayHeader(w, len(matched))
	for _, k := range matched {
		resp.WriteBulk(w, []byte(k))
	}
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// maxTTL bounds expiry times so the arithmetic below cannot overflow.
const maxTTL = 100 * 365 * 24 * time.Hour

// ttlArg parses a positive duration argument in the given unit.
func (s *Server) ttlArg(w *bufio.Writer, cmd string, raw []byte, unit time.Duration) (time.Duration, bool) {
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		resp.WriteError(w, errNotInt)
		return 0, false
	}
	if n <= 0 || n > int64(maxTTL/unit) {
		resp.WriteError(w, fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
		return 0, false
	}
	return time.Duration(n) * unit, true
}

// set handles SET key value [EX seconds | PX milliseconds] [NX | XX].
func (s *Server) set(w *bufio.Writer, args [][]byte, st *connState) {
	var o kvdb.SetOptions
	haveExp := false
	for i := 3; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "nx":
			if o.IfNotExists || o.IfExists {
				resp.WriteError(w, "ERR syntax error")
				return
			}
			o.IfNotExists = true
		case "xx":
			if o.IfNotExists || o.IfExists {
				resp.WriteError(w, "ERR syntax error")
				return
			}
			o.IfExists = true
		case "ex", "px":
			if haveExp || i+1 >= len(args) {
				resp.WriteError(w, "ERR syntax error")
				return
			}
			unit := time.Second
			if strings.EqualFold(string(args[i]), "px") {
				unit = time.Millisecond
			}
			d, ok := s.ttlArg(w, "set", args[i+1], unit)
			if !ok {
				return
			}
			o.ExpireAt = s.db.Now().Add(d)
			haveExp = true
			i++
		default:
			resp.WriteError(w, "ERR syntax error (supported: EX, PX, NX, XX)")
			return
		}
	}
	written, err := s.db.Set(string(args[1]), args[2], o)
	switch {
	case err != nil:
		s.dbErr(w, err)
	case !written:
		st.null(w)
	default:
		resp.WriteSimple(w, "OK")
	}
}

// expire handles EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT. A time in the past
// deletes the key, as in Redis.
func (s *Server) expire(w *bufio.Writer, cmd, key string, raw []byte, unit time.Duration, absolute bool) {
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		resp.WriteError(w, errNotInt)
		return
	}
	now := s.db.Now()
	var at time.Time
	if absolute {
		if n > int64(math.MaxInt64/2/int64(unit)) {
			resp.WriteError(w, fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
			return
		}
		if n < 0 {
			n = 0
		}
		at = time.UnixMilli(0).Add(time.Duration(n) * unit)
	} else {
		if n > int64(maxTTL/unit) {
			resp.WriteError(w, fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
			return
		}
		if n < 0 {
			n = 0
		}
		at = now.Add(time.Duration(n) * unit)
	}
	ok, err := s.db.Expire(key, at)
	if err != nil {
		s.dbErr(w, err)
		return
	}
	resp.WriteInt(w, b2i(ok))
}

// hello implements HELLO [protover [AUTH user pass] [SETNAME name]], the
// handshake modern client libraries send first. It selects RESP2 or RESP3.
func (s *Server) hello(w *bufio.Writer, args [][]byte, st *connState) {
	proto := st.proto
	i := 1
	if len(args) > 1 {
		v, err := strconv.Atoi(string(args[1]))
		if err != nil {
			resp.WriteError(w, "ERR Protocol version is not an integer or out of range")
			return
		}
		if v != 2 && v != 3 {
			resp.WriteError(w, "NOPROTO unsupported protocol version")
			return
		}
		proto = v
		i = 2
	}
	authedHere := false
	for ; i < len(args); i++ {
		opt := strings.ToLower(string(args[i]))
		switch {
		case opt == "auth" && i+2 < len(args):
			if s.cfg.Password == "" || !s.checkPassword(args[i+2]) {
				time.Sleep(100 * time.Millisecond)
				resp.WriteError(w, "WRONGPASS invalid username-password pair or user is disabled.")
				return
			}
			authedHere = true
			i += 2
		case opt == "setname" && i+1 < len(args):
			i++
		default:
			resp.WriteError(w, "ERR Syntax error in HELLO option '"+string(args[i])+"'")
			return
		}
	}
	if !st.authed && !authedHere {
		resp.WriteError(w, "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
		return
	}
	if authedHere {
		st.authed = true
	}
	st.proto = proto

	if proto == 3 {
		resp.WriteMapHeader(w, 7)
	} else {
		resp.WriteArrayHeader(w, 14)
	}
	str := func(k, v string) { resp.WriteBulk(w, []byte(k)); resp.WriteBulk(w, []byte(v)) }
	num := func(k string, v int64) { resp.WriteBulk(w, []byte(k)); resp.WriteInt(w, v) }
	str("server", "kvdb")
	str("version", Version)
	num("proto", int64(proto))
	num("id", st.id)
	str("mode", "standalone")
	str("role", "master")
	resp.WriteBulk(w, []byte("modules"))
	resp.WriteArrayHeader(w, 0)
}
