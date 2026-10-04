// kvserver runs kvdb as a network database. Any Redis client can connect.
//
//	go run ./cmd/kvserver                         # 127.0.0.1:6380, data/kvdb.db
//	go run ./cmd/kvserver -addr 0.0.0.0:6380      # reachable from other machines (needs a password)
//
// Set the password with the KVDB_PASSWORD environment variable (preferred) or -password.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"kvdb"
	"kvdb/internal/server"
)

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "address to listen on")
	dataPath := flag.String("data", filepath.Join("data", "kvdb.db"), "database file (created if missing)")
	password := flag.String("password", "", "require clients to AUTH with this password (or set KVDB_PASSWORD)")
	syncWrites := flag.Bool("sync", false, "fsync after every write: safest, but much slower")
	maxClients := flag.Int("max-clients", 1000, "maximum simultaneous connections")
	idle := flag.Duration("idle-timeout", 0, "disconnect clients idle for this long (0 = never)")
	compactEvery := flag.Duration("compact-every", 30*time.Second, "how often to check whether the file needs compacting (0 = never)")
	backupDir := flag.String("backup-dir", "", "write snapshots of the database into this folder")
	backupEvery := flag.Duration("backup-every", time.Hour, "with -backup-dir: how often to snapshot (skipped if nothing changed)")
	backupKeep := flag.Int("backup-keep", 7, "with -backup-dir: how many snapshots to keep")
	tlsCert := flag.String("tls-cert", "", "PEM certificate file: turns on TLS encryption (needs -tls-key)")
	tlsKey := flag.String("tls-key", "", "PEM private key file for -tls-cert")
	genCert := flag.Bool("gen-cert", false, "write a self-signed certificate and key (to -tls-cert/-tls-key, default kvdb-cert.pem and kvdb-key.pem), then exit")
	certHosts := flag.String("cert-hosts", "localhost,127.0.0.1,::1", "with -gen-cert: comma-separated names and IPs clients will connect to")
	flag.Parse()

	if *genCert {
		generateCert(*tlsCert, *tlsKey, *certHosts)
		return
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatal("-tls-cert and -tls-key must be given together")
	}

	pw := *password
	if pw == "" {
		pw = os.Getenv("KVDB_PASSWORD")
	}
	if !isLoopback(*addr) && pw == "" {
		log.Fatalf("refusing to listen on %s without a password: anyone who can reach it could read and delete your data.\n"+
			"Set KVDB_PASSWORD, or bind to 127.0.0.1 instead.", *addr)
	}

	if err := os.MkdirAll(filepath.Dir(*dataPath), 0o755); err != nil {
		log.Fatal(err)
	}
	db, err := kvdb.Open(*dataPath, kvdb.Options{SyncWrites: *syncWrites})
	if err != nil {
		log.Fatal(err)
	}
	st := db.Stats()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		db.Close()
		log.Fatal(err)
	}
	var tlsCfg *tls.Config
	if *tlsCert != "" {
		tlsCfg, err = server.LoadTLS(*tlsCert, *tlsKey)
		if err != nil {
			db.Close()
			log.Fatal(err)
		}
		ln = tls.NewListener(ln, tlsCfg)
	}
	srv := server.New(db, server.Config{
		Password:       pw,
		MaxClients:     *maxClients,
		IdleTimeout:    *idle,
		CompactEvery:   *compactEvery,
		CompactMinDead: 1 << 20,
		PurgeEvery:     10 * time.Second,
		BackupDir:      *backupDir,
		BackupEvery:    *backupEvery,
		BackupKeep:     *backupKeep,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()

	auth := "no password"
	if pw != "" {
		auth = "password required"
	}
	enc := "unencrypted"
	if tlsCfg != nil {
		enc = "TLS encrypted"
	}
	log.Printf("kvdb %s listening on %s (%s, %s)", server.Version, ln.Addr(), auth, enc)
	if *backupDir != "" {
		log.Printf("backups: every %v into %s (keeping %d, plus one at shutdown)", *backupEvery, absPath(*backupDir), *backupKeep)
	}
	if tlsCfg == nil && !isLoopback(*addr) {
		log.Printf("WARNING: traffic (including the password) is not encrypted. Use -tls-cert/-tls-key, a VPN, or an SSH tunnel.")
	}
	log.Printf("data file: %s (%d keys, %d bytes)", absPath(*dataPath), st.Keys, st.FileSize)
	if strings.HasPrefix(ln.Addr().String(), "127.") || strings.HasPrefix(ln.Addr().String(), "[::1]") {
		log.Printf("only this computer can connect. Press Ctrl+C to stop.")
	}

	select {
	case <-ctx.Done():
		log.Printf("shutting down...")
	case err := <-done:
		if err != nil {
			log.Printf("server stopped: %v", err)
		}
	}
	srv.Close()
	if *backupDir != "" {
		if path, err := srv.BackupNow(); err != nil {
			log.Printf("final backup failed: %v", err)
		} else {
			log.Printf("final backup written: %s", path)
		}
	}
	if err := db.Close(); err != nil {
		log.Fatalf("closing database: %v", err)
	}
	log.Printf("data saved. Bye.")
}

func absPath(p string) string {
	if a, err := filepath.Abs(p); err == nil {
		return a
	}
	return p
}

func generateCert(certPath, keyPath, hosts string) {
	if certPath == "" {
		certPath = "kvdb-cert.pem"
	}
	if keyPath == "" {
		keyPath = "kvdb-key.pem"
	}
	var names []string
	for _, h := range strings.Split(hosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			names = append(names, h)
		}
	}
	if err := server.GenerateSelfSigned(certPath, keyPath, names, 365*24*time.Hour); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Wrote %s and %s (valid for one year, for: %s)\n\n", certPath, keyPath, strings.Join(names, ", "))
	fmt.Printf("Start the server with:\n  kvserver -tls-cert %s -tls-key %s\n\n", certPath, keyPath)
	fmt.Printf("Connect with the shell:\n  kvsh -tls -ca %s\n\n", certPath)
	fmt.Printf("Or from Python (pip install redis):\n  redis.Redis(port=6380, ssl=True, ssl_ca_certs=%q)\n\n", certPath)
	fmt.Printf("Keep %s private. Anyone who has it can impersonate your server.\n", keyPath)
}
