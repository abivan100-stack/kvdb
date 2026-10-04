# kvdb

A key-value database written from scratch in Go. It runs as a **server** that
your applications connect to over the network, and it speaks the **Redis
protocol**, so existing Redis client libraries (Python, Node, Go, Java, ...)
and `redis-cli` work with it unchanged.

```
  your apps                      kvdb server                        disk
 ┌──────────────┐  TCP :6380   ┌──────────────────┐            ┌──────────────┐
 │ Python/Flask │ ───────────▶ │ auth, commands,  │  append    │ data/kvdb.db │
 │ Go, Node ... │ ◀─────────── │ index in RAM     │ ─────────▶ │ (one file)   │
 └──────────────┘   RESP       └──────────────────┘            └──────────────┘
```

Like Postgres, MySQL or Redis, the server keeps its data in files on the
machine it runs on. Every app that connects to that server shares the same data.

## Quick start

You need Go 1.22+ (https://go.dev/dl).

```
# terminal 1: start the database server
go run ./cmd/kvserver

# terminal 2: talk to it
go run ./cmd/kvsh
kvdb> set name vijay
kvdb> get name
kvdb> incr visits
kvdb> keys *
kvdb> info
```

The server prints where its data file is. Stop it with Ctrl+C; it saves and exits cleanly.

### From Python

```
pip install redis
python examples/python/demo.py
```

```python
import redis
db = redis.Redis(port=6380, decode_responses=True)
db.set("name", "vijay")
print(db.get("name"))
```

Any other language works the same way: use its Redis client and point it at
`127.0.0.1:6380`.

## Server options

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `127.0.0.1:6380` | Where to listen. Localhost means only this computer can connect. |
| `-data` | `data/kvdb.db` | Database file (created if missing). |
| `-password` | none | Clients must `AUTH`. Prefer the `KVDB_PASSWORD` environment variable, since flags show up in process lists. |
| `-sync` | off | fsync every write. Safest, but much slower. |
| `-max-clients` | 1000 | Connection limit. |
| `-idle-timeout` | 0 (off) | Disconnect clients idle this long. |
| `-compact-every` | 30s | How often to check whether the file needs compacting. |
| `-tls-cert`, `-tls-key` | none | Turn on TLS encryption (see below). |
| `-backup-dir` | none | Folder for snapshots (see Backups). |
| `-backup-every` | 1h | With `-backup-dir`: snapshot interval. Skipped if nothing changed. |
| `-backup-keep` | 7 | With `-backup-dir`: how many snapshots to keep. |

To let other machines connect, bind to a network address **and** set a password.
The server refuses to start on a non-local address without one:

```
KVDB_PASSWORD=choose-a-long-secret go run ./cmd/kvserver -addr 0.0.0.0:6380
```

### Encryption (TLS)

Without TLS, the password and all data cross the network as plain text. To
encrypt, make a certificate once and start the server with it:

```
go run ./cmd/kvserver -gen-cert                       # writes kvdb-cert.pem and kvdb-key.pem
KVDB_PASSWORD=choose-a-long-secret go run ./cmd/kvserver \
    -addr 0.0.0.0:6380 -tls-cert kvdb-cert.pem -tls-key kvdb-key.pem
```

The certificate is self-signed, so each client must be told to trust that file:

```
go run ./cmd/kvsh -addr myserver:6380 -tls -ca kvdb-cert.pem
```
```python
redis.Redis(host="myserver", port=6380, ssl=True, ssl_ca_certs="kvdb-cert.pem")
```

Connect using a name that is in the certificate: `-gen-cert` covers `localhost`,
`127.0.0.1` and `::1`; add yours with `-cert-hosts "myserver,192.168.1.20"`.
Keep `kvdb-key.pem` private. The server warns at startup if it listens on a
network address without TLS.

### Backups

```
go run ./cmd/kvserver -backup-dir backups -backup-every 30m -backup-keep 10
```

The server writes a consistent snapshot while it keeps running (readers are not
blocked; writers wait for the copy), keeps the newest 10, skips a snapshot when
nothing has changed, and takes a final one when you stop it. `BACKUP` takes one on
demand and returns the file name. Snapshots contain only live data, so they are
also compact. Each one is a normal database file.

**Restore:** stop the server, copy a snapshot over the data file, start again.

```
copy backups\kvdb-20261004T112652.510Z.db data\kvdb.db
```

A backup on the same disk does not protect against that disk dying. Copy the
backup folder somewhere else (another drive, cloud storage) as well.

## Supported commands

| Group | Commands |
|---|---|
| Basics | `PING` `ECHO` `SET` `GET` `MSET` `MGET` `DEL` `EXISTS` `KEYS pattern` `SCAN cursor [MATCH p] [COUNT n]` `DBSIZE` |
| Counters (atomic) | `INCR` `DECR` `INCRBY` `DECRBY` |
| Expiry | `SET key value [EX s \| PX ms] [NX \| XX]` `SETEX` `PSETEX` `SETNX` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Connection | `AUTH` `HELLO` `SELECT 0` `QUIT` `INFO` |
| kvdb only | `COMPACT` rewrites the file keeping only current values and returns the bytes reclaimed. `BACKUP` takes a snapshot (needs `-backup-dir`). |

### Expiry

Keys can expire, which is what you want for sessions, caches and short-lived locks:

```python
db.set("session:42", "alice", ex=3600)          # gone after an hour
db.set("lock:job", "worker-1", nx=True, ex=30)  # only if nobody holds the lock
db.ttl("session:42")                            # seconds left; -1 = never, -2 = missing
db.incr("hits")                                 # INCR keeps a key's expiry
```

An expired key is invisible immediately (reads, `EXISTS`, `KEYS`, `DBSIZE`). A
background job deletes expired keys every 10 seconds to free memory, and compaction
reclaims their disk space. Deadlines are stored in the file, so they survive restarts
and crashes. A time that has already passed deletes the key, as in Redis. Expiry uses
the server's clock; if you change the system time, deadlines move with it.

**File format note:** expiry adds a new record type. Files written before it open
unchanged, but a file that contains expiring keys cannot be read by an older kvdb.

### Listing keys: `SCAN` vs `KEYS`

`KEYS pattern` checks every key in one go and holds up other clients while it runs.
`SCAN` returns a page at a time (redis-py: `db.scan_iter(match="user:*")`), so the
server stays responsive. It walks the data file in order, and promises that any key
that exists for the whole iteration is returned at least once, even if other clients
write, delete or compact meanwhile. A key overwritten during the iteration can show
up twice, so make your loop tolerate duplicates.

`MSET` writes all its keys in one step that no other client can see half-done, and
rejects the whole call if any entry is invalid. It is not crash-atomic: if the power
fails in the middle of the disk write, the first part of the batch can survive.

Not supported: `EXPIRE` options (`NX`/`XX`/`GT`/`LT`), `GET`/`KEEPTTL`/`EXAT`
options on `SET`, lists/hashes/sets, transactions, pub/sub, replication.
Values are arbitrary bytes up to 64 MB.

## How it stays safe

- Every record carries a CRC32. A half-written record from a crash or power cut
  is detected on startup and cut off; everything before it survives.
- Reads verify the checksum, so silent disk corruption is reported, not returned.
- Compaction writes a new file, fsyncs it, then atomically renames it over the
  old one. A crash at any point leaves a valid database.
- `INCR` and friends are atomic across clients.
- Malformed network input cannot crash the server; oversized requests are refused.

## Layout

```
store.go                  the storage engine (append-only log + in-memory index, expiry,
                          compaction, backup, scan)
internal/resp/            Redis wire protocol
internal/server/          TCP server and command handlers
cmd/kvserver/             the database server
cmd/kvsh/                 interactive client (like redis-cli)
cmd/kvweb/                web page that visualises the file as it grows and compacts
cmd/kvcli/                local shell that opens the file directly (no server)
examples/python/demo.py   using the server from Python
```

## Limits

- Every key is held in RAM; values stay on disk.
- One server process per data file.
- Compaction pauses reads and writes while it runs.
- A corrupt record in the middle of the file drops everything after it on startup.
- No per-user permissions and no replication. Snapshots protect against mistakes
  and corruption; keep copies off the machine to protect against disk failure.

## Tests

```
go test -race ./...                                   # unit and end-to-end tests
go test -run '^$' -fuzz '^FuzzReplay$' -fuzztime 30s . # throw random bytes at the file reader
```

GitHub Actions (`.github/workflows/ci.yml`) runs formatting, vet, the tests and a
short fuzz on Linux, Windows and macOS for every push.
