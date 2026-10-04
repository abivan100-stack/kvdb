// kvweb serves a small web page that talks to a real kvdb file, so you can
// watch the append-only log grow, pile up old records, and shrink on compaction.
//
//	go run ./cmd/kvweb            # http://127.0.0.1:8080, file demo.db
//	go run ./cmd/kvweb -db my.db -addr 127.0.0.1:9000
package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"kvdb"
)

//go:embed index.html
var indexHTML []byte

const (
	maxRecordsShown = 1500
	maxKeysShown    = 300
	previewChars    = 60
)

type app struct {
	mu   sync.RWMutex // write-locked only when the *Store itself is swapped
	path string
	db   *kvdb.Store
}

func newApp(path string) (*app, error) {
	db, err := kvdb.Open(path, kvdb.Options{})
	if err != nil {
		return nil, err
	}
	return &app{path: path, db: db}, nil
}

func (a *app) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.index)
	mux.HandleFunc("GET /api/state", a.state)
	mux.HandleFunc("GET /api/get", a.get)
	mux.HandleFunc("POST /api/put", a.put)
	mux.HandleFunc("POST /api/delete", a.del)
	mux.HandleFunc("POST /api/compact", a.compact)
	mux.HandleFunc("POST /api/fill", a.fill)
	mux.HandleFunc("POST /api/crash", a.crash)
	mux.HandleFunc("POST /api/reset", a.reset)
	return mux
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, kvdb.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, kvdb.ErrEmptyKey), errors.Is(err, kvdb.ErrKeyTooLarge), errors.Is(err, kvdb.ErrValueTooLarge):
		status = http.StatusBadRequest
	}
	msg := strings.TrimPrefix(err.Error(), "kvdb: ")
	writeJSON(w, status, map[string]string{"error": msg})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must be valid JSON"})
		return false
	}
	return true
}

func preview(b []byte) string {
	s := strings.ToValidUTF8(string(b), "\uFFFD")
	r := []rune(s)
	if len(r) > previewChars {
		return string(r[:previewChars]) + "…"
	}
	return s
}

// ---- handlers ----

func (a *app) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Write(indexHTML)
}

func (a *app) state(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	recs, total, err := a.db.Inspect(maxRecordsShown)
	if err != nil {
		fail(w, err)
		return
	}
	type rec struct {
		Offset   int64  `json:"offset"`
		Size     int64  `json:"size"`
		Key      string `json:"key"`
		ValueLen uint32 `json:"valueLen"`
		Delete   bool   `json:"delete"`
		Live     bool   `json:"live"`
	}
	outRecs := make([]rec, len(recs))
	for i, x := range recs {
		outRecs[i] = rec{x.Offset, x.Size, x.Key, x.ValueLen, x.Delete, x.Live}
	}

	keys := a.db.Keys()
	sort.Strings(keys)
	type keyRow struct {
		Key     string `json:"key"`
		Preview string `json:"preview"`
		Bytes   int    `json:"bytes"`
	}
	rows := make([]keyRow, 0, len(keys))
	for _, k := range keys {
		if len(rows) >= maxKeysShown {
			break
		}
		v, err := a.db.Get(k)
		if err != nil {
			rows = append(rows, keyRow{Key: k, Preview: "(" + strings.TrimPrefix(err.Error(), "kvdb: ") + ")"})
			continue
		}
		rows = append(rows, keyRow{k, preview(v), len(v)})
	}

	st := a.db.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"file": filepath.Base(a.path),
		"stats": map[string]any{
			"keys": st.Keys, "fileSize": st.FileSize, "liveBytes": st.LiveBytes, "deadBytes": st.DeadBytes,
		},
		"records":      outRecs,
		"totalRecords": total,
		"keys":         rows,
	})
}

func (a *app) get(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	v, err := a.db.Get(r.URL.Query().Get("key"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"value": string(v)})
}

func (a *app) put(w http.ResponseWriter, r *http.Request) {
	var req struct{ Key, Value string }
	if !decode(w, r, &req) {
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if err := a.db.Put(req.Key, []byte(req.Value)); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (a *app) del(w http.ResponseWriter, r *http.Request) {
	var req struct{ Key string }
	if !decode(w, r, &req) {
		return
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	_, err := a.db.Get(req.Key)
	existed := err == nil
	if err := a.db.Delete(req.Key); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"deleted": existed})
}

func (a *app) compact(w http.ResponseWriter, r *http.Request) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	cs, err := a.db.Compact()
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"before": cs.BeforeBytes, "after": cs.AfterBytes, "reclaimed": cs.BeforeBytes - cs.AfterBytes, "keys": cs.Keys,
	})
}

// fill writes random overwrites and deletes over a small set of keys, so old
// records pile up quickly.
func (a *app) fill(w http.ResponseWriter, r *http.Request) {
	var req struct{ Count int }
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<10)).Decode(&req)
	if req.Count < 1 {
		req.Count = 100
	}
	if req.Count > 500 {
		req.Count = 500
	}
	keys := []string{"user:ada", "user:grace", "session:42", "config:theme", "counter", "note:1", "note:2", "note:3"}

	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := 0; i < req.Count; i++ {
		k := keys[rand.Intn(len(keys))]
		var err error
		if rand.Intn(10) == 0 {
			err = a.db.Delete(k)
		} else {
			err = a.db.Put(k, []byte(fmt.Sprintf("value-%d", rand.Intn(100000))))
		}
		if err != nil {
			fail(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]int{"written": req.Count})
}

// crash simulates power loss in the middle of the last write: it closes the
// database, chops the final record in half on disk, and reopens it, which
// forces the real recovery path to run.
func (a *app) crash(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()

	recs, _, err := a.db.Inspect(0)
	if err != nil {
		fail(w, err)
		return
	}
	if len(recs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "the file is empty; save a key first so there is a write to interrupt"})
		return
	}
	last := recs[len(recs)-1]
	if err := a.db.Close(); err != nil {
		fail(w, err)
		return
	}
	torn := last.Size / 2
	if err := os.Truncate(a.path, last.Offset+torn); err != nil {
		fail(w, err)
		return
	}
	db, err := kvdb.Open(a.path, kvdb.Options{})
	if err != nil {
		fail(w, err)
		return
	}
	a.db = db
	op := "save"
	if last.Delete {
		op = "delete"
	}
	writeJSON(w, http.StatusOK, map[string]any{"lostKey": last.Key, "lostOp": op, "partialBytes": torn})
}

func (a *app) reset(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.db.Close(); err != nil {
		fail(w, err)
		return
	}
	if err := os.Remove(a.path); err != nil && !os.IsNotExist(err) {
		fail(w, err)
		return
	}
	db, err := kvdb.Open(a.path, kvdb.Options{})
	if err != nil {
		fail(w, err)
		return
	}
	a.db = db
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "address to listen on (keep it on localhost: there is no login)")
	path := flag.String("db", "demo.db", "database file to open or create")
	flag.Parse()

	a, err := newApp(*path)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("kvdb demo on http://%s  (file: %s)", *addr, *path)
	log.Fatal(http.ListenAndServe(*addr, a.routes()))
}
