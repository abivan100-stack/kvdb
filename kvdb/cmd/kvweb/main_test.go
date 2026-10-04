package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

type stateResp struct {
	Stats struct {
		Keys      int   `json:"keys"`
		FileSize  int64 `json:"fileSize"`
		LiveBytes int64 `json:"liveBytes"`
		DeadBytes int64 `json:"deadBytes"`
	} `json:"stats"`
	Records []struct {
		Live bool `json:"live"`
	} `json:"records"`
	Keys []struct {
		Key, Preview string
	} `json:"keys"`
}

func TestAPIFlow(t *testing.T) {
	a, err := newApp(filepath.Join(t.TempDir(), "web.db"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.routes())
	defer srv.Close()

	do := func(method, path string, body any, out any) int {
		t.Helper()
		var rd *bytes.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			rd = bytes.NewReader(b)
		} else {
			rd = bytes.NewReader(nil)
		}
		req, _ := http.NewRequest(method, srv.URL+path, rd)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if out != nil {
			json.NewDecoder(resp.Body).Decode(out)
		}
		return resp.StatusCode
	}
	state := func() (s stateResp) { do("GET", "/api/state", nil, &s); return }

	if code := do("GET", "/", nil, nil); code != 200 {
		t.Fatalf("index: %d", code)
	}
	if code := do("POST", "/api/crash", nil, nil); code != 400 {
		t.Fatalf("crash on empty file: want 400, got %d", code)
	}
	if code := do("POST", "/api/put", map[string]string{"key": "", "value": "x"}, nil); code != 400 {
		t.Fatalf("empty key: want 400, got %d", code)
	}
	if code := do("GET", "/api/get?key=nope", nil, nil); code != 404 {
		t.Fatalf("missing key: want 404, got %d", code)
	}

	do("POST", "/api/put", map[string]string{"key": "name", "value": "vijay"}, nil)
	do("POST", "/api/put", map[string]string{"key": "name", "value": "vijay anbu"}, nil)
	var got map[string]string
	do("GET", "/api/get?key=name", nil, &got)
	if got["value"] != "vijay anbu" {
		t.Fatalf("get = %v", got)
	}
	s := state()
	if s.Stats.Keys != 1 || len(s.Records) != 2 || s.Records[0].Live || !s.Records[1].Live || s.Stats.DeadBytes == 0 {
		t.Fatalf("unexpected state: %+v", s)
	}

	var del map[string]bool
	do("POST", "/api/delete", map[string]string{"key": "ghost"}, &del)
	if del["deleted"] {
		t.Fatal("deleting a missing key should report deleted=false")
	}

	do("POST", "/api/fill", map[string]int{"count": 200}, nil)
	before := state()
	var cr map[string]int64
	do("POST", "/api/compact", nil, &cr)
	after := state()
	if after.Stats.FileSize >= before.Stats.FileSize || after.Stats.DeadBytes != 0 {
		t.Fatalf("compaction did not shrink: %d -> %d", before.Stats.FileSize, after.Stats.FileSize)
	}
	if after.Stats.Keys != before.Stats.Keys {
		t.Fatalf("compaction changed key count: %d -> %d", before.Stats.Keys, after.Stats.Keys)
	}

	// Simulated power cut: the last record is lost, everything else survives.
	do("POST", "/api/put", map[string]string{"key": "last", "value": "write"}, nil)
	var crash map[string]any
	if code := do("POST", "/api/crash", nil, &crash); code != 200 || crash["lostKey"] != "last" {
		t.Fatalf("crash: %d %v", code, crash)
	}
	if code := do("GET", "/api/get?key=last", nil, nil); code != 404 {
		t.Fatalf("torn record should be gone, got %d", code)
	}
	if code := do("GET", "/api/get?key=name", nil, nil); code != 200 {
		t.Fatalf("earlier data should survive recovery, got %d", code)
	}
	do("POST", "/api/put", map[string]string{"key": "again", "value": "works"}, nil)
	if code := do("GET", "/api/get?key=again", nil, nil); code != 200 {
		t.Fatal("writes should work after recovery")
	}

	do("POST", "/api/reset", nil, nil)
	if s := state(); s.Stats.FileSize != 0 || len(s.Keys) != 0 {
		t.Fatalf("reset left data behind: %+v", s)
	}
}
