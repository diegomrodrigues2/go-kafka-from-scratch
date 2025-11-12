package client

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestSessionReadYourWrites(t *testing.T) {
	var (
		mu            sync.Mutex
		publishedBody []byte
	)

	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			mu.Lock()
			body, err := io.ReadAll(r.Body)
			if err != nil {
				mu.Unlock()
				t.Fatalf("read body: %v", err)
			}
			publishedBody = body
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"offset": 5})
		case http.MethodGet:
			mu.Lock()
			body := publishedBody
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"fromOffset":   5,
				"toOffset":     5,
				"records":      [][]byte{body},
				"deltaVersion": uint64(1),
			})
		default:
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(leaderSrv.Close)

	followerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"fromOffset":   0,
			"toOffset":     0,
			"records":      [][]byte{},
			"deltaVersion": uint64(1),
		})
	}))
	t.Cleanup(followerSrv.Close)

	session, err := NewSession(SessionConfig{
		SessionID:    "user-1",
		LeaderURL:    leaderSrv.URL,
		FollowerURLs: []string{followerSrv.URL},
	})
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	offset, err := session.Publish("demo", 0, []byte("payload"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if offset != 5 {
		t.Fatalf("expected offset 5 got %d", offset)
	}

	records, last, err := session.Fetch("demo", 0, offset, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(records) != 1 || string(records[0]) != "payload" {
		t.Fatalf("expected to read own write, got %v", records)
	}
	if last != 5 {
		t.Fatalf("expected last offset 5 got %d", last)
	}
}

func TestSessionMonotonicReads(t *testing.T) {
	var (
		mu              sync.Mutex
		leaderOffsets   = []uint64{5, 6}
		leaderIndex     int
		followerOffsets = []uint64{0, 4}
		followerIndex   int
	)

	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"offset": leaderOffsets[0]})
		case http.MethodGet:
			mu.Lock()
			idx := leaderIndex
			if idx >= len(leaderOffsets) {
				idx = len(leaderOffsets) - 1
			}
			to := leaderOffsets[idx]
			leaderIndex++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"fromOffset":   to,
				"toOffset":     to,
				"records":      [][]byte{[]byte("ok")},
				"deltaVersion": uint64(to),
			})
		default:
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(leaderSrv.Close)

	followerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		idx := followerIndex
		if idx >= len(followerOffsets) {
			idx = len(followerOffsets) - 1
		}
		to := followerOffsets[idx]
		followerIndex++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"fromOffset":   to,
			"toOffset":     to,
			"records":      [][]byte{},
			"deltaVersion": uint64(to),
		})
	}))
	t.Cleanup(followerSrv.Close)

	session, err := NewSession(SessionConfig{
		SessionID:    "user-2",
		LeaderURL:    leaderSrv.URL,
		FollowerURLs: []string{followerSrv.URL},
	})
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	offset, err := session.Publish("demo", 0, []byte("payload"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if offset != leaderOffsets[0] {
		t.Fatalf("expected offset %d got %d", leaderOffsets[0], offset)
	}

	_, last, err := session.Fetch("demo", 0, offset, 0)
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	if last != leaderOffsets[0] {
		t.Fatalf("expected last %d got %d", leaderOffsets[0], last)
	}

	_, nextLast, err := session.Fetch("demo", 0, offset, 0)
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if nextLast < last {
		t.Fatalf("monotonic read violated: last=%d next=%d", last, nextLast)
	}
}

func TestSessionFetchAtLeastVersion(t *testing.T) {
	var (
		mu             sync.Mutex
		followerHeader string
	)

	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"fromOffset":   1,
				"toOffset":     1,
				"records":      [][]byte{[]byte("leader")},
				"deltaVersion": uint64(2),
			})
		case http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"offset": 1})
		default:
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(leaderSrv.Close)

	followerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		followerHeader = r.Header.Get(readMinVersionHeader)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"fromOffset":   1,
			"toOffset":     1,
			"records":      [][]byte{[]byte("follower")},
			"deltaVersion": uint64(2),
		})
	}))
	t.Cleanup(followerSrv.Close)

	session, err := NewSession(SessionConfig{
		SessionID:    "user-version",
		LeaderURL:    leaderSrv.URL,
		FollowerURLs: []string{followerSrv.URL},
	})
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	if _, err := session.Publish("demo", 0, []byte("payload")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if _, _, err := session.FetchAtLeastVersion("demo", 0, 0, 0, 2); err != nil {
		t.Fatalf("prime fetch: %v", err)
	}

	if _, _, err := session.FetchAtLeastVersion("demo", 0, 0, 0, 2); err != nil {
		t.Fatalf("fetch with version: %v", err)
	}

	mu.Lock()
	header := followerHeader
	mu.Unlock()
	if header != "2" {
		t.Fatalf("expected follower to receive min version header 2, got %s", header)
	}
}
