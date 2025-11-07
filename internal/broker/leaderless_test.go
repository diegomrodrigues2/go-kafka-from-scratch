package broker

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakePeer struct {
	t          *testing.T
	mu         sync.Mutex
	records    [][]byte
	failWrites bool
	writeCh    chan struct{}
}

func newFakePeer(t *testing.T) *fakePeer {
	return &fakePeer{t: t, writeCh: make(chan struct{}, 16)}
}

func (fp *fakePeer) start() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(fp.handle))
}

func (fp *fakePeer) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/replica"):
		fp.handleReplicaWrite(w, r)
	case strings.HasSuffix(r.URL.Path, "/fetch"):
		fp.handleFetch(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (fp *fakePeer) handleReplicaWrite(w http.ResponseWriter, r *http.Request) {
	if fp.failWrites {
		http.Error(w, "fail writes", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	var payload struct {
		Records []string `json:"records"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	for _, rawStr := range payload.Records {
		raw, err := base64.StdEncoding.DecodeString(rawStr)
		if err != nil {
			http.Error(w, "invalid record", http.StatusBadRequest)
			return
		}
		fp.append(raw)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (fp *fakePeer) handleFetch(w http.ResponseWriter, r *http.Request) {
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}
	fp.mu.Lock()
	defer fp.mu.Unlock()
	if offset > len(fp.records) {
		offset = len(fp.records)
	}
	subset := fp.records[offset:]
	resp := struct {
		FromOffset uint64   `json:"fromOffset"`
		ToOffset   uint64   `json:"toOffset"`
		Records    []string `json:"records"`
		RawRecords []string `json:"rawRecords"`
	}{
		FromOffset: uint64(offset),
		ToOffset:   uint64(offset),
		Records:    make([]string, len(subset)),
		RawRecords: make([]string, len(subset)),
	}
	if len(subset) > 0 {
		resp.ToOffset = uint64(offset + len(subset) - 1)
	}
	for i, raw := range subset {
		rec := decodeRecord(raw)
		resp.Records[i] = string(rec.Payload)
		resp.RawRecords[i] = base64.StdEncoding.EncodeToString(raw)
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func (fp *fakePeer) append(raw []byte) {
	fp.mu.Lock()
	fp.records = append(fp.records, append([]byte(nil), raw...))
	fp.mu.Unlock()
	select {
	case fp.writeCh <- struct{}{}:
	default:
	}
}

func (fp *fakePeer) setRecords(records ...[]byte) {
	fp.mu.Lock()
	fp.records = nil
	for _, raw := range records {
		fp.records = append(fp.records, append([]byte(nil), raw...))
	}
	fp.mu.Unlock()
}

func (fp *fakePeer) recordCount() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return len(fp.records)
}

func (fp *fakePeer) waitForRecords(t *testing.T, n int, timeout time.Duration) {
	deadline := time.After(timeout)
	for {
		if fp.recordCount() >= n {
			return
		}
		select {
		case <-fp.writeCh:
		case <-deadline:
			t.Fatalf("timeout waiting for %d records", n)
		}
	}
}

func TestLeaderlessPublishReplicatesToPeers(t *testing.T) {
	dir := t.TempDir()
	peer2 := newFakePeer(t)
	srv2 := peer2.start()
	defer srv2.Close()

	peer3 := newFakePeer(t)
	srv3 := peer3.start()
	defer srv3.Close()

	cfg := Config{
		Cluster: &ClusterConfig{
			BrokerID:        1,
			ReplicationMode: ReplicationModeLeaderless,
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Replicas: []int{1, 2, 3}}},
			},
			Peers: map[int]string{
				1: "",
				2: srv2.URL,
				3: srv3.URL,
			},
		},
	}
	b := NewBroker(cfg)
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	if _, err := b.Publish("demo", 0, nil, []byte("value")); err != nil {
		t.Fatalf("publish leaderless: %v", err)
	}
	peer2.waitForRecords(t, 1, 2*time.Second)
	peer3.waitForRecords(t, 1, 2*time.Second)

	if peer2.recordCount() != 1 || peer3.recordCount() != 1 {
		t.Fatalf("expected both peers to have 1 record, got %d and %d", peer2.recordCount(), peer3.recordCount())
	}
}

func TestLeaderlessPublishQuorumFailure(t *testing.T) {
	dir := t.TempDir()
	peer2 := newFakePeer(t)
	peer2.failWrites = true
	srv2 := peer2.start()
	defer srv2.Close()

	peer3 := newFakePeer(t)
	peer3.failWrites = true
	srv3 := peer3.start()
	defer srv3.Close()

	cfg := Config{
		Cluster: &ClusterConfig{
			BrokerID:        1,
			ReplicationMode: ReplicationModeLeaderless,
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Replicas: []int{1, 2, 3}}},
			},
			Peers: map[int]string{
				1: "",
				2: srv2.URL,
				3: srv3.URL,
			},
		},
	}
	b := NewBroker(cfg)
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	b.stopHintLoop()

	if _, err := b.Publish("demo", 0, nil, []byte("value")); !errors.Is(err, ErrWriteQuorumNotMet) {
		t.Fatalf("expected ErrWriteQuorumNotMet got %v", err)
	}
	if b.hintStore == nil {
		t.Fatalf("expected hint store to be initialized")
	}
	b.hintStore.mu.Lock()
	defer b.hintStore.mu.Unlock()
	if len(b.hintStore.pending[2]) == 0 || len(b.hintStore.pending[3]) == 0 {
		t.Fatalf("expected hints to be stored for failed peers, got %+v", b.hintStore.pending)
	}
}

func TestLeaderlessFetchReadRepair(t *testing.T) {
	dir := t.TempDir()
	peer2 := newFakePeer(t)
	srv2 := peer2.start()
	defer srv2.Close()

	peer3 := newFakePeer(t)
	srv3 := peer3.start()
	defer srv3.Close()

	raw := encodeRecord(2, 1, []byte("from-peer"))
	peer2.setRecords(raw)

	cfg := Config{
		Cluster: &ClusterConfig{
			BrokerID:        1,
			ReplicationMode: ReplicationModeLeaderless,
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Replicas: []int{1, 2, 3}}},
			},
			Peers: map[int]string{
				1: "",
				2: srv2.URL,
				3: srv3.URL,
			},
		},
	}
	b := NewBroker(cfg)
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	recs, last, err := b.Fetch("demo", 0, 0, 0)
	if err != nil {
		t.Fatalf("leaderless fetch: %v", err)
	}
	if len(recs) != 1 || string(recs[0].Payload) != "from-peer" || last != 0 {
		t.Fatalf("unexpected fetch result len=%d last=%d payload=%q", len(recs), last, string(recs[0].Payload))
	}

	peer3.waitForRecords(t, 1, 2*time.Second)

	pl, err := b.getPartition("demo", 0)
	if err != nil {
		t.Fatalf("get partition: %v", err)
	}
	local, _, err := pl.readRecords(0, 0)
	if err != nil {
		t.Fatalf("read local: %v", err)
	}
	if len(local) != 1 || string(local[0].Payload) != "from-peer" {
		t.Fatalf("expected local read repair to append record, got %d records", len(local))
	}
}
