package broker

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

func TestHintStoreEnqueueDrain(t *testing.T) {
	hs := newHintStore()
	hs.enqueue(0, hintEntry{topic: "ignored"})
	if out := hs.drain(); out != nil {
		t.Fatalf("expected nil drain when store empty, got %v", out)
	}

	entry := hintEntry{topic: "demo", partition: 1, raw: []byte("value")}
	hs.enqueue(1, entry)
	drained := hs.drain()
	if len(drained) != 1 || len(drained[1]) != 1 {
		t.Fatalf("expected a single drained entry, got %v", drained)
	}
	if drained[1][0].topic != entry.topic {
		t.Fatalf("unexpected drained topic %s", drained[1][0].topic)
	}
	if out := hs.drain(); out != nil {
		t.Fatalf("expected second drain to be empty, got %v", out)
	}
}

func TestLeaderlessHintEnqueueHelpers(t *testing.T) {
	b := &Broker{
		hintStore:  newHintStore(),
		hintWakeCh: make(chan struct{}, 1),
	}
	b.enqueueHint(2, "demo", 0, []byte("value"), 0, false)
	select {
	case <-b.hintWakeCh:
	default:
		t.Fatalf("expected enqueueHint to wake hint loop")
	}
	if len(b.hintStore.pending[2]) != 1 {
		t.Fatalf("expected pending entry for replica 2, got %v", b.hintStore.pending)
	}

	b.enqueueHintNoWake(3, "demo", 0, []byte("lazy"), 0, false)
	select {
	case <-b.hintWakeCh:
		t.Fatalf("enqueueHintNoWake must not wake hint loop")
	default:
	}
	if len(b.hintStore.pending[3]) != 1 {
		t.Fatalf("expected pending entry for replica 3, got %v", b.hintStore.pending)
	}

	(&Broker{}).notifyHintLoop() // ensure nil channel path
}

func TestBrokerPeerAddress(t *testing.T) {
	var b Broker
	if addr := b.peerAddress(1); addr != "" {
		t.Fatalf("expected empty address without cluster, got %s", addr)
	}
	b.conf.Cluster = &ClusterConfig{
		Peers: map[int]string{
			2: "http://peer",
		},
	}
	if addr := b.peerAddress(2); addr != "http://peer" {
		t.Fatalf("unexpected address %s", addr)
	}
}

func TestSendReplicaRecordsScenarios(t *testing.T) {
	b := &Broker{}
	if err := b.sendReplicaRecords("addr", "demo", 0, nil, nil); err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if err := b.sendReplicaRecords("addr", "demo", 0, [][]byte{{1}}, nil); err == nil {
		t.Fatalf("expected error when http client is nil")
	}

	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer failSrv.Close()

	b.httpClient = failSrv.Client()
	if err := b.sendReplicaRecords(failSrv.URL, "demo", 0, [][]byte{{1}}, nil); err == nil {
		t.Fatalf("expected error when peer returns failure")
	}

	var wrote [][]byte
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		var payload struct {
			Records []string `json:"records"`
		}
		_ = json.Unmarshal(body, &payload)
		for _, rec := range payload.Records {
			val, _ := base64.StdEncoding.DecodeString(rec)
			wrote = append(wrote, val)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer okSrv.Close()

	b.httpClient = okSrv.Client()
	if err := b.sendReplicaRecords(okSrv.URL, "demo", 0, [][]byte{{2}}, nil); err != nil {
		t.Fatalf("unexpected error sending to peer: %v", err)
	}
	if len(wrote) != 1 || wrote[0][0] != 2 {
		t.Fatalf("unexpected payload echoed %v", wrote)
	}
}

func TestFlushHintsScenarios(t *testing.T) {
	var b Broker
	b.flushHints() // nil store path

	b.hintStore = newHintStore()
	b.flushHints() // empty path

	b.conf.Cluster = &ClusterConfig{
		Peers: map[int]string{
			2: "",
		},
	}
	b.enqueueHintNoWake(2, "demo", 0, []byte("v1"), 0, false)
	b.flushHints() // missing address requeues
	if len(b.hintStore.pending[2]) != 1 {
		t.Fatalf("expected hint to remain pending when address missing")
	}

	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()
	b.httpClient = failSrv.Client()
	b.conf.Cluster.Peers[2] = failSrv.URL
	b.flushHints()
	if len(b.hintStore.pending[2]) != 1 {
		t.Fatalf("expected hint to remain after failed delivery")
	}

	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer okSrv.Close()
	b.httpClient = okSrv.Client()
	b.conf.Cluster.Peers[2] = okSrv.URL
	b.flushHints()
	if pending := b.hintStore.pending[2]; len(pending) != 0 {
		t.Fatalf("expected hints to be cleared, got %v", pending)
	}
}

func TestLeaderlessHintLoopLifecycle(t *testing.T) {
	b := &Broker{
		conf:       Config{Cluster: &ClusterConfig{HintDeliveryInterval: 5 * time.Millisecond}},
		hintStore:  newHintStore(),
		hintStopCh: make(chan struct{}),
		hintWakeCh: make(chan struct{}, 1),
		httpClient: newDefaultHTTPClient(),
	}
	done := make(chan struct{})
	go func() {
		b.runHintLoop()
		close(done)
	}()

	b.enqueueHint(2, "demo", 0, []byte("value"), 0, false)
	time.Sleep(10 * time.Millisecond)
	b.notifyHintLoop()
	time.Sleep(10 * time.Millisecond)
	close(b.hintStopCh)
	<-done

	var empty Broker
	empty.startHintLoop() // ensure nil store shortcut
}

func TestAppendReplicaCovers(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "log")
	lg, err := logstore.Open(logPath, 1<<12)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	pl, err := newPartitionLog(lg)
	if err != nil {
		t.Fatalf("partition log: %v", err)
	}
	t.Cleanup(func() { _ = pl.close() })
	t.Cleanup(func() { _ = pl.close() })
	b := &Broker{
		logs: map[string]map[int]*partitionLog{
			"demo": {0: pl},
		},
	}
	t.Cleanup(func() { _ = b.Close() })
	raw := encodeRecord(1, 1, []byte("value"))
	if off, appended, err := b.AppendReplica("demo", 0, raw, 0, false); err != nil || !appended || off != 0 {
		t.Fatalf("append replica failed: off=%d appended=%v err=%v", off, appended, err)
	}
	if _, appended, err := b.AppendReplica("demo", 0, raw, 0, false); err != nil || appended {
		t.Fatalf("expected duplicate to be ignored, appended=%v err=%v", appended, err)
	}
}

func TestFetchLeaderlessQuorumAndRepairs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "log")
	lg, err := logstore.Open(logPath, 1<<12)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	pl, err := newPartitionLog(lg)
	if err != nil {
		t.Fatalf("partition log: %v", err)
	}

	cluster := &ClusterConfig{
		BrokerID:             1,
		ReplicationMode:      ReplicationModeLeaderless,
		LeaderlessReadQuorum: 3,
		Partitions: map[string]map[int]PartitionAssignment{
			"demo": {0: {Replicas: []int{1, 2, 3}}},
		},
		Peers: map[int]string{
			1: "",
			2: "",
			3: "",
		},
	}
	b := &Broker{
		conf:        Config{Cluster: cluster},
		logs:        map[string]map[int]*partitionLog{"demo": {0: pl}},
		hintStore:   newHintStore(),
		assignments: copyAssignments(cluster),
	}
	t.Cleanup(func() { _ = b.Close() })

	// Not enough replicas respond (only local present)
	if _, _, err := b.fetchLeaderless("demo", 0, 0, 0); !errors.Is(err, ErrReadQuorumNotMet) {
		t.Fatalf("expected ErrReadQuorumNotMet, got %v", err)
	}

	// Prepare local record so read-repair has work to do.
	if _, _, _, err := pl.appendLocal(1, []byte("local")); err != nil {
		t.Fatalf("append local: %v", err)
	}

	repairCh := make(chan struct{}, 1)
	peerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"FromOffset": 0,
				"ToOffset":   uint64(0),
				"Records":    []string{},
				"RawRecords": []string{},
			})
		case http.MethodPost:
			select {
			case repairCh <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer peerSrv.Close()

	cluster.LeaderlessReadQuorum = 2
	cluster.Peers[2] = peerSrv.URL
	b.httpClient = peerSrv.Client()

	if _, _, err := b.fetchLeaderless("demo", 0, 0, 0); err != nil {
		t.Fatalf("leaderless fetch: %v", err)
	}
	select {
	case <-repairCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("expected read repair to trigger POST")
	}
}

func TestDispatchReadRepairsHintFallback(t *testing.T) {
	// Provide HTTP server for peer 3 to exercise success path after we assign an address.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	b := &Broker{
		conf: Config{Cluster: &ClusterConfig{
			ReplicationMode: ReplicationModeLeaderless,
			Peers: map[int]string{
				2: "",
				3: server.URL,
			},
		}},
		hintStore:  newHintStore(),
		httpClient: server.Client(),
	}

	union := map[string]PartitionRecord{
		"1:1": {HasMeta: true, OriginID: 1, OriginSeq: 1, Raw: encodeRecord(1, 1, []byte("repair")), CommitVersion: 1},
	}
	nodeKeys := map[int]map[string]struct{}{
		2: {},
		3: {},
	}
	b.dispatchReadRepairs("demo", 0, 1, nodeKeys, union)
	if len(b.hintStore.pending[2]) == 0 {
		t.Fatalf("expected hint for peer without address")
	}
}

func TestFetchReplicaRecordsAndURL(t *testing.T) {
	if _, err := buildLeaderlessFetchURL("%", "demo", 0, 0, 0); err == nil {
		t.Fatalf("expected parse error for invalid base url")
	}

	var count int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		if count == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"FromOffset": 0,
				"ToOffset":   uint64(0),
				"Records":    []string{},
				"RawRecords": []string{base64.StdEncoding.EncodeToString(encodeRecord(1, 1, []byte("peer")))},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"FromOffset": 0,
			"ToOffset":   uint64(0),
			"Records":    []string{"plaintext"},
			"RawRecords": []string{},
		})
	}))
	defer server.Close()

	b := &Broker{httpClient: server.Client()}
	if _, _, err := b.fetchReplicaRecords(server.URL, "demo", 0, 0, 0); err != nil {
		t.Fatalf("expected successful fetch with raw records: %v", err)
	}
	if _, _, err := b.fetchReplicaRecords(server.URL, "demo", 0, 0, 0); err != nil {
		t.Fatalf("expected successful fetch with plaintext records: %v", err)
	}

	if _, _, err := (&Broker{}).fetchReplicaRecords("", "demo", 0, 0, 0); err == nil {
		t.Fatalf("expected error when http client is nil")
	}
}

func TestRecordIdentity(t *testing.T) {
	recWithMeta := PartitionRecord{HasMeta: true, OriginID: 1, OriginSeq: 2}
	recWithout := PartitionRecord{Raw: []byte("plain")}
	if got := recordIdentity(recWithMeta); got != "1:2" {
		t.Fatalf("unexpected meta identity %s", got)
	}
	if got := recordIdentity(recWithout); got == "" || got[:4] != "raw:" {
		t.Fatalf("unexpected raw identity %s", got)
	}
}
