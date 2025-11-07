package broker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestFollowerReplicatesFromLeader ensures the follower fetch loop keeps the
// replica log in sync with the leader.
func TestFollowerReplicatesFromLeader(t *testing.T) {
	leaderDir := t.TempDir()
	followerDir := t.TempDir()

	leaderCfg := Config{
		Cluster: &ClusterConfig{
			BrokerID: 1,
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
		},
	}
	leader := NewBroker(leaderCfg)
	t.Cleanup(func() { _ = leader.Close() })
	if err := leader.EnsurePartition("demo", 0, filepath.Join(leaderDir, "topic=demo-part=0"), 128); err != nil {
		t.Fatalf("leader ensure partition: %v", err)
	}

	leaderMux := http.NewServeMux()
	leaderMux.HandleFunc("/topics/demo/partitions/0/fetch", func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
		if err != nil {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
		recs, last, err := leader.Fetch("demo", 0, offset, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		payload := struct {
			FromOffset uint64   `json:"fromOffset"`
			ToOffset   uint64   `json:"toOffset"`
			Records    []string `json:"records"`
			RawRecords []string `json:"rawRecords"`
		}{
			FromOffset: offset,
			ToOffset:   last,
			Records:    make([]string, len(recs)),
			RawRecords: make([]string, len(recs)),
		}
		for i, rec := range recs {
			payload.Records[i] = string(rec.Payload)
			payload.RawRecords[i] = base64.StdEncoding.EncodeToString(rec.Raw)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	})
	leaderSrv := httptest.NewServer(leaderMux)
	t.Cleanup(leaderSrv.Close)

	followerCfg := Config{
		Cluster: &ClusterConfig{
			BrokerID: 2,
			Peers: map[int]string{
				1: leaderSrv.URL,
			},
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
			ReplicaFetchInterval: 10 * time.Millisecond,
			ReplicaRetryInterval: 10 * time.Millisecond,
		},
	}
	follower := NewBroker(followerCfg)
	follower.SetHTTPClient(leaderSrv.Client())
	t.Cleanup(func() { _ = follower.Close() })
	if err := follower.EnsurePartition("demo", 0, filepath.Join(followerDir, "topic=demo-part=0"), 128); err != nil {
		t.Fatalf("follower ensure partition: %v", err)
	}

	if _, err := leader.Publish("demo", 0, nil, []byte("msg-1")); err != nil {
		t.Fatalf("leader publish: %v", err)
	}
	if _, err := leader.Publish("demo", 0, nil, []byte("msg-2")); err != nil {
		t.Fatalf("leader publish: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		recs, last, err := follower.Fetch("demo", 0, 0, 0)
		if err == nil && len(recs) == 2 && string(recs[0].Payload) == "msg-1" && string(recs[1].Payload) == "msg-2" && last == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower did not replicate records, got %v last=%d err=%v", recs, last, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFollowerFailoverPromotesLeader(t *testing.T) {
	leaderMux := http.NewServeMux()
	leaderMux.HandleFunc("/topics/demo/partitions/0/fetch", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "leader down", http.StatusServiceUnavailable)
	})
	leaderMux.HandleFunc("/cluster/topics/demo/partitions/0/leader", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	leaderSrv := httptest.NewServer(leaderMux)
	t.Cleanup(leaderSrv.Close)

	cfg := Config{
		Cluster: &ClusterConfig{
			BrokerID: 2,
			Peers: map[int]string{
				1: leaderSrv.URL,
			},
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
			ReplicaFetchInterval: 5 * time.Millisecond,
			ReplicaRetryInterval: 5 * time.Millisecond,
			FailoverTimeout:      40 * time.Millisecond,
		},
	}
	followerDir := t.TempDir()
	follower := NewBroker(cfg)
	follower.SetHTTPClient(leaderSrv.Client())
	t.Cleanup(func() { _ = follower.Close() })
	if err := follower.EnsurePartition("demo", 0, filepath.Join(followerDir, "topic=demo-part=0"), 128); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ok, err := follower.IsLeader("demo", 0)
		if err == nil && ok {
			assign, err := follower.PartitionAssignment("demo", 0)
			if err == nil && assign.Epoch > 0 && assign.Leader == 2 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	assign, _ := follower.PartitionAssignment("demo", 0)
	t.Fatalf("follower did not promote to leader, assignment=%+v", assign)
}

func TestMultiLeaderReplication(t *testing.T) {
	assignments := map[string]map[int]PartitionAssignment{
		"demo": {
			0: {
				Leader:   1,
				Replicas: []int{1, 2},
			},
		},
	}
	nodes := newMultiLeaderCluster(t, []int{1, 2}, assignments)

	if _, err := nodes[0].broker.Publish("demo", 0, nil, []byte("from-1")); err != nil {
		t.Fatalf("broker 1 publish: %v", err)
	}
	if _, err := nodes[1].broker.Publish("demo", 0, nil, []byte("from-2")); err != nil {
		t.Fatalf("broker 2 publish: %v", err)
	}

	want := []string{"from-1", "from-2"}
	for _, node := range nodes {
		payloads := waitForPayloads(t, node.broker, "demo", 0, len(want))
		if !containsAll(payloads, want) {
			t.Fatalf("broker %d payloads mismatch: %v", node.id, payloads)
		}
	}
}

func TestMultiLeaderDeduplicatesLoops(t *testing.T) {
	assignments := map[string]map[int]PartitionAssignment{
		"demo": {
			0: {
				Leader:   1,
				Replicas: []int{1, 2, 3},
			},
		},
	}
	nodes := newMultiLeaderCluster(t, []int{1, 2, 3}, assignments)

	if _, err := nodes[0].broker.Publish("demo", 0, nil, []byte("only-once")); err != nil {
		t.Fatalf("broker publish: %v", err)
	}

	for _, node := range nodes {
		payloads := waitForPayloads(t, node.broker, "demo", 0, 1)
		if len(payloads) != 1 || payloads[0] != "only-once" {
			t.Fatalf("broker %d expected single payload, got %v", node.id, payloads)
		}
	}
}

type multiLeaderNode struct {
	id     int
	broker *Broker
	server *httptest.Server
	data   string
	addr   string
}

func newMultiLeaderCluster(t *testing.T, ids []int, assignments map[string]map[int]PartitionAssignment) []*multiLeaderNode {
	t.Helper()
	nodes := make([]*multiLeaderNode, 0, len(ids))
	basePeers := make(map[int]string, len(ids))
	for _, id := range ids {
		basePeers[id] = ""
	}
	for _, id := range ids {
		dataDir := t.TempDir()
		cfg := Config{
			DataDir: dataDir,
			Cluster: &ClusterConfig{
				BrokerID:             id,
				Peers:                copyStringMap(basePeers),
				Partitions:           assignments,
				ReplicationMode:      ReplicationModeMulti,
				ReplicaFetchInterval: 5 * time.Millisecond,
				ReplicaRetryInterval: 5 * time.Millisecond,
			},
			SegmentBytes: 64 << 10,
		}
		b := NewBroker(cfg)
		t.Cleanup(func() { _ = b.Close() })
		srv := newReplicationOnlyServer(t, b, "demo", 0)
		t.Cleanup(srv.Close)
		nodes = append(nodes, &multiLeaderNode{
			id:     id,
			broker: b,
			server: srv,
			data:   dataDir,
			addr:   srv.URL,
		})
	}

	addrMap := make(map[int]string, len(nodes))
	for _, node := range nodes {
		addrMap[node.id] = node.addr
	}
	for _, node := range nodes {
		for peerID, addr := range addrMap {
			node.broker.conf.Cluster.Peers[peerID] = addr
		}
	}

	for _, node := range nodes {
		partPath := filepath.Join(node.data, fmt.Sprintf("topic=demo-part=%d", 0))
		if err := node.broker.EnsurePartition("demo", 0, partPath, 64<<10); err != nil {
			t.Fatalf("ensure partition broker %d: %v", node.id, err)
		}
	}
	return nodes
}

func newReplicationOnlyServer(t *testing.T, b *Broker, topic string, partition int) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(fmt.Sprintf("/topics/%s/partitions/%d/fetch", topic, partition), func(w http.ResponseWriter, r *http.Request) {
		offset, err := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
		if err != nil {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
		recs, last, err := b.Fetch(topic, partition, offset, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		payload := struct {
			FromOffset uint64   `json:"fromOffset"`
			ToOffset   uint64   `json:"toOffset"`
			Records    []string `json:"records"`
			RawRecords []string `json:"rawRecords"`
		}{
			FromOffset: offset,
			ToOffset:   last,
			Records:    make([]string, len(recs)),
			RawRecords: make([]string, len(recs)),
		}
		for i, rec := range recs {
			payload.Records[i] = string(rec.Payload)
			payload.RawRecords[i] = base64.StdEncoding.EncodeToString(rec.Raw)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	})
	return httptest.NewServer(mux)
}

func waitForPayloads(t *testing.T, b *Broker, topic string, partition, expected int) []string {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for {
		recs, _, err := b.Fetch(topic, partition, 0, 0)
		if err == nil && len(recs) >= expected {
			payloads := make([]string, len(recs))
			for i, rec := range recs {
				payloads[i] = string(rec.Payload)
			}
			return payloads
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d records (err=%v)", expected, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func containsAll(values []string, want []string) bool {
	set := make(map[string]int, len(values))
	for _, v := range values {
		set[v]++
	}
	for _, w := range want {
		if set[w] == 0 {
			return false
		}
	}
	return true
}

func copyStringMap(in map[int]string) map[int]string {
	out := make(map[int]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
