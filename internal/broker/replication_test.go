package broker

import (
	"encoding/json"
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
		}{
			FromOffset: offset,
			ToOffset:   last,
			Records:    make([]string, len(recs)),
		}
		for i, rec := range recs {
			payload.Records[i] = string(rec)
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
		if err == nil && len(recs) == 2 && string(recs[0]) == "msg-1" && string(recs[1]) == "msg-2" && last == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower did not replicate records, got %v last=%d err=%v", recs, last, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
