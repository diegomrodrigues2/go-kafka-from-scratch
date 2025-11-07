package broker

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBrokerEnsurePublishFetch(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker()
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure existing partition: %v", err)
	}

	off, err := b.Publish("demo", 0, []byte("k"), []byte("value"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if off != 0 {
		t.Fatalf("expected offset 0 got %d", off)
	}

	recs, last, err := b.Fetch("demo", 0, 0, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 1 || string(recs[0].Payload) != "value" || last != 0 {
		t.Fatalf("unexpected fetch result len=%d last=%d", len(recs), last)
	}

	topics := b.Topics()
	if len(topics) != 1 || topics[0] != "demo" {
		t.Fatalf("unexpected topics: %v", topics)
	}
	if !strings.Contains(b.Describe(), "topics=1") {
		t.Fatalf("describe did not include topic count")
	}
}

func TestBrokerErrors(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker()
	defer func() { _ = b.Close() }()
	path0 := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, path0, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	if _, err := b.Publish("missing", 0, nil, nil); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("expected ErrPartitionNotFound for missing topic, got %v", err)
	}
	if _, err := b.Publish("demo", 1, nil, nil); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("expected ErrPartitionNotFound for missing partition, got %v", err)
	}
	if _, _, err := b.Fetch("missing", 0, 0, 0); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("expected ErrPartitionNotFound for fetch, got %v", err)
	}
}

// TestBrokerPublishNotLeader makes sure followers refuse direct client writes
// and surface leader discovery information.
func TestBrokerPublishNotLeader(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Cluster: &ClusterConfig{
			BrokerID: 2,
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
		},
	}
	b := NewBroker(cfg)
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	if _, err := b.Publish("demo", 0, nil, []byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader got %v", err)
	}
	leaderID, addr, err := b.PartitionLeader("demo", 0)
	if leaderID != 1 {
		t.Fatalf("expected leader id 1 got %d", leaderID)
	}
	if !errors.Is(err, ErrLeaderNotAvailable) {
		t.Fatalf("expected ErrLeaderNotAvailable got %v", err)
	}
	if addr != "" {
		t.Fatalf("expected empty leader address got %s", addr)
	}
}

func TestBrokerCloseError(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker()
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	for _, lg := range b.logs["demo"] {
		if err := lg.close(); err != nil {
			t.Fatalf("close partition log: %v", err)
		}
	}

	if err := b.Close(); err == nil {
		t.Fatalf("expected close error")
	}
}

func TestBrokerNewConfig(t *testing.T) {
	cfg := Config{DataDir: "custom", SegmentBytes: 123}
	b := NewBroker(cfg)
	if b.conf.DataDir != "custom" || b.conf.SegmentBytes != 123 {
		t.Fatalf("config not applied: %+v", b.conf)
	}
}

func TestBrokerEnsurePartitionError(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker()
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := os.WriteFile(partPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := b.EnsurePartition("demo", 0, partPath, 64); err == nil {
		t.Fatalf("expected ensure partition error")
	}
}

func TestBrokerSetPartitionLeader(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Cluster: &ClusterConfig{
			BrokerID: 2,
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
		},
	}
	b := NewBroker(cfg)
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	if err := b.SetPartitionLeader("demo", 0, 2, 4); err != nil {
		t.Fatalf("set leader: %v", err)
	}
	assign, err := b.PartitionAssignment("demo", 0)
	if err != nil {
		t.Fatalf("assignment: %v", err)
	}
	if assign.Leader != 2 || assign.Epoch != 4 {
		t.Fatalf("unexpected assignment %+v", assign)
	}
}

func TestBrokerPromotePartitionLeader(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Cluster: &ClusterConfig{
			BrokerID: 2,
			Partitions: map[string]map[int]PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
		},
	}
	b := NewBroker(cfg)
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	if err := b.promotePartitionLeader("demo", 0); err != nil {
		t.Fatalf("promote: %v", err)
	}
	assign, err := b.PartitionAssignment("demo", 0)
	if err != nil {
		t.Fatalf("assignment: %v", err)
	}
	if assign.Leader != 2 || assign.Epoch == 0 {
		t.Fatalf("expected leader 2 with epoch > 0, got %+v", assign)
	}
}
