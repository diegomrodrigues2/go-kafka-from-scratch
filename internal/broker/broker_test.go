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
	if len(recs) != 1 || string(recs[0]) != "value" || last != 0 {
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

func TestBrokerCloseError(t *testing.T) {
	dir := t.TempDir()
	b := NewBroker()
	defer func() { _ = b.Close() }()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}

	for _, lg := range b.logs["demo"] {
		if err := lg.Close(); err != nil {
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
