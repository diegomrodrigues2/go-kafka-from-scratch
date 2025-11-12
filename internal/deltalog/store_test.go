package deltalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreCommitSequence(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}

	if got := store.NextVersion(); got != 0 {
		t.Fatalf("NextVersion mismatch got=%d want=0", got)
	}

	entry := &Commit{
		TxnID: "txn-1",
		Operations: []Operation{{
			Type:        OperationAdd,
			OriginID:    1,
			OriginSeq:   1,
			OffsetStart: 0,
			OffsetEnd:   0,
			Segment:     "00000000000000000000.log",
			Position:    0,
			Size:        16,
		}},
	}
	version, err := store.Commit(context.Background(), entry, store.NextVersion())
	if err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	if version != 0 {
		t.Fatalf("commit version mismatch got=%d want=0", version)
	}

	if _, err := os.Stat(filepath.Join(store.Dir(), "00000000000000000000.json")); err != nil {
		t.Fatalf("commit file missing: %v", err)
	}

	entry2 := &Commit{TxnID: "txn-2"}
	version, err = store.Commit(context.Background(), entry2, store.NextVersion())
	if err != nil {
		t.Fatalf("second Commit error: %v", err)
	}
	if version != 1 {
		t.Fatalf("second commit version mismatch got=%d want=1", version)
	}
}

func TestStoreCommitConflict(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}

	_, err = store.Commit(context.Background(), &Commit{}, store.NextVersion())
	if err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	if _, err := store.Commit(context.Background(), &Commit{}, 0); err == nil {
		t.Fatalf("expected conflict, got nil")
	} else if err != ErrVersionConflict {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestStoreReplayWithCheckpoint(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}

	for i := 0; i < 2; i++ {
		if _, err := store.Commit(context.Background(), &Commit{}, store.NextVersion()); err != nil {
			t.Fatalf("Commit %d error: %v", i, err)
		}
	}

	checkpoint := &Checkpoint{
		Version: 1,
		Snapshot: Snapshot{
			LastOffset: 10,
			NextOffset: 11,
			Segments: []SegmentState{
				{Name: "00000000000000000000.log", BaseOffset: 0, NextOffset: 11, Size: 256},
			},
			OriginSeqs: map[int]uint64{1: 10},
		},
	}
	if err := store.WriteCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatalf("WriteCheckpoint error: %v", err)
	}

	if _, err := store.Commit(context.Background(), &Commit{}, store.NextVersion()); err != nil {
		t.Fatalf("post-checkpoint Commit error: %v", err)
	}

	replay, err := store.Replay(context.Background())
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}
	if replay.Checkpoint == nil {
		t.Fatalf("expected checkpoint data")
	}
	if replay.Checkpoint.Version != 1 {
		t.Fatalf("checkpoint version mismatch got=%d want=1", replay.Checkpoint.Version)
	}
	if len(replay.Commits) != 1 {
		t.Fatalf("expected 1 commit after checkpoint, got %d", len(replay.Commits))
	}
	if replay.Commits[0].Version != 2 {
		t.Fatalf("expected replay commit version 2, got %d", replay.Commits[0].Version)
	}
}

func TestWriteCheckpointFutureVersion(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}

	if _, err := store.Commit(context.Background(), &Commit{}, store.NextVersion()); err != nil {
		t.Fatalf("Commit error: %v", err)
	}

	ckpt := &Checkpoint{Version: 5}
	if err := store.WriteCheckpoint(context.Background(), ckpt); err == nil {
		t.Fatalf("expected checkpoint range error")
	} else if err != ErrCheckpointOutOfRange {
		t.Fatalf("unexpected checkpoint error %v", err)
	}
}
