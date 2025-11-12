package logstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/deltalog"
)

func TestBeginTransactionRequiresDelta(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer func() { _ = lg.Close() }()
	if _, err := lg.BeginTransaction(); !errors.Is(err, ErrTransactionsDisabled) {
		t.Fatalf("expected ErrTransactionsDisabled, got %v", err)
	}
}

func TestTransactionCommitWritesDelta(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 1<<20, WithDeltaLog())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer func() { _ = lg.Close() }()
	txn, err := lg.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction error: %v", err)
	}
	if err := txn.Append(TxnRecord{Payload: []byte("value"), OriginID: 7, OriginSeq: 42}); err != nil {
		t.Fatalf("Append error: %v", err)
	}
	result, err := txn.Commit(context.Background(), CommitMetadata{TxnID: "txn-commit"})
	if err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	if result.Version != 0 {
		t.Fatalf("commit version mismatch got=%d want=0", result.Version)
	}
	if result.FirstOffset != 0 || result.LastOffset != 0 {
		t.Fatalf("unexpected offset range %+v", result)
	}
	if next := lg.NextOffset(); next != 1 {
		t.Fatalf("next offset mismatch got=%d want=1", next)
	}
	replay, err := lg.delta.Replay(context.Background())
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}
	if len(replay.Commits) != 1 {
		t.Fatalf("expected 1 commit, got %d", len(replay.Commits))
	}
	op := replay.Commits[0].Operations[0]
	if op.OriginID != 7 || op.OriginSeq != 42 {
		t.Fatalf("operation metadata mismatch %+v", op)
	}
}

func TestTransactionAbort(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 1<<20, WithDeltaLog())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer func() { _ = lg.Close() }()
	txn, err := lg.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction error: %v", err)
	}
	if err := txn.Append(TxnRecord{Payload: []byte("pending")}); err != nil {
		t.Fatalf("Append error: %v", err)
	}
	txn.Abort()
	if _, err := txn.Commit(context.Background(), CommitMetadata{}); !errors.Is(err, ErrTransactionClosed) {
		t.Fatalf("expected ErrTransactionClosed after abort, got %v", err)
	}
	if next := lg.NextOffset(); next != 0 {
		t.Fatalf("offset advanced after abort, got %d", next)
	}
}

func TestTransactionCopiesPayload(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 1<<20, WithDeltaLog())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer func() { _ = lg.Close() }()
	txn, err := lg.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction error: %v", err)
	}
	payload := []byte("immutable")
	if err := txn.Append(TxnRecord{Payload: payload}); err != nil {
		t.Fatalf("Append error: %v", err)
	}
	copy(payload, []byte("garbled!!"))
	if _, err := txn.Commit(context.Background(), CommitMetadata{}); err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	records, _, err := lg.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if len(records) != 1 || string(records[0]) != "immutable" {
		t.Fatalf("payload mutated, records=%q", records)
	}
}

func TestDeltaCheckpointCreation(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 1<<20, WithDeltaLog(deltalog.WithCheckpointInterval(1)))
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	defer func() { _ = lg.Close() }()
	txn, err := lg.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction error: %v", err)
	}
	if err := txn.Append(TxnRecord{Payload: []byte("checkpoint")}); err != nil {
		t.Fatalf("Append error: %v", err)
	}
	if _, err := txn.Commit(context.Background(), CommitMetadata{}); err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	replay, err := lg.delta.Replay(context.Background())
	if err != nil {
		t.Fatalf("Replay error: %v", err)
	}
	if replay.Checkpoint == nil {
		t.Fatalf("expected checkpoint to be written")
	}
	if replay.Checkpoint.Snapshot.NextOffset != 1 {
		t.Fatalf("checkpoint snapshot nextOffset mismatch got=%d", replay.Checkpoint.Snapshot.NextOffset)
	}
	deltaDir := filepath.Join(dir, "_delta_log")
	entries, err := os.ReadDir(deltaDir)
	if err != nil {
		t.Fatalf("ReadDir error: %v", err)
	}
	found := false
	for _, ent := range entries {
		if strings.HasSuffix(ent.Name(), ".checkpoint.json") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("checkpoint file not found in %s", deltaDir)
	}
}

func TestDeltaRecoveryTruncatesOrphans(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 1<<20, WithDeltaLog())
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	txn, err := lg.BeginTransaction()
	if err != nil {
		t.Fatalf("BeginTransaction error: %v", err)
	}
	if err := txn.Append(TxnRecord{Payload: []byte("committed")}); err != nil {
		t.Fatalf("Append error: %v", err)
	}
	if _, err := txn.Commit(context.Background(), CommitMetadata{}); err != nil {
		t.Fatalf("Commit error: %v", err)
	}
	if _, err := lg.Append([]byte("orphan")); err != nil {
		t.Fatalf("raw append error: %v", err)
	}
	if err := lg.Close(); err != nil {
		t.Fatalf("close error: %v", err)
	}

	lg2, err := Open(dir, 1<<20, WithDeltaLog())
	if err != nil {
		t.Fatalf("re-open error: %v", err)
	}
	defer func() { _ = lg2.Close() }()
	records, last, err := lg2.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if len(records) != 1 || string(records[0]) != "committed" {
		t.Fatalf("unexpected records after recovery: %q", records)
	}
	if last != 0 {
		t.Fatalf("last offset mismatch got=%d want=0", last)
	}
}
