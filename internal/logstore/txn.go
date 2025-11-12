package logstore

import (
	"context"
	"errors"
	"log"
	"math"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/deltalog"
)

// TxnRecord holds a single payload plus optional origin metadata that will be
// propagated to the delta log.
type TxnRecord struct {
	Payload   []byte
	OriginID  int
	OriginSeq uint64
}

// CommitMetadata allows the caller to thread additional identifiers (like the
// producer id) into the delta log entry.
type CommitMetadata struct {
	TxnID string
}

// CommitResult reports the delta log version as well as the offset range that
// became visible.
type CommitResult struct {
	Version     uint64
	FirstOffset uint64
	LastOffset  uint64
}

// Txn accumulates a batch of records that will be appended atomically once
// Commit is invoked.
type Txn struct {
	log             *Log
	records         []txnRecord
	closed          bool
	versionOverride *uint64
}

type txnRecord struct {
	payload   []byte
	originID  int
	originSeq uint64
}

// BeginTransaction prepares a transactional append session when delta logging
// is enabled for the current log.
func (l *Log) BeginTransaction() (*Txn, error) {
	if !l.DeltaEnabled() {
		return nil, ErrTransactionsDisabled
	}
	return &Txn{log: l}, nil
}

// Append buffers a new record for the current transaction. The payload is
// copied to ensure subsequent caller mutations do not affect the commit.
func (t *Txn) Append(rec TxnRecord) error {
	if t.closed {
		return ErrTransactionClosed
	}
	if len(rec.Payload) == 0 {
		return errors.New("transaction record payload required")
	}
	payload := make([]byte, len(rec.Payload))
	copy(payload, rec.Payload)
	t.records = append(t.records, txnRecord{
		payload:   payload,
		originID:  rec.OriginID,
		originSeq: rec.OriginSeq,
	})
	return nil
}

// ForceVersion instructs the transaction to commit using the provided log version.
func (t *Txn) ForceVersion(version uint64) {
	if t.closed {
		return
	}
	t.versionOverride = &version
}

// Abort drops the buffered records and prevents further use of the transaction.
func (t *Txn) Abort() {
	if t.closed {
		return
	}
	t.closed = true
	t.records = nil
}

// Commit writes the buffered payloads to the underlying log and records the
// associated metadata in the delta log. The offset range is returned once both
// data and metadata are durable.
func (t *Txn) Commit(ctx context.Context, meta CommitMetadata) (CommitResult, error) {
	if t.closed {
		return CommitResult{}, ErrTransactionClosed
	}
	defer func() {
		t.closed = true
		t.records = nil
	}()
	if len(t.records) == 0 {
		return CommitResult{}, ErrEmptyTransaction
	}
	return t.log.commitTransaction(ctx, t.records, meta, t.versionOverride)
}

func (l *Log) commitTransaction(ctx context.Context, records []txnRecord, meta CommitMetadata, versionOverride *uint64) (CommitResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	expectedVersion := l.delta.NextVersion()
	if versionOverride != nil {
		expectedVersion = *versionOverride
	}
	snapshot := l.snapshotState()

	ops, first, last, err := l.appendRecordsLocked(records)
	if err != nil {
		_ = l.rollback(snapshot)
		return CommitResult{}, err
	}
	commit := &deltalog.Commit{
		TxnID:      meta.TxnID,
		Operations: ops,
	}
	version, err := l.delta.Commit(ctx, commit, expectedVersion)
	if err != nil {
		_ = l.rollback(snapshot)
		if errors.Is(err, deltalog.ErrVersionConflict) {
			return CommitResult{}, ErrTxnConflict
		}
		return CommitResult{}, err
	}
	if err := l.maybeCheckpointLocked(ctx, version, last); err != nil {
		log.Printf("log checkpoint error: %v", err)
	}
	return CommitResult{
		Version:     version,
		FirstOffset: first,
		LastOffset:  last,
	}, nil
}

func (l *Log) appendRecordsLocked(records []txnRecord) ([]deltalog.Operation, uint64, uint64, error) {
	if len(records) == 0 {
		return nil, 0, 0, ErrEmptyTransaction
	}
	ops := make([]deltalog.Operation, 0, len(records))
	firstOffset := uint64(math.MaxUint64)
	var lastOffset uint64

	for _, rec := range records {
		for {
			if len(l.segments) == 0 {
				return nil, 0, 0, errors.New("log has no segments")
			}
			seg := l.segments[len(l.segments)-1]
			res, err := seg.appendValue(rec.payload)
			if errors.Is(err, ErrSegmentFull) {
				newSeg, segErr := l.newSegment(seg.NextOffset())
				if segErr != nil {
					return nil, 0, 0, segErr
				}
				l.segments = append(l.segments, newSeg)
				continue
			}
			if err != nil {
				return nil, 0, 0, err
			}
			op := deltalog.Operation{
				Type:        deltalog.OperationAdd,
				OriginID:    rec.originID,
				OriginSeq:   rec.originSeq,
				OffsetStart: res.offset,
				OffsetEnd:   res.offset,
				Segment:     res.segmentName,
				Position:    res.position,
				Size:        res.bytes,
			}
			ops = append(ops, op)
			if firstOffset == math.MaxUint64 {
				firstOffset = res.offset
			}
			lastOffset = res.offset
			break
		}
	}
	return ops, firstOffset, lastOffset, nil
}

type logSnapshot struct {
	segmentCount int
	tail         segmentSnapshot
}

func (l *Log) snapshotState() logSnapshot {
	snap := logSnapshot{segmentCount: len(l.segments)}
	if snap.segmentCount > 0 {
		snap.tail = l.segments[snap.segmentCount-1].snapshot()
	}
	return snap
}

func (l *Log) rollback(snap logSnapshot) error {
	for len(l.segments) > snap.segmentCount {
		lastIdx := len(l.segments) - 1
		seg := l.segments[lastIdx]
		if err := seg.deleteFiles(); err != nil {
			return err
		}
		l.segments = l.segments[:lastIdx]
	}
	if snap.segmentCount == 0 || len(l.segments) == 0 {
		return nil
	}
	return l.segments[snap.segmentCount-1].restore(snap.tail)
}
