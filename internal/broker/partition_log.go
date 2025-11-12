package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/deltalog"
	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

type commitRange struct {
	start   uint64
	end     uint64
	version uint64
}

type partitionLog struct {
	log        *logstore.Log
	useDelta   bool
	mu         sync.Mutex
	originSeq  map[int]uint64
	commitInfo []commitRange
}

// newPartitionLog wraps a logstore.Log with the bookkeeping required for origin sequence tracking.
func newPartitionLog(lg *logstore.Log) (*partitionLog, error) {
	pl := &partitionLog{
		log:       lg,
		useDelta:  lg.DeltaEnabled(),
		originSeq: make(map[int]uint64),
	}
	if err := pl.rebuildOriginSequences(); err != nil {
		return nil, err
	}
	if err := pl.rebuildCommitIndex(); err != nil {
		return nil, err
	}
	return pl, nil
}

// rebuildOriginSequences scans the on-disk log to reconstruct the highest observed sequence per origin.
func (pl *partitionLog) rebuildOriginSequences() error {
	records, _, err := pl.log.ReadFrom(0, 0)
	if err != nil {
		return err
	}
	for _, raw := range records {
		rec := decodeRecord(raw)
		if !rec.HasMeta {
			continue
		}
		if rec.OriginSeq > pl.originSeq[rec.OriginID] {
			pl.originSeq[rec.OriginID] = rec.OriginSeq
		}
	}
	return nil
}

func (pl *partitionLog) rebuildCommitIndex() error {
	if !pl.useDelta {
		return nil
	}
	commits, err := pl.log.DeltaCommits(0)
	if err != nil && !errors.Is(err, logstore.ErrTransactionsDisabled) {
		return err
	}
	var ranges []commitRange
	for _, commit := range commits {
		for _, op := range commit.Operations {
			ranges = append(ranges, commitRange{
				start:   op.OffsetStart,
				end:     op.OffsetEnd,
				version: commit.Version,
			})
		}
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start == ranges[j].start {
			return ranges[i].version < ranges[j].version
		}
		return ranges[i].start < ranges[j].start
	})
	pl.mu.Lock()
	pl.commitInfo = ranges
	pl.mu.Unlock()
	return nil
}

func (pl *partitionLog) resyncCommitIndex() error {
	if !pl.useDelta {
		return nil
	}
	pl.mu.Lock()
	pl.commitInfo = nil
	pl.mu.Unlock()
	return pl.rebuildCommitIndex()
}

// appendLocal encodes the payload with origin metadata, appends it locally and registra metadata de commit.
func (pl *partitionLog) appendLocal(originID int, payload []byte) (offset uint64, seq uint64, version uint64, err error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	nextSeq := pl.originSeq[originID] + 1
	raw := encodeRecord(originID, nextSeq, payload)
	if pl.useDelta {
		res, commitErr := pl.appendEntriesWithTxn([]txnEntry{
			{raw: raw, originID: originID, originSeq: nextSeq},
		}, fmt.Sprintf("local-%d-%d", originID, nextSeq), nil)
		if commitErr != nil {
			return 0, 0, 0, translateTxnError(commitErr)
		}
		offset = res.LastOffset
		version = res.Version
		pl.recordCommitRange(res.Version, res.FirstOffset, res.LastOffset)
	} else {
		offset, err = pl.log.Append(raw)
		if err != nil {
			return 0, 0, 0, err
		}
	}
	pl.originSeq[originID] = nextSeq
	return offset, nextSeq, version, nil
}

// appendLocalBatch stores um lote como única transação e retorna offsets + versão.
func (pl *partitionLog) appendLocalBatch(originID int, payloads [][]byte) ([]uint64, uint64, error) {
	if len(payloads) == 0 {
		return nil, 0, errors.New("batch payloads required")
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	nextSeq := pl.originSeq[originID]
	offsets := make([]uint64, len(payloads))
	if !pl.useDelta {
		for i, payload := range payloads {
			nextSeq++
			raw := encodeRecord(originID, nextSeq, payload)
			off, err := pl.log.Append(raw)
			if err != nil {
				return nil, 0, err
			}
			offsets[i] = off
		}
		pl.originSeq[originID] = nextSeq
		return offsets, 0, nil
	}
	entries := make([]txnEntry, len(payloads))
	for i, payload := range payloads {
		nextSeq++
		raw := encodeRecord(originID, nextSeq, payload)
		entries[i] = txnEntry{raw: raw, originID: originID, originSeq: nextSeq}
	}
	res, err := pl.appendEntriesWithTxn(entries, fmt.Sprintf("local-batch-%d-%d", originID, nextSeq), nil)
	if err != nil {
		return nil, 0, translateTxnError(err)
	}
	for i := range offsets {
		offsets[i] = res.FirstOffset + uint64(i)
	}
	pl.originSeq[originID] = nextSeq
	pl.recordCommitRange(res.Version, res.FirstOffset, res.LastOffset)
	return offsets, res.Version, nil
}

// appendReplica armazena registros replicados e pode impor versão esperada.
func (pl *partitionLog) appendReplica(raw []byte, commitVersion uint64, hasVersion bool) (uint64, bool, error) {
	rec := decodeRecord(raw)

	pl.mu.Lock()
	defer pl.mu.Unlock()

	if rec.HasMeta {
		if rec.OriginSeq <= pl.originSeq[rec.OriginID] {
			return pl.log.NextOffset(), false, nil
		}
	}

	if pl.useDelta {
		var override *uint64
		if hasVersion {
			override = &commitVersion
		}
		res, err := pl.appendEntriesWithTxn([]txnEntry{
			{raw: raw, originID: rec.OriginID, originSeq: rec.OriginSeq},
		}, fmt.Sprintf("replica-%d-%d", rec.OriginID, rec.OriginSeq), override)
		if err != nil {
			return 0, false, translateTxnError(err)
		}
		pl.recordCommitRange(res.Version, res.FirstOffset, res.LastOffset)
		if rec.HasMeta {
			pl.originSeq[rec.OriginID] = rec.OriginSeq
		}
		return res.LastOffset, true, nil
	}

	offset, err := pl.log.Append(raw)
	if err != nil {
		return 0, false, err
	}
	if rec.HasMeta {
		pl.originSeq[rec.OriginID] = rec.OriginSeq
	}
	return offset, true, nil
}

// readRecords returns decoded PartitionRecords starting from offset up to maxBytes from the underlying logstore.
func (pl *partitionLog) readRecords(offset uint64, maxBytes int) ([]PartitionRecord, uint64, error) {
	rawRecords, last, err := pl.log.ReadFrom(offset, maxBytes)
	if err != nil {
		return nil, last, err
	}
	records := make([]PartitionRecord, len(rawRecords))
	for i, raw := range rawRecords {
		records[i] = decodeRecord(raw)
	}
	if len(records) > 0 && pl.useDelta {
		start := last - uint64(len(records)) + 1
		for i := range records {
			if version, ok := pl.commitVersionForOffset(start + uint64(i)); ok {
				records[i].CommitVersion = version
			}
		}
	}
	return records, last, nil
}

// nextOffset exposes the next offset that will be assigned by the log.
func (pl *partitionLog) nextOffset() uint64 {
	return pl.log.NextOffset()
}

// close flushes and closes the underlying log segments.
func (pl *partitionLog) close() error {
	return pl.log.Close()
}

func (pl *partitionLog) commitVersionsFrom(start uint64, count int) []uint64 {
	if count <= 0 || !pl.useDelta {
		return nil
	}
	result := make([]uint64, count)
	for i := 0; i < count; i++ {
		off := start + uint64(i)
		if version, ok := pl.commitVersionForOffset(off); ok {
			result[i] = version
		}
	}
	return result
}

func (pl *partitionLog) commitVersionForOffset(offset uint64) (uint64, bool) {
	for i := len(pl.commitInfo) - 1; i >= 0; i-- {
		r := pl.commitInfo[i]
		if offset >= r.start && offset <= r.end {
			return r.version, true
		}
		if offset > r.end {
			break
		}
	}
	return 0, false
}

func (pl *partitionLog) recordCommitRange(version, start, end uint64) {
	if !pl.useDelta {
		return
	}
	pl.commitInfo = append(pl.commitInfo, commitRange{
		start:   start,
		end:     end,
		version: version,
	})
}

func (pl *partitionLog) applyRemoteCommits(commits []*deltalog.Commit) error {
	if !pl.useDelta || len(commits) == 0 {
		return nil
	}
	pl.mu.Lock()
	defer pl.mu.Unlock()
	for _, commit := range commits {
		if commit == nil {
			continue
		}
		if err := pl.log.ApplyExternalCommit(context.Background(), commit); err != nil {
			if errors.Is(err, logstore.ErrTxnConflict) {
				continue
			}
			return err
		}
		for _, op := range commit.Operations {
			pl.recordCommitRange(commit.Version, op.OffsetStart, op.OffsetEnd)
		}
	}
	return nil
}

type txnEntry struct {
	raw       []byte
	originID  int
	originSeq uint64
}

type commitResult struct {
	FirstOffset uint64
	LastOffset  uint64
	Version     uint64
}

func (pl *partitionLog) appendEntriesWithTxn(entries []txnEntry, txnID string, versionOverride *uint64) (commitResult, error) {
	txn, err := pl.log.BeginTransaction()
	if err != nil {
		return commitResult{}, err
	}
	if versionOverride != nil {
		txn.ForceVersion(*versionOverride)
	}
	for _, entry := range entries {
		rec := logstore.TxnRecord{
			Payload:   entry.raw,
			OriginID:  entry.originID,
			OriginSeq: entry.originSeq,
		}
		if err := txn.Append(rec); err != nil {
			txn.Abort()
			return commitResult{}, err
		}
	}
	res, err := txn.Commit(context.Background(), logstore.CommitMetadata{TxnID: txnID})
	if err != nil {
		return commitResult{}, err
	}
	return commitResult{
		FirstOffset: res.FirstOffset,
		LastOffset:  res.LastOffset,
		Version:     res.Version,
	}, nil
}

func translateTxnError(err error) error {
	if errors.Is(err, logstore.ErrTxnConflict) {
		return ErrTxnConflict
	}
	return err
}
