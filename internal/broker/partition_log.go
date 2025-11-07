package broker

import (
	"sync"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

type partitionLog struct {
	log       *logstore.Log
	mu        sync.Mutex
	originSeq map[int]uint64
}

// newPartitionLog wraps a logstore.Log with the bookkeeping required for origin sequence tracking.
func newPartitionLog(lg *logstore.Log) (*partitionLog, error) {
	pl := &partitionLog{
		log:       lg,
		originSeq: make(map[int]uint64),
	}
	if err := pl.rebuildOriginSequences(); err != nil {
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

// appendLocal encodes the payload with origin metadata, appends it locally and bumps the origin sequence counter.
func (pl *partitionLog) appendLocal(originID int, payload []byte) (uint64, uint64, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	nextSeq := pl.originSeq[originID] + 1
	raw := encodeRecord(originID, nextSeq, payload)
	offset, err := pl.log.Append(raw)
	if err != nil {
		return 0, 0, err
	}
	pl.originSeq[originID] = nextSeq
	return offset, nextSeq, nil
}

// appendReplica stores a raw replicated record if it is newer than the local state, returning whether it was applied.
func (pl *partitionLog) appendReplica(raw []byte) (uint64, bool, error) {
	rec := decodeRecord(raw)

	pl.mu.Lock()
	defer pl.mu.Unlock()

	if rec.HasMeta {
		if rec.OriginSeq <= pl.originSeq[rec.OriginID] {
			return pl.log.NextOffset(), false, nil
		}
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
