package deltalog

import (
	"errors"
	"time"
)

// OperationType identifies the type of change persisted in a commit entry.
type OperationType string

const (
	// OperationAdd declares that a contiguous range of offsets was appended.
	OperationAdd OperationType = "add"
	// OperationRemove allows future support for tombstones/removals.
	OperationRemove OperationType = "remove"
)

// Operation captures the effect of a single change within a transaction.
type Operation struct {
	Type        OperationType `json:"type"`
	OriginID    int           `json:"origin_id"`
	OriginSeq   uint64        `json:"origin_seq"`
	OffsetStart uint64        `json:"offset_start"`
	OffsetEnd   uint64        `json:"offset_end"`
	Segment     string        `json:"segment"`
	Position    int64         `json:"position"`
	Size        int64         `json:"size"`
}

// Commit mirrors a Delta Lake style log entry that captures the operations
// performed atomically inside a transaction.
type Commit struct {
	Version        uint64      `json:"version"`
	Timestamp      time.Time   `json:"timestamp"`
	TxnID          string      `json:"txn_id,omitempty"`
	Operations     []Operation `json:"operations"`
	CheckpointHint bool        `json:"checkpoint_hint,omitempty"`
}

// SegmentState summarizes one on-disk log segment during checkpointing.
type SegmentState struct {
	Name       string `json:"name"`
	BaseOffset uint64 `json:"base_offset"`
	NextOffset uint64 `json:"next_offset"`
	Size       int64  `json:"size"`
}

// Snapshot aggregates the state required to reconstruct a partition log without
// replaying every historical commit.
type Snapshot struct {
	LastOffset uint64            `json:"last_offset"`
	NextOffset uint64            `json:"next_offset"`
	Segments   []SegmentState    `json:"segments"`
	OriginSeqs map[int]uint64    `json:"origin_seqs"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// Checkpoint stores a serialized snapshot tied to a specific commit version.
type Checkpoint struct {
	Version   uint64    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Snapshot  Snapshot  `json:"snapshot"`
}

// Replay bundles the checkpoint (if any) together with the commits that need
// to be applied after it to reach the latest version.
type Replay struct {
	Checkpoint *Checkpoint
	Commits    []*Commit
}

var (
	// ErrVersionConflict is returned when the caller attempts to write a commit
	// with an unexpected next version number.
	ErrVersionConflict = errors.New("delta log version conflict")
	// ErrMissingVersion signals that the on-disk log is missing a required file.
	ErrMissingVersion = errors.New("delta log missing version")
	// ErrCheckpointOutOfRange is emitted when trying to persist a checkpoint for
	// a yet-to-be-written future version.
	ErrCheckpointOutOfRange = errors.New("delta log checkpoint beyond latest version")
)
