package logstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/deltalog"
)

type Log struct {
	mu           sync.RWMutex
	dir          string
	segmentBytes int64
	segments     []*Segment
	delta        *deltalog.Store
}

type Option func(*logOptions)

type logOptions struct {
	enableDelta bool
	deltaOpts   []deltalog.Option
}

// WithDeltaLog enables transactional metadata tracking via the internal delta
// log and forwards the provided store options.
func WithDeltaLog(deltaOpts ...deltalog.Option) Option {
	return func(lo *logOptions) {
		lo.enableDelta = true
		if len(deltaOpts) > 0 {
			lo.deltaOpts = append(lo.deltaOpts, deltaOpts...)
		}
	}
}

func Open(dir string, segmentBytes int64, opts ...Option) (*Log, error) {
	if err := mkdirAllOp(dir, 0o755); err != nil {
		return nil, err
	}

	var cfg logOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	var deltaStore *deltalog.Store
	if cfg.enableDelta {
		store, err := deltalog.Open(dir, cfg.deltaOpts...)
		if err != nil {
			return nil, err
		}
		deltaStore = store
	}

	log := &Log{dir: dir, segmentBytes: segmentBytes, delta: deltaStore}

	if err := log.loadSegments(); err != nil {
		return nil, err
	}

	if len(log.segments) == 0 {
		seg, err := newSegmentOp(dir, 0, segmentBytes)
		if err != nil {
			return nil, err
		}
		log.segments = append(log.segments, seg)
	}

	if log.delta != nil {
		if err := log.reconcileWithDelta(); err != nil {
			return nil, err
		}
	}

	return log, nil
}

func (l *Log) loadSegments() error {
	entries, err := readDirOp(l.dir)
	if err != nil {
		return err
	}

	bases := make([]uint64, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		baseStr := strings.TrimSuffix(name, ".log")
		base, err := strconv.ParseUint(baseStr, 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}

	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })

	for _, base := range bases {
		seg, err := newSegmentOp(l.dir, base, l.segmentBytes)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
	}

	return nil
}

func (l *Log) Append(value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for {
		seg := l.segments[len(l.segments)-1]
		off, err := seg.Append(value)
		if errors.Is(err, ErrSegmentFull) {
			newSeg, err := l.newSegment(seg.NextOffset())
			if err != nil {
				return 0, err
			}
			l.segments = append(l.segments, newSeg)
			continue
		}
		return off, err
	}
}

func (l *Log) ReadFrom(offset uint64, maxBytes int) ([][]byte, uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.segments) == 0 {
		return nil, offset, nil
	}

	segIdx := l.segmentIndex(offset)

	var records [][]byte
	lastOffset := offset
	remaining := maxBytes

	for ; segIdx < len(l.segments); segIdx++ {
		seg := l.segments[segIdx]
		segRecords, segLast, used, err := seg.ReadFrom(offset, remaining)
		if err != nil {
			return records, lastOffset, err
		}
		if len(segRecords) > 0 {
			records = append(records, segRecords...)
			lastOffset = segLast
			if remaining > 0 {
				remaining -= used
				if remaining <= 0 {
					break
				}
			}
			offset = segLast + 1
		} else {
			offset = seg.NextOffset()
		}

		if maxBytes <= 0 && segIdx == len(l.segments)-1 {
			break
		}
	}

	if len(records) == 0 {
		return nil, offset, nil
	}

	return records, lastOffset, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var errs []error
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (l *Log) newSegment(base uint64) (*Segment, error) {
	return newSegmentOp(l.dir, base, l.segmentBytes)
}

func (l *Log) segmentIndex(offset uint64) int {
	for idx, seg := range l.segments {
		if offset >= seg.BaseOffset() && offset < seg.NextOffset() {
			return idx
		}
	}
	if offset < l.segments[0].BaseOffset() {
		return 0
	}
	return len(l.segments) - 1
}

func (l *Log) Path() string {
	return filepath.Clean(l.dir)
}

func (l *Log) String() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return fmt.Sprintf("Log{path=%s segments=%d}", l.dir, len(l.segments))
}

func (l *Log) NextOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[len(l.segments)-1].NextOffset()
}

// DeltaEnabled reports whether the log is tracking transactional metadata.
func (l *Log) DeltaEnabled() bool {
	return l.delta != nil
}

func (l *Log) reconcileWithDelta() error {
	if l.delta == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.reconcileWithDeltaLocked()
}

func (l *Log) reconcileWithDeltaLocked() error {
	replay, err := l.delta.Replay(context.Background())
	if err != nil {
		return err
	}
	expectedNext := l.computeExpectedNext(replay)
	actualNext := l.tailNextOffsetLocked()
	if expectedNext == 0 {
		if replay == nil || (replay.Checkpoint == nil && len(replay.Commits) == 0) {
			return nil
		}
	}
	if expectedNext > actualNext {
		return fmt.Errorf("delta log ahead of data: expected next=%d actual=%d", expectedNext, actualNext)
	}
	if expectedNext < actualNext {
		if err := l.truncateLocked(expectedNext); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) computeExpectedNext(replay *deltalog.Replay) uint64 {
	if replay == nil {
		return 0
	}
	var expected uint64
	if replay.Checkpoint != nil {
		expected = replay.Checkpoint.Snapshot.NextOffset
	}
	for _, commit := range replay.Commits {
		for _, op := range commit.Operations {
			offset := op.OffsetEnd + 1
			if offset > expected {
				expected = offset
			}
		}
	}
	return expected
}

func (l *Log) tailNextOffsetLocked() uint64 {
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[len(l.segments)-1].NextOffset()
}

func (l *Log) truncateLocked(expectedNext uint64) error {
	for len(l.segments) > 0 {
		lastIdx := len(l.segments) - 1
		seg := l.segments[lastIdx]
		if expectedNext >= seg.NextOffset() {
			break
		}
		if expectedNext <= seg.BaseOffset() {
			if err := seg.deleteFiles(); err != nil {
				return err
			}
			l.segments = l.segments[:lastIdx]
			continue
		}
		keep := int(expectedNext - seg.BaseOffset())
		if err := seg.truncateEntries(keep); err != nil {
			return err
		}
		break
	}
	if len(l.segments) == 0 {
		seg, err := l.newSegment(0)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
	}
	return nil
}

func (l *Log) buildSnapshotLocked(lastOffset uint64) deltalog.Snapshot {
	segments := make([]deltalog.SegmentState, len(l.segments))
	for i, seg := range l.segments {
		segments[i] = deltalog.SegmentState{
			Name:       seg.FileName(),
			BaseOffset: seg.BaseOffset(),
			NextOffset: seg.NextOffset(),
			Size:       seg.size,
		}
	}
	nextOffset := uint64(0)
	if len(l.segments) > 0 {
		nextOffset = lastOffset + 1
	}
	return deltalog.Snapshot{
		LastOffset: lastOffset,
		NextOffset: nextOffset,
		Segments:   segments,
	}
}

func (l *Log) maybeCheckpointLocked(ctx context.Context, version, lastOffset uint64) error {
	if l.delta == nil {
		return nil
	}
	interval := l.delta.CheckpointInterval()
	if interval == 0 {
		return nil
	}
	if (version+1)%interval != 0 {
		return nil
	}
	snapshot := l.buildSnapshotLocked(lastOffset)
	ckpt := &deltalog.Checkpoint{
		Version:  version,
		Snapshot: snapshot,
	}
	return l.delta.WriteCheckpoint(ctx, ckpt)
}

func (l *Log) DeltaReplay(ctx context.Context) (*deltalog.Replay, error) {
	if l.delta == nil {
		return nil, ErrTransactionsDisabled
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return l.delta.Replay(ctx)
}

func (l *Log) DeltaVersion() (uint64, bool) {
	if l.delta == nil {
		return 0, false
	}
	return l.delta.CurrentVersion()
}

// DeltaCommits returns the list of commits starting from the provided version.
func (l *Log) DeltaCommits(fromVersion uint64) ([]*deltalog.Commit, error) {
	if l.delta == nil {
		return nil, ErrTransactionsDisabled
	}
	return l.delta.Commits(fromVersion)
}

// ApplyExternalCommit records a commit produced by another replica into the local delta log.
func (l *Log) ApplyExternalCommit(ctx context.Context, commit *deltalog.Commit) error {
	if l.delta == nil {
		return ErrTransactionsDisabled
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return l.delta.ApplyExternalCommit(ctx, commit)
}
