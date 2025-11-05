package logstore

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu           sync.RWMutex
	dir          string
	segmentBytes int64
	segments     []*Segment
}

func Open(dir string, segmentBytes int64) (*Log, error) {
	if err := mkdirAllOp(dir, 0o755); err != nil {
		return nil, err
	}

	log := &Log{dir: dir, segmentBytes: segmentBytes}

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
