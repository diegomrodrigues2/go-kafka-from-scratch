package logstore

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var ErrSegmentFull = errors.New("segment full")

type Segment struct {
	baseOffset uint64
	nextOffset uint64
	store      *os.File
	index      *indexFile
	maxBytes   int64
	size       int64
	logPath    string
	indexPath  string
}

type appendResult struct {
	offset      uint64
	position    int64
	bytes       int64
	segmentName string
}

type segmentSnapshot struct {
	nextOffset uint64
	size       int64
	indexLen   int
}

func newSegment(dir string, baseOffset uint64, maxBytes int64) (*Segment, error) {
	logName := fmt.Sprintf("%020d.log", baseOffset)
	idxName := fmt.Sprintf("%020d.index", baseOffset)
	logPath := filepath.Join(dir, logName)
	idxPath := filepath.Join(dir, idxName)

	store, err := openFileOp(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	idx, err := openIndexOp(idxPath)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	stat, err := statOp(store)
	if err != nil {
		_ = store.Close()
		_ = idx.close()
		return nil, err
	}

	if _, err := seekOp(store, 0, io.SeekEnd); err != nil {
		_ = store.Close()
		_ = idx.close()
		return nil, err
	}

	seg := &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset + uint64(idx.len()),
		store:      store,
		index:      idx,
		maxBytes:   maxBytes,
		size:       stat.Size(),
		logPath:    logPath,
		indexPath:  idxPath,
	}

	return seg, nil
}

func (s *Segment) Append(value []byte) (uint64, error) {
	res, err := s.appendValue(value)
	return res.offset, err
}

func (s *Segment) appendValue(value []byte) (appendResult, error) {
	recordSize := recordHeaderSize + len(value)
	if s.size+int64(recordSize) > s.maxBytes {
		return appendResult{}, ErrSegmentFull
	}

	off := s.nextOffset

	pos, err := seekOp(s.store, 0, io.SeekEnd)
	if err != nil {
		return appendResult{}, err
	}

	header := make([]byte, recordHeaderSize)
	putRecordSize(header, len(value))
	if _, err := writeOp(s.store, header); err != nil {
		return appendResult{}, err
	}
	if _, err := writeOp(s.store, value); err != nil {
		return appendResult{}, err
	}

	rel := uint32(off - s.baseOffset)
	if err := s.index.add(rel, uint32(pos)); err != nil {
		return appendResult{}, err
	}

	s.size += int64(recordSize)
	s.nextOffset++
	return appendResult{
		offset:      off,
		position:    pos,
		bytes:       int64(recordSize),
		segmentName: filepath.Base(s.logPath),
	}, nil
}

func (s *Segment) ReadFrom(offset uint64, maxBytes int) ([][]byte, uint64, int, error) {
	if offset < s.baseOffset {
		offset = s.baseOffset
	}
	if offset >= s.nextOffset {
		return nil, offset, 0, nil
	}

	startOffset := offset

	relStart := offset - s.baseOffset
	startIndex := int(relStart)
	if startIndex < 0 {
		startIndex = 0
	}

	totalBytes := 0
	lastOffset := offset
	var records [][]byte

	for idx := startIndex; ; idx++ {
		entry, ok := s.index.entryAt(idx)
		if !ok {
			break
		}
		absOffset := s.baseOffset + uint64(idx)

		pos := int64(entry.Position)
		header := make([]byte, recordHeaderSize)
		if _, err := readAtOp(s.store, header, pos); err != nil {
			if errors.Is(err, io.EOF) {
				return eofReturn(records, lastOffset, startOffset, totalBytes)
			}
			return records, lastOffset, totalBytes, err
		}
		size := readRecordSize(header)
		recordBytes := make([]byte, size)
		if _, err := readAtOp(s.store, recordBytes, pos+recordHeaderSize); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return eofReturn(records, lastOffset, startOffset, totalBytes)
			}
			return records, lastOffset, totalBytes, err
		}

		projected := totalBytes + recordHeaderSize + size
		if maxBytes > 0 && projected > maxBytes && len(records) > 0 {
			break
		}

		records = append(records, recordBytes)
		totalBytes = projected
		lastOffset = absOffset

		if maxBytes > 0 && totalBytes >= maxBytes {
			break
		}
	}

	return eofReturn(records, lastOffset, startOffset, totalBytes)
}

func eofReturn(records [][]byte, lastOffset, fallback uint64, totalBytes int) ([][]byte, uint64, int, error) {
	if len(records) == 0 {
		return nil, fallback, totalBytes, nil
	}
	return records, lastOffset, totalBytes, nil
}

func (s *Segment) Close() error {
	if err := s.store.Close(); err != nil {
		_ = s.index.close()
		return err
	}
	return s.index.close()
}

func (s *Segment) NextOffset() uint64 {
	return s.nextOffset
}

func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

func (s *Segment) Remaining() int64 {
	return s.maxBytes - s.size
}

func (s *Segment) truncateEntries(entries int) error {
	if entries < 0 || entries > s.index.len() {
		return fmt.Errorf("truncate entries out of range: %d", entries)
	}
	if entries == s.index.len() {
		return nil
	}
	var newSize int64
	if entries == 0 {
		newSize = 0
	} else {
		entry, ok := s.index.entryAt(entries - 1)
		if !ok {
			return fmt.Errorf("missing index entry at %d", entries-1)
		}
		header := make([]byte, recordHeaderSize)
		pos := int64(entry.Position)
		if _, err := readAtOp(s.store, header, pos); err != nil {
			return err
		}
		size := readRecordSize(header)
		newSize = pos + int64(recordHeaderSize+size)
	}
	if err := truncateOp(s.store, newSize); err != nil {
		return err
	}
	if err := s.index.truncate(entries); err != nil {
		return err
	}
	s.size = newSize
	s.nextOffset = s.baseOffset + uint64(entries)
	return nil
}

func (s *Segment) snapshot() segmentSnapshot {
	return segmentSnapshot{
		nextOffset: s.nextOffset,
		size:       s.size,
		indexLen:   s.index.len(),
	}
}

func (s *Segment) restore(snapshot segmentSnapshot) error {
	if err := truncateOp(s.store, snapshot.size); err != nil {
		return err
	}
	if err := s.index.truncate(snapshot.indexLen); err != nil {
		return err
	}
	s.size = snapshot.size
	s.nextOffset = snapshot.nextOffset
	return nil
}

func (s *Segment) FileName() string {
	return filepath.Base(s.logPath)
}

func (s *Segment) deleteFiles() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(s.indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
