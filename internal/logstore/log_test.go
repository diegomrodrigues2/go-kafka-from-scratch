package logstore

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogOpenAppendRead(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(filepath.Join(dir, "ignore.tmp"), []byte("x"), 0o644); err != nil {
		t.Fatalf("preparing directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.log"), []byte("junk"), 0o644); err != nil {
		t.Fatalf("preparing bad file: %v", err)
	}

	lg, err := Open(filepath.Join(dir, "log"), 8)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	if lg.Path() != filepath.Join(dir, "log") {
		t.Fatalf("unexpected path: %s", lg.Path())
	}
	_ = lg.String()

	off0, err := lg.Append([]byte("a"))
	if err != nil {
		t.Fatalf("append first: %v", err)
	}
	if off0 != 0 {
		t.Fatalf("expected first offset 0, got %d", off0)
	}

	off1, err := lg.Append([]byte("b"))
	if err != nil {
		t.Fatalf("append second: %v", err)
	}
	if off1 != 1 {
		t.Fatalf("expected second offset 1, got %d", off1)
	}

	recs, last, err := lg.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(recs) != 2 || string(recs[0]) != "a" || string(recs[1]) != "b" || last != 1 {
		t.Fatalf("unexpected read results: %v last=%d", recs, last)
	}

	recsLimited, lastLimited, err := lg.ReadFrom(0, recordHeaderSize+1)
	if err != nil {
		t.Fatalf("read limited: %v", err)
	}
	if len(recsLimited) != 1 || lastLimited != 0 {
		t.Fatalf("expected one record with last 0, got %d last=%d", len(recsLimited), lastLimited)
	}

	none, next, err := lg.ReadFrom(10, 0)
	if err != nil {
		t.Fatalf("read empty: %v", err)
	}
	if len(none) != 0 || next != off1+1 {
		t.Fatalf("expected empty read with next %d got len=%d next=%d", off1+1, len(none), next)
	}

	if err := lg.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	// Reopen to ensure segments on disk are loaded correctly.
	lg, err = Open(filepath.Join(dir, "log"), 8)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(lg.segments) != 2 {
		t.Fatalf("expected two segments after reopen, got %d", len(lg.segments))
	}
	if err := lg.Close(); err != nil {
		t.Fatalf("close after reopen: %v", err)
	}
}

func TestLogReadFromEmpty(t *testing.T) {
	var lg Log
	recs, next, err := lg.ReadFrom(3, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(recs) != 0 || next != 3 {
		t.Fatalf("expected empty read, got %d next=%d", len(recs), next)
	}
}

func TestLogSegmentIndexBelowBase(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 5, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	lg := &Log{segments: []*Segment{seg}}
	recs, next, err := lg.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 0 || next != seg.NextOffset() {
		t.Fatalf("expected next offset %d got len=%d next=%d", seg.NextOffset(), len(recs), next)
	}
}

func TestLogCloseError(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 32)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, seg := range lg.segments {
		if err := seg.store.Close(); err != nil {
			t.Fatalf("pre-close segment: %v", err)
		}
	}
	if err := lg.Close(); err == nil {
		t.Fatalf("expected close error")
	}
}

func TestSegmentReadFromHandlesTruncation(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 5, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() {
		_ = seg.Close()
	}()

	if _, err := seg.Append([]byte("abc")); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := seg.store.Truncate(int64(recordHeaderSize + 2)); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	recs, last, used, err := seg.ReadFrom(5, 0)
	if err != nil {
		t.Fatalf("read truncated: %v", err)
	}
	if len(recs) != 0 || last != 5 || used != 0 {
		t.Fatalf("expected no records after truncation")
	}
}

func TestSegmentAppendFull(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, recordHeaderSize+1)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if _, err := seg.Append([]byte("x")); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := seg.Append([]byte("y")); !errors.Is(err, ErrSegmentFull) {
		t.Fatalf("expected ErrSegmentFull, got %v", err)
	}
	if seg.Remaining() != 0 {
		t.Fatalf("expected remaining 0, got %d", seg.Remaining())
	}
}

func TestIndexFileOperations(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "00000000000000000000.index")
	idx, err := openIndex(idxPath)
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	defer func() { _ = idx.close() }()

	if idx.len() != 0 {
		t.Fatalf("expected empty index")
	}

	if err := idx.add(1, 10); err != nil {
		t.Fatalf("add entry: %v", err)
	}
	if err := idx.add(2, 20); err != nil {
		t.Fatalf("add entry 2: %v", err)
	}

	entry, err := idx.find(2)
	if err != nil || entry.Position != 20 {
		t.Fatalf("unexpected find result: %+v err=%v", entry, err)
	}
	if _, err := idx.find(99); !errors.Is(err, errIndexEntryNotFound) {
		t.Fatalf("expected errIndexEntryNotFound, got %v", err)
	}

	if e, ok := idx.entryAt(0); !ok || e.RelativeOffset != 1 {
		t.Fatalf("entryAt 0 unexpected")
	}
	if _, ok := idx.entryAt(5); ok {
		t.Fatalf("expected entryAt out of range false")
	}

	if idx.len() != 2 {
		t.Fatalf("expected len 2, got %d", idx.len())
	}

	if err := idx.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	idx, err = openIndex(idxPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = idx.close() }()
	if idx.len() != 2 {
		t.Fatalf("expected len 2 after reopen, got %d", idx.len())
	}
}

func TestOpenIndexCorrupted(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "00000000000000000000.index")
	if err := os.WriteFile(idxPath, []byte("xxxx"), 0o644); err != nil {
		t.Fatalf("write corrupted index: %v", err)
	}
	if _, err := openIndex(idxPath); err == nil {
		t.Fatalf("expected error for corrupted index")
	}
}

func TestRecordEncoding(t *testing.T) {
	buf := make([]byte, recordHeaderSize)
	putRecordSize(buf, 42)
	if got := readRecordSize(buf); got != 42 {
		t.Fatalf("expected 42 got %d", got)
	}
}

func TestLogCloseSuccessAfterError(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 32)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Force creation of another segment to ensure Close handles multiple entries.
	for i := 0; i < 10; i++ {
		if _, err := lg.Append([]byte(fmt.Sprintf("%d", i))); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := lg.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestOpenMkdirFails(t *testing.T) {
	orig := mkdirAllOp
	mkdirAllOp = func(string, os.FileMode) error { return errors.New("mkdir fail") }
	t.Cleanup(func() { mkdirAllOp = orig })
	if _, err := Open(filepath.Join(t.TempDir(), "log"), 16); err == nil {
		t.Fatalf("expected error when mkdir fails")
	}
}

func TestOpenLoadSegmentsError(t *testing.T) {
	origDir := readDirOp
	readDirOp = func(string) ([]os.DirEntry, error) { return nil, errors.New("readdir fail") }
	t.Cleanup(func() { readDirOp = origDir })
	if _, err := Open(filepath.Join(t.TempDir(), "log"), 16); err == nil {
		t.Fatalf("expected error when readdir fails")
	}
}

func TestLoadSegmentsNewSegmentError(t *testing.T) {
	orig := newSegmentOp
	newSegmentOp = func(string, uint64, int64) (*Segment, error) { return nil, errors.New("create segment fail") }
	t.Cleanup(func() { newSegmentOp = orig })

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "00000000000000000000.log"), nil, 0o644); err != nil {
		t.Fatalf("create file: %v", err)
	}
	lg := &Log{dir: dir, segmentBytes: 16}
	if err := lg.loadSegments(); err == nil {
		t.Fatalf("expected error when new segment fails")
	}
}

func TestLogAppendNewSegmentError(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, recordHeaderSize+1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = lg.Close() })

	orig := newSegmentOp
	called := false
	newSegmentOp = func(string, uint64, int64) (*Segment, error) {
		called = true
		return nil, errors.New("boom")
	}
	t.Cleanup(func() { newSegmentOp = orig })

	if _, err := lg.Append([]byte("a")); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := lg.Append([]byte("b")); err == nil {
		t.Fatalf("expected error when creating new segment fails")
	}
	if !called {
		t.Fatalf("expected newSegment to be invoked")
	}
}

func TestLogReadFromSegmentError(t *testing.T) {
	dir := t.TempDir()
	lg, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = lg.Close() }()
	if _, err := lg.Append([]byte("value")); err != nil {
		t.Fatalf("append: %v", err)
	}

	orig := readAtOp
	readAtOp = func(*os.File, []byte, int64) (int, error) { return 0, errors.New("read fail") }
	t.Cleanup(func() { readAtOp = orig })

	if _, _, err := lg.ReadFrom(0, 0); err == nil {
		t.Fatalf("expected read error propagated")
	}
}

func TestNewSegmentOpenFileError(t *testing.T) {
	orig := openFileOp
	openFileOp = func(string, int, os.FileMode) (*os.File, error) { return nil, errors.New("open fail") }
	t.Cleanup(func() { openFileOp = orig })
	if _, err := newSegment(t.TempDir(), 0, 64); err == nil {
		t.Fatalf("expected open error")
	}
}

func TestNewSegmentIndexError(t *testing.T) {
	orig := openIndexOp
	openIndexOp = func(string) (*indexFile, error) { return nil, errors.New("index fail") }
	t.Cleanup(func() { openIndexOp = orig })
	if _, err := newSegment(t.TempDir(), 0, 64); err == nil {
		t.Fatalf("expected index error")
	}
}

func TestNewSegmentStatError(t *testing.T) {
	dir := t.TempDir()
	origStat := statOp
	statOp = func(f *os.File) (os.FileInfo, error) {
		if strings.HasSuffix(f.Name(), ".log") {
			return nil, errors.New("stat fail")
		}
		return origStat(f)
	}
	t.Cleanup(func() { statOp = origStat })
	if _, err := newSegment(dir, 0, 64); err == nil {
		t.Fatalf("expected stat error")
	}
}

func TestNewSegmentSeekError(t *testing.T) {
	dir := t.TempDir()
	origSeek := seekOp
	seekOp = func(f *os.File, off int64, whence int) (int64, error) {
		if strings.HasSuffix(f.Name(), ".log") && whence == io.SeekEnd {
			return 0, errors.New("seek fail")
		}
		return origSeek(f, off, whence)
	}
	t.Cleanup(func() { seekOp = origSeek })
	if _, err := newSegment(dir, 0, 64); err == nil {
		t.Fatalf("expected seek error")
	}
}

func TestSegmentAppendSeekError(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	origSeek := seekOp
	seekOp = func(*os.File, int64, int) (int64, error) { return 0, errors.New("seek fail") }
	t.Cleanup(func() { seekOp = origSeek })

	if _, err := seg.Append([]byte("x")); err == nil {
		t.Fatalf("expected seek error")
	}
}

func TestSegmentAppendWriteHeaderError(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	origSeek := seekOp
	seekOp = func(f *os.File, offset int64, whence int) (int64, error) { return f.Seek(offset, whence) }
	t.Cleanup(func() { seekOp = origSeek })

	origWrite := writeOp
	writeOp = func(*os.File, []byte) (int, error) { return 0, errors.New("write fail") }
	t.Cleanup(func() { writeOp = origWrite })

	if _, err := seg.Append([]byte("x")); err == nil {
		t.Fatalf("expected write error")
	}
}

func TestSegmentAppendWriteValueError(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	origWrite := writeOp
	call := 0
	writeOp = func(f *os.File, b []byte) (int, error) {
		if call == 0 {
			call++
			return f.Write(b)
		}
		return 0, errors.New("write value fail")
	}
	t.Cleanup(func() { writeOp = origWrite })

	if _, err := seg.Append([]byte("x")); err == nil {
		t.Fatalf("expected value write error")
	}
}

func TestSegmentAppendIndexError(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if err := seg.index.close(); err != nil {
		t.Fatalf("close index: %v", err)
	}

	if _, err := seg.Append([]byte("x")); err == nil {
		t.Fatalf("expected index error")
	}
}

func TestSegmentReadFromHeaderError(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if _, err := seg.Append([]byte("x")); err != nil {
		t.Fatalf("append: %v", err)
	}

	origReadAt := readAtOp
	readAtOp = func(*os.File, []byte, int64) (int, error) { return 0, errors.New("header read fail") }
	t.Cleanup(func() { readAtOp = origReadAt })

	if _, _, _, err := seg.ReadFrom(0, 0); err == nil {
		t.Fatalf("expected header read error")
	}
}

func TestSegmentReadFromValueError(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if _, err := seg.Append([]byte("x")); err != nil {
		t.Fatalf("append: %v", err)
	}

	origReadAt := readAtOp
	call := 0
	readAtOp = func(f *os.File, b []byte, off int64) (int, error) {
		if call == 0 {
			call++
			return f.ReadAt(b, off)
		}
		return 0, errors.New("value read fail")
	}
	t.Cleanup(func() { readAtOp = origReadAt })

	if _, _, _, err := seg.ReadFrom(0, 0); err == nil {
		t.Fatalf("expected value read error")
	}
}

func TestIndexOpenFileError(t *testing.T) {
	dir := t.TempDir()
	badPath := filepath.Join(dir, "missing", "file.index")
	if _, err := openIndex(badPath); err == nil {
		t.Fatalf("expected open error for missing parent")
	}
}

func TestIndexLoadStatError(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "idx")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp: %v", err)
	}
	idx := &indexFile{file: f}
	if err := idx.load(); err == nil {
		t.Fatalf("expected stat error on closed file")
	}
}

func TestIndexLoadSeekError(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
		_ = w.Close()
	})
	idx := &indexFile{file: r}
	if err := idx.load(); err == nil {
		t.Fatalf("expected seek error on pipe")
	}
}

func TestIndexLoadReadError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx")
	if err := os.WriteFile(path, make([]byte, 8), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	origReadFull := readFullOp
	readFullOp = func(r io.Reader, buf []byte) (int, error) { return 0, errors.New("readfull fail") }
	t.Cleanup(func() { readFullOp = origReadFull })
	idx := &indexFile{file: f}
	if err := idx.load(); err == nil {
		t.Fatalf("expected read error")
	}
	_ = f.Close()
}

func TestIndexAddError(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "index")
	idx, err := openIndex(idxPath)
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	defer func() { _ = idx.close() }()

	if err := idx.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := idx.add(1, 2); err == nil {
		t.Fatalf("expected add error with closed file")
	}
}

func TestOpenNewSegmentError(t *testing.T) {
	orig := newSegmentOp
	newSegmentOp = func(string, uint64, int64) (*Segment, error) { return nil, errors.New("segment create fail") }
	t.Cleanup(func() { newSegmentOp = orig })
	if _, err := Open(filepath.Join(t.TempDir(), "log"), 16); err == nil {
		t.Fatalf("expected new segment error in open")
	}
}

func TestLoadSegmentsSkipsInvalid(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ignore.tmp"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write ignore: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.log"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	lg := &Log{dir: dir, segmentBytes: 16}
	if err := lg.loadSegments(); err != nil {
		t.Fatalf("load segments: %v", err)
	}
	if len(lg.segments) != 0 {
		t.Fatalf("expected no segments loaded")
	}
}

func TestIndexLoadUnexpectedEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx")
	if err := os.WriteFile(path, make([]byte, 8), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()
	origReadFull := readFullOp
	readFullOp = func(r io.Reader, buf []byte) (int, error) { return 0, io.ErrUnexpectedEOF }
	t.Cleanup(func() { readFullOp = origReadFull })
	idx := &indexFile{file: f}
	if err := idx.load(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIndexLoadSeekEndError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx")
	if err := os.WriteFile(path, make([]byte, 0), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = f.Close() }()
	origSeek := seekOp
	call := 0
	seekOp = func(file *os.File, off int64, whence int) (int64, error) {
		if call == 0 {
			call++
			return file.Seek(off, whence)
		}
		return 0, errors.New("seek end fail")
	}
	t.Cleanup(func() { seekOp = origSeek })
	idx := &indexFile{file: f}
	if err := idx.load(); err == nil {
		t.Fatalf("expected seek end error")
	}
}

func TestSegmentReadFromEOF(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	seg.index.entries = append(seg.index.entries, indexEntry{RelativeOffset: 0, Position: 0})
	seg.nextOffset = seg.baseOffset + 1

	origReadAt := readAtOp
	called := false
	readAtOp = func(*os.File, []byte, int64) (int, error) {
		called = true
		return 0, io.EOF
	}
	t.Cleanup(func() { readAtOp = origReadAt })

	recs, last, used, err := seg.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !called {
		t.Fatalf("expected readAt to be called")
	}
	if len(recs) != 0 || last != 0 || used != 0 {
		t.Fatalf("expected empty result on EOF")
	}
}

func TestSegmentReadFromValueEOF(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if _, err := seg.Append([]byte("xy")); err != nil {
		t.Fatalf("append: %v", err)
	}

	origReadAt := readAtOp
	call := 0
	readAtOp = func(f *os.File, b []byte, off int64) (int, error) {
		if call == 0 {
			call++
			return f.ReadAt(b, off)
		}
		return 0, io.EOF
	}
	t.Cleanup(func() { readAtOp = origReadAt })

	recs, last, used, err := seg.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 0 || last != 0 || used != 0 {
		t.Fatalf("expected empty result after value EOF")
	}
}

func TestSegmentReadFromMaxBytesBreak(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if _, err := seg.Append([]byte("a")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := seg.Append([]byte("b")); err != nil {
		t.Fatalf("append second: %v", err)
	}

	recs, last, used, err := seg.ReadFrom(0, recordHeaderSize+1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 1 || last != 0 || used != recordHeaderSize+1 {
		t.Fatalf("expected single record due to max bytes limit")
	}

	recs, last, used, err = seg.ReadFrom(0, recordHeaderSize+len("ab")+recordHeaderSize)
	if err != nil {
		t.Fatalf("read second: %v", err)
	}
	if len(recs) != 2 || last != 1 {
		t.Fatalf("expected two records, got %d last=%d", len(recs), last)
	}
	_ = used
}

func TestSegmentReadFromProjectedBreak(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if _, err := seg.Append([]byte("a")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := seg.Append([]byte("b")); err != nil {
		t.Fatalf("append second: %v", err)
	}

	recs, last, used, err := seg.ReadFrom(0, recordHeaderSize+3)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 1 || last != 0 || used != recordHeaderSize+1 {
		t.Fatalf("expected break before exceeding max bytes")
	}
}

func TestSegmentReadFromSkipOffsets(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()

	if _, err := seg.Append([]byte("a")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := seg.Append([]byte("b")); err != nil {
		t.Fatalf("append second: %v", err)
	}

	recs, last, _, err := seg.ReadFrom(1, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 1 || string(recs[0]) != "b" || last != 1 {
		t.Fatalf("expected to skip first record, got %v last=%d", recs, last)
	}
}

func TestSegmentReadFromLargeOffset(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, 64)
	if err != nil {
		t.Fatalf("new segment: %v", err)
	}
	defer func() { _ = seg.Close() }()
	seg.nextOffset = math.MaxUint64

	recs, last, used, err := seg.ReadFrom(math.MaxUint64-1, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 0 || last != math.MaxUint64-1 || used != 0 {
		t.Fatalf("expected empty read for large offset")
	}
}
