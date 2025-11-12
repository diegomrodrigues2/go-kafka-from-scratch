package logstore

import (
	"io"
	"os"
)

var (
	mkdirAllOp   = os.MkdirAll
	readDirOp    = os.ReadDir
	openFileOp   = os.OpenFile
	statOp       = func(f *os.File) (os.FileInfo, error) { return f.Stat() }
	seekOp       = func(f *os.File, offset int64, whence int) (int64, error) { return f.Seek(offset, whence) }
	readAtOp     = func(f *os.File, b []byte, off int64) (int, error) { return f.ReadAt(b, off) }
	writeOp      = func(f *os.File, b []byte) (int, error) { return f.Write(b) }
	truncateOp   = func(f *os.File, size int64) error { return f.Truncate(size) }
	readFullOp   = io.ReadFull
	openIndexOp  = func(path string) (*indexFile, error) { return openIndex(path) }
	newSegmentOp = func(dir string, base uint64, maxBytes int64) (*Segment, error) {
		return newSegment(dir, base, maxBytes)
	}
)
