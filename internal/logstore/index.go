package logstore

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type indexEntry struct {
	RelativeOffset uint32
	Position       uint32
}

var errIndexEntryNotFound = errors.New("index entry not found")

type indexFile struct {
	file    *os.File
	entries []indexEntry
}

func openIndex(path string) (*indexFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	idx := &indexFile{file: f}
	if err := idx.load(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return idx, nil
}

func (i *indexFile) load() error {
	stat, err := i.file.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size%8 != 0 {
		return errors.New("corrupted index file")
	}
	entries := make([]indexEntry, 0, size/8)
	buf := make([]byte, 8)
	if _, err := i.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for {
		if _, err := io.ReadFull(i.file, buf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		entry := indexEntry{
			RelativeOffset: binary.BigEndian.Uint32(buf[0:4]),
			Position:       binary.BigEndian.Uint32(buf[4:8]),
		}
		entries = append(entries, entry)
	}
	i.entries = entries
	if _, err := i.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func (i *indexFile) close() error {
	return i.file.Close()
}

func (i *indexFile) add(relative uint32, position uint32) error {
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[0:4], relative)
	binary.BigEndian.PutUint32(buf[4:8], position)
	if _, err := i.file.Write(buf[:]); err != nil {
		return err
	}
	i.entries = append(i.entries, indexEntry{RelativeOffset: relative, Position: position})
	return nil
}

func (i *indexFile) find(relative uint64) (indexEntry, error) {
	rel := uint32(relative)
	for _, entry := range i.entries {
		if entry.RelativeOffset == rel {
			return entry, nil
		}
	}
	return indexEntry{}, errIndexEntryNotFound
}

func (i *indexFile) entryAt(idx int) (indexEntry, bool) {
	if idx < 0 || idx >= len(i.entries) {
		return indexEntry{}, false
	}
	return i.entries[idx], true
}

func (i *indexFile) len() int {
	return len(i.entries)
}
