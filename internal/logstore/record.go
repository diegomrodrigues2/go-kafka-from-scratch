package logstore

import "encoding/binary"

const recordHeaderSize = 4

func putRecordSize(buf []byte, size int) {
	binary.BigEndian.PutUint32(buf, uint32(size))
}

func readRecordSize(buf []byte) int {
	return int(binary.BigEndian.Uint32(buf))
}
