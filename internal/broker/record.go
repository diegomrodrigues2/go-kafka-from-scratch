package broker

import "encoding/binary"

const (
	recordMagic0    = 'M'
	recordMagic1    = 'L'
	recordVersion1  = 1
	recordHeaderLen = 3 + 4 + 8 // magic(2) + version + originID + originSeq
)

// PartitionRecord represents a log entry together with the metadata required to
// deduplicate replicated writes.
type PartitionRecord struct {
	Raw       []byte
	Payload   []byte
	OriginID  int
	OriginSeq uint64
	HasMeta   bool
}

func encodeRecord(originID int, seq uint64, payload []byte) []byte {
	if originID < 0 {
		originID = 0
	}
	buf := make([]byte, recordHeaderLen+len(payload))
	buf[0] = recordMagic0
	buf[1] = recordMagic1
	buf[2] = recordVersion1
	binary.BigEndian.PutUint32(buf[3:7], uint32(originID))
	binary.BigEndian.PutUint64(buf[7:15], seq)
	copy(buf[recordHeaderLen:], payload)
	return buf
}

func decodeRecord(raw []byte) PartitionRecord {
	rec := PartitionRecord{
		Raw:       raw,
		Payload:   raw,
		OriginID:  0,
		OriginSeq: 0,
		HasMeta:   false,
	}
	if len(raw) < recordHeaderLen {
		return rec
	}
	if raw[0] != recordMagic0 || raw[1] != recordMagic1 {
		return rec
	}
	switch raw[2] {
	case recordVersion1:
		if len(raw) < recordHeaderLen {
			return rec
		}
		origin := binary.BigEndian.Uint32(raw[3:7])
		seq := binary.BigEndian.Uint64(raw[7:15])
		rec.OriginID = int(origin)
		rec.OriginSeq = seq
		rec.Payload = raw[recordHeaderLen:]
		rec.HasMeta = true
	default:
		return rec
	}
	return rec
}
