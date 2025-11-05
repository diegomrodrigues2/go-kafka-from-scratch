package types

import "time"

type Offset = uint64

type Message struct {
	Offset    Offset
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

type TopicPartition struct {
	Topic     string
	Partition int
}
