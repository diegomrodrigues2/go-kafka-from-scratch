package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

var ErrPartitionNotFound = errors.New("partition not found")

type Broker struct {
	mu   sync.RWMutex
	logs map[string]map[int]*logstore.Log
	conf Config
}

func NewBroker(opts ...Config) *Broker {
	cfg := DefaultConfig()
	if len(opts) > 0 {
		cfg = opts[0]
	}
	return &Broker{
		logs: make(map[string]map[int]*logstore.Log),
		conf: cfg,
	}
}

func (b *Broker) EnsurePartition(topic string, partition int, path string, segmentBytes int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.logs[topic] == nil {
		b.logs[topic] = make(map[int]*logstore.Log)
	}
	if _, ok := b.logs[topic][partition]; ok {
		return nil
	}

	logPath := filepath.Clean(path)
	lg, err := logstore.Open(logPath, segmentBytes)
	if err != nil {
		return err
	}
	b.logs[topic][partition] = lg
	return nil
}

func (b *Broker) getPartition(topic string, partition int) (*logstore.Log, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	parts := b.logs[topic]
	if parts == nil {
		return nil, ErrPartitionNotFound
	}
	lg, ok := parts[partition]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	return lg, nil
}

func (b *Broker) Publish(topic string, partition int, key, value []byte) (uint64, error) {
	_ = key
	lg, err := b.getPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	return lg.Append(value)
}

func (b *Broker) Fetch(topic string, partition int, offset uint64, maxBytes int) ([][]byte, uint64, error) {
	lg, err := b.getPartition(topic, partition)
	if err != nil {
		return nil, offset, err
	}
	return lg.ReadFrom(offset, maxBytes)
}

func (b *Broker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.logs))
	for topic := range b.logs {
		out = append(out, topic)
	}
	return out
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var errs []error
	for _, parts := range b.logs {
		for _, lg := range parts {
			if err := lg.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (b *Broker) Describe() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return fmt.Sprintf("Broker{topics=%d}", len(b.logs))
}
