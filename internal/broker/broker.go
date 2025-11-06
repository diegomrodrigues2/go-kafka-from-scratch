package broker

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

var (
	ErrPartitionNotFound  = errors.New("partition not found")
	ErrNotLeader          = errors.New("broker is not leader for partition")
	ErrLeaderNotAvailable = errors.New("leader not available")
)

type topicPartition struct {
	topic     string
	partition int
}

// Broker owns the local partition logs and optionally coordinates follower
// replication when running as part of a cluster.
type Broker struct {
	mu          sync.RWMutex
	logs        map[string]map[int]*logstore.Log
	conf        Config
	assignments map[string]map[int]PartitionAssignment
	followers   map[topicPartition]*followerReplicator
	httpClient  *http.Client
	wg          sync.WaitGroup
}

// NewBroker builds a broker using the provided configuration (falling back to
// DefaultConfig when none is supplied) and prepares the in-memory state needed
// for partition management.
func NewBroker(opts ...Config) *Broker {
	cfg := DefaultConfig()
	if len(opts) > 0 {
		cfg = opts[0]
	}
	if cfg.Cluster != nil {
		cfg.Cluster = cfg.Cluster.withDefaults()
	}
	return &Broker{
		logs:        make(map[string]map[int]*logstore.Log),
		conf:        cfg,
		assignments: copyAssignments(cfg.Cluster),
		followers:   make(map[topicPartition]*followerReplicator),
		httpClient:  newDefaultHTTPClient(),
	}
}

func newDefaultHTTPClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second}
}

func copyAssignments(cluster *ClusterConfig) map[string]map[int]PartitionAssignment {
	if cluster == nil || len(cluster.Partitions) == 0 {
		return nil
	}
	out := make(map[string]map[int]PartitionAssignment, len(cluster.Partitions))
	for topic, parts := range cluster.Partitions {
		partCopy := make(map[int]PartitionAssignment, len(parts))
		for p, assign := range parts {
			assignCopy := assign
			if assign.Replicas != nil {
				assignCopy.Replicas = append([]int(nil), assign.Replicas...)
			}
			partCopy[p] = assignCopy
		}
		out[topic] = partCopy
	}
	return out
}

// EnsurePartition opens (or creates) the log for a topic-partition and starts
// the follower loop when this broker is configured as a replica.
func (b *Broker) EnsurePartition(topic string, partition int, path string, segmentBytes int64) error {
	logPath := filepath.Clean(path)

	b.mu.RLock()
	if parts := b.logs[topic]; parts != nil {
		if lg, ok := parts[partition]; ok {
			b.mu.RUnlock()
			b.startFollowerReplication(topic, partition, lg)
			return nil
		}
	}
	b.mu.RUnlock()

	lg, err := logstore.Open(logPath, segmentBytes)
	if err != nil {
		return err
	}

	b.mu.Lock()
	if b.logs[topic] == nil {
		b.logs[topic] = make(map[int]*logstore.Log)
	}
	if existing, ok := b.logs[topic][partition]; ok {
		b.mu.Unlock()
		_ = lg.Close()
		b.startFollowerReplication(topic, partition, existing)
		return nil
	}
	b.logs[topic][partition] = lg
	b.mu.Unlock()

	b.startFollowerReplication(topic, partition, lg)
	return nil
}

// startFollowerReplication wires the background goroutine that continually
// pulls data from the leader into the local log when we act as a follower.
func (b *Broker) startFollowerReplication(topic string, partition int, lg *logstore.Log) {
	cluster := b.conf.Cluster
	if cluster == nil {
		return
	}
	assign, ok := b.partitionAssignment(topic, partition)
	if !ok {
		return
	}
	brokerID := cluster.BrokerID
	if brokerID == 0 {
		return
	}
	if assign.Leader == brokerID {
		return
	}
	if !assign.HasReplica(brokerID) {
		return
	}
	leaderAddr, ok := cluster.Peers[assign.Leader]
	if !ok || leaderAddr == "" {
		return
	}
	tp := topicPartition{topic: topic, partition: partition}

	b.mu.Lock()
	if _, exists := b.followers[tp]; exists {
		b.mu.Unlock()
		return
	}
	client := b.httpClient
	fetchInterval := cluster.ReplicaFetchInterval
	retryInterval := cluster.ReplicaRetryInterval
	rep := newFollowerReplicator(topic, partition, lg, leaderAddr, client, fetchInterval, retryInterval)
	b.followers[tp] = rep
	b.mu.Unlock()

	rep.start(&b.wg)
}

// SetHTTPClient overrides the client used for inter-broker communication,
// primarily useful in tests where a custom transport is injected.
func (b *Broker) SetHTTPClient(client *http.Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if client == nil {
		b.httpClient = newDefaultHTTPClient()
		return
	}
	b.httpClient = client
}

// getPartition returns the log handle for a topic-partition if it has been
// created on this broker.
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

// ensureLeader validates this broker is allowed to accept writes for the given
// partition according to the cluster assignment.
func (b *Broker) ensureLeader(topic string, partition int) error {
	cluster := b.conf.Cluster
	if cluster == nil || cluster.BrokerID == 0 {
		return nil
	}
	assign, ok := b.partitionAssignment(topic, partition)
	if !ok {
		return ErrPartitionNotFound
	}
	if assign.Leader == cluster.BrokerID {
		return nil
	}
	if assign.HasReplica(cluster.BrokerID) {
		return ErrNotLeader
	}
	return ErrPartitionNotFound
}

// Publish appends a record to the partition log returning the assigned offset.
// When the broker is not the leader it rejects the request.
func (b *Broker) Publish(topic string, partition int, key, value []byte) (uint64, error) {
	_ = key
	if err := b.ensureLeader(topic, partition); err != nil {
		return 0, err
	}
	lg, err := b.getPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	return lg.Append(value)
}

// Fetch reads records from the partition log starting at the supplied offset.
// It is used by both clients and follower brokers.
func (b *Broker) Fetch(topic string, partition int, offset uint64, maxBytes int) ([][]byte, uint64, error) {
	lg, err := b.getPartition(topic, partition)
	if err != nil {
		return nil, offset, err
	}
	return lg.ReadFrom(offset, maxBytes)
}

// Topics lists the names of the topics currently loaded on this broker.
func (b *Broker) Topics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.logs))
	for topic := range b.logs {
		out = append(out, topic)
	}
	return out
}

// stopFollowers asks every follower replicator goroutine to exit.
func (b *Broker) stopFollowers() {
	b.mu.Lock()
	followers := make([]*followerReplicator, 0, len(b.followers))
	for tp, rep := range b.followers {
		followers = append(followers, rep)
		delete(b.followers, tp)
	}
	b.mu.Unlock()
	for _, rep := range followers {
		rep.stop()
	}
}

// Close shuts the broker down gracefully ensuring replicators are stopped and
// partition logs are closed.
func (b *Broker) Close() error {
	b.stopFollowers()
	b.wg.Wait()

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

// Describe returns a lightweight textual description useful during debugging.
func (b *Broker) Describe() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return fmt.Sprintf("Broker{topics=%d}", len(b.logs))
}

// PartitionLeader reports the broker id and address that lead the given
// partition according to the cluster assignment.
func (b *Broker) PartitionLeader(topic string, partition int) (int, string, error) {
	assign, ok := b.partitionAssignment(topic, partition)
	if !ok {
		return 0, "", ErrPartitionNotFound
	}
	var addr string
	if b.conf.Cluster != nil && b.conf.Cluster.Peers != nil {
		addr = b.conf.Cluster.Peers[assign.Leader]
	}
	if assign.Leader == 0 || addr == "" {
		return assign.Leader, addr, ErrLeaderNotAvailable
	}
	return assign.Leader, addr, nil
}

// partitionAssignment looks up the assignment entry for a topic-partition pair.
func (b *Broker) partitionAssignment(topic string, partition int) (PartitionAssignment, bool) {
	if b.assignments == nil {
		return PartitionAssignment{}, false
	}
	parts, ok := b.assignments[topic]
	if !ok {
		return PartitionAssignment{}, false
	}
	assign, ok := parts[partition]
	return assign, ok
}
