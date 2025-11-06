package broker

import "time"

const (
	defaultReplicaFetchInterval = 200 * time.Millisecond
	defaultReplicaRetryInterval = time.Second
)

// Config groups the local broker settings together with optional cluster data.
// When Cluster is nil the broker behaves as a standalone node.
type Config struct {
	DataDir      string
	SegmentBytes int64
	Cluster      *ClusterConfig
}

// ClusterConfig tells the broker how it fits into the replicated deployment:
// which id it has, which peers it can talk to and which partitions it leads or
// follows.
type ClusterConfig struct {
	BrokerID             int
	Peers                map[int]string
	Partitions           map[string]map[int]PartitionAssignment
	ReplicaFetchInterval time.Duration
	ReplicaRetryInterval time.Duration
}

// PartitionAssignment points to the leader and the replica set for a
// topic/partition pair.
type PartitionAssignment struct {
	Leader   int
	Replicas []int
}

// HasReplica reports whether the provided broker id is part of the replica set.
func (pa PartitionAssignment) HasReplica(id int) bool {
	for _, rep := range pa.Replicas {
		if rep == id {
			return true
		}
	}
	return false
}

// DefaultConfig returns the basic configuration for a single broker running on
// the local filesystem.
func DefaultConfig() Config {
	return Config{
		DataDir:      "./data",
		SegmentBytes: 128 << 20,
		Cluster:      nil,
	}
}

// withDefaults clones the cluster configuration and fills in fetch/retry
// intervals when they were left unspecified.
func (cc *ClusterConfig) withDefaults() *ClusterConfig {
	if cc == nil {
		return nil
	}
	cp := *cc
	if cp.ReplicaFetchInterval == 0 {
		cp.ReplicaFetchInterval = defaultReplicaFetchInterval
	}
	if cp.ReplicaRetryInterval == 0 {
		cp.ReplicaRetryInterval = defaultReplicaRetryInterval
	}
	return &cp
}
