package broker

import "time"

const (
	defaultReplicaFetchInterval = 200 * time.Millisecond
	defaultReplicaRetryInterval = time.Second
	defaultFailoverTimeout      = 3 * time.Second

	defaultLeaderlessWriteQuorum = 2
	defaultLeaderlessReadQuorum  = 2
	defaultHintDeliveryInterval  = 2 * time.Second
)

type ReplicationMode string

const (
	ReplicationModeSingle     ReplicationMode = "single"
	ReplicationModeMulti      ReplicationMode = "multi"
	ReplicationModeLeaderless ReplicationMode = "leaderless"
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
	BrokerID              int
	Peers                 map[int]string
	Partitions            map[string]map[int]PartitionAssignment
	ReplicaFetchInterval  time.Duration
	ReplicaRetryInterval  time.Duration
	FailoverTimeout       time.Duration
	ReplicationMode       ReplicationMode
	LeaderlessWriteQuorum int
	LeaderlessReadQuorum  int
	HintDeliveryInterval  time.Duration
}

// PartitionAssignment points to the leader and the replica set for a
// topic/partition pair.
type PartitionAssignment struct {
	Leader   int
	Replicas []int
	Epoch    uint64
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

// PreferredSuccessor returns the replica that should become leader after the
// supplied leader fails based on the replica ordering.
func (pa PartitionAssignment) PreferredSuccessor(currentLeader int) int {
	if len(pa.Replicas) == 0 {
		return 0
	}
	idx := -1
	for i, rep := range pa.Replicas {
		if rep == currentLeader {
			idx = i
			break
		}
	}
	start := idx + 1
	if idx == -1 {
		start = 0
	}
	for i := start; i < len(pa.Replicas); i++ {
		if pa.Replicas[i] != currentLeader {
			return pa.Replicas[i]
		}
	}
	for _, rep := range pa.Replicas {
		if rep != currentLeader {
			return rep
		}
	}
	return 0
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
	if cp.FailoverTimeout == 0 {
		cp.FailoverTimeout = defaultFailoverTimeout
	}
	if cp.ReplicationMode == "" {
		cp.ReplicationMode = ReplicationModeSingle
	}
	if cp.isLeaderless() {
		if cp.LeaderlessWriteQuorum <= 0 {
			cp.LeaderlessWriteQuorum = defaultLeaderlessWriteQuorum
		}
		if cp.LeaderlessReadQuorum <= 0 {
			cp.LeaderlessReadQuorum = defaultLeaderlessReadQuorum
		}
		if cp.HintDeliveryInterval == 0 {
			cp.HintDeliveryInterval = defaultHintDeliveryInterval
		}
	}
	return &cp
}

func (cc *ClusterConfig) isMultiLeader() bool {
	return cc != nil && cc.ReplicationMode == ReplicationModeMulti
}

func (cc *ClusterConfig) isLeaderless() bool {
	return cc != nil && cc.ReplicationMode == ReplicationModeLeaderless
}
