package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

var (
	ErrPartitionNotFound   = errors.New("partition not found")
	ErrNotLeader           = errors.New("broker is not leader for partition")
	ErrLeaderNotAvailable  = errors.New("leader not available")
	ErrStaleLeaderEpoch    = errors.New("stale leader epoch")
	ErrReplicaNotInSet     = errors.New("broker is not in replica set")
	ErrPromotionNotAllowed = errors.New("promotion not allowed for this replica")
	ErrWriteQuorumNotMet   = errors.New("write quorum not satisfied")
	ErrReadQuorumNotMet    = errors.New("read quorum not satisfied")
)

type topicPartition struct {
	topic     string
	partition int
}

// Broker owns the local partition logs and optionally coordinates follower
// replication when running as part of a cluster.
type Broker struct {
	mu          sync.RWMutex
	assignMu    sync.RWMutex
	logs        map[string]map[int]*partitionLog
	conf        Config
	assignments map[string]map[int]PartitionAssignment
	followers   map[topicPartition]*followerReplicator
	peers       map[topicPartition]map[int]*peerReplicator
	httpClient  *http.Client
	hintStore   *hintStore
	hintStopCh  chan struct{}
	hintWakeCh  chan struct{}
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
	b := &Broker{
		logs:        make(map[string]map[int]*partitionLog),
		conf:        cfg,
		assignments: copyAssignments(cfg.Cluster),
		followers:   make(map[topicPartition]*followerReplicator),
		peers:       make(map[topicPartition]map[int]*peerReplicator),
		httpClient:  newDefaultHTTPClient(),
	}
	if b.leaderlessEnabled() {
		b.hintStore = newHintStore()
		b.hintStopCh = make(chan struct{})
		b.hintWakeCh = make(chan struct{}, 1)
		b.startHintLoop()
	}
	return b
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
		if pl, ok := parts[partition]; ok {
			b.mu.RUnlock()
			b.refreshPartitionMetadata(topic, partition)
			b.startFollowerReplication(topic, partition, pl)
			return nil
		}
	}
	b.mu.RUnlock()

	lg, err := logstore.Open(logPath, segmentBytes)
	if err != nil {
		return err
	}
	pl, err := newPartitionLog(lg)
	if err != nil {
		_ = lg.Close()
		return err
	}

	b.mu.Lock()
	if b.logs[topic] == nil {
		b.logs[topic] = make(map[int]*partitionLog)
	}
	if existing, ok := b.logs[topic][partition]; ok {
		b.mu.Unlock()
		_ = pl.close()
		b.startFollowerReplication(topic, partition, existing)
		return nil
	}
	b.logs[topic][partition] = pl
	b.mu.Unlock()

	b.refreshPartitionMetadata(topic, partition)
	b.startFollowerReplication(topic, partition, pl)
	return nil
}

// startFollowerReplication wires the background goroutine that continually
// pulls data from the leader into the local log when we act as a follower.
func (b *Broker) startFollowerReplication(topic string, partition int, pl *partitionLog) {
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
	if cluster.isLeaderless() {
		b.stopFollowerReplication(topic, partition)
		b.stopPeerReplication(topic, partition)
		return
	}
	if cluster.isMultiLeader() {
		if !assign.HasReplica(brokerID) {
			b.stopPeerReplication(topic, partition)
			return
		}
		b.ensurePeerReplication(topic, partition, pl, assign)
		return
	}
	// ensure any leftover peer replicators are stopped when switching modes.
	b.stopPeerReplication(topic, partition)
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
	failoverTimeout := cluster.FailoverTimeout
	rep := newFollowerReplicator(b, topic, partition, pl, leaderAddr, client, fetchInterval, retryInterval, failoverTimeout)
	b.followers[tp] = rep
	b.mu.Unlock()
	rep.onStopped = func() {
		b.unregisterFollower(tp, rep)
	}

	rep.start(&b.wg)
}

// unregisterFollower removes the replicator from the broker map when it stops.
func (b *Broker) unregisterFollower(tp topicPartition, rep *followerReplicator) {
	b.mu.Lock()
	defer b.mu.Unlock()
	current, ok := b.followers[tp]
	if ok && current == rep {
		delete(b.followers, tp)
	}
}

// stopFollowerReplication stops the follower replicator for the given partition
// if it is currently running.
func (b *Broker) stopFollowerReplication(topic string, partition int) {
	tp := topicPartition{topic: topic, partition: partition}
	b.mu.Lock()
	rep, ok := b.followers[tp]
	if ok {
		delete(b.followers, tp)
	}
	b.mu.Unlock()
	if ok {
		rep.stop()
	}
}

// ensureFollowerReplication ensures the follower loop is running when this
// broker is configured as a replica for the partition but not the leader.
func (b *Broker) ensureFollowerReplication(topic string, partition int) {
	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return
	}
	b.startFollowerReplication(topic, partition, pl)
}

func (b *Broker) ensurePeerReplication(topic string, partition int, pl *partitionLog, assign PartitionAssignment) {
	cluster := b.conf.Cluster
	if cluster == nil {
		return
	}
	if pl == nil {
		var err error
		pl, err = b.getPartition(topic, partition)
		if err != nil {
			return
		}
	}
	tp := topicPartition{topic: topic, partition: partition}
	b.mu.Lock()
	peerMap := b.peers[tp]
	if peerMap == nil {
		peerMap = make(map[int]*peerReplicator)
		b.peers[tp] = peerMap
	}
	var toStart []*peerReplicator
	wanted := make(map[int]struct{}, len(assign.Replicas))
	for _, peerID := range assign.Replicas {
		if peerID == cluster.BrokerID {
			continue
		}
		addr := cluster.Peers[peerID]
		if addr == "" {
			continue
		}
		wanted[peerID] = struct{}{}
		if _, exists := peerMap[peerID]; exists {
			continue
		}
		rep := newPeerReplicator(b, topic, partition, peerID, addr, pl, b.httpClient, cluster.ReplicaFetchInterval, cluster.ReplicaRetryInterval)
		peerMap[peerID] = rep
		toStart = append(toStart, rep)
	}
	var toStop []*peerReplicator
	for peerID, rep := range peerMap {
		if _, ok := wanted[peerID]; ok {
			continue
		}
		toStop = append(toStop, rep)
		delete(peerMap, peerID)
	}
	if len(peerMap) == 0 {
		delete(b.peers, tp)
	}
	b.mu.Unlock()
	for _, rep := range toStart {
		rep.start(&b.wg)
	}
	for _, rep := range toStop {
		rep.stop()
	}
}

func (b *Broker) stopPeerReplication(topic string, partition int) {
	tp := topicPartition{topic: topic, partition: partition}
	b.mu.Lock()
	peers := b.peers[tp]
	if peers != nil {
		delete(b.peers, tp)
	}
	b.mu.Unlock()
	for _, rep := range peers {
		rep.stop()
	}
}

// refreshPartitionMetadata pulls the latest leader/epoch information for the
// partition from peers so that restarted brokers do not assume stale roles.
func (b *Broker) refreshPartitionMetadata(topic string, partition int) {
	cluster := b.conf.Cluster
	if cluster == nil || len(cluster.Peers) == 0 {
		return
	}

	var best partitionInfoPayload
	for peerID, addr := range cluster.Peers {
		if peerID == cluster.BrokerID || addr == "" {
			continue
		}
		info, err := b.fetchPartitionMetadata(addr, topic, partition)
		if err != nil {
			continue
		}
		if info.Epoch > best.Epoch {
			best = info
		}
	}
	if best.Epoch == 0 {
		return
	}

	b.assignMu.Lock()
	defer b.assignMu.Unlock()
	if b.assignments == nil {
		b.assignments = make(map[string]map[int]PartitionAssignment)
	}
	parts := b.assignments[topic]
	if parts == nil {
		parts = make(map[int]PartitionAssignment)
		b.assignments[topic] = parts
	}
	assign := parts[partition]
	if best.Epoch > assign.Epoch {
		assign.Leader = best.Leader
		assign.Epoch = best.Epoch
		if len(best.Replicas) > 0 {
			assign.Replicas = append([]int(nil), best.Replicas...)
		}
		parts[partition] = assign
	}
}

// partitionInfoPayload mirrors the JSON structure exposed by the cluster
// metadata endpoint.
type partitionInfoPayload struct {
	Leader   int    `json:"leader"`
	Epoch    uint64 `json:"epoch"`
	Replicas []int  `json:"replicas"`
}

// fetchPartitionMetadata retrieves partition metadata from the given peer.
func (b *Broker) fetchPartitionMetadata(addr, topic string, partition int) (partitionInfoPayload, error) {
	var info partitionInfoPayload
	client := b.httpClient
	if client == nil || addr == "" {
		return info, fmt.Errorf("missing client or address")
	}
	urlStr, err := buildClusterPartitionURL(addr, topic, partition)
	if err != nil {
		return info, err
	}
	resp, err := client.Get(urlStr)
	if err != nil {
		return info, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return info, fmt.Errorf("metadata request failed with status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return info, err
	}
	return info, nil
}

// buildClusterPartitionURL constructs the cluster metadata endpoint URL for a
// topic-partition, optionally appending extra path segments.
func buildClusterPartitionURL(baseAddr, topic string, partition int, extra ...string) (string, error) {
	u, err := url.Parse(baseAddr)
	if err != nil {
		return "", err
	}
	components := []string{u.Path, "cluster", "topics", url.PathEscape(topic), "partitions", strconv.Itoa(partition)}
	if len(extra) > 0 {
		components = append(components, extra...)
	}
	u.Path = path.Join(components...)
	return u.String(), nil
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
func (b *Broker) getPartition(topic string, partition int) (*partitionLog, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	parts := b.logs[topic]
	if parts == nil {
		return nil, ErrPartitionNotFound
	}
	pl, ok := parts[partition]
	if !ok {
		return nil, ErrPartitionNotFound
	}
	return pl, nil
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
	if cluster.isMultiLeader() {
		if assign.HasReplica(cluster.BrokerID) {
			return nil
		}
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
	if b.leaderlessEnabled() {
		return b.publishLeaderless(topic, partition, value)
	}
	if err := b.ensureLeader(topic, partition); err != nil {
		return 0, err
	}
	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	originID := 0
	if b.conf.Cluster != nil {
		originID = b.conf.Cluster.BrokerID
	}
	offset, _, err := pl.appendLocal(originID, value)
	return offset, err
}

// Fetch reads records from the partition log starting at the supplied offset.
// It is used by both clients and follower brokers.
func (b *Broker) Fetch(topic string, partition int, offset uint64, maxBytes int) ([]PartitionRecord, uint64, error) {
	if b.leaderlessEnabled() {
		return b.fetchLeaderless(topic, partition, offset, maxBytes)
	}
	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return nil, offset, err
	}
	return pl.readRecords(offset, maxBytes)
}

// EndOffset returns the next offset that will be assigned for the partition.
func (b *Broker) EndOffset(topic string, partition int) (uint64, error) {
	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	return pl.nextOffset(), nil
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
	b.stopPeerReplications()
}

func (b *Broker) stopPeerReplications() {
	b.mu.Lock()
	var reps []*peerReplicator
	for tp, peerMap := range b.peers {
		for _, rep := range peerMap {
			reps = append(reps, rep)
		}
		delete(b.peers, tp)
	}
	b.mu.Unlock()
	for _, rep := range reps {
		rep.stop()
	}
}

// Close shuts the broker down gracefully ensuring replicators are stopped and
// partition logs are closed.
func (b *Broker) Close() error {
	b.stopFollowers()
	b.stopHintLoop()
	b.wg.Wait()

	b.mu.Lock()
	defer b.mu.Unlock()
	var errs []error
	for _, parts := range b.logs {
		for _, pl := range parts {
			if err := pl.close(); err != nil {
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

// ReplicationMode reports the current cluster replication mode (single, multi or leaderless).
func (b *Broker) ReplicationMode() ReplicationMode {
	cluster := b.conf.Cluster
	if cluster == nil {
		return ReplicationModeSingle
	}
	return cluster.ReplicationMode
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

// IsLeader reports whether this broker is the current leader for the partition.
func (b *Broker) IsLeader(topic string, partition int) (bool, error) {
	cluster := b.conf.Cluster
	if cluster == nil || cluster.BrokerID == 0 {
		return true, nil
	}
	assign, ok := b.partitionAssignment(topic, partition)
	if !ok {
		return false, ErrPartitionNotFound
	}
	return assign.Leader == cluster.BrokerID, nil
}

// PartitionAssignment exposes the current metadata for the topic/partition
// pair, including the leader id, epoch and replica list.
func (b *Broker) PartitionAssignment(topic string, partition int) (PartitionAssignment, error) {
	assign, ok := b.partitionAssignment(topic, partition)
	if !ok {
		return PartitionAssignment{}, ErrPartitionNotFound
	}
	return assign, nil
}

// SetPartitionLeader updates the leader/epoch for a partition when receiving
// announcements from other brokers.
func (b *Broker) SetPartitionLeader(topic string, partition int, leaderID int, epoch uint64) error {
	assign, err := b.updateAssignment(topic, partition, func(assign *PartitionAssignment) error {
		// Allow forcing the initial leader (epoch 0) when we only had static
		// metadata.
		if assign.Epoch == 0 && epoch == 0 {
			assign.Leader = leaderID
			return nil
		}
		if epoch == 0 {
			epoch = assign.Epoch
		}
		if len(assign.Replicas) > 0 && !assign.HasReplica(leaderID) {
			return ErrReplicaNotInSet
		}
		if epoch < assign.Epoch {
			if assign.Leader == leaderID {
				return nil
			}
			return ErrStaleLeaderEpoch
		}
		if epoch == assign.Epoch {
			if assign.Leader == leaderID {
				return nil
			}
			return ErrStaleLeaderEpoch
		}
		assign.Leader = leaderID
		assign.Epoch = epoch
		return nil
	})
	if err != nil {
		return err
	}
	b.reconcileReplica(topic, partition, assign)
	return nil
}

// partitionAssignment looks up the assignment entry for a topic-partition pair.
func (b *Broker) partitionAssignment(topic string, partition int) (PartitionAssignment, bool) {
	b.assignMu.RLock()
	defer b.assignMu.RUnlock()
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

// updateAssignment executes the provided function while holding the assignment
// mutex, persisting the resulting metadata back into the map.
func (b *Broker) updateAssignment(topic string, partition int, fn func(*PartitionAssignment) error) (PartitionAssignment, error) {
	b.assignMu.Lock()
	defer b.assignMu.Unlock()
	if b.assignments == nil {
		return PartitionAssignment{}, ErrPartitionNotFound
	}
	parts, ok := b.assignments[topic]
	if !ok {
		return PartitionAssignment{}, ErrPartitionNotFound
	}
	assign, ok := parts[partition]
	if !ok {
		return PartitionAssignment{}, ErrPartitionNotFound
	}
	if fn != nil {
		if err := fn(&assign); err != nil {
			return assign, err
		}
	}
	parts[partition] = assign
	return assign, nil
}

// reconcileReplica ensures the local broker is in the correct state (leader or
// follower) after a metadata update.
func (b *Broker) reconcileReplica(topic string, partition int, assign PartitionAssignment) {
	cluster := b.conf.Cluster
	if cluster == nil || cluster.BrokerID == 0 {
		return
	}
	if cluster.isLeaderless() {
		b.stopFollowerReplication(topic, partition)
		b.stopPeerReplication(topic, partition)
		return
	}
	if cluster.isMultiLeader() {
		// ensure legacy follower loops are torn down when switching modes
		b.stopFollowerReplication(topic, partition)
		if !assign.HasReplica(cluster.BrokerID) {
			b.stopPeerReplication(topic, partition)
			return
		}
		b.ensurePeerReplication(topic, partition, nil, assign)
		return
	}
	if assign.Leader == cluster.BrokerID {
		b.stopFollowerReplication(topic, partition)
		return
	}
	if !assign.HasReplica(cluster.BrokerID) {
		b.stopFollowerReplication(topic, partition)
		return
	}
	b.ensureFollowerReplication(topic, partition)
}

// handleReplicaPromotion is invoked by follower replicators once the leader is
// deemed unhealthy for longer than the configured failover timeout.
func (b *Broker) handleReplicaPromotion(topic string, partition int) {
	if err := b.promotePartitionLeader(topic, partition); err != nil {
		log.Printf("auto promotion topic=%s partition=%d failed: %v", topic, partition, err)
	}
}

// promotePartitionLeader transitions the broker into the leader role when it is
// the next eligible replica according to the configured priority order.
func (b *Broker) promotePartitionLeader(topic string, partition int) error {
	cluster := b.conf.Cluster
	if cluster == nil || cluster.BrokerID == 0 {
		return errors.New("cluster configuration required for promotion")
	}
	assign, err := b.updateAssignment(topic, partition, func(assign *PartitionAssignment) error {
		if !assign.HasReplica(cluster.BrokerID) {
			return ErrReplicaNotInSet
		}
		if assign.Leader == cluster.BrokerID {
			if assign.Epoch == 0 {
				assign.Epoch = 1
			}
			return nil
		}
		successor := assign.PreferredSuccessor(assign.Leader)
		if successor != 0 && successor != cluster.BrokerID {
			return ErrPromotionNotAllowed
		}
		assign.Leader = cluster.BrokerID
		assign.Epoch++
		if assign.Epoch == 0 {
			assign.Epoch = 1
		}
		return nil
	})
	if err != nil {
		return err
	}
	if assign.Leader != cluster.BrokerID {
		return nil
	}
	log.Printf("broker %d promoted to leader topic=%s partition=%d epoch=%d", cluster.BrokerID, topic, partition, assign.Epoch)
	b.reconcileReplica(topic, partition, assign)
	b.broadcastLeaderChange(topic, partition, assign)
	return nil
}

// broadcastLeaderChange notifies the rest of the cluster about the new leader
// so that they can update their routing tables.
func (b *Broker) broadcastLeaderChange(topic string, partition int, assign PartitionAssignment) {
	cluster := b.conf.Cluster
	if cluster == nil || len(cluster.Peers) == 0 {
		return
	}
	payload := struct {
		LeaderID int    `json:"leaderId"`
		Epoch    uint64 `json:"epoch"`
	}{
		LeaderID: assign.Leader,
		Epoch:    assign.Epoch,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("unable to marshal leader announcement: %v", err)
		return
	}
	for peerID, addr := range cluster.Peers {
		if peerID == cluster.BrokerID || addr == "" {
			continue
		}
		go func(target string) {
			if err := b.postLeaderAnnouncement(target, topic, partition, body); err != nil {
				log.Printf("leader announcement to %s failed: %v", target, err)
			}
		}(addr)
	}
}

// postLeaderAnnouncement sends the leader update payload to a peer.
func (b *Broker) postLeaderAnnouncement(addr, topic string, partition int, body []byte) error {
	client := b.httpClient
	if client == nil {
		return errors.New("http client not configured")
	}
	urlStr, err := buildClusterPartitionURL(addr, topic, partition, "leader")
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("leader announcement returned %d", resp.StatusCode)
	}
	return nil
}
