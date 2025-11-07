package broker

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type hintEntry struct {
	topic     string
	partition int
	raw       []byte
}

type hintStore struct {
	mu      sync.Mutex
	pending map[int][]hintEntry
}

// newHintStore creates an empty hinted handoff buffer.
func newHintStore() *hintStore {
	return &hintStore{pending: make(map[int][]hintEntry)}
}

// enqueue appends a hint for the provided destination replica, ignoring invalid ids.
func (hs *hintStore) enqueue(dest int, entry hintEntry) {
	if dest == 0 {
		return
	}
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.pending[dest] = append(hs.pending[dest], entry)
}

// drain returns the pending hints and clears the store so callers can replay entries.
func (hs *hintStore) drain() map[int][]hintEntry {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if len(hs.pending) == 0 {
		return nil
	}
	out := hs.pending
	hs.pending = make(map[int][]hintEntry)
	return out
}

// leaderlessEnabled reports whether the broker is configured for leaderless quorums.
func (b *Broker) leaderlessEnabled() bool {
	return b != nil && b.conf.Cluster != nil && b.conf.Cluster.isLeaderless()
}

// publishLeaderless fan-outs a client write to every replica and enforces the configured write quorum.
func (b *Broker) publishLeaderless(topic string, partition int, value []byte) (uint64, error) {
	cluster := b.conf.Cluster
	if cluster == nil {
		return 0, ErrPartitionNotFound
	}
	assign, ok := b.partitionAssignment(topic, partition)
	if !ok || len(assign.Replicas) == 0 {
		return 0, ErrPartitionNotFound
	}
	brokerID := cluster.BrokerID
	if brokerID == 0 || !assign.HasReplica(brokerID) {
		return 0, ErrPartitionNotFound
	}

	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return 0, err
	}
	offset, seq, err := pl.appendLocal(brokerID, value)
	if err != nil {
		return 0, err
	}
	raw := encodeRecord(brokerID, seq, value)

	total := len(assign.Replicas)
	if total == 0 {
		total = 1
	}
	required := cluster.LeaderlessWriteQuorum
	if required <= 0 || required > total {
		required = total
	}
	success := 1 // local append
	var targets []struct {
		id   int
		addr string
	}
	var errs []error
	for _, peerID := range assign.Replicas {
		if peerID == brokerID {
			continue
		}
		addr := b.peerAddress(peerID)
		if addr == "" {
			errs = append(errs, fmt.Errorf("peer %d address unavailable", peerID))
			b.enqueueHint(peerID, topic, partition, raw)
			continue
		}
		targets = append(targets, struct {
			id   int
			addr string
		}{id: peerID, addr: addr})
	}

	if len(targets) > 0 {
		type writeResult struct {
			id  int
			err error
		}
		results := make(chan writeResult, len(targets))
		for _, tgt := range targets {
			go func(target struct {
				id   int
				addr string
			}) {
				err := b.sendReplicaRecords(target.addr, topic, partition, [][]byte{raw})
				results <- writeResult{id: target.id, err: err}
			}(tgt)
		}
		for range targets {
			res := <-results
			if res.err == nil {
				success++
				continue
			}
			errs = append(errs, fmt.Errorf("peer %d: %w", res.id, res.err))
			b.enqueueHint(res.id, topic, partition, raw)
		}
	}

	if success < required {
		if len(errs) > 0 {
			return offset, fmt.Errorf("%w: %w", ErrWriteQuorumNotMet, errors.Join(errs...))
		}
		return offset, ErrWriteQuorumNotMet
	}
	if len(errs) > 0 {
		log.Printf("leaderless publish topic=%s partition=%d partial replication: %v", topic, partition, errors.Join(errs...))
	}
	return offset, nil
}

// enqueueHint stores a hinted handoff entry and wakes the delivery loop.
func (b *Broker) enqueueHint(dest int, topic string, partition int, raw []byte) {
	b.enqueueHintInternal(dest, topic, partition, raw, true)
}

// enqueueHintNoWake stores a hint without signalling the worker (used while already flushing).
func (b *Broker) enqueueHintNoWake(dest int, topic string, partition int, raw []byte) {
	b.enqueueHintInternal(dest, topic, partition, raw, false)
}

// enqueueHintInternal centralises hint creation so both public helpers share the same logic.
func (b *Broker) enqueueHintInternal(dest int, topic string, partition int, raw []byte, wake bool) {
	if dest == 0 || b.hintStore == nil {
		return
	}
	entry := hintEntry{
		topic:     topic,
		partition: partition,
	}
	if raw != nil {
		entry.raw = append([]byte(nil), raw...)
	}
	b.hintStore.enqueue(dest, entry)
	if wake {
		b.notifyHintLoop()
	}
}

// peerAddress resolves the HTTP base URL of a replica or returns an empty string when unknown.
func (b *Broker) peerAddress(peerID int) string {
	if b.conf.Cluster == nil || b.conf.Cluster.Peers == nil {
		return ""
	}
	return b.conf.Cluster.Peers[peerID]
}

// sendReplicaRecords pushes raw records into the peer replica ingest endpoint using the broker HTTP client.
func (b *Broker) sendReplicaRecords(addr, topic string, partition int, records [][]byte) error {
	if len(records) == 0 {
		return nil
	}
	client := b.httpClient
	if client == nil {
		return errors.New("http client not configured")
	}
	urlStr, err := buildClusterPartitionURL(addr, topic, partition, "replica")
	if err != nil {
		return err
	}
	payload := struct {
		Records []string `json:"records"`
	}{
		Records: make([]string, len(records)),
	}
	for i, raw := range records {
		payload.Records[i] = base64.StdEncoding.EncodeToString(raw)
	}
	body, err := json.Marshal(payload)
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
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("replica write returned %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

// startHintLoop spins up the hinted handoff goroutine when leaderless mode is enabled.
func (b *Broker) startHintLoop() {
	if b.hintStore == nil || b.hintStopCh == nil {
		return
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.runHintLoop()
	}()
}

// stopHintLoop terminates the hinted handoff goroutine if it was running.
func (b *Broker) stopHintLoop() {
	if b.hintStopCh == nil {
		return
	}
	select {
	case <-b.hintStopCh:
		return
	default:
		close(b.hintStopCh)
	}
}

// notifyHintLoop wakes the hinted handoff goroutine when new hints are enqueued.
func (b *Broker) notifyHintLoop() {
	if b.hintWakeCh == nil {
		return
	}
	select {
	case b.hintWakeCh <- struct{}{}:
	default:
	}
}

// runHintLoop drains hinted handoff entries whenever timers fire, wake-ups arrive or the broker shuts down.
func (b *Broker) runHintLoop() {
	cluster := b.conf.Cluster
	interval := defaultHintDeliveryInterval
	if cluster != nil && cluster.HintDeliveryInterval > 0 {
		interval = cluster.HintDeliveryInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.hintStopCh:
			b.flushHints()
			return
		case <-ticker.C:
			b.flushHints()
		case <-b.hintWakeCh:
			b.flushHints()
		}
	}
}

// flushHints drains and attempts to deliver pending hints, requeueing entries when delivery fails.
func (b *Broker) flushHints() {
	if b.hintStore == nil {
		return
	}
	batches := b.hintStore.drain()
	if len(batches) == 0 {
		return
	}
	for peerID, entries := range batches {
		addr := b.peerAddress(peerID)
		if addr == "" {
			for _, entry := range entries {
				b.enqueueHintNoWake(peerID, entry.topic, entry.partition, entry.raw)
			}
			continue
		}
		for _, entry := range entries {
			if err := b.sendReplicaRecords(addr, entry.topic, entry.partition, [][]byte{entry.raw}); err != nil {
				log.Printf("hint delivery peer=%d topic=%s partition=%d failed: %v", peerID, entry.topic, entry.partition, err)
				b.enqueueHint(peerID, entry.topic, entry.partition, entry.raw)
			}
		}
	}
}

// AppendReplica stores the provided raw record for the given partition,
// returning whether it was appended (true) or discarded as a duplicate.
func (b *Broker) AppendReplica(topic string, partition int, raw []byte) (uint64, bool, error) {
	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return 0, false, err
	}
	return pl.appendReplica(raw)
}

// fetchLeaderless issues a quorum read to peers, merges the superset of records and triggers repairs when needed.
func (b *Broker) fetchLeaderless(topic string, partition int, offset uint64, maxBytes int) ([]PartitionRecord, uint64, error) {
	cluster := b.conf.Cluster
	if cluster == nil {
		return nil, offset, ErrPartitionNotFound
	}
	assign, ok := b.partitionAssignment(topic, partition)
	if !ok || len(assign.Replicas) == 0 {
		return nil, offset, ErrPartitionNotFound
	}
	localID := cluster.BrokerID
	if localID == 0 || !assign.HasReplica(localID) {
		return nil, offset, ErrPartitionNotFound
	}
	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return nil, offset, err
	}

	type readResponse struct {
		id      int
		records []PartitionRecord
		last    uint64
		err     error
	}

	respCh := make(chan readResponse, len(assign.Replicas))
	var wg sync.WaitGroup
	for _, peerID := range assign.Replicas {
		addr := ""
		if peerID != localID {
			addr = b.peerAddress(peerID)
		}
		wg.Add(1)
		go func(id int, addr string) {
			defer wg.Done()
			var recs []PartitionRecord
			var last uint64
			var resErr error
			switch {
			case id == localID:
				recs, last, resErr = pl.readRecords(offset, maxBytes)
			case addr == "":
				resErr = fmt.Errorf("peer %d address unavailable", id)
			default:
				recs, last, resErr = b.fetchReplicaRecords(addr, topic, partition, offset, maxBytes)
			}
			respCh <- readResponse{id: id, records: recs, last: last, err: resErr}
		}(peerID, addr)
	}
	wg.Wait()
	close(respCh)

	required := cluster.LeaderlessReadQuorum
	total := len(assign.Replicas)
	if required <= 0 || required > total {
		required = total
	}

	successes := 0
	responses := make(map[int]readResponse, len(assign.Replicas))
	nodeKeys := make(map[int]map[string]struct{}, len(assign.Replicas))
	union := make(map[string]PartitionRecord)
	for resp := range respCh {
		if resp.err != nil {
			continue
		}
		successes++
		responses[resp.id] = resp
		keys := make(map[string]struct{}, len(resp.records))
		for _, rec := range resp.records {
			key := recordIdentity(rec)
			keys[key] = struct{}{}
			if _, exists := union[key]; !exists {
				union[key] = rec
			}
		}
		nodeKeys[resp.id] = keys
	}

	if successes < required {
		return nil, offset, fmt.Errorf("%w (%d/%d replicas)", ErrReadQuorumNotMet, successes, required)
	}

	localResp, ok := responses[localID]
	if !ok {
		return nil, offset, fmt.Errorf("local replica unavailable for leaderless read")
	}

	localKeys := nodeKeys[localID]
	if localKeys == nil {
		localKeys = make(map[string]struct{})
		nodeKeys[localID] = localKeys
	}

	needsRefresh := false
	for key, rec := range union {
		if _, present := localKeys[key]; present {
			continue
		}
		if _, appended, appendErr := pl.appendReplica(rec.Raw); appendErr != nil {
			log.Printf("leaderless read repair append topic=%s partition=%d failed: %v", topic, partition, appendErr)
			continue
		} else if appended {
			needsRefresh = true
			localKeys[key] = struct{}{}
		}
	}

	resultRecords := localResp.records
	resultLast := localResp.last
	if needsRefresh {
		if refreshed, last, refreshErr := pl.readRecords(offset, maxBytes); refreshErr == nil {
			resultRecords = refreshed
			resultLast = last
		} else {
			log.Printf("leaderless read refresh topic=%s partition=%d failed: %v", topic, partition, refreshErr)
		}
	}

	go b.dispatchReadRepairs(topic, partition, localID, nodeKeys, union)
	return resultRecords, resultLast, nil
}

// dispatchReadRepairs sends the missing records to lagging replicas or stores hints when peers are unreachable.
func (b *Broker) dispatchReadRepairs(topic string, partition int, localID int, nodeKeys map[int]map[string]struct{}, union map[string]PartitionRecord) {
	for peerID, keys := range nodeKeys {
		if peerID == localID {
			continue
		}
		var missing [][]byte
		for key, rec := range union {
			if keys != nil {
				if _, ok := keys[key]; ok {
					continue
				}
			}
			missing = append(missing, append([]byte(nil), rec.Raw...))
		}
		if len(missing) == 0 {
			continue
		}
		addr := b.peerAddress(peerID)
		if addr == "" {
			for _, raw := range missing {
				b.enqueueHintNoWake(peerID, topic, partition, raw)
			}
			continue
		}
		go func(id int, target string, payload [][]byte) {
			if err := b.sendReplicaRecords(target, topic, partition, payload); err != nil {
				log.Printf("leaderless read repair peer=%d topic=%s partition=%d failed: %v", id, topic, partition, err)
				for _, raw := range payload {
					b.enqueueHint(id, topic, partition, raw)
				}
			}
		}(peerID, addr, missing)
	}
}

// fetchReplicaRecords retrieves raw records from a peer and decodes them into PartitionRecord slices.
func (b *Broker) fetchReplicaRecords(addr, topic string, partition int, offset uint64, maxBytes int) ([]PartitionRecord, uint64, error) {
	client := b.httpClient
	if client == nil {
		return nil, offset, errors.New("http client not configured")
	}
	urlStr, err := buildLeaderlessFetchURL(addr, topic, partition, offset, maxBytes)
	if err != nil {
		return nil, offset, err
	}
	resp, err := client.Get(urlStr)
	if err != nil {
		return nil, offset, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, offset, fmt.Errorf("fetch from peer returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		ToOffset   uint64   `json:"toOffset"`
		Records    []string `json:"records"`
		RawRecords []string `json:"rawRecords"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, offset, err
	}
	rawList := make([][]byte, 0, len(payload.RawRecords))
	if len(payload.RawRecords) > 0 {
		for _, raw := range payload.RawRecords {
			val, decErr := base64.StdEncoding.DecodeString(raw)
			if decErr != nil {
				return nil, offset, decErr
			}
			rawList = append(rawList, val)
		}
	} else {
		for _, rec := range payload.Records {
			rawList = append(rawList, []byte(rec))
		}
	}
	out := make([]PartitionRecord, len(rawList))
	for i, raw := range rawList {
		out[i] = decodeRecord(raw)
	}
	return out, payload.ToOffset, nil
}

// buildLeaderlessFetchURL produces the peer fetch URL with offset/maxBytes/raw parameters.
func buildLeaderlessFetchURL(baseAddr, topic string, partition int, offset uint64, maxBytes int) (string, error) {
	base, err := url.Parse(baseAddr)
	if err != nil {
		return "", err
	}
	topicEscaped := url.PathEscape(topic)
	base.Path = path.Join(base.Path, "topics", topicEscaped, "partitions", strconv.Itoa(partition), "fetch")
	q := base.Query()
	q.Set("offset", strconv.FormatUint(offset, 10))
	if maxBytes > 0 {
		q.Set("maxBytes", strconv.Itoa(maxBytes))
	}
	q.Set("raw", "1")
	base.RawQuery = q.Encode()
	return base.String(), nil
}

// recordIdentity generates a stable deduplication key derived from metadata when present.
func recordIdentity(rec PartitionRecord) string {
	if rec.HasMeta {
		return fmt.Sprintf("%d:%d", rec.OriginID, rec.OriginSeq)
	}
	return "raw:" + base64.StdEncoding.EncodeToString(rec.Raw)
}
