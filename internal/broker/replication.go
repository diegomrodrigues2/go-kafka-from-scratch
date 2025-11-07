package broker

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// followerReplicator continuously pulls data from the leader and appends it to
// the local log to keep this broker's replica up to date.
type followerReplicator struct {
	broker          *Broker
	topic           string
	partition       int
	part            *partitionLog
	leaderAddr      string
	client          *http.Client
	fetchInterval   time.Duration
	retryInterval   time.Duration
	failoverTimeout time.Duration
	stopCh          chan struct{}
	doneCh          chan struct{}
	onStopped       func()
	promoted        atomic.Bool
}

// newFollowerReplicator wires together the helper responsible for fetching
// from the leader. Tests can pass a custom HTTP client or timing configuration.
func newFollowerReplicator(b *Broker, topic string, partition int, pl *partitionLog, leaderAddr string, client *http.Client, fetchInterval, retryInterval, failoverTimeout time.Duration) *followerReplicator {
	if client == nil {
		client = newDefaultHTTPClient()
	}
	if fetchInterval <= 0 {
		fetchInterval = defaultReplicaFetchInterval
	}
	if retryInterval <= 0 {
		retryInterval = defaultReplicaRetryInterval
	}
	if failoverTimeout <= 0 {
		failoverTimeout = defaultFailoverTimeout
	}
	return &followerReplicator{
		broker:          b,
		topic:           topic,
		partition:       partition,
		part:            pl,
		leaderAddr:      leaderAddr,
		client:          client,
		fetchInterval:   fetchInterval,
		retryInterval:   retryInterval,
		failoverTimeout: failoverTimeout,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
}

// start launches the replication loop in a new goroutine and keeps track of it
// via the provided wait group.
func (fr *followerReplicator) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fr.run()
	}()
}

// stop signals the goroutine to exit and waits until it has fully shut down.
func (fr *followerReplicator) stop() {
	select {
	case <-fr.stopCh:
	default:
		close(fr.stopCh)
	}
	<-fr.doneCh
}

// run performs the fetch-append cycle until stopped, backing off on errors and
// waiting briefly when no new data is available.
func (fr *followerReplicator) run() {
	defer close(fr.doneCh)
	if fr.onStopped != nil {
		defer fr.onStopped()
	}
	if fr.part == nil {
		return
	}
	offset := fr.part.nextOffset()
	lastSuccess := time.Now()
	for {
		select {
		case <-fr.stopCh:
			return
		default:
		}

		records, lastOffset, err := fr.fetch(offset)
		if err != nil {
			log.Printf("follower replicate topic=%s partition=%d fetch error: %v", fr.topic, fr.partition, err)
			if fr.failoverTimeout > 0 && time.Since(lastSuccess) >= fr.failoverTimeout {
				if fr.triggerPromotion(err) {
					return
				}
			}
			if !fr.wait(fr.retryInterval) {
				return
			}
			continue
		}
		lastSuccess = time.Now()

		if len(records) == 0 {
			if !fr.wait(fr.fetchInterval) {
				return
			}
			continue
		}

		nextOffset := offset
		appendErr := false
		for _, rec := range records {
			appendedOffset, appended, err := fr.part.appendReplica(rec)
			if err != nil {
				log.Printf("follower replicate topic=%s partition=%d append error: %v", fr.topic, fr.partition, err)
				appendErr = true
				break
			}
			if appended {
				nextOffset = appendedOffset + 1
			}
		}

		if appendErr {
			offset = nextOffset
			if !fr.wait(fr.retryInterval) {
				return
			}
			continue
		}

		if lastOffset+1 > nextOffset {
			nextOffset = lastOffset + 1
		}
		offset = nextOffset
	}
}

// wait sleeps for the provided duration unless the replicator is asked to stop.
func (fr *followerReplicator) wait(delay time.Duration) bool {
	if delay <= 0 {
		delay = defaultReplicaFetchInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-fr.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

// triggerPromotion asks the broker to promote this replica to leader.
func (fr *followerReplicator) triggerPromotion(cause error) bool {
	if fr.broker == nil {
		return false
	}
	if !fr.promoted.CompareAndSwap(false, true) {
		return false
	}
	log.Printf("follower topic=%s partition=%d initiating promotion: %v", fr.topic, fr.partition, cause)
	go fr.broker.handleReplicaPromotion(fr.topic, fr.partition)
	return true
}

// fetch retrieves records from the leader starting at the supplied offset and
// returns the payload alongside the highest offset fetched.
func (fr *followerReplicator) fetch(offset uint64) ([][]byte, uint64, error) {
	return fetchPartitionBatch(fr.client, fr.leaderAddr, fr.topic, fr.partition, offset)
}

type peerReplicator struct {
	broker        *Broker
	topic         string
	partition     int
	peerID        int
	peerAddr      string
	part          *partitionLog
	client        *http.Client
	fetchInterval time.Duration
	retryInterval time.Duration
	stopCh        chan struct{}
	doneCh        chan struct{}
}

func newPeerReplicator(b *Broker, topic string, partition int, peerID int, peerAddr string, pl *partitionLog, client *http.Client, fetchInterval, retryInterval time.Duration) *peerReplicator {
	if client == nil {
		client = newDefaultHTTPClient()
	}
	if fetchInterval <= 0 {
		fetchInterval = defaultReplicaFetchInterval
	}
	if retryInterval <= 0 {
		retryInterval = defaultReplicaRetryInterval
	}
	return &peerReplicator{
		broker:        b,
		topic:         topic,
		partition:     partition,
		peerID:        peerID,
		peerAddr:      peerAddr,
		part:          pl,
		client:        client,
		fetchInterval: fetchInterval,
		retryInterval: retryInterval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

func (pr *peerReplicator) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		pr.run()
	}()
}

func (pr *peerReplicator) stop() {
	select {
	case <-pr.stopCh:
	default:
		close(pr.stopCh)
	}
	<-pr.doneCh
}

func (pr *peerReplicator) run() {
	defer close(pr.doneCh)
	if pr.part == nil {
		return
	}
	offset := uint64(0)
	for {
		select {
		case <-pr.stopCh:
			return
		default:
		}

		records, lastOffset, err := fetchPartitionBatch(pr.client, pr.peerAddr, pr.topic, pr.partition, offset)
		if err != nil {
			log.Printf("peer replicate topic=%s partition=%d peer=%d fetch error: %v", pr.topic, pr.partition, pr.peerID, err)
			if !pr.wait(pr.retryInterval) {
				return
			}
			continue
		}

		if len(records) == 0 {
			if lastOffset+1 > offset {
				offset = lastOffset + 1
			}
			if !pr.wait(pr.fetchInterval) {
				return
			}
			continue
		}

		appendErr := false
		for _, rec := range records {
			if _, _, err := pr.part.appendReplica(rec); err != nil {
				log.Printf("peer replicate topic=%s partition=%d peer=%d append error: %v", pr.topic, pr.partition, pr.peerID, err)
				appendErr = true
				break
			}
		}
		if appendErr {
			if !pr.wait(pr.retryInterval) {
				return
			}
			continue
		}
		if lastOffset+1 > offset {
			offset = lastOffset + 1
		}
	}
}

func (pr *peerReplicator) wait(delay time.Duration) bool {
	if delay <= 0 {
		delay = defaultReplicaFetchInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-pr.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

func fetchPartitionBatch(client *http.Client, baseAddr, topic string, partition int, offset uint64) ([][]byte, uint64, error) {
	urlStr, err := buildReplicaFetchURL(baseAddr, topic, partition, offset)
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
	var result [][]byte
	if len(payload.RawRecords) > 0 {
		result = make([][]byte, len(payload.RawRecords))
		for i, raw := range payload.RawRecords {
			val, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return nil, offset, err
			}
			result[i] = val
		}
	} else {
		result = make([][]byte, len(payload.Records))
		for i, rec := range payload.Records {
			result[i] = []byte(rec)
		}
	}
	return result, payload.ToOffset, nil
}

func buildReplicaFetchURL(baseAddr, topic string, partition int, offset uint64) (string, error) {
	base, err := url.Parse(baseAddr)
	if err != nil {
		return "", err
	}
	topicEscaped := url.PathEscape(topic)
	base.Path = path.Join(base.Path, "topics", topicEscaped, "partitions", strconv.Itoa(partition), "fetch")
	q := base.Query()
	q.Set("offset", strconv.FormatUint(offset, 10))
	q.Set("raw", "1")
	base.RawQuery = q.Encode()
	return base.String(), nil
}
