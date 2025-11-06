package broker

import (
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
	"time"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

// followerReplicator continuously pulls data from the leader and appends it to
// the local log to keep this broker's replica up to date.
type followerReplicator struct {
	topic         string
	partition     int
	log           *logstore.Log
	leaderAddr    string
	client        *http.Client
	fetchInterval time.Duration
	retryInterval time.Duration
	stopCh        chan struct{}
	doneCh        chan struct{}
}

// newFollowerReplicator wires together the helper responsible for fetching
// from the leader. Tests can pass a custom HTTP client or timing configuration.
func newFollowerReplicator(topic string, partition int, lg *logstore.Log, leaderAddr string, client *http.Client, fetchInterval, retryInterval time.Duration) *followerReplicator {
	if client == nil {
		client = newDefaultHTTPClient()
	}
	if fetchInterval <= 0 {
		fetchInterval = defaultReplicaFetchInterval
	}
	if retryInterval <= 0 {
		retryInterval = defaultReplicaRetryInterval
	}
	return &followerReplicator{
		topic:         topic,
		partition:     partition,
		log:           lg,
		leaderAddr:    leaderAddr,
		client:        client,
		fetchInterval: fetchInterval,
		retryInterval: retryInterval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
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
	offset := fr.log.NextOffset()
	for {
		select {
		case <-fr.stopCh:
			return
		default:
		}

		records, lastOffset, err := fr.fetch(offset)
		if err != nil {
			log.Printf("follower replicate topic=%s partition=%d fetch error: %v", fr.topic, fr.partition, err)
			if !fr.wait(fr.retryInterval) {
				return
			}
			continue
		}

		if len(records) == 0 {
			if !fr.wait(fr.fetchInterval) {
				return
			}
			continue
		}

		nextOffset := offset
		appendErr := false
		for _, rec := range records {
			appended, err := fr.log.Append(rec)
			if err != nil {
				log.Printf("follower replicate topic=%s partition=%d append error: %v", fr.topic, fr.partition, err)
				appendErr = true
				break
			}
			nextOffset = appended + 1
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

// fetch retrieves records from the leader starting at the supplied offset and
// returns the payload alongside the highest offset fetched.
func (fr *followerReplicator) fetch(offset uint64) ([][]byte, uint64, error) {
	urlStr, err := fr.buildFetchURL(offset)
	if err != nil {
		return nil, offset, err
	}
	resp, err := fr.client.Get(urlStr)
	if err != nil {
		return nil, offset, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, offset, fmt.Errorf("fetch from leader returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		ToOffset uint64   `json:"toOffset"`
		Records  []string `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, offset, err
	}
	result := make([][]byte, len(payload.Records))
	for i, rec := range payload.Records {
		result[i] = []byte(rec)
	}
	return result, payload.ToOffset, nil
}

// buildFetchURL constructs the leader fetch endpoint including the next offset
// to be replicated.
func (fr *followerReplicator) buildFetchURL(offset uint64) (string, error) {
	base, err := url.Parse(fr.leaderAddr)
	if err != nil {
		return "", err
	}
	topicEscaped := url.PathEscape(fr.topic)
	base.Path = path.Join(base.Path, "topics", topicEscaped, "partitions", strconv.Itoa(fr.partition), "fetch")
	q := base.Query()
	q.Set("offset", strconv.FormatUint(offset, 10))
	base.RawQuery = q.Encode()
	return base.String(), nil
}
