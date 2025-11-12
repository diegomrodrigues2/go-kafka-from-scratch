package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/deltalog"
)

func buildPartitionDeltaURL(baseAddr, topic string, partition int, fromVersion uint64) (string, error) {
	if baseAddr == "" {
		return "", errors.New("base address required")
	}
	u, err := url.Parse(baseAddr)
	if err != nil {
		return "", err
	}
	topicEscaped := url.PathEscape(topic)
	u.Path = path.Join(u.Path, "topics", topicEscaped, "partitions", strconv.Itoa(partition), "delta")
	q := u.Query()
	q.Set("fromVersion", strconv.FormatUint(fromVersion, 10))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (b *Broker) syncDeltaFromPeer(topic string, partition int, peerAddr string) error {
	if b == nil {
		return errors.New("broker not configured")
	}
	pl, err := b.getPartition(topic, partition)
	if err != nil {
		return err
	}
	if peerAddr == "" {
		return errors.New("peer address required for delta sync")
	}
	client := b.httpClient
	if client == nil {
		client = newDefaultHTTPClient()
	}
	fromVersion := uint64(0)
	if v, ok := pl.log.DeltaVersion(); ok {
		fromVersion = v + 1
	}
	urlStr, err := buildPartitionDeltaURL(peerAddr, topic, partition, fromVersion)
	if err != nil {
		return err
	}
	resp, err := client.Get(urlStr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("delta sync fetch failed: %s", resp.Status)
	}
	var payload struct {
		Checkpoint *deltalog.Checkpoint `json:"checkpoint,omitempty"`
		Commits    []*deltalog.Commit   `json:"commits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return err
	}
	if len(payload.Commits) == 0 {
		return nil
	}
	return pl.applyRemoteCommits(payload.Commits)
}

// ResyncPartitionFromPeer fetches missing delta log commits from the provided peer URL.
func (b *Broker) ResyncPartitionFromPeer(topic string, partition int, peerAddr string) error {
	return b.syncDeltaFromPeer(topic, partition, peerAddr)
}
