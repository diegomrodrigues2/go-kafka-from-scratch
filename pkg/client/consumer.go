package client

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Consumer struct{ BaseURL string }

// FetchResult contains the payload returned by a consumer fetch request.
type FetchResult struct {
	FromOffset     uint64
	ToOffset       uint64
	Records        [][]byte
	CommitVersions []*uint64
	DeltaVersion   *uint64
}

// FetchWithMetadata retrieves records together with commit metadata (if provided by the broker).
func (c Consumer) FetchWithMetadata(topic string, partition int, off uint64, maxBytes int) (FetchResult, error) {
	url := fmt.Sprintf("%s/topics/%s/partitions/%d/fetch?offset=%d&maxBytes=%d",
		c.BaseURL, topic, partition, off, maxBytes)
	resp, err := http.Get(url)
	if err != nil {
		return FetchResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return FetchResult{}, fmt.Errorf("fetch failed: %s", resp.Status)
	}
	var out struct {
		FromOffset     uint64    `json:"fromOffset"`
		ToOffset       uint64    `json:"toOffset"`
		Records        [][]byte  `json:"records"`
		CommitVersions []*uint64 `json:"commitVersions"`
		DeltaVersion   *uint64   `json:"deltaVersion"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return FetchResult{}, err
	}
	return FetchResult{
		FromOffset:     out.FromOffset,
		ToOffset:       out.ToOffset,
		Records:        out.Records,
		CommitVersions: out.CommitVersions,
		DeltaVersion:   out.DeltaVersion,
	}, nil
}

// Fetch keeps backward compatibility by returning only records and offset.
func (c Consumer) Fetch(topic string, partition int, off uint64, maxBytes int) ([][]byte, uint64, error) {
	res, err := c.FetchWithMetadata(topic, partition, off, maxBytes)
	if err != nil {
		return nil, off, err
	}
	return res.Records, res.ToOffset, nil
}
