package client

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Consumer struct{ BaseURL string }

func (c Consumer) Fetch(topic string, partition int, off uint64, maxBytes int) ([][]byte, uint64, error) {
	url := fmt.Sprintf("%s/topics/%s/partitions/%d/fetch?offset=%d&maxBytes=%d",
		c.BaseURL, topic, partition, off, maxBytes)
	resp, err := http.Get(url)
	if err != nil {
		return nil, off, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, off, fmt.Errorf("fetch failed: %s", resp.Status)
	}
	var out struct {
		FromOffset uint64
		ToOffset   uint64
		Records    [][]byte
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, off, err
	}
	return out.Records, out.ToOffset, nil
}
