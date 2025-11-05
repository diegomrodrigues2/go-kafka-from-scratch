package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Producer struct{ BaseURL string }

func (p Producer) Publish(topic string, partition int, v []byte) (uint64, error) {
	url := fmt.Sprintf("%s/topics/%s/partitions/%d", p.BaseURL, topic, partition)
	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(v))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("publish failed: %s", resp.Status)
	}
	var out struct{ Offset uint64 }
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Offset, nil
}
