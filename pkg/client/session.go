package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const readMinOffsetHeader = "X-Read-Min-Offset"

// SessionConfig describes how a client session connects to a replicated cluster.
type SessionConfig struct {
	// SessionID is an opaque identifier used to select a sticky follower.
	SessionID string
	// LeaderURL must point to the broker that leads the partition(s) accessed by the session.
	LeaderURL string
	// FollowerURLs lists follower brokers that can serve read requests.
	FollowerURLs []string
	// HTTPClient optionally overrides the client used for requests.
	HTTPClient *http.Client
}

// Session tracks per-topic offsets in order to provide read-your-writes and
// monotonic read guarantees when talking to a replicated cluster.
type Session struct {
	cfg            SessionConfig
	httpClient     *http.Client
	stickyFollower string

	mu          sync.Mutex
	lastWritten map[string]uint64
	lastRead    map[string]uint64
}

// NewSession creates a new session ensuring the configuration is valid.
func NewSession(cfg SessionConfig) (*Session, error) {
	if cfg.LeaderURL == "" {
		return nil, fmt.Errorf("leader url is required")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &Session{
		cfg:            cfg,
		httpClient:     client,
		stickyFollower: selectFollower(cfg.SessionID, cfg.FollowerURLs),
		lastWritten:    make(map[string]uint64),
		lastRead:       make(map[string]uint64),
	}, nil
}

// Publish appends a record via the leader and remembers the returned offset.
func (s *Session) Publish(topic string, partition int, value []byte) (uint64, error) {
	base := fmt.Sprintf("%s/topics/%s/partitions/%d", s.cfg.LeaderURL, topic, partition)
	offset, err := s.publishWithRedirect(base, value, 0)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.lastWritten[key(topic, partition)] = offset
	s.mu.Unlock()
	return offset, nil
}

func (s *Session) publishWithRedirect(endpoint string, value []byte, depth int) (uint64, error) {
	if depth > 5 {
		return 0, fmt.Errorf("too many publish redirects")
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(value))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if shouldRedirect(resp.StatusCode) {
		loc, err := resp.Location()
		if err != nil {
			return 0, fmt.Errorf("publish redirect: %w", err)
		}
		next := loc.String()
		if !loc.IsAbs() {
			base, parseErr := url.Parse(endpoint)
			if parseErr == nil {
				next = base.ResolveReference(loc).String()
			}
		}
		return s.publishWithRedirect(next, value, depth+1)
	}

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("publish failed: %s %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var out struct {
		Offset uint64 `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.Offset, nil
}

// Fetch retrieves records while enforcing read-your-writes and monotonic reads.
func (s *Session) Fetch(topic string, partition int, offset uint64, maxBytes int) ([][]byte, uint64, error) {
	key := key(topic, partition)

	s.mu.Lock()
	lastWritten := s.lastWritten[key]
	lastRead := s.lastRead[key]
	s.mu.Unlock()

	requiredOffset := lastRead
	if lastWritten > requiredOffset {
		requiredOffset = lastWritten
	}

	baseURL := s.selectBaseURL(key, requiredOffset, lastRead)
	endpoint := composeFetchURL(baseURL, topic, partition, offset, maxBytes, requiredOffset)

	records, toOffset, err := s.fetchWithRedirect(endpoint, requiredOffset, 0)
	if err != nil {
		return nil, offset, err
	}

	if requiredOffset > 0 && toOffset < requiredOffset && !sameEndpoint(baseURL, s.cfg.LeaderURL) {
		leaderURL := composeFetchURL(s.cfg.LeaderURL, topic, partition, offset, maxBytes, requiredOffset)
		records, toOffset, err = s.fetchWithRedirect(leaderURL, requiredOffset, 0)
		if err != nil {
			return nil, offset, err
		}
	}

	if requiredOffset > 0 && toOffset < requiredOffset {
		return nil, offset, fmt.Errorf("consistency guarantee violated: toOffset=%d required=%d", toOffset, requiredOffset)
	}

	s.mu.Lock()
	if toOffset > s.lastRead[key] {
		s.lastRead[key] = toOffset
	}
	s.mu.Unlock()

	return records, toOffset, nil
}

func (s *Session) selectBaseURL(key string, requiredOffset, lastRead uint64) string {
	if requiredOffset > lastRead && requiredOffset > 0 {
		return s.cfg.LeaderURL
	}
	if s.stickyFollower != "" {
		return s.stickyFollower
	}
	return s.cfg.LeaderURL
}

func (s *Session) fetchWithRedirect(endpoint string, minOffset uint64, depth int) ([][]byte, uint64, error) {
	if depth > 5 {
		return nil, 0, fmt.Errorf("too many redirects")
	}
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, err
	}
	if minOffset > 0 {
		req.Header.Set(readMinOffsetHeader, strconv.FormatUint(minOffset, 10))
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if shouldRedirect(resp.StatusCode) {
		loc, err := resp.Location()
		if err != nil {
			return nil, 0, fmt.Errorf("redirect without location")
		}
		next := loc.String()
		if !loc.IsAbs() {
			base, parseErr := url.Parse(endpoint)
			if parseErr == nil {
				next = base.ResolveReference(loc).String()
			}
		}
		return s.fetchWithRedirect(next, minOffset, depth+1)
	}

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, 0, fmt.Errorf("fetch failed: %s %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var out struct {
		FromOffset uint64   `json:"fromOffset"`
		ToOffset   uint64   `json:"toOffset"`
		Records    [][]byte `json:"records"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, 0, err
	}

	return out.Records, out.ToOffset, nil
}

func shouldRedirect(status int) bool {
	switch status {
	case http.StatusMovedPermanently,
		http.StatusFound,
		http.StatusSeeOther,
		http.StatusTemporaryRedirect,
		http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func composeFetchURL(baseURL, topic string, partition int, offset uint64, maxBytes int, minOffset uint64) string {
	sb := &strings.Builder{}
	sb.WriteString(fmt.Sprintf("%s/topics/%s/partitions/%d/fetch?offset=%d", baseURL, topic, partition, offset))
	if maxBytes > 0 {
		sb.WriteString("&maxBytes=")
		sb.WriteString(strconv.Itoa(maxBytes))
	}
	if minOffset > 0 {
		sb.WriteString("&minOffset=")
		sb.WriteString(strconv.FormatUint(minOffset, 10))
	}
	return sb.String()
}

func selectFollower(sessionID string, followers []string) string {
	if len(followers) == 0 {
		return ""
	}
	if sessionID == "" {
		return followers[0]
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(sessionID))
	idx := int(h.Sum32()) % len(followers)
	return followers[idx]
}

func key(topic string, partition int) string {
	return fmt.Sprintf("%s:%d", topic, partition)
}

func sameEndpoint(a, b string) bool {
	return strings.TrimSuffix(a, "/") == strings.TrimSuffix(b, "/")
}
