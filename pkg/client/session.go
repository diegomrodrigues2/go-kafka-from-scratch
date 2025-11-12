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

const (
	readMinOffsetHeader  = "X-Read-Min-Offset"
	readMinVersionHeader = "X-Read-Min-Version"
)

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
	lastVersion map[string]uint64
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
		lastVersion:    make(map[string]uint64),
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
	return s.fetchInternal(topic, partition, offset, maxBytes, 0)
}

// FetchAtLeastVersion enforces that the replica has applied at least minVersion commits.
func (s *Session) FetchAtLeastVersion(topic string, partition int, offset uint64, maxBytes int, minVersion uint64) ([][]byte, uint64, error) {
	return s.fetchInternal(topic, partition, offset, maxBytes, minVersion)
}

func (s *Session) fetchInternal(topic string, partition int, offset uint64, maxBytes int, minVersion uint64) ([][]byte, uint64, error) {
	key := key(topic, partition)

	s.mu.Lock()
	lastWritten := s.lastWritten[key]
	lastRead := s.lastRead[key]
	lastVersion := s.lastVersion[key]
	s.mu.Unlock()

	requiredOffset := lastRead
	if lastWritten > requiredOffset {
		requiredOffset = lastWritten
	}
	requiredVersion := lastVersion
	if minVersion > requiredVersion {
		requiredVersion = minVersion
	}

	baseURL := s.selectBaseURL(key, requiredOffset, lastRead, requiredVersion, lastVersion)
	endpoint := composeFetchURL(baseURL, topic, partition, offset, maxBytes, requiredOffset, requiredVersion)

	resp, err := s.fetchWithRedirect(endpoint, requiredOffset, requiredVersion, 0)
	if err != nil {
		return nil, offset, err
	}
	records := resp.Records
	toOffset := resp.ToOffset

	if requiredOffset > 0 && toOffset < requiredOffset && !sameEndpoint(baseURL, s.cfg.LeaderURL) {
		leaderURL := composeFetchURL(s.cfg.LeaderURL, topic, partition, offset, maxBytes, requiredOffset, requiredVersion)
		resp, err = s.fetchWithRedirect(leaderURL, requiredOffset, requiredVersion, 0)
		if err != nil {
			return nil, offset, err
		}
		records = resp.Records
		toOffset = resp.ToOffset
	}

	if requiredOffset > 0 && toOffset < requiredOffset {
		return nil, offset, fmt.Errorf("consistency guarantee violated: toOffset=%d required=%d", toOffset, requiredOffset)
	}
	if requiredVersion > 0 {
		if resp.DeltaVersion == nil || *resp.DeltaVersion < requiredVersion {
			return nil, offset, fmt.Errorf("consistency guarantee violated: version=%v required=%d", resp.DeltaVersion, requiredVersion)
		}
	}

	s.mu.Lock()
	if toOffset > s.lastRead[key] {
		s.lastRead[key] = toOffset
	}
	if resp.DeltaVersion != nil && *resp.DeltaVersion > s.lastVersion[key] {
		s.lastVersion[key] = *resp.DeltaVersion
	}
	s.mu.Unlock()

	return records, toOffset, nil
}

func (s *Session) selectBaseURL(key string, requiredOffset, lastRead, requiredVersion, lastVersion uint64) string {
	if (requiredOffset > lastRead && requiredOffset > 0) || (requiredVersion > lastVersion && requiredVersion > 0) {
		return s.cfg.LeaderURL
	}
	if s.stickyFollower != "" {
		return s.stickyFollower
	}
	return s.cfg.LeaderURL
}

type fetchPayload struct {
	Records      [][]byte
	ToOffset     uint64
	DeltaVersion *uint64
}

func (s *Session) fetchWithRedirect(endpoint string, minOffset, minVersion uint64, depth int) (fetchPayload, error) {
	if depth > 5 {
		return fetchPayload{}, fmt.Errorf("too many redirects")
	}
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return fetchPayload{}, err
	}
	if minOffset > 0 {
		req.Header.Set(readMinOffsetHeader, strconv.FormatUint(minOffset, 10))
	}
	if minVersion > 0 {
		req.Header.Set(readMinVersionHeader, strconv.FormatUint(minVersion, 10))
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fetchPayload{}, err
	}
	defer resp.Body.Close()

	if shouldRedirect(resp.StatusCode) {
		loc, err := resp.Location()
		if err != nil {
			return fetchPayload{}, fmt.Errorf("redirect without location")
		}
		next := loc.String()
		if !loc.IsAbs() {
			base, parseErr := url.Parse(endpoint)
			if parseErr == nil {
				next = base.ResolveReference(loc).String()
			}
		}
		return s.fetchWithRedirect(next, minOffset, minVersion, depth+1)
	}

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fetchPayload{}, fmt.Errorf("fetch failed: %s %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var out struct {
		FromOffset     uint64    `json:"fromOffset"`
		ToOffset       uint64    `json:"toOffset"`
		Records        [][]byte  `json:"records"`
		CommitVersions []*uint64 `json:"commitVersions"`
		DeltaVersion   *uint64   `json:"deltaVersion"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fetchPayload{}, err
	}

	return fetchPayload{
		Records:      out.Records,
		ToOffset:     out.ToOffset,
		DeltaVersion: out.DeltaVersion,
	}, nil
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

func composeFetchURL(baseURL, topic string, partition int, offset uint64, maxBytes int, minOffset, minVersion uint64) string {
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
	if minVersion > 0 {
		sb.WriteString("&minVersion=")
		sb.WriteString(strconv.FormatUint(minVersion, 10))
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
