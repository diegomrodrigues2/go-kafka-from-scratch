package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/broker"
	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/deltalog"
)

const (
	headerReadMinOffset  = "X-Read-Min-Offset"
	headerReadMinVersion = "X-Read-Min-Version"
)

// Server exposes the broker API over HTTP.
type Server struct {
	b *broker.Broker
}

// NewHTTP wraps a broker with HTTP handlers.
func NewHTTP(b *broker.Broker) *Server { return &Server{b: b} }

// Routes wires the REST endpoints into the provided mux.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /topics/{topic}/partitions/{p}", s.handlePublish)
	mux.HandleFunc("POST /topics/{topic}/partitions/{p}/batch", s.handlePublishBatch)
	mux.HandleFunc("GET /topics/{topic}/partitions/{p}/fetch", s.handleFetch)
	mux.HandleFunc("GET /topics/{topic}/partitions/{p}/delta", s.handleDeltaLog)
	mux.HandleFunc("GET /topics/{topic}/partitions/{p}/metrics", s.handlePartitionMetrics)
	mux.HandleFunc("GET /cluster/topics/{topic}/partitions/{p}", s.handlePartitionInfo)
	mux.HandleFunc("POST /cluster/topics/{topic}/partitions/{p}/leader", s.handleUpdateLeader)
	mux.HandleFunc("POST /cluster/topics/{topic}/partitions/{p}/replica", s.handleReplicaWrite)
	mux.HandleFunc("POST /cluster/topics/{topic}/partitions/{p}/resync", s.handleResync)
}

// handlePublish accepts a message body, forwards it to the broker and encodes
// the resulting offset as JSON.
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	off, err := s.b.Publish(topic, p, nil, payload)
	if err != nil {
		s.respondPublishError(w, topic, p, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"offset": off})
}

// handlePublishBatch appends multiple records atomically (when supported) using transactional commits.
func (s *Server) handlePublishBatch(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var payload struct {
		Records  []string  `json:"records"`
		Versions []*uint64 `json:"versions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if len(payload.Records) == 0 {
		http.Error(w, "records required", http.StatusBadRequest)
		return
	}
	values := make([][]byte, len(payload.Records))
	for i, rec := range payload.Records {
		data, decErr := base64.StdEncoding.DecodeString(rec)
		if decErr != nil {
			http.Error(w, "records must be base64 encoded", http.StatusBadRequest)
			return
		}
		values[i] = data
	}
	offsets, version, hasVersion, err := s.b.PublishBatch(topic, p, values)
	if err != nil {
		s.respondPublishError(w, topic, p, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	resp := map[string]any{"offsets": offsets}
	if hasVersion {
		resp["commitVersion"] = version
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// respondPublishError inspects the broker error and maps it to an HTTP status,
// optionally instructing the client to redirect to the leader.
func (s *Server) respondPublishError(w http.ResponseWriter, topic string, partition int, err error) {
	switch {
	case errors.Is(err, broker.ErrPartitionNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, broker.ErrWriteQuorumNotMet):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	case errors.Is(err, broker.ErrTxnConflict):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, broker.ErrNotLeader):
		leaderID, leaderAddr, leaderErr := s.b.PartitionLeader(topic, partition)
		switch {
		case leaderErr == nil && leaderAddr != "":
			if loc, buildErr := buildPartitionPublishURL(leaderAddr, topic, partition); buildErr == nil {
				w.Header().Set("Location", loc)
			}
			http.Error(w, fmt.Sprintf("redirect to leader %d", leaderID), http.StatusTemporaryRedirect)
		case leaderErr != nil && errors.Is(leaderErr, broker.ErrLeaderNotAvailable):
			http.Error(w, broker.ErrLeaderNotAvailable.Error(), http.StatusServiceUnavailable)
		case leaderErr != nil && errors.Is(leaderErr, broker.ErrPartitionNotFound):
			http.Error(w, broker.ErrPartitionNotFound.Error(), http.StatusNotFound)
		default:
			http.Error(w, broker.ErrNotLeader.Error(), http.StatusConflict)
		}
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleFetch streams records to the caller starting from the requested offset.
func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	off, err := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}
	max, err := strconv.Atoi(r.URL.Query().Get("maxBytes"))
	if err != nil && r.URL.Query().Get("maxBytes") != "" {
		http.Error(w, "invalid maxBytes", http.StatusBadRequest)
		return
	}

	minOffset, hasMin := uint64(0), false
	if hdr := r.Header.Get(headerReadMinOffset); hdr != "" {
		val, err := strconv.ParseUint(hdr, 10, 64)
		if err != nil {
			http.Error(w, "invalid X-Read-Min-Offset", http.StatusBadRequest)
			return
		}
		minOffset, hasMin = val, true
	}
	if qmin := r.URL.Query().Get("minOffset"); qmin != "" {
		val, err := strconv.ParseUint(qmin, 10, 64)
		if err != nil {
			http.Error(w, "invalid minOffset", http.StatusBadRequest)
			return
		}
		minOffset, hasMin = val, true
	}

	minVersion, hasMinVersion := uint64(0), false
	if hdr := r.Header.Get(headerReadMinVersion); hdr != "" {
		val, err := strconv.ParseUint(hdr, 10, 64)
		if err != nil {
			http.Error(w, "invalid X-Read-Min-Version", http.StatusBadRequest)
			return
		}
		minVersion, hasMinVersion = val, true
	}
	if qmv := r.URL.Query().Get("minVersion"); qmv != "" {
		val, err := strconv.ParseUint(qmv, 10, 64)
		if err != nil {
			http.Error(w, "invalid minVersion", http.StatusBadRequest)
			return
		}
		minVersion, hasMinVersion = val, true
	}

	if hasMin && s.b.ReplicationMode() != broker.ReplicationModeLeaderless {
		endOffset, err := s.b.EndOffset(topic, p)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if endOffset <= minOffset {
			isLeader, err := s.b.IsLeader(topic, p)
			if err != nil && !errors.Is(err, broker.ErrPartitionNotFound) {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !isLeader {
				leaderID, leaderAddr, leaderErr := s.b.PartitionLeader(topic, p)
				if leaderErr != nil {
					http.Error(w, leaderErr.Error(), http.StatusServiceUnavailable)
					return
				}
				if loc, buildErr := buildPartitionFetchURL(leaderAddr, topic, p, off, max, minOffset, true); buildErr == nil {
					w.Header().Set("Location", loc)
				}
				http.Error(w, fmt.Sprintf("redirect to leader %d for consistent read", leaderID), http.StatusTemporaryRedirect)
				return
			}
			http.Error(w, "requested offset not yet available", http.StatusServiceUnavailable)
			return
		}
	}

	var metrics broker.PartitionMetrics
	var metricsErr error
	if hasMinVersion || r.URL.Query().Get("deltaVersion") == "1" {
		metrics, metricsErr = s.b.PartitionMetrics(topic, p)
		if metricsErr != nil && !errors.Is(metricsErr, broker.ErrPartitionNotFound) {
			http.Error(w, metricsErr.Error(), http.StatusInternalServerError)
			return
		}
	}
	if hasMinVersion {
		if metricsErr != nil || metrics.DeltaVersion == nil || *metrics.DeltaVersion < minVersion {
			isLeader, err := s.b.IsLeader(topic, p)
			if err != nil && !errors.Is(err, broker.ErrPartitionNotFound) {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !isLeader {
				leaderID, leaderAddr, leaderErr := s.b.PartitionLeader(topic, p)
				if leaderErr != nil {
					http.Error(w, leaderErr.Error(), http.StatusServiceUnavailable)
					return
				}
				if loc, buildErr := buildPartitionFetchURL(leaderAddr, topic, p, off, max, minOffset, hasMin); buildErr == nil {
					w.Header().Set("Location", loc)
				}
				http.Error(w, fmt.Sprintf("redirect to leader %d for consistent version", leaderID), http.StatusTemporaryRedirect)
				return
			}
			http.Error(w, "requested version not yet available", http.StatusServiceUnavailable)
			return
		}
	}

	recs, last, err := s.b.Fetch(topic, p, off, max)
	if err != nil {
		if errors.Is(err, broker.ErrPartitionNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if errors.Is(err, broker.ErrReadQuorumNotMet) {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rawParam := strings.ToLower(r.URL.Query().Get("raw"))
	includeRaw := rawParam == "1" || rawParam == "true" || rawParam == "yes"
	records := make([]string, len(recs))
	var rawRecords []string
	if includeRaw {
		rawRecords = make([]string, len(recs))
	}
	var startOffset uint64
	if len(recs) > 0 {
		startOffset = last - uint64(len(recs)) + 1
		versions, versionErr := s.b.PartitionCommitVersions(topic, p, startOffset, len(recs))
		if versionErr != nil && !errors.Is(versionErr, broker.ErrPartitionNotFound) {
			http.Error(w, versionErr.Error(), http.StatusInternalServerError)
			return
		}
		if len(versions) == len(recs) {
			for i := range recs {
				if versions[i] != 0 {
					recs[i].CommitVersion = versions[i]
				}
			}
		}
	}
	var commitVersions []*uint64
	haveVersions := false
	for i, rec := range recs {
		records[i] = string(rec.Payload)
		if includeRaw {
			rawRecords[i] = base64.StdEncoding.EncodeToString(rec.Raw)
		}
		if rec.CommitVersion > 0 {
			if !haveVersions {
				commitVersions = make([]*uint64, len(recs))
				haveVersions = true
			}
			version := rec.CommitVersion
			commitVersions[i] = &version
		} else if haveVersions {
			commitVersions[i] = nil
		}
	}

	w.Header().Set("Content-Type", "application/json")
	payload := struct {
		FromOffset     uint64    `json:"fromOffset"`
		ToOffset       uint64    `json:"toOffset"`
		Records        []string  `json:"records"`
		RawRecords     []string  `json:"rawRecords,omitempty"`
		CommitVersions []*uint64 `json:"commitVersions,omitempty"`
		DeltaVersion   *uint64   `json:"deltaVersion,omitempty"`
	}{
		FromOffset: off,
		ToOffset:   last,
		Records:    records,
		RawRecords: rawRecords,
	}
	if haveVersions {
		payload.CommitVersions = commitVersions
	}
	if metrics.DeltaVersion != nil {
		payload.DeltaVersion = metrics.DeltaVersion
	}
	_ = json.NewEncoder(w).Encode(payload)
}

// handleDeltaLog surfaces the transactional metadata for a partition.
func (s *Server) handleDeltaLog(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	fromVersion := uint64(0)
	if val := r.URL.Query().Get("fromVersion"); val != "" {
		v, parseErr := strconv.ParseUint(val, 10, 64)
		if parseErr != nil {
			http.Error(w, "invalid fromVersion", http.StatusBadRequest)
			return
		}
		fromVersion = v
	}
	var (
		toVersion uint64
		hasTo     bool
	)
	if val := r.URL.Query().Get("toVersion"); val != "" {
		v, parseErr := strconv.ParseUint(val, 10, 64)
		if parseErr != nil {
			http.Error(w, "invalid toVersion", http.StatusBadRequest)
			return
		}
		toVersion, hasTo = v, true
	}
	replay, err := s.b.DeltaReplay(topic, p)
	if err != nil {
		switch {
		case errors.Is(err, broker.ErrPartitionNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, broker.ErrTransactionsDisabled):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	var commits []*deltalog.Commit
	for _, commit := range replay.Commits {
		if commit.Version < fromVersion {
			continue
		}
		if hasTo && commit.Version > toVersion {
			continue
		}
		commits = append(commits, commit)
	}
	resp := struct {
		Checkpoint *deltalog.Checkpoint `json:"checkpoint,omitempty"`
		Commits    []*deltalog.Commit   `json:"commits"`
	}{
		Checkpoint: replay.Checkpoint,
		Commits:    commits,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// handlePartitionMetrics returns a lightweight view of partition state.
func (s *Server) handlePartitionMetrics(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	metrics, err := s.b.PartitionMetrics(topic, p)
	if err != nil {
		if errors.Is(err, broker.ErrPartitionNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(metrics)
}

// handlePartitionInfo exposes the cluster metadata (leader, epoch, replicas)
// for a given topic-partition so peers can resynchronise after restarts.
func (s *Server) handlePartitionInfo(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	assign, err := s.b.PartitionAssignment(topic, p)
	if err != nil {
		if errors.Is(err, broker.ErrPartitionNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"leader":   assign.Leader,
		"epoch":    assign.Epoch,
		"replicas": assign.Replicas,
	})
}

// handleUpdateLeader processes leadership announcements from peers during
// failover, ensuring we update our metadata and start/stop follower loops.
func (s *Server) handleUpdateLeader(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var payload struct {
		LeaderID int    `json:"leaderId"`
		Epoch    uint64 `json:"epoch"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if payload.LeaderID == 0 {
		http.Error(w, "leaderId required", http.StatusBadRequest)
		return
	}
	if err := s.b.SetPartitionLeader(topic, p, payload.LeaderID, payload.Epoch); err != nil {
		switch {
		case errors.Is(err, broker.ErrPartitionNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, broker.ErrStaleLeaderEpoch):
			http.Error(w, err.Error(), http.StatusConflict)
		case errors.Is(err, broker.ErrReplicaNotInSet):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleReplicaWrite accepts raw records (base64 encoded) coming from peers or read-repair flows.
func (s *Server) handleReplicaWrite(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	var payload struct {
		Records  []string  `json:"records"`
		Versions []*uint64 `json:"versions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	if len(payload.Records) == 0 {
		http.Error(w, "records required", http.StatusBadRequest)
		return
	}
	for idx, rawStr := range payload.Records {
		raw, decErr := base64.StdEncoding.DecodeString(rawStr)
		if decErr != nil {
			http.Error(w, "invalid record encoding", http.StatusBadRequest)
			return
		}
		var version uint64
		hasVersion := false
		if idx < len(payload.Versions) && payload.Versions[idx] != nil {
			version = *payload.Versions[idx]
			hasVersion = true
		}
		if _, appended, appendErr := s.b.AppendReplica(topic, p, raw, version, hasVersion); appendErr != nil {
			if errors.Is(appendErr, broker.ErrPartitionNotFound) {
				http.Error(w, appendErr.Error(), http.StatusNotFound)
				return
			}
			http.Error(w, appendErr.Error(), http.StatusInternalServerError)
			return
		} else if !appended {
			continue
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleResync requests that the local broker rebuild commit metadata for the partition.
func (s *Server) handleResync(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	p, err := strconv.Atoi(r.PathValue("p"))
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}
	if err := s.b.ResyncPartition(topic, p); err != nil {
		switch {
		case errors.Is(err, broker.ErrPartitionNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// buildPartitionPublishURL returns the leader endpoint the client should hit
// for publishing to the given partition.
func buildPartitionPublishURL(baseAddr, topic string, partition int) (string, error) {
	u, err := url.Parse(baseAddr)
	if err != nil {
		return "", err
	}
	topicEscaped := url.PathEscape(topic)
	u.Path = path.Join(u.Path, "topics", topicEscaped, "partitions", strconv.Itoa(partition))
	return u.String(), nil
}

func buildPartitionFetchURL(baseAddr, topic string, partition int, offset uint64, maxBytes int, minOffset uint64, includeMin bool) (string, error) {
	u, err := url.Parse(baseAddr)
	if err != nil {
		return "", err
	}
	topicEscaped := url.PathEscape(topic)
	u.Path = path.Join(u.Path, "topics", topicEscaped, "partitions", strconv.Itoa(partition), "fetch")
	q := u.Query()
	q.Set("offset", strconv.FormatUint(offset, 10))
	if maxBytes > 0 {
		q.Set("maxBytes", strconv.Itoa(maxBytes))
	}
	if includeMin {
		q.Set("minOffset", strconv.FormatUint(minOffset, 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
