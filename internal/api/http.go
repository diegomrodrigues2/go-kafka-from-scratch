package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/broker"
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
	mux.HandleFunc("GET /topics/{topic}/partitions/{p}/fetch", s.handleFetch)
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

// respondPublishError inspects the broker error and maps it to an HTTP status,
// optionally instructing the client to redirect to the leader.
func (s *Server) respondPublishError(w http.ResponseWriter, topic string, partition int, err error) {
	switch {
	case errors.Is(err, broker.ErrPartitionNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
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

	recs, last, err := s.b.Fetch(topic, p, off, max)
	if err != nil {
		if errors.Is(err, broker.ErrPartitionNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	records := make([]string, len(recs))
	for i, rec := range recs {
		records[i] = string(rec)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"fromOffset": off,
		"toOffset":   last,
		"records":    records,
	})
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
