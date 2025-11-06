package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/broker"
)

type Server struct {
	b *broker.Broker
}

func NewHTTP(b *broker.Broker) *Server { return &Server{b: b} }

func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("POST /topics/{topic}/partitions/{p}", s.handlePublish)
	mux.HandleFunc("GET /topics/{topic}/partitions/{p}/fetch", s.handleFetch)
}

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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"offset": off})
}

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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert [][]byte to []string so JSON encoding returns plain text instead of base64
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
