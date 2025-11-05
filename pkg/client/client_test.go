package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProducerPublish(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"offset": 42})
	}))
	t.Cleanup(srv.Close)

	p := Producer{BaseURL: srv.URL}
	off, err := p.Publish("topic", 1, []byte("value"))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if off != 42 {
		t.Fatalf("expected offset 42 got %d", off)
	}
}

func TestProducerPublishHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)

	p := Producer{BaseURL: srv.URL}
	if _, err := p.Publish("topic", 0, []byte("x")); err == nil {
		t.Fatalf("expected error")
	}
}

func TestProducerPublishJSONError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	}))
	t.Cleanup(srv.Close)

	p := Producer{BaseURL: srv.URL}
	if _, err := p.Publish("topic", 0, []byte("x")); err == nil {
		t.Fatalf("expected decode error")
	}
}

func TestProducerPublishRequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	p := Producer{BaseURL: url}
	if _, err := p.Publish("topic", 0, []byte("x")); err == nil {
		t.Fatalf("expected request error")
	}
}

func TestConsumerFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"fromOffset": 0,
			"toOffset":   5,
			"records":    [][]byte{[]byte("a"), []byte("b")},
		})
	}))
	t.Cleanup(srv.Close)

	c := Consumer{BaseURL: srv.URL}
	recs, last, err := c.Fetch("topic", 1, 0, 100)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(recs) != 2 || last != 5 {
		t.Fatalf("unexpected fetch result len=%d last=%d", len(recs), last)
	}
}

func TestConsumerFetchHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := Consumer{BaseURL: srv.URL}
	if _, _, err := c.Fetch("topic", 0, 0, 0); err == nil {
		t.Fatalf("expected error")
	}
}

func TestConsumerFetchDecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("bad json"))
	}))
	t.Cleanup(srv.Close)

	c := Consumer{BaseURL: srv.URL}
	if _, _, err := c.Fetch("topic", 0, 0, 0); err == nil {
		t.Fatalf("expected decode error")
	}
}

func TestConsumerFetchRequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	c := Consumer{BaseURL: url}
	if _, _, err := c.Fetch("topic", 0, 0, 0); err == nil {
		t.Fatalf("expected request error")
	}
}
