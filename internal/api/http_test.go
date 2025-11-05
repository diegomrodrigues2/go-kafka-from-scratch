package api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/broker"
)

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read error") }
func (errReader) Close() error             { return nil }

func setupBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir := t.TempDir()
	b := broker.NewBroker()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 128); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func TestServerPublishSuccess(t *testing.T) {
	b := setupBroker(t)
	srv := NewHTTP(b)

	req := httptest.NewRequest(http.MethodPost, "/topics/demo/partitions/0", bytes.NewReader([]byte("hello")))
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handlePublish(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("offset")) {
		t.Fatalf("expected offset in response: %s", rec.Body.String())
	}
}

func TestServerPublishInvalidPartition(t *testing.T) {
	b := setupBroker(t)
	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodPost, "/topics/demo/partitions/x", bytes.NewReader(nil))
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "notanint")
	rec := httptest.NewRecorder()
	srv.handlePublish(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request got %d", rec.Code)
	}
}

func TestServerPublishReadError(t *testing.T) {
	b := setupBroker(t)
	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodPost, "/topics/demo/partitions/0", nil)
	req.Body = errReader{}
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handlePublish(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 got %d", rec.Code)
	}
}

func TestServerPublishBrokerError(t *testing.T) {
	b := broker.NewBroker()
	t.Cleanup(func() { _ = b.Close() })
	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodPost, "/topics/demo/partitions/0", bytes.NewReader([]byte("x")))
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handlePublish(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 got %d", rec.Code)
	}
}

func TestServerFetchSuccess(t *testing.T) {
	b := setupBroker(t)
	if _, err := b.Publish("demo", 0, nil, []byte("value")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	srv := NewHTTP(b)

	req := httptest.NewRequest(http.MethodGet, "/topics/demo/partitions/0/fetch?offset=0", nil)
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handleFetch(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte("records")) {
		t.Fatalf("expected records in body: %s", rec.Body.String())
	}
}

func TestServerFetchInvalidPartition(t *testing.T) {
	b := setupBroker(t)
	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodGet, "/topics/demo/partitions/x/fetch", nil)
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "x")
	rec := httptest.NewRecorder()
	srv.handleFetch(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request got %d", rec.Code)
	}
}

func TestServerFetchInvalidOffset(t *testing.T) {
	b := setupBroker(t)
	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodGet, "/topics/demo/partitions/0/fetch?offset=abc", nil)
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handleFetch(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request got %d", rec.Code)
	}
}

func TestServerFetchInvalidMaxBytes(t *testing.T) {
	b := setupBroker(t)
	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodGet, "/topics/demo/partitions/0/fetch?offset=0&maxBytes=x", nil)
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handleFetch(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request got %d", rec.Code)
	}
}

func TestServerFetchBrokerError(t *testing.T) {
	b := broker.NewBroker()
	t.Cleanup(func() { _ = b.Close() })
	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodGet, "/topics/demo/partitions/0/fetch?offset=0", nil)
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handleFetch(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 got %d", rec.Code)
	}
}

func TestRoutesIntegration(t *testing.T) {
	b := setupBroker(t)
	srv := NewHTTP(b)
	mux := http.NewServeMux()
	srv.Routes(mux)

	req := httptest.NewRequest(http.MethodPost, "/topics/demo/partitions/0", bytes.NewReader([]byte("payload")))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from mux got %d", rec.Code)
	}
}
