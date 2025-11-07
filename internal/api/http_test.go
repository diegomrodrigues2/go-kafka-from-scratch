package api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 got %d", rec.Code)
	}
}

// TestServerPublishRedirectsToLeader verifies the API hints clients to publish
// to the leader when the current broker is a follower.
func TestServerPublishRedirectsToLeader(t *testing.T) {
	leaderMux := http.NewServeMux()
	leaderMux.HandleFunc("/topics/demo/partitions/0/fetch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"fromOffset":0,"toOffset":0,"records":[]}`))
	})
	leaderSrv := httptest.NewServer(leaderMux)
	t.Cleanup(leaderSrv.Close)

	cfg := broker.Config{
		Cluster: &broker.ClusterConfig{
			BrokerID: 2,
			Peers: map[int]string{
				1: leaderSrv.URL,
			},
			Partitions: map[string]map[int]broker.PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
			ReplicaFetchInterval: 10 * time.Millisecond,
			ReplicaRetryInterval: 10 * time.Millisecond,
		},
	}
	b := broker.NewBroker(cfg)
	b.SetHTTPClient(leaderSrv.Client())
	dir := t.TempDir()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 128); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	srv := NewHTTP(b)
	req := httptest.NewRequest(http.MethodPost, "/topics/demo/partitions/0", bytes.NewReader([]byte("payload")))
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	srv.handlePublish(rec, req)
	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("expected redirect got %d", rec.Code)
	}
	location := rec.Header().Get("Location")
	expected, err := buildPartitionPublishURL(leaderSrv.URL, "demo", 0)
	if err != nil {
		t.Fatalf("build url: %v", err)
	}
	if location != expected {
		t.Fatalf("expected location %s got %s", expected, location)
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

func TestServerPartitionInfo(t *testing.T) {
	cfg := broker.Config{
		Cluster: &broker.ClusterConfig{
			BrokerID: 1,
			Partitions: map[string]map[int]broker.PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}, Epoch: 3}},
			},
		},
	}
	b := broker.NewBroker(cfg)
	dir := t.TempDir()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/cluster/topics/demo/partitions/0", nil)
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	NewHTTP(b).handlePartitionInfo(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"leader":1`) || !strings.Contains(body, `"epoch":3`) {
		t.Fatalf("unexpected body %s", body)
	}
}

func TestServerUpdateLeader(t *testing.T) {
	cfg := broker.Config{
		Cluster: &broker.ClusterConfig{
			BrokerID: 2,
			Partitions: map[string]map[int]broker.PartitionAssignment{
				"demo": {0: {Leader: 1, Replicas: []int{1, 2}}},
			},
		},
	}
	b := broker.NewBroker(cfg)
	dir := t.TempDir()
	partPath := filepath.Join(dir, "topic=demo-part=0")
	if err := b.EnsurePartition("demo", 0, partPath, 64); err != nil {
		t.Fatalf("ensure partition: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	req := httptest.NewRequest(http.MethodPost, "/cluster/topics/demo/partitions/0/leader", bytes.NewReader([]byte(`{"leaderId":2,"epoch":1}`)))
	req.SetPathValue("topic", "demo")
	req.SetPathValue("p", "0")
	rec := httptest.NewRecorder()
	NewHTTP(b).handleUpdateLeader(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 got %d", rec.Code)
	}

	assign, err := b.PartitionAssignment("demo", 0)
	if err != nil {
		t.Fatalf("assignment: %v", err)
	}
	if assign.Leader != 2 || assign.Epoch != 1 {
		t.Fatalf("expected leader 2 epoch 1 got %+v", assign)
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
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 got %d", rec.Code)
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
