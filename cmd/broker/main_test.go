package main

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestGetenv(t *testing.T) {
	key := "BROKER_TEST_ENV"
	os.Unsetenv(key)
	if got := getenv(key, "fallback"); got != "fallback" {
		t.Fatalf("expected fallback got %s", got)
	}
	t.Setenv(key, "value")
	if got := getenv(key, "fallback"); got != "value" {
		t.Fatalf("expected value got %s", got)
	}
}

func TestRunSuccess(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("TOPIC", "demo")
	t.Setenv("ADDR", ":9090")

	var called bool
	origListen := listenAndServe
	listenAndServe = func(addr string, handler http.Handler) error {
		called = true
		if addr != ":9090" {
			t.Fatalf("expected addr :9090 got %s", addr)
		}
		if handler == nil {
			t.Fatalf("handler nil")
		}
		return nil
	}
	t.Cleanup(func() { listenAndServe = origListen })

	if err := run(); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !called {
		t.Fatalf("expected listen to be called")
	}

	if _, err := os.Stat(filepath.Join(dataDir, "topic=demo-part=0")); err != nil {
		t.Fatalf("expected partition directory: %v", err)
	}
}

func TestRunMkdirError(t *testing.T) {
	t.Setenv("DATA_DIR", "ignored")
	t.Setenv("TOPIC", "demo")
	orig := mkdirAll
	mkdirAll = func(string, os.FileMode) error { return errors.New("boom") }
	t.Cleanup(func() { mkdirAll = orig })
	if err := run(); err == nil {
		t.Fatalf("expected error from run")
	}
}

func TestRunEnsurePartitionError(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("TOPIC", "demo")
	t.Setenv("ADDR", ":9999")

	partPath := filepath.Join(dataDir, "topic=demo-part=0")
	if err := os.WriteFile(partPath, []byte("x"), 0o644); err != nil {
		t.Fatalf("write part file: %v", err)
	}

	origListen := listenAndServe
	listenAndServe = func(string, http.Handler) error {
		t.Fatalf("listen should not be called on error")
		return nil
	}
	t.Cleanup(func() { listenAndServe = origListen })

	if err := run(); err == nil {
		t.Fatalf("expected ensure partition error")
	}
}

func TestMainSuccess(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("TOPIC", "demo")
	t.Setenv("ADDR", ":8081")

	origListen := listenAndServe
	listenAndServe = func(string, http.Handler) error { return nil }
	t.Cleanup(func() { listenAndServe = origListen })

	var exitCalled bool
	origExit := exitFn
	exitFn = func(code int) {
		exitCalled = true
		if code != 0 {
			t.Fatalf("unexpected exit code %d", code)
		}
	}
	t.Cleanup(func() { exitFn = origExit })

	main()
	if exitCalled {
		t.Fatalf("exit should not be called on success")
	}
}

func TestMainError(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("DATA_DIR", dataDir)
	t.Setenv("TOPIC", "demo")
	t.Setenv("ADDR", ":8082")

	origListen := listenAndServe
	listenAndServe = func(string, http.Handler) error { return errors.New("fail") }
	t.Cleanup(func() { listenAndServe = origListen })

	var exitCode int
	var mu sync.Mutex
	origExit := exitFn
	exitFn = func(code int) {
		mu.Lock()
		exitCode = code
		mu.Unlock()
	}
	t.Cleanup(func() { exitFn = origExit })

	main()
	mu.Lock()
	defer mu.Unlock()
	if exitCode != 1 {
		t.Fatalf("expected exit code 1 got %d", exitCode)
	}
}
