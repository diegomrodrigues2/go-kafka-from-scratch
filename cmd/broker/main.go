package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/api"
	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/broker"
)

var (
	listenAndServe = http.ListenAndServe
	mkdirAll       = os.MkdirAll
	exitFn         = os.Exit
)

// main runs the standalone broker process.
func main() {
	if err := run(); err != nil {
		log.Printf("broker error: %v", err)
		exitFn(1)
	}
}

// run initialises the broker, wires the HTTP API and starts listening for
// client requests.
func run() error {
	dataDir := getenv("DATA_DIR", "./data")
	topic := getenv("TOPIC", "demo")
	partition := 0

	cfg := broker.DefaultConfig()
	cfg.EnableTransactions = getenvBool("ENABLE_TRANSACTIONS", true)
	b := broker.NewBroker(cfg)
	defer func() { _ = b.Close() }()
	if err := mkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	partPath := filepath.Join(dataDir, "topic="+topic+"-part=0")
	if err := b.EnsurePartition(topic, partition, partPath, 128<<20); err != nil {
		return err
	}

	mux := http.NewServeMux()
	api.NewHTTP(b).Routes(mux)
	addr := getenv("ADDR", ":8080")
	log.Printf("broker listening on %s", addr)
	return listenAndServe(addr, mux)
}

// getenv returns the environment variable value falling back to a default when
// it is not set.
func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvBool(k string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}
