package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/api"
	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/broker"
)

var (
	listenAndServe = http.ListenAndServe
	mkdirAll       = os.MkdirAll
	exitFn         = os.Exit
)

func main() {
	if err := run(); err != nil {
		log.Printf("broker error: %v", err)
		exitFn(1)
	}
}

func run() error {
	dataDir := getenv("DATA_DIR", "./data")
	topic := getenv("TOPIC", "demo")
	partition := 0

	b := broker.NewBroker()
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

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
