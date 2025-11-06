package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/api"
	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/broker"
	"github.com/diegomrodrigues2/go-kafka-from-scratch/pkg/client"
)

var letterBytes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

type brokerNode struct {
	id      int
	addr    string
	baseURL string
	dataDir string
	broker  *broker.Broker
	server  *http.Server
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatalf("replication example failed: %v", err)
	}
}

func run(ctx context.Context) error {
	const (
		topic        = "demo"
		partition    = 0
		recordCount  = 1000
		segmentBytes = 8 << 20
	)

	nodesConfig := []struct {
		id   int
		addr string
	}{
		{1, "127.0.0.1:19091"},
		{2, "127.0.0.1:19092"},
		{3, "127.0.0.1:19093"},
	}

	peers := make(map[int]string, len(nodesConfig))
	for _, cfg := range nodesConfig {
		peers[cfg.id] = fmt.Sprintf("http://%s", cfg.addr)
	}

	assignments := map[string]map[int]broker.PartitionAssignment{
		topic: {
			partition: {
				Leader:   nodesConfig[0].id,
				Replicas: []int{nodesConfig[0].id, nodesConfig[1].id, nodesConfig[2].id},
			},
		},
	}

	var (
		nodes []*brokerNode
		wg    sync.WaitGroup
	)

	for _, cfg := range nodesConfig {
		dataDir := filepath.Join("data", fmt.Sprintf("example-node-%d", cfg.id))
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return fmt.Errorf("node %d mkdir: %w", cfg.id, err)
		}

		clusterCfg := &broker.ClusterConfig{
			BrokerID:   cfg.id,
			Peers:      peers,
			Partitions: assignments,
		}

		b := broker.NewBroker(broker.Config{
			DataDir:      dataDir,
			SegmentBytes: segmentBytes,
			Cluster:      clusterCfg,
		})

		partPath := filepath.Join(dataDir, fmt.Sprintf("topic=%s-part=%d", topic, partition))
		if err := b.EnsurePartition(topic, partition, partPath, segmentBytes); err != nil {
			return fmt.Errorf("node %d ensure partition: %w", cfg.id, err)
		}

		mux := http.NewServeMux()
		api.NewHTTP(b).Routes(mux)

		server := &http.Server{
			Addr:    cfg.addr,
			Handler: mux,
		}

		node := &brokerNode{
			id:      cfg.id,
			addr:    cfg.addr,
			dataDir: dataDir,
			baseURL: peers[cfg.id],
			broker:  b,
			server:  server,
		}

		node.start(&wg)
		nodes = append(nodes, node)
	}

	defer shutdownNodes(nodes, &wg)

	// Give the HTTP servers a moment to start.
	time.Sleep(250 * time.Millisecond)

	leader := nodes[0]
	if err := seedLeader(leader.broker, topic, partition, recordCount); err != nil {
		return fmt.Errorf("seed leader: %w", err)
	}
	log.Printf("seeded leader with %d random records", recordCount)

	for _, replica := range nodes[1:] {
		if err := waitForReplica(ctx, replica, topic, partition, recordCount); err != nil {
			return err
		}
	}

	followerURLs := make([]string, 0, len(nodes)-1)
	for _, replica := range nodes[1:] {
		followerURLs = append(followerURLs, replica.baseURL)
	}
	session, err := client.NewSession(client.SessionConfig{
		SessionID:    "example-session",
		LeaderURL:    leader.baseURL,
		FollowerURLs: followerURLs,
	})
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	consistencyPayload := []byte("consistency-check")
	latestOffset, err := session.Publish(topic, partition, consistencyPayload)
	if err != nil {
		return fmt.Errorf("session publish: %w", err)
	}
	records, lastSeen, err := session.Fetch(topic, partition, latestOffset, 0)
	if err != nil {
		return fmt.Errorf("session fetch: %w", err)
	}
	log.Printf("session read-your-writes verified at offset=%d lastSeen=%d records=%d", latestOffset, lastSeen, len(records))

	log.Println("cluster ready: press Ctrl+C to shut down")
	log.Printf("leader publish endpoint:   %s/topics/%s/partitions/%d", leader.baseURL, topic, partition)
	for _, replica := range nodes[1:] {
		log.Printf("replica fetch endpoint:   %s/topics/%s/partitions/%d/fetch?offset=0&maxBytes=0", replica.baseURL, topic, partition)
	}

	<-ctx.Done()
	return nil
}

func (n *brokerNode) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("starting broker %d on http://%s", n.id, n.addr)
		if err := n.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("broker %d server error: %v", n.id, err)
		}
	}()
}

func shutdownNodes(nodes []*brokerNode, wg *sync.WaitGroup) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for _, node := range nodes {
		if node == nil {
			continue
		}
		if node.server != nil {
			if err := node.server.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("broker %d shutdown error: %v", node.id, err)
			}
		}
		if node.broker != nil {
			if err := node.broker.Close(); err != nil {
				log.Printf("broker %d close error: %v", node.id, err)
			}
		}
	}

	wg.Wait()
}

func seedLeader(b *broker.Broker, topic string, partition, count int) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < count; i++ {
		payload := randomPayload(rng, i)
		if _, err := b.Publish(topic, partition, nil, payload); err != nil {
			return fmt.Errorf("publish record %d: %w", i, err)
		}
		if (i+1)%200 == 0 {
			log.Printf("seeded %d/%d records", i+1, count)
		}
	}
	return nil
}

func randomPayload(rng *rand.Rand, idx int) []byte {
	const payloadSize = 32
	buf := make([]byte, payloadSize)
	for i := range buf {
		buf[i] = letterBytes[rng.Intn(len(letterBytes))]
	}
	return []byte(fmt.Sprintf("msg-%04d payload=%s", idx, string(buf)))
}

func waitForReplica(parent context.Context, node *brokerNode, topic string, partition, expected int) error {
	waitCtx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("replica %d did not catch up: %w", node.id, waitCtx.Err())
		case <-ticker.C:
			records, _, err := node.broker.Fetch(topic, partition, 0, 0)
			if err != nil {
				if errors.Is(err, broker.ErrPartitionNotFound) {
					continue
				}
				return fmt.Errorf("replica %d fetch: %w", node.id, err)
			}
			if len(records) >= expected {
				log.Printf("replica %d caught up with %d records", node.id, len(records))
				return nil
			}
		}
	}
}
