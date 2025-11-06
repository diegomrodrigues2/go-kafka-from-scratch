# Go Kafka From Scratch

A simplified Kafka-like message broker implementation in Go, demonstrating core distributed log concepts—append-only logs, offset-based consumption, segment-based storage—and now single-leader replication with follower catch-up.

## Architecture Overview

This implementation follows a **layered architecture** with clear separation of concerns:

```mermaid
graph TB
    subgraph "Client Layer"
        Producer[Producer Client]
        Consumer[Consumer Client]
    end

    subgraph "API Layer"
        HTTP[HTTP REST API]
    end

    subgraph "Broker Layer"
        Broker[Broker<br/>Topic/Partition Manager]
    end

    subgraph "Storage Layer"
        Log[Log<br/>Multi-Segment Manager]
        Segment[Segment<br/>File Pair Manager]
        Index[Index<br/>Offset→Position Map]
    end

    Producer -->|"POST /topics/{topic}/partitions/{p}"| HTTP
    Consumer -->|"GET /topics/{topic}/partitions/{p}/fetch"| HTTP
    HTTP --> Broker
    Broker -->|"async fetch"| Follower[Follower Replicator]
    Broker --> Log
    Log --> Segment
    Segment --> Index
```

## Core Modules

### 1. Broker (`internal/broker/`)
Manages multiple topics and partitions, routing requests to appropriate logs and coordinating replication when cluster metadata is configured.

**Key Operations:**
- `EnsurePartition()` - Create/ensure partition exists
- `Publish()` - Append messages to partition
- `Fetch()` - Read messages from offset
- `Topics()` - List available topics
- `PartitionLeader()` - Discover the leader for a topic-partition

### 2. LogStore (`internal/logstore/`)
Storage engine with three components:

- **Log** (`log.go`) - Manages segment sequence, handles rotation
- **Segment** (`segment.go`) - Fixed-size file pair (.log + .index)
- **Index** (`index.go`) - Maps offsets to file positions

### 3. API Layer (`internal/api/`)
HTTP REST API exposing broker operations and enforcing consistency hints (for example, `X-Read-Min-Offset` to divert lagging followers).

```
POST   /topics/{topic}/partitions/{p}           ? Publish message
GET    /topics/{topic}/partitions/{p}/fetch     ? Fetch messages (?minOffset= / X-Read-Min-Offset supported)
```

### 4. Client Libraries (`pkg/client/`)
High-level APIs for producers and consumers, plus session helpers that remember per-topic offsets to deliver read-your-writes and monotonic reads across replicas.

### 5. Replication (`internal/broker/replication.go`)
Background worker that keeps follower replicas in sync by polling the leader via the HTTP fetch API and appending records locally using the existing logstore.

## Data Flow Diagrams

### Message Publishing Flow (Leader)

```mermaid
sequenceDiagram
    participant P as Producer
    participant API as HTTP API
    participant B as Broker
    participant L as Log
    participant S as Segment
    participant I as Index

    P->>API: POST /topics/demo/partitions/0
    API->>B: Publish(topic, partition, message)
    B->>L: Append(message)
    L->>S: Append(message)
    S->>S: Write size header (4 bytes)
    S->>S: Write message payload
    S->>I: Add(offset, position)
    I-->>S: Index updated
    S-->>L: Offset assigned
    L-->>B: Offset returned
    B-->>API: Offset returned
    API-->>P: {"offset": 42}
```

### Replication Flow (Follower)

```mermaid
sequenceDiagram
    participant F as Follower Broker
    participant R as Replicator Goroutine
    participant L as Leader Broker
    participant LL as Leader Log
    participant FL as Follower Log

    loop every fetch interval
        R->>L: GET /topics/demo/partitions/0/fetch?offset=n
        L->>LL: ReadFrom(n)
        LL-->>L: records, lastOffset
        L-->>R: JSON payload
        R->>FL: Append(record)
        FL-->>R: offset assigned
        R->>R: advance next offset
    end
```

### Message Consumption Flow

```mermaid
sequenceDiagram
    participant C as Consumer
    participant API as HTTP API
    participant B as Broker
    participant L as Log
    participant S as Segment
    participant I as Index

    C->>API: GET /fetch?offset=10&maxBytes=1024
    API->>B: Fetch(topic, partition, offset, maxBytes)
    B->>L: ReadFrom(offset, maxBytes)
    L->>L: Find segment containing offset
    L->>S: ReadFrom(offset, maxBytes)
    S->>I: Get position for offset
    I-->>S: File position returned
    S->>S: Read records from position
    S-->>L: Records + lastOffset
    L-->>B: Records batch
    B-->>API: Records batch
    API-->>C: {"records": [...], "toOffset": 15}
```

### Segment Rotation Flow

```mermaid
stateDiagram-v2
    [*] --> ActiveSegment: Segment created
    ActiveSegment --> CheckSize: Append message
    CheckSize --> AppendOK: Size < MaxBytes
    CheckSize --> CreateNew: Size >= MaxBytes
    AppendOK --> ActiveSegment
    CreateNew --> NewSegment: Calculate nextOffset
    NewSegment --> WriteFiles: Create .log + .index
    WriteFiles --> RetryAppend: Files created
    RetryAppend --> ActiveSegment: Message appended
```

## Storage Architecture

### Directory Structure

```
{DATA_DIR}/
└── topic={topic}-part={partition}/
    ├── 00000000000000000000.log     # Segment data file
    ├── 00000000000000000000.index   # Segment index file
    ├── 00000000000000000100.log     # Next segment
    ├── 00000000000000000100.index
    └── ...
```

### File Formats

```mermaid
graph 
    subgraph "Data File (.log)"
        R1[Record 1<br/>Size: 4B<br/>Payload: NB]
        R2[Record 2<br/>Size: 4B<br/>Payload: NB]
        R3[...]
        R1 --> R2 --> R3
    end

    subgraph "Index File (.index)"
        E1[Entry 1<br/>RelOffset: 4B<br/>Position: 4B]
        E2[Entry 2<br/>RelOffset: 4B<br/>Position: 4B]
        E3[...]
        E1 --> E2 --> E3
    end
```

**Data File:** Append-only sequence of records (4-byte size + payload)
**Index File:** Offset-to-position mappings (8 bytes per entry)

## Component Interactions

```mermaid
graph LR
    subgraph "Broker Components"
        direction TB
        B[Broker]
        TM[Topics Map<br/>map string → Partitions]
        PM[Partitions Map<br/>map int → Log]

        B --> TM
        TM --> PM
    end

    subgraph "Log Components"
        direction TB
        L[Log]
        SA[Segments Array<br/>Segment]

        L --> SA
    end

    subgraph "Segment Components"
        direction TB
        S[Segment]
        DF[Data File<br/>.log]
        IF[Index File<br/>.index]

        S --> DF
        S --> IF
    end

    PM --> L
    SA --> S
```

## Running the Broker

### Configuration (Environment Variables)

```bash
DATA_DIR=./data    # Storage directory
TOPIC=demo         # Initial topic to create
ADDR=:8080         # HTTP listen address
```

### Start Server

```bash
# Using Go
go run cmd/broker/main.go

# Using Docker
docker build -t kafka-broker .
docker run -p 8080:8080 -v $(pwd)/data:/data kafka-broker
```

## Usage Examples

### Publishing Messages

```bash
# Publish a message
curl -X POST http://localhost:8080/topics/demo/partitions/0 \
     -H "Content-Type: application/octet-stream" \
     -d "Hello Kafka"

# Response: {"offset": 0}
```

### Consuming Messages

```bash
# Fetch messages from offset
curl "http://localhost:8080/topics/demo/partitions/0/fetch?offset=0&maxBytes=1024"

# Response: {"fromOffset": 0, "toOffset": 1, "records": ["SGVsbG8gS2Fma2E="]}
```

### Using Client Libraries

```go
import "github.com/yourusername/go-kafka-from-scratch/pkg/client"

// Producer
producer := client.Producer{BaseURL: "http://localhost:8080"}
offset, err := producer.Publish("demo", 0, []byte("Hello"))

// Consumer
consumer := client.Consumer{BaseURL: "http://localhost:8080"}
records, lastOffset, err := consumer.Fetch("demo", 0, 0, 1024)
```

### Consistency-Aware Sessions

```go
session, err := client.NewSession(client.SessionConfig{
    SessionID:    "user-123",                     // stick reads to the same follower
    LeaderURL:    "http://leader:8080",           // leader handles writes and lag-free reads
    FollowerURLs: []string{"http://follower:8081"},
})
if err != nil {
    log.Fatal(err)
}

off, _ := session.Publish("demo", 0, []byte("hello"))
records, last, _ := session.Fetch("demo", 0, off, 0) // guaranteed to see the write
```

The session automatically adds `minOffset`/`X-Read-Min-Offset` when reading from followers and falls back to the leader if a replica is behind, ensuring read-your-writes and monotonic reads.


### Running a Follower Replica (Programmatic Example)

Replication is activated when the broker is supplied with cluster metadata describing leaders, peers and replica sets. While the CLI entry point (`cmd/broker/main.go`) runs a standalone node, you can wire followers programmatically:

```go
cluster := &broker.ClusterConfig{
    BrokerID: 2, // this broker id
    Peers: map[int]string{
        1: "http://localhost:8080", // leader base URL
    },
    Partitions: map[string]map[int]broker.PartitionAssignment{
        "demo": {
            0: {Leader: 1, Replicas: []int{1, 2}},
        },
    },
}
follower := broker.NewBroker(broker.Config{Cluster: cluster})
follower.SetHTTPClient(&http.Client{Timeout: time.Second}) // optional custom client
_ = follower.EnsurePartition("demo", 0, "./data/topic=demo-part=0", 128<<20)
```

Once configured, the follower will continuously poll the leader’s fetch endpoint and mirror the log locally. Writes issued to a follower via the HTTP API are rejected with a redirect to the leader.

## Design Decisions

### Strengths
- Simple, understandable codebase
- Clean separation of concerns
- Excellent testability (100% test coverage)
- Segment-based storage (scalable)
- Asynchronous single-leader replication for higher availability
- Optional client sessions offer read-your-writes and monotonic reads
- Standard Go project layout

### Limitations
- Static cluster assignments (no controller or dynamic rebalancing)
- No automatic leader failover or election
- Client-managed consistency (sessions must steer reads to suitable replicas)
- No consumer groups or offset commits
- No retention policies or compaction
- No authentication/authorization

## Project Structure

```
go-kafka-from-scratch/
├── cmd/broker/          # Application entry point
├── internal/            # Internal packages
│   ├── api/             # HTTP REST endpoints
│   ├── broker/          # Topic/partition management
│   ├── logstore/        # Storage engine (log/segment/index)
│   └── types/           # Shared data structures
├── pkg/client/          # Public client libraries
└── test/                # Integration tests
```

## Testing

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Integration tests
go test ./test/...
```

## Development

```bash
# Build
make build

# Run tests
make test

# Docker build
make docker-build
```

## Key Concepts Demonstrated

1. **Append-Only Logs** - Messages are never modified, only appended
2. **Offset-Based Consumption** - Each message has a sequential offset
3. **Segment-Based Storage** - Logs split into fixed-size segments for efficiency
4. **Index Files** - Enable O(1) offset-to-position lookups
5. **Concurrent Access Control** - Read-write mutexes for thread safety

## Use Cases

- Educational purposes (learning Kafka internals)
- Single-node message queue
- Development/testing environments
- Prototype/proof-of-concept systems

## License

See LICENSE file for details.
