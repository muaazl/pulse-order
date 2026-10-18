# PulseOrder — Real-Time Order Ingestion, Saga Orchestration & Surge-Demand Pipeline

**PulseOrder** is an industry-grade, event-driven marketplace backend built with **Go (Golang), Redis, Apache Kafka (KRaft), Apache Flink, Elasticsearch, Prometheus, and Grafana**.

It solves the three hardest failure modes of high-concurrency commerce backends during flash-sale traffic spikes:
1. **Downstream Database Exhaustion & Duplicate Retries:** Shielded at the edge via a **Redis Speed Layer** (Single-script Lua Sliding-Window Rate Limiter returning `429 Too Many Requests`, Atomic `SET NX EX` Idempotency-Key Guard, and `singleflight` Cache Stampede protection).
2. **Distributed Multi-Service Transaction Locks:** Decoupled via **Apache Kafka** (Partitioned by `user_id` and `zone_id` across priority queues) and coordinated by an **Orchestration-based Saga Worker** with automatic reverse-order **Compensating Transactions** (restoring reserved SKU inventory when payment declines).
3. **Static Pricing Under Localized Demand Surges:** Computed in real time via a **5-Minute Sliding Window (hopping every 30 seconds)** in **Apache Flink** (plus a zero-JVM Go fallback engine) and indexed into **Elasticsearch** for sub-50ms faceted search and aggregations.

---

## 1. System Architecture & Data Flow

```text
[Client / Load Tester (cmd/loadgen)]
       │
       ▼ (HTTP POST /api/v1/orders + Idempotency-Key)
┌─────────────────────────────────────────────────────────────┐
│ Go API Gateway Service (cmd/api)                            │
│  ├─ Middleware 1: Redis Sliding-Window Rate Limiter (429)   │
│  ├─ Middleware 2: Redis Idempotency-Key Guard (SET NX EX)   │
│  ├─ Domain: OrderBuilder (Builder Pattern + Catalog Lookup) │
│  ├─ Pricing: Fetches live zone multiplier (Singleflight)    │
│  └─ Profiling & Metrics: /metrics (Prometheus) & /debug/pprof│
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼ (Publishes OrderAccepted — Key: user_id / zone_id)
┌─────────────────────────────────────────────────────────────┐
│ Apache Kafka Broker (KRaft Mode — 3 Partitions per Topic)   │
│  ├─ Topic: orders.priority.high (VIP / Priority Orders)     │
│  ├─ Topic: orders.standard      (Regular Orders)            │
│  └─ Topic: orders.events        (All State Transitions)     │
└──────┬───────────────────────────┬──────────────────────────┘
       │                           │
       ▼ (Weighted Priority Drain) ▼ (Stream Ingestion)
┌──────────────────────────┐  ┌───────────────────────────────┐
│ Go Saga Worker (cmd/worker)│ │ Apache Flink / Go Window Eng  │
│  ├─ Step 1: Reserve Stock│  │  ├─ 5-Min Sliding Window      │
│  ├─ Step 2: Charge Paymt │  │  ├─ 30-Sec Hop Slide          │
│  ├─ Step 3: Confirm Order│  │  └─ Writes Surge Multiplier ──┼─► [Redis Cache]
│  └─ Compensations:       │  └───────────────────────────────┘
│     Refund + Restore Stk │
└──────┬───────────────────┘
       │ (Publishes OrderFinalizedEvent)
       ▼
┌─────────────────────────────────────────────────────────────┐
│ Elasticsearch Indexer Worker -> [Elasticsearch: pulse_orders]│
└─────────────────────────────────────────────────────────────┘
```

---

## 2. CTO & System Design Interview Cheat Sheet (Mapped to Code)

| Engineering Concept | Architectural Problem Solved | Exact File & Symbol Reference |
| :--- | :--- | :--- |
| **HTTP Status Code Precision** | Distinguishing sync reads (`200`), sync creation (`201`), async ingestion (`202` + `status_url`), validation errors (`400`), authentication (`401`) vs. authorization (`403`), and backpressure (`429`). | [`cmd/api/main.go`](file:///d:/GitHub/pulse-order/cmd/api/main.go) (`handleCreateOrder`, `handleCreateMerchant`), [`internal/middleware/auth.go`](file:///d:/GitHub/pulse-order/internal/middleware/auth.go) |
| **Sliding-Window Rate Limiter (Redis Lua)** | Fixed windows allow 2x boundary bursts; multi-step Go commands suffer from TOCTOU races. A single Lua script executes `ZREMRANGEBYSCORE -> ZCARD -> ZADD -> PEXPIRE` atomically and computes `Retry-After`. | [`internal/middleware/ratelimit.go`](file:///d:/GitHub/pulse-order/internal/middleware/ratelimit.go) (`slidingWindowLua`, `SlidingWindowRateLimiter`) |
| **Idempotency-Key Guard (`SET NX EX`)** | Mobile network retries on dropped connections cause double-charging. Atomic `SET idemp:{key} "IN_PROGRESS" NX EX 300` locks in-flight execution and replays cached `202 Accepted` responses (`X-Idempotency-Replayed: true`). | [`internal/middleware/idempotency.go`](file:///d:/GitHub/pulse-order/internal/middleware/idempotency.go) (`IdempotencyGuard`) |
| **Cache Stampede Prevention (`singleflight`)** | When `surge:zone:{zone_id}` expires during a flash sale, 1,000 concurrent goroutines miss Redis simultaneously. `singleflight.Group.Do` coalesces all in-flight misses into **1** fallback execution. | [`internal/cache/surge_cache.go`](file:///d:/GitHub/pulse-order/internal/cache/surge_cache.go) (`SurgeCache.GetZoneMultiplier`) |
| **Go Encapsulation & Defensive Copies** | Unexported fields on `Order` (`id`, `totalAmount`, `status`, `items`) prevent external packages from mutating state or bypassing invariants. `Items()` returns a defensive slice copy; custom `MarshalJSON` handles wire serialization. | [`internal/domain/order.go`](file:///d:/GitHub/pulse-order/internal/domain/order.go) (`Order`, `AuditMetadata`) |
| **Composition Over Inheritance** | Go embeds `AuditMetadata` (`CreatedAt`, `UpdatedAt`, `Version`) into `Order` and `Merchant` for flat memory layout and uses small consumer-defined implicit interfaces. | [`internal/domain/order.go`](file:///d:/GitHub/pulse-order/internal/domain/order.go), [`internal/domain/interfaces.go`](file:///d:/GitHub/pulse-order/internal/domain/interfaces.go) |
| **Fluent Builder Pattern ("Parse, Don't Validate")** | Enforces all pricing, discount, surge, and catalog SKU invariants in `.Build()` before an `*Order` pointer can ever exist in memory. | [`internal/domain/order_builder.go`](file:///d:/GitHub/pulse-order/internal/domain/order_builder.go) (`OrderBuilder.Build`) |
| **Kafka Partition Keys (`user_id` vs. `zone_id`)** | Kafka guarantees ordering only within a partition. Work queues hash `user_id` for strict per-customer sequential processing; `orders.events` hashes `zone_id` to co-locate zone events for Flink windowing. | [`internal/messaging/kafka.go`](file:///d:/GitHub/pulse-order/internal/messaging/kafka.go) (`KafkaPublisher`) |
| **Priority Queuing via Two-Level `select`** | Standard Go `select` chooses ready channels uniformly at random (50/50). Nested non-blocking `select` with `default` drains `orders.priority.high` before `orders.standard`. | [`internal/messaging/priority_consumer.go`](file:///d:/GitHub/pulse-order/internal/messaging/priority_consumer.go) (`NextPriorityMessage`) |
| **Orchestration Saga & Compensations** | Avoids holding distributed 2PC locks across network payment calls. Commits local steps (`ReserveInventory` -> `ProcessPayment` -> `ConfirmOrder`) and walks backward (`CompensateInventoryStep`) on payment failure (`FAILED_COMPENSATED`). | [`internal/saga/order_saga.go`](file:///d:/GitHub/pulse-order/internal/saga/order_saga.go) (`OrderSagaOrchestrator.Execute`) |
| **Deadlock Prevention (Lock Ordering + Timeouts)** | Breaks the Circular Wait Coffman condition by sorting account IDs lexicographically (`user-alice` before `user-bob`) and enforces a `500ms` timeout via a 1-buffered channel mutex. | [`internal/concurrency/wallet_lock.go`](file:///d:/GitHub/pulse-order/internal/concurrency/wallet_lock.go) (`WalletManager.Transfer`) |
| **Real-Time Surge Pricing (5m Sliding Window / 30s Hop)** | Eliminates pricing cliffs at 5-minute tumbling window boundaries. Evaluates `[now - 5m, now]` every 30s (`<20: 1.0x`, `20–50: 1.3x`, `51–100: 1.6x`, `>100: 2.0x`). | [`flink-job/surge_window_job.py`](file:///d:/GitHub/pulse-order/flink-job/surge_window_job.py), [`internal/cache/surge_window.go`](file:///d:/GitHub/pulse-order/internal/cache/surge_window.go) |
| **CQRS Search & Aggregations (Elasticsearch)** | Inverted index full-text search + bitset filters + columnar Doc Values aggregations (`count_by_status`, `avg_surge_by_zone`) in `<50ms`. | [`internal/search/elasticsearch.go`](file:///d:/GitHub/pulse-order/internal/search/elasticsearch.go) (`BuildSearchDSL`, `Client.Search`) |
| **Observability (`Prometheus`, `Grafana`, `pprof`)** | RED metrics with normalized route labels to prevent cardinality explosion, auto-provisioned Grafana dashboard, and live 100Hz CPU & 512KB Heap `pprof` Flame Graphs. | [`internal/observability/metrics.go`](file:///d:/GitHub/pulse-order/internal/observability/metrics.go), [`internal/observability/pprof.go`](file:///d:/GitHub/pulse-order/internal/observability/pprof.go) |

---

## 3. Curated Marketplace Dataset

PulseOrder ships with an embedded dataset ([`internal/domain/catalog_data.json`](file:///d:/GitHub/pulse-order/internal/domain/catalog_data.json)) automatically seeded into Redis on startup:
- **6 Merchants across 6 Zones:** `zone-downtown`, `zone-midtown`, `zone-uptown`, `zone-westside`, `zone-eastside`, `zone-airport`
- **14 Realistic SKUs:** e.g., `TECH-MBP-14` (\$1,999.00, stock: 50), `TECH-AIRPODS-PRO` (\$249.00, stock: 200), `FOOD-TRUFFLE-BURGER` (\$22.50, stock: 300), `GAME-PS5-CTRL` (\$199.99, stock: 120), `TRVL-ANC-HEADPHONES` (\$348.00, stock: 100)
- **6 Pre-funded User Wallets:** `user-alice` (\$5,000), `user-bob` (\$5,000), `user-charlie` (\$3,500), `user-diana` (\$7,500), `user-vip-01` (\$25,000), `user-vip-02` (\$25,000)

---

## 4. Quick Start & Commands

### Run Unit & Concurrency Race Tests
```bash
go test -v ./...
go test -race -v ./internal/...
```

### Start Full Docker Infrastructure
```bash
# Starts Redis, Kafka (KRaft), Elasticsearch, Prometheus (:9090), and Grafana (:3000)
docker compose up -d

# Optional: Also start the JVM Apache Flink cluster (:8081)
docker compose --profile flink up -d
```

### Run Services & 10x Traffic Spike Load Generator
```bash
# Terminal 1: Start Go HTTP API Gateway (:8080)
go run ./cmd/api/main.go

# Terminal 2: Start Go Kafka Saga & Elasticsearch Indexer Worker
go run ./cmd/worker/main.go

# Terminal 3: Run 10x Traffic Spike, Rate-Limit, Saga Rollback & CPU Profile Generator
go run ./cmd/loadgen/main.go
```

### Inspect Live CPU & Heap Flame Graphs (`pprof`)
```bash
# Open interactive Flame Graph UI on http://localhost:8082 from captured cpu.prof
go tool pprof -http=:8082 cpu.prof

# Or sample live from the running API server:
make profile-cpu
make profile-heap
```

---

## 5. Milestone Progress

- [x] **Milestone 1:** Foundation, Domain Layer & Builder Pattern (`internal/domain/`, `internal/config/`, `Makefile`)
- [x] **Milestone 2:** Docker Infrastructure + Redis Speed Layer (`429` Rate Limiting, Idempotency, Curated Catalog Dataset, Deadlock-Free Concurrency)
- [x] **Milestone 3:** Kafka Event Backbone, Priority Queues & Saga Pattern Orchestrator (`internal/messaging/`, `internal/saga/`, `cmd/worker/`)
- [x] **Milestone 4:** Real-Time Surge Pricing (`flink-job/surge_window_job.py` + `internal/cache/surge_window.go`)
- [x] **Milestone 5:** Elasticsearch Indexing & Full-Text Order Search (`internal/search/`)
- [x] **Milestone 6:** Observability (`Prometheus`, `Grafana`, `pprof`) & Traffic Spike Load Generator (`internal/observability/`, `deploy/grafana/`, `cmd/loadgen/`)
