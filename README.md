# PulseOrder — Real-Time Order Ingestion, Saga Orchestration & Surge-Demand Pipeline

**PulseOrder** is a high-concurrency, event-driven marketplace backend built with **Go (Golang), Redis, Apache Kafka (KRaft), Apache Flink, Elasticsearch, Prometheus, and Grafana**.

## System Architecture

```text
[Client / Load Tester]
       │
       ▼ (HTTP POST /api/v1/orders + Idempotency-Key)
┌─────────────────────────────────────────────────────────────┐
│ Go API Gateway Service (cmd/api)                            │
│  ├─ Middleware 1: Redis Sliding-Window Rate Limiter (429)   │
│  ├─ Middleware 2: Redis Idempotency-Key Guard               │
│  ├─ Domain: OrderBuilder (Builder Pattern + Validation)     │
│  ├─ Pricing: Fetches live zone multiplier from Redis        │
│  └─ Profiling & Metrics: /metrics (Prometheus) & /debug/pprof│
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼ (Publishes OrderCreatedEvent — Partition Key: user_id / zone_id)
┌─────────────────────────────────────────────────────────────┐
│ Apache Kafka Broker                                         │
│  ├─ Topic: orders.priority.high (VIP / Priority Orders)     │
│  ├─ Topic: orders.standard      (Regular Orders)            │
│  └─ Topic: orders.events        (All State Transitions)     │
└──────┬───────────────────────────┬──────────────────────────┘
       │                           │
       ▼ (Consumer Group: saga)    ▼ (Stream Ingestion)
┌──────────────────────────┐  ┌───────────────────────────────┐
│ Go Saga Worker (cmd/worker)│ │ Apache Flink Stream Processor │
│  ├─ Step 1: Reserve Stock│  │  ├─ 5-Min Sliding Window      │
│  ├─ Step 2: Charge Paymt │  │  ├─ Aggregates orders/zone    │
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

## Milestone Progress

- [x] **Milestone 1:** Foundation, Domain Layer & Builder Pattern (`internal/domain/`, `internal/config/`, `Makefile`)
- [ ] **Milestone 2:** Docker Infrastructure + Redis Speed Layer (`429` Rate Limiting, Idempotency, Deadlock-Free Concurrency)
- [ ] **Milestone 3:** Kafka Event Backbone, Priority Queues & Saga Pattern Orchestrator
- [ ] **Milestone 4:** Real-Time Surge Pricing (Apache Flink Sliding Windows + Go Fallback)
- [ ] **Milestone 5:** Elasticsearch Indexing & Full-Text Order Search
- [ ] **Milestone 6:** Observability (`Prometheus`, `Grafana`, `pprof`) & Traffic Spike Load Generator
