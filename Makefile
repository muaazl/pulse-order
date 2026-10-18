.PHONY: test test-race up down run-api run-worker load-test profile-cpu profile-heap clean

# Run all unit tests with verbose output
test:
	go test -v ./internal/...

# Run unit tests with Go's race detector enabled
test-race:
	go test -race -v ./internal/...

# Start full Docker infrastructure (Redis, Kafka KRaft, Elasticsearch, Flink, Prometheus, Grafana)
up:
	docker compose up -d

# Tear down Docker infrastructure and volumes
down:
	docker compose down -v

# Start the HTTP API Gateway service (cmd/api)
run-api:
	go run ./cmd/api/main.go

# Start the Kafka Saga Orchestrator & Elasticsearch Indexer worker (cmd/worker)
run-worker:
	go run ./cmd/worker/main.go

# Run the concurrent traffic & surge spike load generator (cmd/loadgen)
load-test:
	go run ./cmd/loadgen/main.go

# Capture a 30-second live CPU profile from the API server and launch interactive pprof web UI
profile-cpu:
	go tool pprof -http=:8082 "http://localhost:8080/debug/pprof/profile?seconds=30"

# Capture a live Heap memory allocation profile from the API server and launch interactive pprof web UI
profile-heap:
	go tool pprof -http=:8082 "http://localhost:8080/debug/pprof/heap"

# Clean build artifacts
clean:
	go clean -testcache
