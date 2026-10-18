package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"pulseorder/internal/domain"
)

func main() {
	baseURL := flag.String("url", "http://localhost:8080", "PulseOrder API Gateway base URL")
	concurrencyLevel := flag.Int("c", 20, "Number of concurrent load-generator worker goroutines")
	spikeOrders := flag.Int("orders", 110, "Number of distinct orders to fire during the 10x zone-downtown surge spike")
	captureProfile := flag.Bool("profile", true, "Capture a live CPU profile from /debug/pprof/profile to cpu.prof during the spike")
	flag.Parse()

	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 200,
		},
	}

	fmt.Println("================================================================================")
	fmt.Println("  PULSEORDER — 10x TRAFFIC SPIKE, SURGE, SAGA ROLLBACK & PPROF LOAD GENERATOR")
	fmt.Println("================================================================================")
	fmt.Printf("Target Gateway : %s\n", *baseURL)
	fmt.Printf("Concurrency    : %d goroutines\n", *concurrencyLevel)
	fmt.Printf("Spike Volume   : %d orders into zone-downtown\n\n", *spikeOrders)

	catalog := domain.DefaultCatalog()
	allSKUs := catalog.AllSKUs()

	var (
		status202Count     atomic.Int64
		status429Count     atomic.Int64
		idempotentReplays  atomic.Int64
		sagaRollbacks      atomic.Int64
		walletTransfersOK  atomic.Int64
		latenciesMu        sync.Mutex
		latencies          []time.Duration
	)

	recordLatency := func(d time.Duration) {
		latenciesMu.Lock()
		latencies = append(latencies, d)
		latenciesMu.Unlock()
	}

	// Optional: Capture live CPU profile in background during the traffic spike
	var profileWg sync.WaitGroup
	if *captureProfile {
		profileWg.Add(1)
		go func() {
			defer profileWg.Done()
			profClient := &http.Client{Timeout: 15 * time.Second}
			resp, err := profClient.Get(*baseURL + "/debug/pprof/profile?seconds=2")
			if err != nil {
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode == http.StatusOK {
				if out, createErr := os.Create("cpu.prof"); createErr == nil {
					_, _ = io.Copy(out, resp.Body)
					_ = out.Close()
				}
			}
		}()
	}

	startOverall := time.Now()

	// -------------------------------------------------------------------------
	// PHASE 1: 10x Traffic Spike in `zone-downtown` (1.0x -> 1.3x -> 1.6x -> 2.0x)
	// -------------------------------------------------------------------------
	fmt.Println("[Phase 1/5] Firing 10x Traffic Spike into `zone-downtown` to trigger 2.0x Surge...")
	jobsCh := make(chan int, *spikeOrders)
	for i := 1; i <= *spikeOrders; i++ {
		jobsCh <- i
	}
	close(jobsCh)

	var phase1Wg sync.WaitGroup
	for w := 0; w < *concurrencyLevel; w++ {
		phase1Wg.Add(1)
		go func() {
			defer phase1Wg.Done()
			for seq := range jobsCh {
				skuItem := allSKUs[seq%len(allSKUs)]
				userID := fmt.Sprintf("user-spike-%04d", seq)
				idemKey := fmt.Sprintf("spike-idem-%d-%04d", startOverall.UnixNano(), seq)
				isVIP := seq%5 == 0

				payload, _ := json.Marshal(map[string]any{
					"user_id":     userID,
					"zone_id":     "zone-downtown",
					"items":       []map[string]any{{"sku": skuItem.SKU, "quantity": 1}},
					"promo_code":  "SAVE10",
					"is_priority": isVIP,
				})

				reqStart := time.Now()
				req, _ := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/orders", bytes.NewReader(payload))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", idemKey)
				req.Header.Set("X-User-ID", userID)

				resp, err := client.Do(req)
				recordLatency(time.Since(reqStart))
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				if resp.StatusCode == http.StatusAccepted {
					status202Count.Add(1)
				} else if resp.StatusCode == http.StatusTooManyRequests {
					status429Count.Add(1)
				}
			}
		}()
	}
	phase1Wg.Wait()

	// -------------------------------------------------------------------------
	// PHASE 2: Sliding-Window Rate Limiter Burst (`429 Too Many Requests`)
	// -------------------------------------------------------------------------
	fmt.Println("[Phase 2/5] Simulating noisy client burst (25 reqs from `user-flood-bot` -> HTTP 429)...")
	var phase2Wg sync.WaitGroup
	for i := 1; i <= 25; i++ {
		phase2Wg.Add(1)
		go func(seq int) {
			defer phase2Wg.Done()
			payload := `{"user_id":"user-flood-bot","zone_id":"zone-midtown","items":[{"sku":"FOOD-MATCHA-LATTE","quantity":1}]}`
			reqStart := time.Now()
			req, _ := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/orders", bytes.NewBufferString(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", fmt.Sprintf("flood-idem-%d-%d", startOverall.UnixNano(), seq))
			req.Header.Set("X-User-ID", "user-flood-bot")

			resp, err := client.Do(req)
			recordLatency(time.Since(reqStart))
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			switch resp.StatusCode {
			case http.StatusAccepted:
				status202Count.Add(1)
			case http.StatusTooManyRequests:
				status429Count.Add(1)
			}
		}(i)
	}
	phase2Wg.Wait()

	// -------------------------------------------------------------------------
	// PHASE 3: Idempotency-Key Duplicate Retry Verification
	// -------------------------------------------------------------------------
	fmt.Println("[Phase 3/5] Verifying Redis Idempotency-Key Guard (duplicate retries -> cached 202)...")
	sharedIdemKey := fmt.Sprintf("idemp-verify-%d", startOverall.UnixNano())
	for retry := 0; retry < 4; retry++ {
		payload := `{"user_id":"user-idemp-tester","zone_id":"zone-uptown","items":[{"sku":"GROC-ESPRESSO-BEANS","quantity":2}]}`
		req, _ := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/orders", bytes.NewBufferString(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", sharedIdemKey)
		req.Header.Set("X-User-ID", "user-idemp-tester")

		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.Header.Get("X-Idempotency-Replayed") == "true" {
			idempotentReplays.Add(1)
		}
	}

	// -------------------------------------------------------------------------
	// PHASE 4: Saga Payment Failure & Compensating Inventory Rollback
	// -------------------------------------------------------------------------
	fmt.Println("[Phase 4/5] Injecting `FAIL_PAYMENT` orders to trigger Saga stock compensation...")
	var rollbackOrderIDs []string
	for i := 1; i <= 8; i++ {
		userID := fmt.Sprintf("user-saga-fail-%02d", i)
		payload, _ := json.Marshal(map[string]any{
			"user_id":     userID,
			"zone_id":     "zone-westside",
			"items":       []map[string]any{{"sku": "GAME-PS5-CTRL", "quantity": 2}},
			"promo_code":  "FAIL_PAYMENT",
			"is_priority": true,
		})
		req, _ := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/orders", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("saga-fail-%d-%d", startOverall.UnixNano(), i))
		req.Header.Set("X-User-ID", userID)

		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var body struct {
			OrderID string `json:"order_id"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		_ = resp.Body.Close()
		if body.OrderID != "" {
			status202Count.Add(1)
			rollbackOrderIDs = append(rollbackOrderIDs, body.OrderID)
		}
	}

	// -------------------------------------------------------------------------
	// PHASE 5: Deadlock-Free Bidirectional Wallet Transfers
	// -------------------------------------------------------------------------
	fmt.Println("[Phase 5/5] Executing concurrent bidirectional wallet transfers (user-alice <-> user-bob)...")
	var phase5Wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		phase5Wg.Add(2)
		go func(seq int) {
			defer phase5Wg.Done()
			p := `{"from_user_id":"user-alice","to_user_id":"user-bob","amount":10.00}`
			req, _ := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/wallets/transfer", bytes.NewBufferString(p))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-User-ID", fmt.Sprintf("wallet-worker-a-%d", seq))
			if resp, err := client.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					walletTransfersOK.Add(1)
				}
			}
		}(i)
		go func(seq int) {
			defer phase5Wg.Done()
			p := `{"from_user_id":"user-bob","to_user_id":"user-alice","amount":10.00}`
			req, _ := http.NewRequest(http.MethodPost, *baseURL+"/api/v1/wallets/transfer", bytes.NewBufferString(p))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-User-ID", fmt.Sprintf("wallet-worker-b-%d", seq))
			if resp, err := client.Do(req); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					walletTransfersOK.Add(1)
				}
			}
		}(i)
	}
	phase5Wg.Wait()

	// Brief pause to allow async Saga workers to finalize compensation status
	time.Sleep(150 * time.Millisecond)
	for _, oid := range rollbackOrderIDs {
		resp, err := client.Get(*baseURL + "/api/v1/orders/" + oid)
		if err != nil {
			continue
		}
		var ord struct {
			Status string `json:"status"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&ord)
		_ = resp.Body.Close()
		if ord.Status == string(domain.StatusFailedCompensated) {
			sagaRollbacks.Add(1)
		}
	}

	// Query live Surge Multiplier in `zone-downtown`
	var downtownSurge float64 = 1.0
	if resp, err := client.Get(*baseURL + "/api/v1/surge/zone-downtown"); err == nil {
		var sBody struct {
			SurgeMultiplier float64 `json:"surge_multiplier"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&sBody)
		_ = resp.Body.Close()
		if sBody.SurgeMultiplier > 0 {
			downtownSurge = sBody.SurgeMultiplier
		}
	}

	profileWg.Wait()
	totalElapsed := time.Since(startOverall)

	p50, p95, p99 := computePercentiles(latencies)

	fmt.Println("\n================================================================================")
	fmt.Println("                      PULSEORDER LOAD TEST SUMMARY REPORT                       ")
	fmt.Println("================================================================================")
	fmt.Printf("Total Duration                  : %v\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf("HTTP 202 Accepted (Orders)      : %d\n", status202Count.Load())
	fmt.Printf("HTTP 429 Rate Limited (Blocked) : %d\n", status429Count.Load())
	fmt.Printf("Idempotent Replays Verified     : %d\n", idempotentReplays.Load())
	fmt.Printf("Saga Rollbacks (Compensated)    : %d / %d\n", sagaRollbacks.Load(), len(rollbackOrderIDs))
	fmt.Printf("Deadlock-Free Wallet Transfers  : %d\n", walletTransfersOK.Load())
	fmt.Printf("Live `zone-downtown` Surge      : %.2fx (threshold >100 orders => 2.00x)\n", downtownSurge)
	fmt.Printf("Latency Percentiles             : p50=%v | p95=%v | p99=%v\n", p50, p95, p99)
	if _, err := os.Stat("cpu.prof"); err == nil {
		fmt.Printf("CPU Flame Graph Profile Saved   : cpu.prof (inspect via `go tool pprof -http=:8082 cpu.prof`)\n")
	}
	fmt.Println("================================================================================")
}

func computePercentiles(samples []time.Duration) (p50, p95, p99 time.Duration) {
	if len(samples) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := func(pct float64) int {
		pos := int(float64(len(sorted)-1) * pct)
		if pos < 0 {
			return 0
		}
		return pos
	}
	return sorted[idx(0.50)].Round(100 * time.Microsecond),
		sorted[idx(0.95)].Round(100 * time.Microsecond),
		sorted[idx(0.99)].Round(100 * time.Microsecond)
}

