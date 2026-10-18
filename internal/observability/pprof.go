package observability

import (
	"net/http"
	"net/http/pprof"
	"runtime"
)

// ============================================================================
// ARCHITECTURAL TEACHING: GO RUNTIME PROFILING (`net/http/pprof`) & FLAME GRAPHS
// ============================================================================
// WHY EXPOSE `pprof` IN PRODUCTION GO SERVICES?
// Metrics (Prometheus) tell you *THAT* p99 latency spiked from 8ms to 250ms;
// Profiling (`pprof`) tells you *WHICH EXACT FUNCTION & LINE OF CODE* caused it.
//
// HOW `pprof` WORKS WITH <1% OVERHEAD:
//   1. CPU Profiling (`/debug/pprof/profile?seconds=30`):
//      When triggered, the Go runtime registers a 100Hz OS timer (`SIGPROF`).
//      100 times per second, it unwinds the program counters of active goroutines
//      into a ring buffer. When inactive, CPU profiling overhead is 0%.
//   2. Heap / Allocation Profiling (`/debug/pprof/heap` & `allocs`):
//      Controlled by `runtime.MemProfileRate` (default: 1 sample per 512KB of
//      heap allocations). Identifies objects escaping to the heap that cause
//      `go_gc_duration_seconds` Stop-The-World / mark-assist spikes.
//   3. Goroutine & Mutex Profiling (`/debug/pprof/goroutine` & `mutex`):
//      Dumps live goroutine call stacks to immediately spot leaked goroutines
//      or lock contention hot-spots.
//
// WHY REGISTER ON OUR EXPLICIT `http.ServeMux`?
// Importing `_ "net/http/pprof"` only registers routes on `http.DefaultServeMux`.
// Because production servers use an isolated `http.NewServeMux()` rather than
// the global `DefaultServeMux`, we explicitly mount the `pprof` handlers here.

// RegisterPprofRoutes attaches all standard `net/http/pprof` endpoints onto `mux`.
func RegisterPprofRoutes(mux *http.ServeMux) {
	// Enable mutex and block profiling sampling for contention visibility
	runtime.SetMutexProfileFraction(5)
	runtime.SetBlockProfileRate(1000)

	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	// Named runtime profiles
	mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("GET /debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("GET /debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("GET /debug/pprof/block", pprof.Handler("block"))
	mux.Handle("GET /debug/pprof/mutex", pprof.Handler("mutex"))
	mux.Handle("GET /debug/pprof/threadcreate", pprof.Handler("threadcreate"))
}

