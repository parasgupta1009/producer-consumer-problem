# Worker Pool with Bounded Concurrency

A Go implementation of a thread-safe worker pool that runs at most **N tasks concurrently**, with futures, graceful/forced shutdown, and real-time metrics.

## Features

- **Bounded concurrency** — exactly N workers, never more
- **Future-based results** — `Submit(task) → Future`, retrieve results asynchronously
- **Backpressure** — configurable queue size; blocks producers when full
- **Graceful shutdown** — finish in-flight + queued tasks, reject new ones
- **Force shutdown** — cancel pending tasks immediately, return them to caller
- **Panic recovery** — panicking tasks don't crash workers; errors route to futures
- **Metrics** — active workers, queued, completed, failed (lock-free atomics)
- **Zero data races** — passes `go test -race` under high concurrency

## Installation

```bash
go get github.com/consumer/workerpool
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/consumer/workerpool"
)

func main() {
    // Create a pool with 4 workers and queue capacity of 100
    pool, _ := workerpool.New(workerpool.Config{
        Workers:   4,
        QueueSize: 100,
    })
    defer pool.Shutdown()

    // Submit a task
    future, _ := pool.Submit(func() (any, error) {
        return "hello from worker", nil
    })

    // Get the result (blocks until ready)
    result, err := future.Get()
    fmt.Println(result, err) // "hello from worker" <nil>
}
```

## API

### Create Pool

```go
pool, err := workerpool.New(workerpool.Config{
    Workers:   4,   // number of concurrent workers (must be >= 1)
    QueueSize: 100, // buffered queue capacity (0 = synchronous handoff)
})
```

### Submit Tasks

```go
future, err := pool.Submit(func() (any, error) {
    // your work here
    return result, nil
})
if err != nil {
    // pool is shut down
}
```

### Retrieve Results

```go
// Block until done
val, err := future.Get()

// Block with timeout
val, err, ok := future.GetWithTimeout(5 * time.Second)
if !ok {
    // timed out
}

// Non-blocking check
select {
case <-future.Done():
    val, err := future.Get()
default:
    // not ready yet
}
```

### Shutdown

```go
// Graceful: finish all in-flight and queued tasks, then stop
pool.Shutdown()

// Immediate: cancel pending tasks, return them
pending := pool.ShutdownNow()
fmt.Printf("cancelled %d tasks\n", len(pending))
```

### Metrics

```go
m := pool.Metrics()
fmt.Printf("Active: %d, Queued: %d, Completed: %d, Failed: %d\n",
    m.ActiveWorkers, m.Queued, m.Completed, m.Failed)
```

## Architecture

```
  Submitter           Pool                    Workers
  ─────────           ────                    ───────
                  ┌─────────────┐
  Submit() ────> │  Job Channel │ ────> Worker 1 ──> Future
  Submit() ────> │  (buffered)  │ ────> Worker 2 ──> Future
  Submit() ────> │              │ ────> Worker 3 ──> Future
                  └─────────────┘         ...
                        │            ────> Worker N ──> Future
                   close(chan)
                   on Shutdown
```

- **Fixed N goroutines** pull from a shared buffered channel
- Each task gets a dedicated result channel routed back to its Future
- Shutdown closes the channel; workers exit their `range` loop naturally
- RWMutex prevents send-on-closed-channel races during concurrent submit + shutdown

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Task returns error | `future.Get()` returns `(value, error)` |
| Task panics | Panic recovered, wrapped as error with stack trace |
| Submit after shutdown | Returns `(nil, ErrPoolShutdown)` |
| Task cancelled by ShutdownNow | `future.Get()` returns `(nil, ErrTaskCancelled)` |

## Testing

```bash
# Run all 52 tests
go test ./workerpool/... -v

# With race detector
go test ./workerpool/... -race

# Run E2E tests only
go test ./workerpool/... -run TestE2E -v

# Stress test (20 goroutines × 200 tasks with random panics)
go test ./workerpool/... -run TestStressRace -race -count=5

# Coverage
go test ./workerpool/... -coverprofile=coverage.out
go tool cover -func=coverage.out
```

### Test Coverage by Category

| Category | Tests | What's Verified |
|----------|-------|-----------------|
| Config validation | 4 | Invalid inputs rejected |
| Future | 7 | Blocking, timeout, idempotency, concurrency |
| Pool core | 6 | Result routing, queuing, bounded concurrency |
| Panic recovery | 5 | String/int/nil panics, worker survival |
| Graceful shutdown | 5 | Drain, reject new, idempotent |
| Force shutdown | 5 | Return pending, cancel futures |
| Metrics | 6 | Active/queued/completed/failed accuracy |
| Race conditions | 4 | Concurrent submitters, full queue, shutdown races |
| Edge cases | 7 | N=1, N=100, 10K rapid submits, timeouts |
| E2E integration | 2 | Full lifecycle, force shutdown flow |
| Stress | 1 | 4000 tasks with random panics + errors |

## Project Structure

```
workerpool/
├── errors.go      — Sentinel errors (ErrPoolShutdown, ErrTaskCancelled)
├── types.go       — Interfaces (WorkerPool, Future), structs (Config, PoolMetrics)
├── future.go      — Future implementation (async result with Done channel)
├── pool.go        — Pool implementation (workers, submit, shutdown)
├── pool_test.go   — 50 unit/integration tests
└── e2e_test.go    — 2 end-to-end lifecycle tests
```

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Channel-based fixed pool | Idiomatic Go, natural backpressure, simple shutdown |
| Atomic counters for metrics | Lock-free reads on hot path |
| RWMutex for submit/shutdown | Prevents send-on-closed-channel without blocking concurrent submits |
| sync.Once for shutdown | Idempotent, safe from multiple goroutines |
| Per-task result channel | 1:1 result routing, no shared state between tasks |
| Panic recovery inside loop | Worker survives, continues processing next task |

## License

MIT
