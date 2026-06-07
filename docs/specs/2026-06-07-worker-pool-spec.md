---
title: "Track E: Worker Pool with Bounded Concurrency" Technical Specification
author: Paras Gupta
date: 2026-06-07
status: Draft
reviewers: []
---

# Track E: Worker Pool with Bounded Concurrency

## Executive Summary

This specification defines a bounded-concurrency worker pool in Go that accepts tasks via `Submit(task) -> Future`, guarantees at most N tasks execute simultaneously, provides graceful and immediate shutdown semantics, and exposes real-time metrics. The design uses a channel-based fixed worker pool (Approach 1) for its simplicity, idiomatic Go patterns, and predictable resource behavior.

---

## 1. Project Overview & Problem Explanation

### 1.1 What is a Worker Pool?

A worker pool is a concurrency pattern that maintains a fixed set of worker goroutines to process tasks from a shared queue. Instead of spawning an unbounded number of goroutines (one per task), the pool constrains execution to at most N concurrent tasks, queuing excess work until a worker becomes available.

The pattern consists of three core components:
- **Submitter**: The caller that produces tasks and receives a future (handle) to retrieve results later.
- **Queue**: A buffer holding tasks awaiting execution (implemented as a Go channel).
- **Workers**: A fixed set of N goroutines that pull tasks from the queue, execute them, and route results back through futures.

### 1.2 Why Bounded Concurrency Matters

**Resource Exhaustion Prevention**

Unbounded goroutine creation can overwhelm system resources. Each goroutine consumes ~2-8KB of stack memory (growing as needed). Spawning 1M goroutines for 1M tasks consumes gigabytes of memory and causes severe GC pressure. A bounded pool caps memory usage at O(N) regardless of task volume.

**Backpressure**

When downstream systems (databases, APIs, file systems) have finite capacity, unbounded concurrency creates thundering herd problems. A worker pool naturally applies backpressure: when all N workers are busy, new submissions block (or queue up to a bounded limit), signaling the producer to slow down.

**Predictability**

Fixed concurrency makes system behavior deterministic under load. Operations teams can reason about resource usage, set appropriate limits for database connection pools, and capacity-plan accurately. Bursty workloads are smoothed into predictable throughput.

**Fairness and Ordering**

A shared queue with FIFO semantics ensures tasks are processed in submission order (within the constraints of concurrent execution), preventing starvation.

### 1.3 Goals

- Provide a reusable, type-safe worker pool with bounded concurrency (at most N tasks running simultaneously).
- Expose a `Future` abstraction for async result retrieval with timeout support.
- Support graceful shutdown (drain in-flight) and immediate shutdown (cancel pending).
- Expose real-time metrics: active workers, queued tasks, completed count, failed count.
- Achieve zero data races under `go test -race`.

### 1.4 Non-Goals

- Dynamic resizing of the worker pool at runtime.
- Task prioritization or priority queues.
- Distributed worker pool across multiple processes/nodes.
- Retry logic or circuit breaking (those belong in the task itself).
- Generic type parameters (uses `interface{}` for broad compatibility; generics can be layered later).

---

## 2. Three Approaches with Tradeoffs

### Approach 1: Channel-based Fixed Worker Pool (Classic Go)

**Design:** Spawn exactly N goroutines at pool creation time. Each worker loops, pulling jobs from a shared buffered channel. When the channel is empty, workers block. When the channel is full, submitters block (providing natural backpressure).

```
                    +-----------+
  Submit() ------> | Job Chan  | ------> Worker 1 ---> Future result
  Submit() ------> | (buffered)| ------> Worker 2 ---> Future result
  Submit() ------> |           | ------> Worker 3 ---> Future result
                    +-----------+           ...
                                  ------> Worker N ---> Future result
```

**Benefits:**
- Simple and idiomatic Go; channels are the natural coordination primitive.
- Predictable resource usage: exactly N goroutines, fixed channel buffer.
- Pre-warmed workers: no goroutine creation latency on the hot path.
- Natural backpressure via channel blocking when queue is full.
- Easy to reason about shutdown: close the channel, workers exit their `range` loop.

**Drawbacks:**
- N goroutines always alive even during idle periods (minimal cost in Go, ~2KB each).
- Queue size must be decided at construction time (though this is arguably a benefit for backpressure).
- `shutdownNow()` mid-task requires context cancellation plumbing; cannot interrupt a running goroutine otherwise.
- No work stealing; long tasks block one worker while others may be idle (FIFO assignment, not shortest-queue).

---

### Approach 2: Semaphore-based Dynamic Pool

**Design:** Use a buffered channel of capacity N as a counting semaphore. For each submitted task, spawn a new goroutine that first acquires the semaphore (sends to the buffered channel), executes the task, then releases (receives from the channel).

```go
sem := make(chan struct{}, N)

func submit(task) {
    go func() {
        sem <- struct{}{}  // acquire (blocks if N already running)
        defer func() { <-sem }() // release
        task()
    }()
}
```

**Benefits:**
- No idle goroutines: goroutines exist only while tasks are in-flight or queued.
- Naturally adapts to load; zero overhead at rest.
- Simpler shutdown: stop spawning new goroutines, wait for semaphore to drain.
- Each goroutine has its own stack; no shared mutable state beyond the semaphore.

**Drawbacks:**
- Goroutine creation overhead per task (allocation, scheduling).
- Under sustained high load, may create many goroutines waiting on the semaphore (unbounded queue of waiting goroutines).
- GC pressure from frequent goroutine allocation/deallocation.
- Harder to track "worker identity" for metrics/debugging.
- No natural queue size limit without additional mechanism.

---

### Approach 3: Work-Stealing Pool

**Design:** Create N workers, each with a local double-ended queue (deque). Tasks are initially assigned to a random worker's deque. When a worker's deque is empty, it attempts to steal tasks from other workers' deques (from the opposite end to reduce contention).

```
  Worker 1: [Task A, Task B, Task C]  <-- push/pop from bottom
  Worker 2: [Task D]                   <-- steal from top of Worker 1
  Worker 3: []                         <-- steal from top of Worker 1
```

**Benefits:**
- Better cache locality: a worker processes its own tasks sequentially.
- Reduced contention: workers primarily access their own deque; stealing is the exception.
- Good for heterogeneous task sizes: fast workers steal from slow workers' queues.
- Used in production systems (Java ForkJoinPool, Tokio, Go's own scheduler).

**Drawbacks:**
- Significantly more complex implementation (lock-free deques, atomic operations, ABA problem).
- Overhead of stealing logic may exceed benefit for uniform small tasks.
- Harder to maintain FIFO ordering guarantees.
- Harder to reason about correctness; subtle race conditions in steal operations.
- Overkill for the stated requirements (bounded concurrency + futures + shutdown).
- Testing is significantly more difficult; non-deterministic execution order.

---

## 3. Recommended Approach

**Recommendation: Approach 1 (Channel-based Fixed Worker Pool)**

**Justification:**

1. **Simplicity and correctness**: The channel-based approach leverages Go's built-in concurrency primitives exactly as they were designed. Channels provide atomic enqueue/dequeue, blocking semantics, and graceful close propagation -- all requirements of this system -- without additional synchronization.

2. **Requirements fit**: The specification requires bounded concurrency (exactly N), FIFO queuing, futures, and shutdown semantics. Approach 1 delivers all four with minimal moving parts. Approach 2 does not naturally bound the queue of waiting goroutines. Approach 3 is over-engineered for the requirements.

3. **Predictable resource model**: Exactly N goroutines + 1 buffered channel makes capacity planning trivial. Memory usage is O(N + QueueSize) and constant regardless of submission rate.

4. **Shutdown semantics**: Closing a Go channel naturally signals all workers to drain and exit. This maps directly to `shutdown()`. For `shutdownNow()`, combining channel close with context cancellation gives immediate termination of in-flight work.

5. **Testability**: Deterministic worker count makes it straightforward to prove the N-bounded invariant. Channel semantics make race condition testing reliable.

6. **Idiomatic Go**: This pattern is well-understood by Go engineers, reducing onboarding friction and review burden.

The minimal drawbacks (N idle goroutines, fixed queue size) are non-issues in practice: N goroutines cost ~N*2KB of memory, and a fixed queue size is desirable for backpressure.

---

## 4. API Design

### 4.1 Core Interfaces

```go
package workerpool

import "time"

// WorkerPool manages a fixed set of worker goroutines that process submitted tasks.
type WorkerPool interface {
    // Submit enqueues a task for execution. Returns a Future to retrieve the result.
    // Returns ErrPoolShutdown if the pool is shutting down or terminated.
    Submit(task func() (interface{}, error)) (Future, error)

    // Shutdown initiates graceful shutdown. No new tasks are accepted.
    // In-flight and queued tasks are allowed to complete.
    // Blocks until all workers have exited.
    Shutdown()

    // ShutdownNow initiates immediate shutdown. No new tasks are accepted.
    // In-flight tasks are cancelled via context. Pending (queued but not started)
    // tasks are drained and returned to the caller.
    // Blocks until all workers have exited.
    ShutdownNow() []func() (interface{}, error)

    // Metrics returns a snapshot of pool metrics.
    Metrics() PoolMetrics
}

// Future represents the eventual result of an asynchronous task.
type Future interface {
    // Get blocks until the result is available and returns it.
    // If the task panicked, the panic value is wrapped as an error.
    Get() (interface{}, error)

    // GetWithTimeout blocks until the result is available or the timeout expires.
    // Returns (result, err, true) if completed, or (nil, nil, false) if timed out.
    GetWithTimeout(timeout time.Duration) (interface{}, error, bool)

    // Done returns a channel that is closed when the result is available.
    Done() <-chan struct{}
}
```

### 4.2 Constructor

```go
// Config holds worker pool configuration.
type Config struct {
    // Workers is the number of concurrent workers (N). Must be >= 1.
    Workers int

    // QueueSize is the capacity of the job queue buffer. Must be >= 0.
    // A size of 0 means submissions block until a worker is available.
    QueueSize int
}

// New creates a new WorkerPool with the given configuration.
// Workers are started immediately.
func New(cfg Config) (WorkerPool, error)
```

### 4.3 Usage Example

```go
pool, err := workerpool.New(workerpool.Config{Workers: 4, QueueSize: 100})
if err != nil {
    log.Fatal(err)
}
defer pool.Shutdown()

future, err := pool.Submit(func() (interface{}, error) {
    result, err := doExpensiveWork()
    return result, err
})
if err != nil {
    log.Fatal(err) // pool shut down
}

// Non-blocking check
select {
case <-future.Done():
    result, err := future.Get()
    // use result
default:
    // not ready yet
}

// Blocking with timeout
result, err, ok := future.GetWithTimeout(5 * time.Second)
if !ok {
    log.Println("timed out waiting for result")
}
```

---

## 5. Data Models

### 5.1 Pool State Enum

```go
type poolState int32

const (
    stateRunning      poolState = iota // Accepting tasks, workers active
    stateShuttingDown                  // Rejecting new tasks, draining in-flight
    stateTerminated                    // All workers exited, pool dead
)
```

### 5.2 WorkerPool Struct

```go
type pool struct {
    cfg     Config
    state   atomic.Int32       // poolState, accessed atomically
    jobs    chan job            // buffered channel, capacity = cfg.QueueSize
    wg      sync.WaitGroup     // tracks active workers for shutdown coordination
    cancel  context.CancelFunc // cancels the pool-wide context (for shutdownNow)
    ctx     context.Context    // pool-wide context, cancelled on shutdownNow

    // Metrics (atomic for lock-free reads)
    activeWorkers atomic.Int64
    queued        atomic.Int64
    completed     atomic.Int64
    failed        atomic.Int64

    // Shutdown coordination
    shutdownOnce sync.Once      // ensures shutdown logic runs exactly once
    done         chan struct{}   // closed when pool is fully terminated
}
```

### 5.3 Job Struct

```go
// job wraps a task with the channel to deliver its result.
type job struct {
    task   func() (interface{}, error)
    result chan<- outcome // write-only end; the future holds the read end
    ctx    context.Context
}
```

### 5.4 Outcome Struct

```go
// outcome is the result of a task execution, delivered through the future.
type outcome struct {
    value interface{}
    err   error
}
```

### 5.5 Future Struct

```go
type future struct {
    done   chan struct{}   // closed when result is available
    result <-chan outcome  // receives exactly one outcome
    once   sync.Once      // ensures result is read exactly once
    val    interface{}     // cached result
    err    error           // cached error
}
```

### 5.6 PoolMetrics Struct

```go
// PoolMetrics is an immutable snapshot of pool statistics.
type PoolMetrics struct {
    ActiveWorkers int64 // Number of workers currently executing tasks
    Queued        int64 // Number of tasks waiting in the queue
    Completed     int64 // Total tasks that completed successfully
    Failed        int64 // Total tasks that completed with error (including panics)
}
```

---

## 6. Concurrency Design

### 6.1 Channel Architecture

```
                          Pool-wide context (ctx)
                                  |
                                  v
  ┌──────────┐          ┌─────────────────┐         ┌──────────────┐
  │ Submitter│──Submit──>│   jobs channel  │──pull──>│   Worker 1   │
  │          │          │  (cap=QueueSize) │         │  goroutine   │
  └──────────┘          │                 │──pull──>│   Worker 2   │
       │                │                 │         │  goroutine   │
       │                │                 │──pull──>│   Worker 3   │
       │                └─────────────────┘         │     ...      │
       │                        ^                   │   Worker N   │
       │                        │                   └──────┬───────┘
       │                   close(jobs)                     │
       │                   on shutdown                     │
       │                                                  │
       v                                                  v
  ┌──────────┐                                    ┌──────────────┐
  │  Future  │<──────── outcome channel ──────────│ Task Result  │
  │  (read)  │         (cap=1, per task)          │   (write)    │
  └──────────┘                                    └──────────────┘
```

### 6.2 Worker Loop

Each worker goroutine runs an identical loop:

```go
func (p *pool) worker(id int) {
    defer p.wg.Done()

    for j := range p.jobs {
        p.activeWorkers.Add(1)
        p.queued.Add(-1)

        result := p.executeTask(j)

        j.result <- result
        close(j.result) // signal future

        p.activeWorkers.Add(-1)
        if result.err != nil {
            p.failed.Add(1)
        } else {
            p.completed.Add(1)
        }
    }
}
```

When `p.jobs` is closed, `range` exits naturally, and the worker goroutine terminates.

### 6.3 Future Result Routing

Each task submission creates a dedicated `outcome` channel (capacity 1). The future holds the read end; the worker writes the single result and closes it. This ensures:
- Results route to the correct submitter (1:1 mapping).
- No shared result state between tasks.
- `Future.Done()` can be implemented by wrapping the channel close signal.

```go
func (p *pool) Submit(task func() (interface{}, error)) (Future, error) {
    if p.state.Load() != int32(stateRunning) {
        return nil, ErrPoolShutdown
    }

    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)
    j := job{task: task, result: resultCh, ctx: p.ctx}

    p.queued.Add(1)
    select {
    case p.jobs <- j:
        return f, nil
    default:
        // Check state again in case of race with shutdown
        if p.state.Load() != int32(stateRunning) {
            p.queued.Add(-1)
            return nil, ErrPoolShutdown
        }
        // Queue is full; block until space available or pool shuts down
        select {
        case p.jobs <- j:
            return f, nil
        case <-p.ctx.Done():
            p.queued.Add(-1)
            return nil, ErrPoolShutdown
        }
    }
}
```

### 6.4 Mutex vs Atomic Usage Decisions

| Resource | Mechanism | Rationale |
|----------|-----------|-----------|
| Pool state | `atomic.Int32` | Single word, compare-and-swap for transitions, no critical section needed |
| Active workers counter | `atomic.Int64` | Increment/decrement only, no compound operation |
| Queued counter | `atomic.Int64` | Same as above |
| Completed/failed counters | `atomic.Int64` | Monotonically increasing, no compound operation |
| Shutdown coordination | `sync.Once` | Ensures exactly-once execution of shutdown logic |
| Worker lifecycle | `sync.WaitGroup` | Standard pattern for waiting on N goroutines |
| Future result caching | `sync.Once` | Ensures result is read from channel exactly once |

**Design principle:** No mutexes are needed in this design. All shared state is either single-word atomic, coordinated by channels, or protected by `sync.Once`. This eliminates deadlock risk entirely.

### 6.5 Context Cancellation Flow

```
ShutdownNow() called
        │
        v
  p.cancel() ──────────────────────────────────┐
        │                                       │
        v                                       v
  close(p.jobs)                     Workers check j.ctx.Done()
        │                           inside executeTask()
        v                                       │
  Workers exit range loop                       v
  (no more new jobs)              In-flight tasks get ctx.Err()
        │                          if they respect the context
        v
  p.wg.Wait() ── all workers done
        │
        v
  Pool is Terminated
```

---

## 7. Shutdown State Machine

### 7.1 State Transition Diagram

```
                 Shutdown()           all workers exit
  ┌─────────┐  ──────────>  ┌───────────────┐  ──────────>  ┌────────────┐
  │ Running │               │ ShuttingDown  │               │ Terminated │
  └─────────┘  <── (never)  └───────────────┘  <── (never)  └────────────┘
       │                           ^
       │     ShutdownNow()         │
       └───────────────────────────┘
              (also transitions
               to ShuttingDown,
               then Terminated)
```

State transitions are **irreversible**. Once shutting down, the pool never returns to running.

### 7.2 `Shutdown()` Semantics

```go
func (p *pool) Shutdown() {
    p.shutdownOnce.Do(func() {
        // 1. Transition state: reject new submissions immediately
        p.state.Store(int32(stateShuttingDown))

        // 2. Close the jobs channel: workers will drain remaining jobs
        //    then exit their range loop
        close(p.jobs)

        // 3. Wait for all workers to finish their current task and exit
        p.wg.Wait()

        // 4. Transition to terminated
        p.state.Store(int32(stateTerminated))
        close(p.done)
    })

    // If called multiple times, wait for completion
    <-p.done
}
```

**Guarantees:**
- All previously submitted tasks (in-flight AND queued) will complete.
- New `Submit()` calls return `ErrPoolShutdown` immediately.
- `Shutdown()` blocks until all work is done.
- Calling `Shutdown()` multiple times is safe (idempotent, all calls block until done).

### 7.3 `ShutdownNow()` Semantics

```go
func (p *pool) ShutdownNow() []func() (interface{}, error) {
    var pending []func() (interface{}, error)

    p.shutdownOnce.Do(func() {
        // 1. Transition state
        p.state.Store(int32(stateShuttingDown))

        // 2. Cancel context: signals in-flight tasks to abort
        p.cancel()

        // 3. Close the jobs channel
        close(p.jobs)

        // 4. Drain pending jobs from the channel (not yet started)
        for j := range p.jobs {
            pending = append(pending, j.task)
            p.queued.Add(-1)
            // Signal the future that this task was cancelled
            j.result <- outcome{nil, ErrTaskCancelled}
            close(j.result)
        }

        // 5. Wait for in-flight workers to notice cancellation and exit
        p.wg.Wait()

        // 6. Transition to terminated
        p.state.Store(int32(stateTerminated))
        close(p.done)
    })

    <-p.done
    return pending
}
```

**Guarantees:**
- In-flight tasks receive a cancelled context (they should check `ctx.Done()`).
- Pending (queued, not started) tasks are returned to the caller without execution.
- Futures for cancelled tasks receive `ErrTaskCancelled`.
- Blocks until all workers have exited.

### 7.4 Important Note on Channel Drain Race

After `close(p.jobs)`, workers racing to read remaining items vs. `ShutdownNow()` draining is safe because:
- Multiple receivers on a closed channel is valid in Go.
- Each item is received by exactly one reader (either a worker or the drain loop).
- Items received by workers will execute (workers check `ctx.Done()` within the task, not before pulling).

To ensure `ShutdownNow()` captures pending tasks, we close the channel AND cancel the context. Workers that pull a job after cancellation will see `ctx.Err() != nil` and can short-circuit.

---

## 8. Metrics Design

### 8.1 Counter Definitions

| Metric | Type | Description |
|--------|------|-------------|
| `activeWorkers` | `atomic.Int64` | Goroutines currently executing a task (0 to N) |
| `queued` | `atomic.Int64` | Tasks in the channel awaiting pickup (0 to QueueSize) |
| `completed` | `atomic.Int64` | Tasks that returned a nil error (monotonically increasing) |
| `failed` | `atomic.Int64` | Tasks that returned non-nil error or panicked (monotonically increasing) |

### 8.2 Increment/Decrement Points

```
Submit() succeeds:
    queued++

Worker pulls job from channel:
    activeWorkers++
    queued--

Task completes successfully:
    activeWorkers--
    completed++

Task completes with error or panic:
    activeWorkers--
    failed++

ShutdownNow drains a pending task:
    queued--
    failed++ (task never executed, future gets ErrTaskCancelled)
```

### 8.3 Thread-Safety Guarantees

- All counters use `atomic.Int64` with `Add()` and `Load()` operations.
- `Metrics()` reads all counters atomically but does NOT provide a consistent snapshot across all four counters simultaneously (this would require a mutex and is deemed unnecessary for monitoring purposes).
- If a fully consistent snapshot is required, a `sync.Mutex` alternative can be provided, but the atomic approach is preferred for hot-path performance.

```go
func (p *pool) Metrics() PoolMetrics {
    return PoolMetrics{
        ActiveWorkers: p.activeWorkers.Load(),
        Queued:        p.queued.Load(),
        Completed:     p.completed.Load(),
        Failed:        p.failed.Load(),
    }
}
```

### 8.4 Derived Metrics (computed by consumers)

| Metric | Formula |
|--------|---------|
| Total submitted | `completed + failed + queued + activeWorkers` |
| Utilization | `activeWorkers / N` |
| Error rate | `failed / (completed + failed)` |
| Queue pressure | `queued / QueueSize` |

---

## 9. Error Handling

### 9.1 Task Panics

Workers wrap task execution in a `recover()` block. Panics are captured and converted to errors delivered through the future.

```go
func (p *pool) executeTask(j job) (out outcome) {
    defer func() {
        if r := recover(); r != nil {
            stack := debug.Stack()
            out = outcome{
                value: nil,
                err:   fmt.Errorf("task panicked: %v\n%s", r, stack),
            }
        }
    }()

    // Check context before executing
    select {
    case <-j.ctx.Done():
        return outcome{nil, j.ctx.Err()}
    default:
    }

    value, err := j.task()
    return outcome{value, err}
}
```

**Guarantees:**
- A panicking task never crashes the worker goroutine.
- The worker continues processing subsequent tasks.
- The panic value and stack trace are preserved in the error.
- The future's `Get()` returns the wrapped panic as an error.

### 9.2 Context Cancellation

When `ShutdownNow()` is called, the pool-wide context is cancelled. Workers check this context before executing each task. If a task is long-running and respects context:

```go
pool.Submit(func() (interface{}, error) {
    for i := 0; i < 1000000; i++ {
        select {
        case <-ctx.Done():
            return nil, ctx.Err() // early exit
        default:
        }
        // do work
    }
    return result, nil
})
```

Tasks that do NOT check context will run to completion even after `ShutdownNow()`. This is by design -- Go cannot preempt a goroutine. The pool's contract is to **signal** cancellation; cooperative cancellation depends on the task.

### 9.3 Submit After Shutdown

```go
func (p *pool) Submit(task func() (interface{}, error)) (Future, error) {
    if p.state.Load() != int32(stateRunning) {
        return nil, ErrPoolShutdown
    }
    // ...
}
```

- Returns `(nil, ErrPoolShutdown)` immediately.
- No task is enqueued.
- The caller can handle this by using a different pool or queueing externally.

### 9.4 Sentinel Errors

```go
var (
    // ErrPoolShutdown is returned when Submit is called on a non-running pool.
    ErrPoolShutdown = errors.New("worker pool is shut down")

    // ErrTaskCancelled is returned through a future when a task was pending
    // during ShutdownNow and was never executed.
    ErrTaskCancelled = errors.New("task was cancelled before execution")

    // ErrTimeout is not a sentinel; GetWithTimeout returns a bool instead.
)
```

### 9.5 Future.Get() After Pool Termination

If the pool terminates and a future's task was:
- **Completed**: `Get()` returns the result normally (result is already in the channel).
- **Cancelled by ShutdownNow**: `Get()` returns `(nil, ErrTaskCancelled)`.
- **Never submitted** (shouldn't happen): `Get()` blocks forever. To prevent this, always check the error from `Submit()`.

`GetWithTimeout()` protects against indefinite blocking in all edge cases.

---

## 10. Testability Strategy

### 10.1 Proving N-Bounded Concurrency

```go
func TestMaxConcurrency(t *testing.T) {
    const N = 5
    pool, _ := workerpool.New(workerpool.Config{Workers: N, QueueSize: 100})
    defer pool.Shutdown()

    var (
        currentActive atomic.Int64
        maxObserved   atomic.Int64
    )

    var futures []workerpool.Future
    for i := 0; i < N*10; i++ {
        f, _ := pool.Submit(func() (interface{}, error) {
            cur := currentActive.Add(1)
            // Update max observed (CAS loop)
            for {
                old := maxObserved.Load()
                if cur <= old || maxObserved.CompareAndSwap(old, cur) {
                    break
                }
            }
            time.Sleep(10 * time.Millisecond) // hold the slot
            currentActive.Add(-1)
            return nil, nil
        })
        futures = append(futures, f)
    }

    // Wait for all to complete
    for _, f := range futures {
        f.Get()
    }

    observed := maxObserved.Load()
    if observed > int64(N) {
        t.Fatalf("concurrency exceeded N=%d, observed=%d", N, observed)
    }
    if observed < int64(N) {
        t.Logf("warning: max concurrency %d < N=%d (timing-dependent)", observed, N)
    }
}
```

### 10.2 Result Routing Correctness

```go
func TestResultRouting(t *testing.T) {
    pool, _ := workerpool.New(workerpool.Config{Workers: 4, QueueSize: 100})
    defer pool.Shutdown()

    const taskCount = 100
    futures := make([]workerpool.Future, taskCount)

    for i := 0; i < taskCount; i++ {
        id := i // capture
        f, err := pool.Submit(func() (interface{}, error) {
            time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
            return id * 2, nil // deterministic result based on ID
        })
        require.NoError(t, err)
        futures[i] = f
    }

    for i, f := range futures {
        result, err := f.Get()
        require.NoError(t, err)
        require.Equal(t, i*2, result.(int), "future %d got wrong result", i)
    }
}
```

### 10.3 Shutdown Tests

```go
func TestGracefulShutdown(t *testing.T) {
    pool, _ := workerpool.New(workerpool.Config{Workers: 2, QueueSize: 10})

    var completed atomic.Int64
    for i := 0; i < 10; i++ {
        pool.Submit(func() (interface{}, error) {
            time.Sleep(10 * time.Millisecond)
            completed.Add(1)
            return nil, nil
        })
    }

    pool.Shutdown() // should block until all 10 complete

    assert.Equal(t, int64(10), completed.Load())
}

func TestShutdownRejectsNew(t *testing.T) {
    pool, _ := workerpool.New(workerpool.Config{Workers: 2, QueueSize: 10})
    pool.Shutdown()

    _, err := pool.Submit(func() (interface{}, error) { return nil, nil })
    assert.ErrorIs(t, err, workerpool.ErrPoolShutdown)
}

func TestShutdownNowReturnsPending(t *testing.T) {
    pool, _ := workerpool.New(workerpool.Config{Workers: 1, QueueSize: 100})

    // Submit a slow task to occupy the single worker
    pool.Submit(func() (interface{}, error) {
        time.Sleep(1 * time.Second)
        return nil, nil
    })

    // Submit many fast tasks that will queue up
    for i := 0; i < 50; i++ {
        pool.Submit(func() (interface{}, error) { return nil, nil })
    }

    time.Sleep(10 * time.Millisecond) // let the slow task start

    pending := pool.ShutdownNow()
    // Most of the 50 queued tasks should be returned
    assert.Greater(t, len(pending), 0)
}
```

### 10.4 Race Condition Tests

All tests should be run with the race detector:

```bash
go test -race -count=10 ./workerpool/...
```

Additional race-focused tests:

```go
func TestConcurrentSubmitAndShutdown(t *testing.T) {
    pool, _ := workerpool.New(workerpool.Config{Workers: 4, QueueSize: 100})

    var wg sync.WaitGroup
    // Concurrent submitters
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                pool.Submit(func() (interface{}, error) {
                    time.Sleep(time.Millisecond)
                    return nil, nil
                })
            }
        }()
    }

    // Shutdown while submitters are active
    time.Sleep(5 * time.Millisecond)
    pool.Shutdown()
    wg.Wait() // submitters may get ErrPoolShutdown, that's fine
}

func TestPanicDoesNotKillWorker(t *testing.T) {
    pool, _ := workerpool.New(workerpool.Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    // Submit a panicking task
    f1, _ := pool.Submit(func() (interface{}, error) {
        panic("boom")
    })

    // Submit a normal task after the panic
    f2, _ := pool.Submit(func() (interface{}, error) {
        return "ok", nil
    })

    _, err := f1.Get()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "panic")

    val, err := f2.Get()
    assert.NoError(t, err)
    assert.Equal(t, "ok", val)
}
```

### 10.5 Metrics Tests

```go
func TestMetricsAccuracy(t *testing.T) {
    pool, _ := workerpool.New(workerpool.Config{Workers: 2, QueueSize: 10})

    started := make(chan struct{})
    release := make(chan struct{})

    // Submit tasks that block until released
    for i := 0; i < 2; i++ {
        pool.Submit(func() (interface{}, error) {
            started <- struct{}{}
            <-release
            return nil, nil
        })
    }

    // Wait for both workers to be active
    <-started
    <-started

    m := pool.Metrics()
    assert.Equal(t, int64(2), m.ActiveWorkers)

    close(release)
    pool.Shutdown()

    m = pool.Metrics()
    assert.Equal(t, int64(0), m.ActiveWorkers)
    assert.Equal(t, int64(2), m.Completed)
}
```

---

## 11. Tradeoffs Comparison Table

| Dimension | Approach 1: Channel-based Fixed Pool | Approach 2: Semaphore-based Dynamic | Approach 3: Work-Stealing |
|-----------|--------------------------------------|-------------------------------------|---------------------------|
| **Implementation Complexity** | Low (< 200 LOC) | Low-Medium (~150 LOC) | High (500+ LOC) |
| **Memory (idle)** | O(N) -- N goroutines always alive (~N * 2KB) | O(1) -- no idle goroutines | O(N) -- N goroutines + N deques |
| **Memory (loaded)** | O(N + Q) -- fixed and predictable | O(N + waiting goroutines) -- unbounded waiting | O(N + total tasks distributed) |
| **Submission Latency** | O(1) channel send (or block if full) | O(1) goroutine spawn + possible semaphore wait | O(1) deque push |
| **Task Execution Latency** | Minimal (pre-warmed workers) | Goroutine creation overhead (~1us) | Minimal (pre-warmed) + steal overhead |
| **Throughput (small tasks)** | Excellent (no per-task allocation) | Good (GC pressure from goroutines) | Good (steal overhead may dominate) |
| **Throughput (large tasks)** | Good (FIFO, no rebalancing) | Good (same as fixed pool effectively) | Excellent (load balancing via stealing) |
| **Backpressure** | Natural (channel blocks when full) | No natural limit on waiting goroutines | Per-worker queue; complex backpressure |
| **Shutdown (graceful)** | Trivial (close channel + WaitGroup) | Medium (track active goroutines) | Complex (coordinate N deques + steals) |
| **Shutdown (immediate)** | Medium (context cancellation + drain) | Medium (cancel + wait for semaphore) | Complex (cancel + drain N deques) |
| **Metrics Accuracy** | High (centralized counters) | Medium (distributed goroutine tracking) | Low (distributed state, race-prone) |
| **Testability** | High (deterministic N workers) | Medium (non-deterministic goroutine count) | Low (non-deterministic steal order) |
| **FIFO Ordering** | Guaranteed (single channel) | Guaranteed (single semaphore gate) | Not guaranteed (steal reorders) |
| **Cache Locality** | Low (tasks distributed round-robin) | Low (same as channel-based) | High (worker affinity to own deque) |
| **Go Idiomaticity** | High (channels, range, WaitGroup) | Medium (semaphore pattern is known but less common) | Low (custom data structures) |
| **Debugging** | Easy (N named goroutines) | Hard (ephemeral goroutines, pprof noise) | Hard (steal logic, non-determinism) |

### Summary Recommendation

For a system requiring bounded concurrency with futures, graceful/immediate shutdown, and metrics, **Approach 1 (Channel-based Fixed Pool)** is the clear winner. It delivers all requirements with minimal complexity, zero external dependencies, full testability, and idiomatic Go patterns. The marginal memory cost of N idle goroutines (typically 10-100KB total) is negligible compared to the operational and correctness benefits.

Approach 2 is a valid alternative for fire-and-forget workloads where futures are not needed and idle resource cost is a concern (e.g., serverless environments).

Approach 3 should only be considered for specialized workloads with highly variable task durations where throughput optimization justifies the implementation and testing complexity.

---

## 12. Appendix

### 12.1 File Structure

```
workerpool/
  pool.go          -- WorkerPool implementation
  future.go        -- Future implementation
  metrics.go       -- PoolMetrics struct and helpers
  errors.go        -- Sentinel errors
  pool_test.go     -- All tests
  doc.go           -- Package documentation
  example_test.go  -- Runnable examples
```

### 12.2 Glossary

| Term | Definition |
|------|------------|
| Worker | A goroutine that pulls tasks from the job channel and executes them |
| Task | A `func() (interface{}, error)` submitted for asynchronous execution |
| Future | A handle representing the eventual result of a submitted task |
| Job | Internal struct wrapping a task with its result delivery channel |
| Backpressure | Mechanism to slow producers when consumers cannot keep up |
| Bounded concurrency | Guaranteeing at most N tasks execute simultaneously |

### 12.3 References

- [Go Concurrency Patterns (Google I/O 2012)](https://go.dev/talks/2012/concurrency.slide)
- [Go Blog: Share Memory By Communicating](https://go.dev/blog/codelab-share)
- [Java ThreadPoolExecutor](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ThreadPoolExecutor.html) (inspiration for shutdown semantics)
- [Go sync/atomic package](https://pkg.go.dev/sync/atomic)
- [Go context package](https://pkg.go.dev/context)

---

## 13. Open Questions

1. **Queue size default**: Should the default `QueueSize` be 0 (synchronous handoff, maximum backpressure) or something like `N * 10` (buffer for burst absorption)?

2. **Task context propagation**: Should `Submit()` accept a caller-provided `context.Context` in addition to the pool-wide context? This would allow per-task cancellation without shutting down the entire pool.

3. **Generic types**: Should we use Go generics (`Submit[T any](func() (T, error)) Future[T]`) for type safety, or keep `interface{}` for Go 1.17 compatibility?

4. **Panic policy**: Should panics increment the `failed` counter (current design) or a separate `panicked` counter for differentiation in monitoring?

5. **Dynamic resizing**: Is there a future requirement to resize the pool (add/remove workers) at runtime without restart? This would significantly change the architecture.

6. **Task timeout**: Should the pool enforce a maximum task duration, or is this solely the task's responsibility via context?

7. **Queue overflow policy**: When the queue is full, should `Submit()` block indefinitely, return an error immediately, or accept a timeout parameter? Current design blocks (with escape via context cancellation).
