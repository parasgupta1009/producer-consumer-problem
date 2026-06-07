# Worker Pool — TDD Implementation Plan

## Overview

This plan follows strict **Red-Green-Refactor** TDD discipline:
1. **Red**: Write a failing test that defines expected behavior
2. **Green**: Write the minimal code to make the test pass
3. **Refactor**: Clean up without changing behavior

Implementation order is bottom-up: start with the simplest component (errors/types), build up to Future, then Pool core, then Shutdown, then Metrics.

---

## Phase 0: Project Scaffolding

```
workerpool/
  errors.go        — Sentinel errors
  types.go         — PoolMetrics, Config, poolState, outcome, job structs
  future.go        — Future interface + implementation
  pool.go          — WorkerPool interface + implementation
  pool_test.go     — All tests (single file for cohesion)
```

```bash
mkdir -p workerpool
go mod init (if needed)
```

---

## Phase 1: Sentinel Errors & Types (Foundation)

### Cycle 1.1: Sentinel Errors Exist

**Red:**
```go
func TestSentinelErrors(t *testing.T) {
    assert.NotNil(t, ErrPoolShutdown)
    assert.NotNil(t, ErrTaskCancelled)
    assert.ErrorIs(t, ErrPoolShutdown, ErrPoolShutdown)
    assert.ErrorIs(t, ErrTaskCancelled, ErrTaskCancelled)
}
```

**Green:** Define `ErrPoolShutdown` and `ErrTaskCancelled` in `errors.go`.

### Cycle 1.2: Config Validation

**Red:**
```go
func TestConfigValidation(t *testing.T) {
    _, err := New(Config{Workers: 0, QueueSize: 10})
    assert.Error(t, err) // workers must be >= 1

    _, err = New(Config{Workers: -1, QueueSize: 10})
    assert.Error(t, err)

    _, err = New(Config{Workers: 5, QueueSize: -1})
    assert.Error(t, err) // queue size must be >= 0

    _, err = New(Config{Workers: 5, QueueSize: 0})
    assert.NoError(t, err) // 0 queue = synchronous handoff (valid)
}
```

**Green:** Implement `New()` with validation only (no workers yet).

---

## Phase 2: Future (Isolated, No Pool Dependency)

### Cycle 2.1: Future.Get() Blocks Until Result Available

**Red:**
```go
func TestFutureGetBlocksUntilResult(t *testing.T) {
    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)

    go func() {
        time.Sleep(50 * time.Millisecond)
        resultCh <- outcome{value: 42, err: nil}
    }()

    val, err := f.Get()
    assert.NoError(t, err)
    assert.Equal(t, 42, val)
}
```

**Green:** Implement `future` struct with `Get()` that reads from channel.

### Cycle 2.2: Future.Get() Returns Error

**Red:**
```go
func TestFutureGetReturnsError(t *testing.T) {
    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)

    expectedErr := errors.New("task failed")
    resultCh <- outcome{value: nil, err: expectedErr}

    val, err := f.Get()
    assert.Nil(t, val)
    assert.ErrorIs(t, err, expectedErr)
}
```

**Green:** Already works if Cycle 2.1 is correct.

### Cycle 2.3: Future.Get() Is Idempotent (Multiple Calls Return Same Result)

**Red:**
```go
func TestFutureGetIdempotent(t *testing.T) {
    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)
    resultCh <- outcome{value: "hello", err: nil}

    val1, err1 := f.Get()
    val2, err2 := f.Get()

    assert.Equal(t, val1, val2)
    assert.Equal(t, err1, err2)
    assert.Equal(t, "hello", val1)
}
```

**Green:** Use `sync.Once` to cache the result on first read.

### Cycle 2.4: Future.GetWithTimeout() — Success Before Timeout

**Red:**
```go
func TestFutureGetWithTimeoutSuccess(t *testing.T) {
    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)

    go func() {
        time.Sleep(10 * time.Millisecond)
        resultCh <- outcome{value: 99, err: nil}
    }()

    val, err, ok := f.GetWithTimeout(1 * time.Second)
    assert.True(t, ok)
    assert.NoError(t, err)
    assert.Equal(t, 99, val)
}
```

**Green:** Implement `GetWithTimeout` with `select` + `time.After`.

### Cycle 2.5: Future.GetWithTimeout() — Timeout Expires

**Red:**
```go
func TestFutureGetWithTimeoutExpires(t *testing.T) {
    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)
    // Never send a result

    val, err, ok := f.GetWithTimeout(50 * time.Millisecond)
    assert.False(t, ok)
    assert.Nil(t, val)
    assert.Nil(t, err)
}
```

**Green:** Return `(nil, nil, false)` on timeout path.

### Cycle 2.6: Future.Done() Channel Closes When Result Arrives

**Red:**
```go
func TestFutureDoneChannel(t *testing.T) {
    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)

    select {
    case <-f.Done():
        t.Fatal("Done should not be closed yet")
    default:
        // expected: not done yet
    }

    resultCh <- outcome{value: "done", err: nil}

    // Give goroutine time to propagate
    time.Sleep(10 * time.Millisecond)

    select {
    case <-f.Done():
        // expected: done now
    default:
        t.Fatal("Done should be closed after result arrives")
    }
}
```

**Green:** Spawn a goroutine in `newFuture` that waits for result, caches it, and closes the `done` channel.

### Cycle 2.7: Future.Get() Concurrent Access (Race Safety)

**Red:**
```go
func TestFutureGetConcurrentAccess(t *testing.T) {
    resultCh := make(chan outcome, 1)
    f := newFuture(resultCh)
    resultCh <- outcome{value: 7, err: nil}

    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            val, err := f.Get()
            assert.NoError(t, err)
            assert.Equal(t, 7, val)
        }()
    }
    wg.Wait()
}
```

**Green:** `sync.Once` in Get() handles this. Run with `-race`.

---

## Phase 3: Pool Core — Submit & Execute

### Cycle 3.1: Pool Creation Starts N Workers

**Red:**
```go
func TestPoolCreatesWorkers(t *testing.T) {
    pool, err := New(Config{Workers: 4, QueueSize: 10})
    assert.NoError(t, err)
    defer pool.Shutdown()

    // Pool should accept tasks immediately (workers are running)
    f, err := pool.Submit(func() (interface{}, error) {
        return "working", nil
    })
    assert.NoError(t, err)

    val, err := f.Get()
    assert.NoError(t, err)
    assert.Equal(t, "working", val)
}
```

**Green:** Implement `New()` that spawns N goroutines reading from `jobs` channel. Implement `Submit()` that sends to `jobs`. Minimal worker loop.

### Cycle 3.2: Submit Returns Correct Results (Basic)

**Red:**
```go
func TestSubmitReturnsCorrectResult(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        return 42, nil
    })

    val, err := f.Get()
    assert.NoError(t, err)
    assert.Equal(t, 42, val)
}
```

**Green:** Already works from 3.1.

### Cycle 3.3: Submit Routes Results to Correct Future (Many Tasks)

**Red:**
```go
func TestResultRoutingCorrectness(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 100})
    defer pool.Shutdown()

    const taskCount = 200
    futures := make([]Future, taskCount)

    for i := 0; i < taskCount; i++ {
        id := i
        f, err := pool.Submit(func() (interface{}, error) {
            time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
            return id * 3, nil
        })
        require.NoError(t, err)
        futures[i] = f
    }

    for i, f := range futures {
        val, err := f.Get()
        assert.NoError(t, err)
        assert.Equal(t, i*3, val.(int), "future %d got wrong result", i)
    }
}
```

**Green:** Already works if each submit creates its own `outcome` channel.

### Cycle 3.4: Tasks Execute With Non-Nil Error

**Red:**
```go
func TestSubmitTaskReturnsError(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    expectedErr := errors.New("computation failed")
    f, _ := pool.Submit(func() (interface{}, error) {
        return nil, expectedErr
    })

    val, err := f.Get()
    assert.Nil(t, val)
    assert.ErrorIs(t, err, expectedErr)
}
```

**Green:** Worker passes error through outcome channel.

### Cycle 3.5: Tasks Queue When All Workers Busy

**Red:**
```go
func TestTasksQueueWhenWorkersBusy(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    blocker := make(chan struct{})

    // Occupy both workers
    for i := 0; i < 2; i++ {
        pool.Submit(func() (interface{}, error) {
            <-blocker
            return nil, nil
        })
    }

    time.Sleep(20 * time.Millisecond) // let workers pick up blocking tasks

    // This task must queue (not be dropped)
    f, err := pool.Submit(func() (interface{}, error) {
        return "queued-and-executed", nil
    })
    assert.NoError(t, err)

    // Verify it hasn't completed yet (workers blocked)
    select {
    case <-f.Done():
        t.Fatal("task should still be queued")
    case <-time.After(50 * time.Millisecond):
        // expected
    }

    // Unblock workers
    close(blocker)

    val, err := f.Get()
    assert.NoError(t, err)
    assert.Equal(t, "queued-and-executed", val)
}
```

**Green:** Buffered jobs channel handles queueing naturally.

### Cycle 3.6: Bounded Concurrency — Never Exceeds N

**Red:**
```go
func TestBoundedConcurrencyNeverExceedsN(t *testing.T) {
    const N = 3
    pool, _ := New(Config{Workers: N, QueueSize: 100})
    defer pool.Shutdown()

    var (
        currentActive atomic.Int64
        maxObserved   atomic.Int64
        violations    atomic.Int64
    )

    const taskCount = N * 20
    futures := make([]Future, taskCount)

    for i := 0; i < taskCount; i++ {
        f, _ := pool.Submit(func() (interface{}, error) {
            cur := currentActive.Add(1)
            if cur > int64(N) {
                violations.Add(1)
            }
            // CAS loop for max
            for {
                old := maxObserved.Load()
                if cur <= old || maxObserved.CompareAndSwap(old, cur) {
                    break
                }
            }
            time.Sleep(5 * time.Millisecond)
            currentActive.Add(-1)
            return nil, nil
        })
        futures[i] = f
    }

    for _, f := range futures {
        f.Get()
    }

    assert.Equal(t, int64(0), violations.Load(), "concurrency exceeded N")
    assert.LessOrEqual(t, maxObserved.Load(), int64(N))
}
```

**Green:** Fixed N workers pulling from single channel guarantees this.

### Cycle 3.7: Bounded Concurrency — Actually Uses All N Workers

**Red:**
```go
func TestAllWorkersAreUtilized(t *testing.T) {
    const N = 4
    pool, _ := New(Config{Workers: N, QueueSize: 100})
    defer pool.Shutdown()

    var maxObserved atomic.Int64
    var currentActive atomic.Int64

    futures := make([]Future, N*10)
    for i := 0; i < N*10; i++ {
        f, _ := pool.Submit(func() (interface{}, error) {
            cur := currentActive.Add(1)
            for {
                old := maxObserved.Load()
                if cur <= old || maxObserved.CompareAndSwap(old, cur) {
                    break
                }
            }
            time.Sleep(20 * time.Millisecond)
            currentActive.Add(-1)
            return nil, nil
        })
        futures[i] = f
    }

    for _, f := range futures {
        f.Get()
    }

    // All N workers should have been active at some point
    assert.Equal(t, int64(N), maxObserved.Load(),
        "expected all %d workers to be utilized", N)
}
```

**Green:** Already works with N pre-warmed workers and enough tasks.

### Cycle 3.8: Zero QueueSize — Synchronous Handoff

**Red:**
```go
func TestZeroQueueSizeSynchronousHandoff(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 0})
    defer pool.Shutdown()

    // With queue=0, submit only succeeds when a worker is immediately available
    f, err := pool.Submit(func() (interface{}, error) {
        return "sync", nil
    })
    assert.NoError(t, err)

    val, _ := f.Get()
    assert.Equal(t, "sync", val)
}
```

**Green:** `make(chan job, 0)` is valid — blocks until a receiver is ready.

---

## Phase 4: Error Handling & Panic Recovery

### Cycle 4.1: Task Panic Is Recovered and Delivered as Error

**Red:**
```go
func TestTaskPanicRecovered(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        panic("something went wrong")
    })

    val, err := f.Get()
    assert.Nil(t, val)
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "panic")
    assert.Contains(t, err.Error(), "something went wrong")
}
```

**Green:** Add `defer recover()` in worker's task execution.

### Cycle 4.2: Panic Does Not Kill the Worker (Subsequent Tasks Succeed)

**Red:**
```go
func TestPanicDoesNotKillWorker(t *testing.T) {
    pool, _ := New(Config{Workers: 1, QueueSize: 10}) // single worker!
    defer pool.Shutdown()

    // First: panic
    f1, _ := pool.Submit(func() (interface{}, error) {
        panic("crash")
    })
    _, err := f1.Get()
    assert.Error(t, err)

    // Second: should still work (same worker recovered)
    f2, _ := pool.Submit(func() (interface{}, error) {
        return "still alive", nil
    })
    val, err := f2.Get()
    assert.NoError(t, err)
    assert.Equal(t, "still alive", val)
}
```

**Green:** `recover()` inside the for-loop iteration, not outside.

### Cycle 4.3: Panic with Non-String Value

**Red:**
```go
func TestPanicWithNonStringValue(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        panic(42) // int panic
    })

    _, err := f.Get()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "42")
}
```

**Green:** Use `fmt.Errorf("task panicked: %v", r)` to handle any panic type.

### Cycle 4.4: Panic with Nil Value

**Red:**
```go
func TestPanicWithNilValue(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        panic(nil)
    })

    _, err := f.Get()
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "panic")
}
```

**Green:** Check `r != nil` after recover; but `panic(nil)` in Go 1.21+ triggers a non-nil `*runtime.PanicNilError`. Handle appropriately.

### Cycle 4.5: Task Returns Both Value and Error

**Red:**
```go
func TestTaskReturnsBothValueAndError(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        return "partial", errors.New("partial error")
    })

    val, err := f.Get()
    assert.Equal(t, "partial", val)
    assert.Error(t, err)
    assert.Equal(t, "partial error", err.Error())
}
```

**Green:** Pass through both value and error as-is.

---

## Phase 5: Shutdown (Graceful)

### Cycle 5.1: Shutdown Blocks Until In-Flight Tasks Complete

**Red:**
```go
func TestShutdownWaitsForInFlight(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})

    var completed atomic.Int64

    for i := 0; i < 5; i++ {
        pool.Submit(func() (interface{}, error) {
            time.Sleep(30 * time.Millisecond)
            completed.Add(1)
            return nil, nil
        })
    }

    time.Sleep(10 * time.Millisecond) // let tasks start
    pool.Shutdown()                    // should block

    assert.Equal(t, int64(5), completed.Load())
}
```

**Green:** `close(jobs)` + `wg.Wait()` in Shutdown.

### Cycle 5.2: Shutdown Allows Queued Tasks to Complete

**Red:**
```go
func TestShutdownDrainsQueuedTasks(t *testing.T) {
    pool, _ := New(Config{Workers: 1, QueueSize: 20})

    results := make([]Future, 10)
    for i := 0; i < 10; i++ {
        id := i
        results[i], _ = pool.Submit(func() (interface{}, error) {
            time.Sleep(5 * time.Millisecond)
            return id, nil
        })
    }

    pool.Shutdown()

    // ALL tasks should have completed
    for i, f := range results {
        val, err := f.Get()
        assert.NoError(t, err)
        assert.Equal(t, i, val)
    }
}
```

**Green:** `range p.jobs` naturally drains after close.

### Cycle 5.3: Submit After Shutdown Returns Error

**Red:**
```go
func TestSubmitAfterShutdownReturnsError(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    pool.Shutdown()

    f, err := pool.Submit(func() (interface{}, error) {
        return "should not execute", nil
    })

    assert.Nil(t, f)
    assert.ErrorIs(t, err, ErrPoolShutdown)
}
```

**Green:** Check `state != stateRunning` at start of Submit.

### Cycle 5.4: Shutdown Is Idempotent (Multiple Calls Safe)

**Red:**
```go
func TestShutdownIdempotent(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})

    pool.Submit(func() (interface{}, error) {
        time.Sleep(20 * time.Millisecond)
        return nil, nil
    })

    // Call shutdown from multiple goroutines simultaneously
    var wg sync.WaitGroup
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            pool.Shutdown() // should not panic
        }()
    }
    wg.Wait()
}
```

**Green:** `sync.Once` wraps shutdown logic.

### Cycle 5.5: Shutdown With Empty Pool (No Tasks Submitted)

**Red:**
```go
func TestShutdownEmptyPool(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 10})

    done := make(chan struct{})
    go func() {
        pool.Shutdown()
        close(done)
    }()

    select {
    case <-done:
        // expected: shutdown returns quickly
    case <-time.After(1 * time.Second):
        t.Fatal("shutdown on empty pool should not hang")
    }
}
```

**Green:** Workers exit `range` immediately when channel closes with nothing in it.

---

## Phase 6: ShutdownNow (Immediate)

### Cycle 6.1: ShutdownNow Returns Pending Tasks

**Red:**
```go
func TestShutdownNowReturnsPendingTasks(t *testing.T) {
    pool, _ := New(Config{Workers: 1, QueueSize: 50})

    // Block the single worker
    blocker := make(chan struct{})
    pool.Submit(func() (interface{}, error) {
        <-blocker
        return nil, nil
    })

    time.Sleep(10 * time.Millisecond) // worker picks up blocker

    // Queue up more tasks
    for i := 0; i < 20; i++ {
        pool.Submit(func() (interface{}, error) {
            return "should not run", nil
        })
    }

    close(blocker) // unblock worker so it can exit

    pending := pool.ShutdownNow()
    assert.Greater(t, len(pending), 0, "should have returned pending tasks")
}
```

**Green:** Drain channel after close, collect tasks.

### Cycle 6.2: ShutdownNow Cancels Context for In-Flight Tasks

**Red:**
```go
func TestShutdownNowCancelsContext(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})

    ctxCancelled := make(chan bool, 1)
    pool.Submit(func() (interface{}, error) {
        // This task respects context (simulated via pool's internal ctx)
        time.Sleep(500 * time.Millisecond)
        return nil, nil
    })

    // Note: since tasks don't receive ctx directly in this design,
    // we test that ShutdownNow completes in bounded time
    time.Sleep(10 * time.Millisecond)

    done := make(chan struct{})
    go func() {
        pool.ShutdownNow()
        close(done)
    }()

    select {
    case <-done:
        _ = ctxCancelled // shutdown completed
    case <-time.After(2 * time.Second):
        t.Fatal("ShutdownNow should not hang indefinitely")
    }
}
```

**Green:** Call `p.cancel()` in ShutdownNow.

### Cycle 6.3: Futures for Cancelled Tasks Return ErrTaskCancelled

**Red:**
```go
func TestShutdownNowCancelledFuturesReturnError(t *testing.T) {
    pool, _ := New(Config{Workers: 1, QueueSize: 50})

    // Block the worker
    blocker := make(chan struct{})
    pool.Submit(func() (interface{}, error) {
        <-blocker
        return nil, nil
    })

    time.Sleep(10 * time.Millisecond)

    // Submit tasks that will be pending
    pendingFutures := make([]Future, 10)
    for i := 0; i < 10; i++ {
        f, _ := pool.Submit(func() (interface{}, error) {
            return "never", nil
        })
        pendingFutures[i] = f
    }

    close(blocker)
    pool.ShutdownNow()

    // At least some futures should get ErrTaskCancelled
    cancelledCount := 0
    for _, f := range pendingFutures {
        if f == nil {
            continue
        }
        _, err := f.Get()
        if errors.Is(err, ErrTaskCancelled) {
            cancelledCount++
        }
    }
    assert.Greater(t, cancelledCount, 0)
}
```

**Green:** Send `ErrTaskCancelled` through the future's result channel during drain.

### Cycle 6.4: Submit After ShutdownNow Returns Error

**Red:**
```go
func TestSubmitAfterShutdownNowReturnsError(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    pool.ShutdownNow()

    _, err := pool.Submit(func() (interface{}, error) {
        return nil, nil
    })
    assert.ErrorIs(t, err, ErrPoolShutdown)
}
```

**Green:** Same state check as graceful shutdown.

### Cycle 6.5: ShutdownNow With Empty Queue Returns Empty Slice

**Red:**
```go
func TestShutdownNowEmptyQueueReturnsEmpty(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})

    pending := pool.ShutdownNow()
    assert.Empty(t, pending)
}
```

**Green:** Empty drain loop returns nil slice.

---

## Phase 7: Metrics

### Cycle 7.1: Initial Metrics Are Zero

**Red:**
```go
func TestInitialMetricsZero(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 10})
    defer pool.Shutdown()

    m := pool.Metrics()
    assert.Equal(t, int64(0), m.ActiveWorkers)
    assert.Equal(t, int64(0), m.Queued)
    assert.Equal(t, int64(0), m.Completed)
    assert.Equal(t, int64(0), m.Failed)
}
```

**Green:** Atomic fields default to zero.

### Cycle 7.2: ActiveWorkers Reflects Running Tasks

**Red:**
```go
func TestMetricsActiveWorkers(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 10})
    defer pool.Shutdown()

    started := make(chan struct{}, 3)
    release := make(chan struct{})

    for i := 0; i < 3; i++ {
        pool.Submit(func() (interface{}, error) {
            started <- struct{}{}
            <-release
            return nil, nil
        })
    }

    // Wait for all 3 to start
    for i := 0; i < 3; i++ {
        <-started
    }

    m := pool.Metrics()
    assert.Equal(t, int64(3), m.ActiveWorkers)

    close(release)
    time.Sleep(20 * time.Millisecond)

    m = pool.Metrics()
    assert.Equal(t, int64(0), m.ActiveWorkers)
}
```

**Green:** `activeWorkers.Add(1)` on pickup, `activeWorkers.Add(-1)` on complete.

### Cycle 7.3: Queued Counter Reflects Waiting Tasks

**Red:**
```go
func TestMetricsQueued(t *testing.T) {
    pool, _ := New(Config{Workers: 1, QueueSize: 20})
    defer pool.Shutdown()

    blocker := make(chan struct{})

    // Block the single worker
    pool.Submit(func() (interface{}, error) {
        <-blocker
        return nil, nil
    })

    time.Sleep(10 * time.Millisecond)

    // Queue 5 more tasks
    for i := 0; i < 5; i++ {
        pool.Submit(func() (interface{}, error) {
            return nil, nil
        })
    }

    time.Sleep(10 * time.Millisecond)
    m := pool.Metrics()
    assert.Equal(t, int64(5), m.Queued)

    close(blocker)
    time.Sleep(50 * time.Millisecond)

    m = pool.Metrics()
    assert.Equal(t, int64(0), m.Queued)
}
```

**Green:** `queued.Add(1)` on submit, `queued.Add(-1)` on pickup.

### Cycle 7.4: Completed Counter Increments on Success

**Red:**
```go
func TestMetricsCompleted(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 10})
    defer pool.Shutdown()

    futures := make([]Future, 10)
    for i := 0; i < 10; i++ {
        futures[i], _ = pool.Submit(func() (interface{}, error) {
            return nil, nil
        })
    }

    for _, f := range futures {
        f.Get()
    }

    m := pool.Metrics()
    assert.Equal(t, int64(10), m.Completed)
    assert.Equal(t, int64(0), m.Failed)
}
```

**Green:** `completed.Add(1)` when task returns nil error.

### Cycle 7.5: Failed Counter Increments on Error and Panic

**Red:**
```go
func TestMetricsFailed(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 10})
    defer pool.Shutdown()

    // 3 tasks that return errors
    for i := 0; i < 3; i++ {
        f, _ := pool.Submit(func() (interface{}, error) {
            return nil, errors.New("fail")
        })
        f.Get()
    }

    // 2 tasks that panic
    for i := 0; i < 2; i++ {
        f, _ := pool.Submit(func() (interface{}, error) {
            panic("crash")
        })
        f.Get()
    }

    m := pool.Metrics()
    assert.Equal(t, int64(5), m.Failed)
    assert.Equal(t, int64(0), m.Completed)
}
```

**Green:** `failed.Add(1)` when task returns non-nil error OR panics.

### Cycle 7.6: Metrics Are Consistent After Mixed Workload

**Red:**
```go
func TestMetricsConsistencyAfterMixedWorkload(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 50})

    var futures []Future
    for i := 0; i < 50; i++ {
        idx := i
        f, _ := pool.Submit(func() (interface{}, error) {
            if idx%5 == 0 {
                return nil, errors.New("planned failure")
            }
            if idx%7 == 0 {
                panic("planned panic")
            }
            return idx, nil
        })
        futures = append(futures, f)
    }

    for _, f := range futures {
        f.Get()
    }

    pool.Shutdown()
    m := pool.Metrics()

    total := m.Completed + m.Failed
    assert.Equal(t, int64(50), total, "all tasks should be accounted for")
    assert.Equal(t, int64(0), m.ActiveWorkers)
    assert.Equal(t, int64(0), m.Queued)
}
```

**Green:** Already covered by correct increment logic.

---

## Phase 8: Race Conditions & Concurrency Edge Cases

### Cycle 8.1: Concurrent Submitters (Many Goroutines Submitting)

**Red:**
```go
func TestConcurrentSubmitters(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 200})
    defer pool.Shutdown()

    var wg sync.WaitGroup
    var totalSubmitted atomic.Int64

    for g := 0; g < 10; g++ {
        wg.Add(1)
        go func(goroutineID int) {
            defer wg.Done()
            for i := 0; i < 50; i++ {
                id := goroutineID*50 + i
                f, err := pool.Submit(func() (interface{}, error) {
                    return id, nil
                })
                if err == nil {
                    totalSubmitted.Add(1)
                    val, _ := f.Get()
                    assert.Equal(t, id, val)
                }
            }
        }(g)
    }

    wg.Wait()
    assert.Equal(t, int64(500), totalSubmitted.Load())
}
```

**Green:** Channel send is goroutine-safe. Run with `-race`.

### Cycle 8.2: Concurrent Submit and Shutdown Race

**Red:**
```go
func TestConcurrentSubmitDuringShutdown(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 100})

    var wg sync.WaitGroup

    // Submitters
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                f, err := pool.Submit(func() (interface{}, error) {
                    time.Sleep(time.Millisecond)
                    return nil, nil
                })
                if err != nil {
                    assert.ErrorIs(t, err, ErrPoolShutdown)
                    return
                }
                f.Get()
            }
        }()
    }

    // Shutdown mid-flight
    time.Sleep(5 * time.Millisecond)
    pool.Shutdown()

    wg.Wait() // should not hang or panic
}
```

**Green:** State check + channel operations are safe. No panics on closed channel.

### Cycle 8.3: Submit to Full Queue Blocks Until Space Available

**Red:**
```go
func TestSubmitBlocksOnFullQueue(t *testing.T) {
    pool, _ := New(Config{Workers: 1, QueueSize: 2})
    defer pool.Shutdown()

    blocker := make(chan struct{})

    // Fill: 1 in worker + 2 in queue = 3 total
    pool.Submit(func() (interface{}, error) { <-blocker; return nil, nil })
    time.Sleep(5 * time.Millisecond)
    pool.Submit(func() (interface{}, error) { return "q1", nil })
    pool.Submit(func() (interface{}, error) { return "q2", nil })

    // 4th submit should block
    submitted := make(chan struct{})
    go func() {
        pool.Submit(func() (interface{}, error) { return "q3", nil })
        close(submitted)
    }()

    select {
    case <-submitted:
        t.Fatal("submit should have blocked (queue full)")
    case <-time.After(50 * time.Millisecond):
        // expected: blocked
    }

    // Unblock and verify it eventually submits
    close(blocker)
    select {
    case <-submitted:
        // expected: unblocked
    case <-time.After(1 * time.Second):
        t.Fatal("submit should have unblocked after worker freed")
    }
}
```

**Green:** Channel send blocks when buffer full; unblocks when worker reads.

### Cycle 8.4: Multiple Future.Get() from Different Goroutines

**Red:**
```go
func TestMultipleGetFromDifferentGoroutines(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        time.Sleep(20 * time.Millisecond)
        return "shared", nil
    })

    var wg sync.WaitGroup
    for i := 0; i < 20; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            val, err := f.Get()
            assert.NoError(t, err)
            assert.Equal(t, "shared", val)
        }()
    }
    wg.Wait()
}
```

**Green:** `sync.Once` ensures single read; all goroutines get cached result.

---

## Phase 9: Edge Cases

### Cycle 9.1: Single Worker Pool (N=1)

**Red:**
```go
func TestSingleWorkerPool(t *testing.T) {
    pool, _ := New(Config{Workers: 1, QueueSize: 10})
    defer pool.Shutdown()

    results := make([]Future, 5)
    for i := 0; i < 5; i++ {
        id := i
        results[i], _ = pool.Submit(func() (interface{}, error) {
            return id, nil
        })
    }

    for i, f := range results {
        val, _ := f.Get()
        assert.Equal(t, i, val)
    }
}
```

### Cycle 9.2: Large N (100 Workers)

**Red:**
```go
func TestLargeWorkerCount(t *testing.T) {
    pool, _ := New(Config{Workers: 100, QueueSize: 1000})
    defer pool.Shutdown()

    var completed atomic.Int64
    futures := make([]Future, 1000)

    for i := 0; i < 1000; i++ {
        futures[i], _ = pool.Submit(func() (interface{}, error) {
            completed.Add(1)
            return nil, nil
        })
    }

    for _, f := range futures {
        f.Get()
    }

    assert.Equal(t, int64(1000), completed.Load())
}
```

### Cycle 9.3: Task Returns Nil Value (Not Error)

**Red:**
```go
func TestTaskReturnsNilValue(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        return nil, nil
    })

    val, err := f.Get()
    assert.Nil(t, val)
    assert.NoError(t, err)
}
```

### Cycle 9.4: Rapid Submit-Get Cycles (No Leaks)

**Red:**
```go
func TestRapidSubmitGetNoLeaks(t *testing.T) {
    pool, _ := New(Config{Workers: 4, QueueSize: 10})
    defer pool.Shutdown()

    for i := 0; i < 10000; i++ {
        f, err := pool.Submit(func() (interface{}, error) {
            return nil, nil
        })
        require.NoError(t, err)
        f.Get()
    }

    m := pool.Metrics()
    assert.Equal(t, int64(10000), m.Completed)
    assert.Equal(t, int64(0), m.ActiveWorkers)
    assert.Equal(t, int64(0), m.Queued)
}
```

### Cycle 9.5: GetWithTimeout After Result Already Available

**Red:**
```go
func TestGetWithTimeoutAlreadyAvailable(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})
    defer pool.Shutdown()

    f, _ := pool.Submit(func() (interface{}, error) {
        return "instant", nil
    })

    time.Sleep(20 * time.Millisecond) // let task complete

    val, err, ok := f.GetWithTimeout(1 * time.Millisecond)
    assert.True(t, ok)
    assert.NoError(t, err)
    assert.Equal(t, "instant", val)
}
```

### Cycle 9.6: Long-Running Tasks Don't Block Shutdown Forever

**Red:**
```go
func TestShutdownWithLongRunningTasks(t *testing.T) {
    pool, _ := New(Config{Workers: 2, QueueSize: 10})

    pool.Submit(func() (interface{}, error) {
        time.Sleep(100 * time.Millisecond)
        return nil, nil
    })

    time.Sleep(10 * time.Millisecond)

    done := make(chan struct{})
    go func() {
        pool.Shutdown()
        close(done)
    }()

    select {
    case <-done:
        // Graceful: waited for task to finish
    case <-time.After(5 * time.Second):
        t.Fatal("shutdown hung — workers never exited")
    }
}
```

---

## Phase 10: Integration / End-to-End

### Cycle 10.1: Full Lifecycle Test

**Red:**
```go
func TestFullLifecycle(t *testing.T) {
    pool, err := New(Config{Workers: 3, QueueSize: 20})
    require.NoError(t, err)

    // Submit mixed workload
    var futures []Future
    for i := 0; i < 30; i++ {
        idx := i
        f, err := pool.Submit(func() (interface{}, error) {
            time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
            if idx%10 == 0 {
                return nil, fmt.Errorf("task %d failed", idx)
            }
            return idx, nil
        })
        require.NoError(t, err)
        futures = append(futures, f)
    }

    // Collect results
    for i, f := range futures {
        val, err := f.Get()
        if i%10 == 0 {
            assert.Error(t, err)
        } else {
            assert.NoError(t, err)
            assert.Equal(t, i, val)
        }
    }

    // Check metrics
    m := pool.Metrics()
    assert.Equal(t, int64(27), m.Completed) // 30 - 3 failures (0,10,20)
    assert.Equal(t, int64(3), m.Failed)

    // Shutdown
    pool.Shutdown()

    // Verify rejection
    _, err = pool.Submit(func() (interface{}, error) { return nil, nil })
    assert.ErrorIs(t, err, ErrPoolShutdown)
}
```

### Cycle 10.2: Stress Test With Race Detector

**Red:**
```go
func TestStressWithRaceDetector(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping stress test in short mode")
    }

    pool, _ := New(Config{Workers: 8, QueueSize: 500})

    var wg sync.WaitGroup
    for i := 0; i < 20; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 200; j++ {
                f, err := pool.Submit(func() (interface{}, error) {
                    if rand.Intn(100) < 5 {
                        panic("random panic")
                    }
                    if rand.Intn(100) < 10 {
                        return nil, errors.New("random error")
                    }
                    return rand.Int(), nil
                })
                if err != nil {
                    continue
                }
                if rand.Intn(2) == 0 {
                    f.Get()
                } else {
                    f.GetWithTimeout(10 * time.Millisecond)
                }
            }
        }()
    }

    time.Sleep(100 * time.Millisecond)
    go pool.Shutdown()
    wg.Wait()
}
```

---

## Implementation Order Summary

| Phase | Component | Tests | LOC Est. |
|-------|-----------|-------|----------|
| 0 | Scaffolding | — | 10 |
| 1 | Errors + Config | 2 | 30 |
| 2 | Future | 7 | 60 |
| 3 | Pool Core (Submit + Execute) | 8 | 100 |
| 4 | Error Handling + Panic | 5 | 30 |
| 5 | Graceful Shutdown | 5 | 40 |
| 6 | ShutdownNow | 5 | 50 |
| 7 | Metrics | 6 | 30 |
| 8 | Race Conditions | 4 | — (test only) |
| 9 | Edge Cases | 6 | — (test only) |
| 10 | Integration | 2 | — (test only) |
| **TOTAL** | | **50** | **~350** |

---

## Test Execution Commands

```bash
# Run all tests
go test ./workerpool/... -v

# Run with race detector (MANDATORY)
go test ./workerpool/... -race -count=5

# Run specific phase
go test ./workerpool/... -run TestFuture -v
go test ./workerpool/... -run TestBounded -v
go test ./workerpool/... -run TestShutdown -v

# Run stress test
go test ./workerpool/... -run TestStress -race -count=3

# Coverage
go test ./workerpool/... -coverprofile=coverage.out
go tool cover -func=coverage.out
```
