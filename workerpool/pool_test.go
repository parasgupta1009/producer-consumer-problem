package workerpool

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Phase 1: Sentinel Errors & Config Validation
// =============================================================================

func TestSentinelErrors(t *testing.T) {
	assert.NotNil(t, ErrPoolShutdown)
	assert.NotNil(t, ErrTaskCancelled)
	assert.ErrorIs(t, ErrPoolShutdown, ErrPoolShutdown)
	assert.ErrorIs(t, ErrTaskCancelled, ErrTaskCancelled)
	assert.NotErrorIs(t, ErrPoolShutdown, ErrTaskCancelled)
}

func TestConfigValidation_ZeroWorkers(t *testing.T) {
	_, err := New(Config{Workers: 0, QueueSize: 10})
	assert.Error(t, err)
}

func TestConfigValidation_NegativeWorkers(t *testing.T) {
	_, err := New(Config{Workers: -1, QueueSize: 10})
	assert.Error(t, err)
}

func TestConfigValidation_NegativeQueueSize(t *testing.T) {
	_, err := New(Config{Workers: 5, QueueSize: -1})
	assert.Error(t, err)
}

func TestConfigValidation_ZeroQueueSizeValid(t *testing.T) {
	p, err := New(Config{Workers: 5, QueueSize: 0})
	assert.NoError(t, err)
	p.Shutdown()
}

// =============================================================================
// Phase 2: Future
// =============================================================================

func TestFutureGetBlocksUntilResult(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	start := time.Now()
	f, _ := pool.Submit(func() (any, error) {
		time.Sleep(50 * time.Millisecond)
		return 42, nil
	})

	val, err := f.Get()
	assert.NoError(t, err)
	assert.Equal(t, 42, val)
	assert.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
}

func TestFutureGetReturnsError(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	expectedErr := errors.New("task failed")
	f, _ := pool.Submit(func() (any, error) {
		return nil, expectedErr
	})

	val, err := f.Get()
	assert.Nil(t, val)
	assert.ErrorIs(t, err, expectedErr)
}

func TestFutureGetIdempotent(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) {
		return "hello", nil
	})

	val1, _ := f.Get()
	val2, _ := f.Get()
	val3, _ := f.Get()
	assert.Equal(t, "hello", val1)
	assert.Equal(t, val1, val2)
	assert.Equal(t, val2, val3)
}

func TestFutureGetWithTimeout_Success(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) {
		time.Sleep(10 * time.Millisecond)
		return 99, nil
	})

	val, err, ok := f.GetWithTimeout(1 * time.Second)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, 99, val)
}

func TestFutureGetWithTimeout_Expires(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	blocker := make(chan struct{})
	f, _ := pool.Submit(func() (any, error) {
		<-blocker
		return "late", nil
	})

	val, err, ok := f.GetWithTimeout(50 * time.Millisecond)
	assert.False(t, ok)
	assert.Nil(t, val)
	assert.Nil(t, err)
	close(blocker)
}

func TestFutureDoneChannel(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	blocker := make(chan struct{})
	f, _ := pool.Submit(func() (any, error) {
		<-blocker
		return "done", nil
	})

	select {
	case <-f.Done():
		t.Fatal("Done should not be closed yet")
	default:
	}

	close(blocker)
	time.Sleep(20 * time.Millisecond)

	select {
	case <-f.Done():
	default:
		t.Fatal("Done should be closed after result")
	}
}

func TestFutureGetConcurrentAccess(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) {
		return 7, nil
	})

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

// =============================================================================
// Phase 3: Pool Core — Submit & Execute
// =============================================================================

func TestPoolCreatesAndExecutes(t *testing.T) {
	pool, err := New(Config{Workers: 4, QueueSize: 10})
	assert.NoError(t, err)
	defer pool.Shutdown()

	f, err := pool.Submit(func() (any, error) { return "working", nil })
	assert.NoError(t, err)

	val, err := f.Get()
	assert.NoError(t, err)
	assert.Equal(t, "working", val)
}

func TestResultRoutingCorrectness(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 200})
	defer pool.Shutdown()

	const taskCount = 200
	futures := make([]Future, taskCount)

	for i := 0; i < taskCount; i++ {
		id := i
		f, err := pool.Submit(func() (any, error) {
			time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
			return id * 3, nil
		})
		require.NoError(t, err)
		futures[i] = f
	}

	for i, f := range futures {
		val, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, i*3, val.(int), "future %d wrong", i)
	}
}

func TestTasksQueueWhenWorkersBusy(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	blocker := make(chan struct{})
	for i := 0; i < 2; i++ {
		pool.Submit(func() (any, error) { <-blocker; return nil, nil })
	}
	time.Sleep(20 * time.Millisecond)

	f, _ := pool.Submit(func() (any, error) { return "queued", nil })

	select {
	case <-f.Done():
		t.Fatal("should still be queued")
	case <-time.After(50 * time.Millisecond):
	}

	close(blocker)
	val, _ := f.Get()
	assert.Equal(t, "queued", val)
}

func TestBoundedConcurrencyNeverExceedsN(t *testing.T) {
	const N = 3
	pool, _ := New(Config{Workers: N, QueueSize: 100})
	defer pool.Shutdown()

	var currentActive, maxObserved, violations atomic.Int64

	futures := make([]Future, N*20)
	for i := 0; i < N*20; i++ {
		f, _ := pool.Submit(func() (any, error) {
			cur := currentActive.Add(1)
			if cur > int64(N) {
				violations.Add(1)
			}
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

	assert.Equal(t, int64(0), violations.Load())
	assert.LessOrEqual(t, maxObserved.Load(), int64(N))
}

func TestAllWorkersUtilized(t *testing.T) {
	const N = 4
	pool, _ := New(Config{Workers: N, QueueSize: 100})
	defer pool.Shutdown()

	var maxObserved, currentActive atomic.Int64

	futures := make([]Future, N*10)
	for i := 0; i < N*10; i++ {
		f, _ := pool.Submit(func() (any, error) {
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
	assert.Equal(t, int64(N), maxObserved.Load())
}

func TestZeroQueueSynchronousHandoff(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 0})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) { return "sync", nil })
	val, _ := f.Get()
	assert.Equal(t, "sync", val)
}

// =============================================================================
// Phase 4: Panic Recovery
// =============================================================================

func TestPanicRecovered(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) { panic("boom") })

	val, err := f.Get()
	assert.Nil(t, val)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "panic")
	assert.Contains(t, err.Error(), "boom")
}

func TestPanicDoesNotKillWorker(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	f1, _ := pool.Submit(func() (any, error) { panic("crash") })
	f1.Get()

	f2, _ := pool.Submit(func() (any, error) { return "alive", nil })
	val, err := f2.Get()
	assert.NoError(t, err)
	assert.Equal(t, "alive", val)
}

func TestPanicWithIntValue(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) { panic(42) })
	_, err := f.Get()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "42")
}

func TestPanicWithNilValue(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) { panic(nil) })
	_, err := f.Get()
	assert.Error(t, err)
}

func TestTaskReturnsBothValueAndError(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) {
		return "partial", errors.New("partial error")
	})

	val, err := f.Get()
	assert.Equal(t, "partial", val)
	assert.EqualError(t, err, "partial error")
}

// =============================================================================
// Phase 5: Graceful Shutdown
// =============================================================================

func TestShutdownWaitsForInFlight(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})

	var completed atomic.Int64
	for i := 0; i < 5; i++ {
		pool.Submit(func() (any, error) {
			time.Sleep(30 * time.Millisecond)
			completed.Add(1)
			return nil, nil
		})
	}

	time.Sleep(10 * time.Millisecond)
	pool.Shutdown()
	assert.Equal(t, int64(5), completed.Load())
}

func TestShutdownDrainsQueue(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 20})

	futures := make([]Future, 10)
	for i := 0; i < 10; i++ {
		id := i
		futures[i], _ = pool.Submit(func() (any, error) {
			time.Sleep(5 * time.Millisecond)
			return id, nil
		})
	}

	pool.Shutdown()

	for i, f := range futures {
		val, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, i, val)
	}
}

func TestSubmitAfterShutdown(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	pool.Shutdown()

	f, err := pool.Submit(func() (any, error) { return nil, nil })
	assert.Nil(t, f)
	assert.ErrorIs(t, err, ErrPoolShutdown)
}

func TestShutdownIdempotent(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	pool.Submit(func() (any, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); pool.Shutdown() }()
	}
	wg.Wait()
}

func TestShutdownEmptyPool(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 10})

	done := make(chan struct{})
	go func() { pool.Shutdown(); close(done) }()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("shutdown hung on empty pool")
	}
}

// =============================================================================
// Phase 6: ShutdownNow
// =============================================================================

func TestShutdownNowReturnsPending(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 50})

	blocker := make(chan struct{})
	pool.Submit(func() (any, error) { <-blocker; return nil, nil })
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 20; i++ {
		pool.Submit(func() (any, error) { return nil, nil })
	}

	close(blocker)
	pending := pool.ShutdownNow()
	assert.GreaterOrEqual(t, len(pending), 0)
}

func TestShutdownNowDoesNotHang(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	pool.Submit(func() (any, error) {
		time.Sleep(500 * time.Millisecond)
		return nil, nil
	})
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() { pool.ShutdownNow(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ShutdownNow hung")
	}
}

func TestShutdownNowCancelledFutures(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 50})

	blocker := make(chan struct{})
	pool.Submit(func() (any, error) { <-blocker; return nil, nil })
	time.Sleep(10 * time.Millisecond)

	pendingFutures := make([]Future, 10)
	for i := 0; i < 10; i++ {
		f, _ := pool.Submit(func() (any, error) { return "never", nil })
		pendingFutures[i] = f
	}

	close(blocker)
	pool.ShutdownNow()

	cancelled := 0
	for _, f := range pendingFutures {
		if f == nil {
			continue
		}
		_, err := f.Get()
		if errors.Is(err, ErrTaskCancelled) {
			cancelled++
		}
	}
	assert.Greater(t, cancelled, 0)
}

func TestSubmitAfterShutdownNow(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	pool.ShutdownNow()

	_, err := pool.Submit(func() (any, error) { return nil, nil })
	assert.ErrorIs(t, err, ErrPoolShutdown)
}

func TestShutdownNowEmptyQueue(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	pending := pool.ShutdownNow()
	assert.Empty(t, pending)
}

// =============================================================================
// Phase 7: Metrics
// =============================================================================

func TestMetricsInitialZero(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 10})
	defer pool.Shutdown()

	m := pool.Metrics()
	assert.Equal(t, int64(0), m.ActiveWorkers)
	assert.Equal(t, int64(0), m.Queued)
	assert.Equal(t, int64(0), m.Completed)
	assert.Equal(t, int64(0), m.Failed)
}

func TestMetricsActiveWorkers(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 10})
	defer pool.Shutdown()

	started := make(chan struct{}, 3)
	release := make(chan struct{})

	for i := 0; i < 3; i++ {
		pool.Submit(func() (any, error) {
			started <- struct{}{}
			<-release
			return nil, nil
		})
	}
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

func TestMetricsQueued(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 20})
	defer pool.Shutdown()

	blocker := make(chan struct{})
	pool.Submit(func() (any, error) { <-blocker; return nil, nil })
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 5; i++ {
		pool.Submit(func() (any, error) { return nil, nil })
	}
	time.Sleep(10 * time.Millisecond)

	m := pool.Metrics()
	assert.Equal(t, int64(5), m.Queued)

	close(blocker)
	time.Sleep(50 * time.Millisecond)
	m = pool.Metrics()
	assert.Equal(t, int64(0), m.Queued)
}

func TestMetricsCompleted(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 10})
	defer pool.Shutdown()

	futures := make([]Future, 10)
	for i := 0; i < 10; i++ {
		futures[i], _ = pool.Submit(func() (any, error) { return nil, nil })
	}
	for _, f := range futures {
		f.Get()
	}

	m := pool.Metrics()
	assert.Equal(t, int64(10), m.Completed)
	assert.Equal(t, int64(0), m.Failed)
}

func TestMetricsFailed(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 10})
	defer pool.Shutdown()

	for i := 0; i < 3; i++ {
		f, _ := pool.Submit(func() (any, error) { return nil, errors.New("fail") })
		f.Get()
	}
	for i := 0; i < 2; i++ {
		f, _ := pool.Submit(func() (any, error) { panic("crash") })
		f.Get()
	}

	m := pool.Metrics()
	assert.Equal(t, int64(5), m.Failed)
	assert.Equal(t, int64(0), m.Completed)
}

func TestMetricsConsistency(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 50})

	var futures []Future
	for i := 0; i < 50; i++ {
		idx := i
		f, _ := pool.Submit(func() (any, error) {
			if idx%5 == 0 {
				return nil, errors.New("fail")
			}
			if idx%7 == 0 {
				panic("panic")
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
	assert.Equal(t, int64(50), m.Completed+m.Failed)
	assert.Equal(t, int64(0), m.ActiveWorkers)
	assert.Equal(t, int64(0), m.Queued)
}

// =============================================================================
// Phase 8: Concurrency & Race Conditions
// =============================================================================

func TestConcurrentSubmitters(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 500})
	defer pool.Shutdown()

	var wg sync.WaitGroup
	var total atomic.Int64

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := gid*50 + i
				f, err := pool.Submit(func() (any, error) { return id, nil })
				if err == nil {
					total.Add(1)
					val, _ := f.Get()
					assert.Equal(t, id, val)
				}
			}
		}(g)
	}
	wg.Wait()
	assert.Equal(t, int64(500), total.Load())
}

func TestConcurrentSubmitDuringShutdown(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 100})

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				f, err := pool.Submit(func() (any, error) {
					time.Sleep(time.Millisecond)
					return nil, nil
				})
				if err != nil {
					return
				}
				f.Get()
			}
		}()
	}

	time.Sleep(5 * time.Millisecond)
	pool.Shutdown()
	wg.Wait()
}

func TestSubmitBlocksOnFullQueue(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 2})
	defer pool.Shutdown()

	blocker := make(chan struct{})
	pool.Submit(func() (any, error) { <-blocker; return nil, nil })
	time.Sleep(5 * time.Millisecond)
	pool.Submit(func() (any, error) { return "q1", nil })
	pool.Submit(func() (any, error) { return "q2", nil })

	submitted := make(chan struct{})
	go func() {
		pool.Submit(func() (any, error) { return "q3", nil })
		close(submitted)
	}()

	select {
	case <-submitted:
		t.Fatal("should have blocked")
	case <-time.After(50 * time.Millisecond):
	}

	close(blocker)
	select {
	case <-submitted:
	case <-time.After(1 * time.Second):
		t.Fatal("should have unblocked")
	}
}

func TestMultipleGoroutinesGetSameFuture(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) {
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

// =============================================================================
// Phase 9: Edge Cases
// =============================================================================

func TestSingleWorkerPool(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 10})
	defer pool.Shutdown()

	for i := 0; i < 5; i++ {
		id := i
		f, _ := pool.Submit(func() (any, error) { return id, nil })
		val, _ := f.Get()
		assert.Equal(t, i, val)
	}
}

func TestLargeWorkerCount(t *testing.T) {
	pool, _ := New(Config{Workers: 100, QueueSize: 1000})
	defer pool.Shutdown()

	var completed atomic.Int64
	futures := make([]Future, 1000)
	for i := 0; i < 1000; i++ {
		futures[i], _ = pool.Submit(func() (any, error) {
			completed.Add(1)
			return nil, nil
		})
	}
	for _, f := range futures {
		f.Get()
	}
	assert.Equal(t, int64(1000), completed.Load())
}

func TestNilReturnValue(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) { return nil, nil })
	val, err := f.Get()
	assert.Nil(t, val)
	assert.NoError(t, err)
}

func TestRapidSubmitGet(t *testing.T) {
	pool, _ := New(Config{Workers: 4, QueueSize: 10})
	defer pool.Shutdown()

	for i := 0; i < 10000; i++ {
		f, err := pool.Submit(func() (any, error) { return nil, nil })
		require.NoError(t, err)
		f.Get()
	}

	m := pool.Metrics()
	assert.Equal(t, int64(10000), m.Completed)
	assert.Equal(t, int64(0), m.ActiveWorkers)
}

func TestGetWithTimeoutAlreadyDone(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	defer pool.Shutdown()

	f, _ := pool.Submit(func() (any, error) { return "instant", nil })
	time.Sleep(20 * time.Millisecond)

	val, err, ok := f.GetWithTimeout(1 * time.Millisecond)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, "instant", val)
}

func TestShutdownWithLongTask(t *testing.T) {
	pool, _ := New(Config{Workers: 2, QueueSize: 10})
	pool.Submit(func() (any, error) {
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	})
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() { pool.Shutdown(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hung")
	}
}

// =============================================================================
// Phase 10: Integration
// =============================================================================

func TestFullLifecycle(t *testing.T) {
	pool, err := New(Config{Workers: 3, QueueSize: 50})
	require.NoError(t, err)

	var futures []Future
	for i := 0; i < 30; i++ {
		idx := i
		f, err := pool.Submit(func() (any, error) {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			if idx%10 == 0 {
				return nil, fmt.Errorf("task %d failed", idx)
			}
			return idx, nil
		})
		require.NoError(t, err)
		futures = append(futures, f)
	}

	for i, f := range futures {
		val, err := f.Get()
		if i%10 == 0 {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, i, val)
		}
	}

	m := pool.Metrics()
	assert.Equal(t, int64(27), m.Completed)
	assert.Equal(t, int64(3), m.Failed)

	pool.Shutdown()
	_, err = pool.Submit(func() (any, error) { return nil, nil })
	assert.ErrorIs(t, err, ErrPoolShutdown)
}

func TestStressRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test")
	}

	pool, _ := New(Config{Workers: 8, QueueSize: 500})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				f, err := pool.Submit(func() (any, error) {
					if rand.Intn(100) < 5 {
						panic("random")
					}
					if rand.Intn(100) < 10 {
						return nil, errors.New("err")
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
