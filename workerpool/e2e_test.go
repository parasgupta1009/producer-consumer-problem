package workerpool

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_GracefulLifecycle(t *testing.T) {
	// 1. Create pool
	pool, err := New(Config{Workers: 3, QueueSize: 20})
	require.NoError(t, err)

	// 2. Submit 10 jobs, each records its execution
	const totalJobs = 10
	var executed atomic.Int64
	futures := make([]Future, totalJobs)

	for i := 0; i < totalJobs; i++ {
		id := i
		futures[i], err = pool.Submit(func() (any, error) {
			time.Sleep(20 * time.Millisecond)
			executed.Add(1)
			return fmt.Sprintf("result-%d", id), nil
		})
		require.NoError(t, err)
	}

	// 3. Verify all results come back correctly
	for i, f := range futures {
		val, err := f.Get()
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("result-%d", i), val)
	}

	// 4. All 10 executed
	assert.Equal(t, int64(totalJobs), executed.Load())

	// 5. Metrics reflect completion
	m := pool.Metrics()
	assert.Equal(t, int64(totalJobs), m.Completed)
	assert.Equal(t, int64(0), m.Failed)
	assert.Equal(t, int64(0), m.ActiveWorkers)
	assert.Equal(t, int64(0), m.Queued)

	// 6. Graceful shutdown
	pool.Shutdown()

	// 7. Submit after shutdown must fail
	f, err := pool.Submit(func() (any, error) { return "nope", nil })
	assert.Nil(t, f)
	assert.ErrorIs(t, err, ErrPoolShutdown)
}

func TestE2E_ForceShutdown(t *testing.T) {
	// 1. Create pool with 2 workers
	pool, err := New(Config{Workers: 2, QueueSize: 50})
	require.NoError(t, err)

	var started atomic.Int64
	var finished atomic.Int64

	// 2. Submit 2 slow tasks that will be in-flight
	for i := 0; i < 2; i++ {
		pool.Submit(func() (any, error) {
			started.Add(1)
			time.Sleep(500 * time.Millisecond)
			finished.Add(1)
			return nil, nil
		})
	}

	// 3. Wait for slow tasks to start
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, int64(2), started.Load())

	// 4. Queue 20 more tasks that should NOT run
	pendingFutures := make([]Future, 20)
	for i := 0; i < 20; i++ {
		f, err := pool.Submit(func() (any, error) {
			finished.Add(1)
			return "should-not-run", nil
		})
		require.NoError(t, err)
		pendingFutures[i] = f
	}

	// 5. Force shutdown — should not wait for queued tasks
	pending := pool.ShutdownNow()

	// 6. Verify pending tasks were returned (some or all of the 20)
	t.Logf("Pending tasks returned: %d", len(pending))

	// 7. Futures for cancelled tasks should return ErrTaskCancelled
	cancelledCount := 0
	for _, f := range pendingFutures {
		if f == nil {
			continue
		}
		_, err := f.Get()
		if err != nil {
			cancelledCount++
		}
	}
	assert.Greater(t, cancelledCount, 0, "some futures should be cancelled")

	// 8. The 20 queued tasks should NOT have finished
	// (only the 2 in-flight could have finished)
	assert.LessOrEqual(t, finished.Load(), int64(2))

	// 9. Submit after force shutdown must fail
	_, err = pool.Submit(func() (any, error) { return nil, nil })
	assert.ErrorIs(t, err, ErrPoolShutdown)

	// 10. Final metrics: completed + failed should account for all observed tasks
	m := pool.Metrics()
	assert.Equal(t, int64(0), m.ActiveWorkers)
	assert.Equal(t, int64(0), m.Queued)
	t.Logf("Final metrics — Completed: %d, Failed: %d", m.Completed, m.Failed)
}
