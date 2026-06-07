package workerpool

import "time"

type poolState int32

const (
	stateRunning      poolState = iota
	stateShuttingDown
	stateTerminated
)

type Config struct {
	Workers   int
	QueueSize int
}

type PoolMetrics struct {
	ActiveWorkers int64
	Queued        int64
	Completed     int64
	Failed        int64
}

type outcome struct {
	value any
	err   error
}

type job struct {
	task   func() (any, error)
	result chan<- outcome
}

type WorkerPool interface {
	Submit(task func() (any, error)) (Future, error)
	Shutdown()
	ShutdownNow() []func() (any, error)
	Metrics() PoolMetrics
}

type Future interface {
	Get() (any, error)
	GetWithTimeout(timeout time.Duration) (any, error, bool)
	Done() <-chan struct{}
}
