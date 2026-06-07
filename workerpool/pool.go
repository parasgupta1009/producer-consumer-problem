package workerpool

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type pool struct {
	cfg    Config
	state  atomic.Int32
	jobs   chan job
	wg     sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
	mu     sync.RWMutex // protects jobs channel close vs send

	activeWorkers atomic.Int64
	queued        atomic.Int64
	completed     atomic.Int64
	failed        atomic.Int64

	shutdownOnce sync.Once
	done         chan struct{}
}

func New(cfg Config) (WorkerPool, error) {
	if cfg.Workers < 1 {
		return nil, fmt.Errorf("workers must be >= 1, got %d", cfg.Workers)
	}
	if cfg.QueueSize < 0 {
		return nil, fmt.Errorf("queue size must be >= 0, got %d", cfg.QueueSize)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &pool{
		cfg:    cfg,
		jobs:   make(chan job, cfg.QueueSize),
		cancel: cancel,
		ctx:    ctx,
		done:   make(chan struct{}),
	}
	p.state.Store(int32(stateRunning))

	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	return p, nil
}

func (p *pool) worker() {
	defer p.wg.Done()

	for j := range p.jobs {
		p.activeWorkers.Add(1)
		p.queued.Add(-1)

		res := p.executeTask(j)
		j.result <- res

		p.activeWorkers.Add(-1)
		if res.err != nil {
			p.failed.Add(1)
		} else {
			p.completed.Add(1)
		}
	}
}

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

	value, err := j.task()
	return outcome{value, err}
}

func (p *pool) Submit(task func() (any, error)) (Future, error) {
	p.mu.RLock()
	if poolState(p.state.Load()) != stateRunning {
		p.mu.RUnlock()
		return nil, ErrPoolShutdown
	}

	resultCh := make(chan outcome, 1)
	f := newFuture(resultCh)
	j := job{task: task, result: resultCh}

	p.queued.Add(1)

	select {
	case p.jobs <- j:
		p.mu.RUnlock()
		return f, nil
	case <-p.ctx.Done():
		p.mu.RUnlock()
		p.queued.Add(-1)
		resultCh <- outcome{nil, ErrPoolShutdown}
		return nil, ErrPoolShutdown
	}
}

func (p *pool) Shutdown() {
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.state.Store(int32(stateShuttingDown))
		p.cancel()
		close(p.jobs)
		p.mu.Unlock()

		p.wg.Wait()
		p.state.Store(int32(stateTerminated))
		close(p.done)
	})
	<-p.done
}

func (p *pool) ShutdownNow() []func() (any, error) {
	var pending []func() (any, error)

	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.state.Store(int32(stateShuttingDown))
		p.cancel()
		close(p.jobs)
		p.mu.Unlock()

		for j := range p.jobs {
			pending = append(pending, j.task)
			p.queued.Add(-1)
			p.failed.Add(1)
			j.result <- outcome{nil, ErrTaskCancelled}
		}

		p.wg.Wait()
		p.state.Store(int32(stateTerminated))
		close(p.done)
	})

	<-p.done
	return pending
}

func (p *pool) Metrics() PoolMetrics {
	return PoolMetrics{
		ActiveWorkers: p.activeWorkers.Load(),
		Queued:        p.queued.Load(),
		Completed:     p.completed.Load(),
		Failed:        p.failed.Load(),
	}
}
