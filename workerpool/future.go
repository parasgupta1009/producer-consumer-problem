package workerpool

import (
	"sync"
	"time"
)

type future struct {
	resultCh <-chan outcome
	done     chan struct{}
	once     sync.Once
	val      any
	err      error
}

func newFuture(resultCh chan outcome) *future {
	f := &future{
		resultCh: resultCh,
		done:     make(chan struct{}),
	}
	go f.await()
	return f
}

func (f *future) await() {
	res := <-f.resultCh
	f.once.Do(func() {
		f.val = res.value
		f.err = res.err
	})
	close(f.done)
}

func (f *future) Get() (any, error) {
	<-f.done
	return f.val, f.err
}

func (f *future) GetWithTimeout(timeout time.Duration) (any, error, bool) {
	select {
	case <-f.done:
		return f.val, f.err, true
	case <-time.After(timeout):
		return nil, nil, false
	}
}

func (f *future) Done() <-chan struct{} {
	return f.done
}
