package workerpool

import "errors"

var (
	ErrPoolShutdown  = errors.New("worker pool is shut down")
	ErrTaskCancelled = errors.New("task was cancelled before execution")
)
