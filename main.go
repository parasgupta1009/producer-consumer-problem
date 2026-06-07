package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/consumer/workerpool"
)

func main() {
	pool, err := workerpool.New(workerpool.Config{Workers: 3, QueueSize: 10})
	if err != nil {
		panic(err)
	}

	fmt.Println("=== Worker Pool Demo (3 workers, queue=10) ===")
	fmt.Println()

	// Submit 10 tasks that compute squares
	fmt.Println("--- Submitting 10 tasks (compute squares) ---")
	futures := make([]workerpool.Future, 10)
	for i := 0; i < 10; i++ {
		id := i
		futures[i], _ = pool.Submit(func() (any, error) {
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			return id * id, nil
		})
	}

	// Collect results
	for i, f := range futures {
		val, err := f.Get()
		if err != nil {
			fmt.Printf("  Task %d: ERROR: %v\n", i, err)
		} else {
			fmt.Printf("  Task %d: %d² = %d\n", i, i, val)
		}
	}

	fmt.Println()
	fmt.Println("--- Metrics after squares ---")
	m := pool.Metrics()
	fmt.Printf("  Active: %d | Queued: %d | Completed: %d | Failed: %d\n",
		m.ActiveWorkers, m.Queued, m.Completed, m.Failed)

	// Submit a task that panics
	fmt.Println()
	fmt.Println("--- Submitting a panicking task ---")
	f, _ := pool.Submit(func() (any, error) {
		panic("oops!")
	})
	_, err = f.Get()
	fmt.Printf("  Panic recovered as error: %v\n", err)

	// Submit a task with timeout
	fmt.Println()
	fmt.Println("--- Submit slow task, get with 50ms timeout ---")
	f, _ = pool.Submit(func() (any, error) {
		time.Sleep(200 * time.Millisecond)
		return "slow result", nil
	})
	val, _, ok := f.GetWithTimeout(50 * time.Millisecond)
	if !ok {
		fmt.Println("  Timed out! (expected)")
	} else {
		fmt.Printf("  Got: %v\n", val)
	}

	// Wait for it properly
	val, _ = f.Get()
	fmt.Printf("  After waiting: %v\n", val)

	// Shutdown
	fmt.Println()
	fmt.Println("--- Graceful Shutdown ---")
	pool.Shutdown()

	// Try submit after shutdown
	_, err = pool.Submit(func() (any, error) { return nil, nil })
	fmt.Printf("  Submit after shutdown: %v\n", err)

	fmt.Println()
	fmt.Println("--- Final Metrics ---")
	m = pool.Metrics()
	fmt.Printf("  Active: %d | Queued: %d | Completed: %d | Failed: %d\n",
		m.ActiveWorkers, m.Queued, m.Completed, m.Failed)
}
