package queue

import (
	"errors"

	"jobqueue/internal/store"
)

var ErrQueueFull = errors.New("job queue is full, try again later")

// Queue wraps a buffered channel to provide bounded backpressure.
// If the channel is full, Submit returns ErrQueueFull immediately
// instead of blocking the caller indefinitely.
type Queue struct {
	jobs chan store.Job
}

func NewQueue(capacity int) *Queue {
	return &Queue{
		jobs: make(chan store.Job, capacity),
	}
}

// Submit attempts a non-blocking send. Returns ErrQueueFull if the
// buffer is saturated, which is the system's backpressure signal.
func (q *Queue) Submit(j store.Job) error {
	select {
	case q.jobs <- j:
		return nil
	default:
		return ErrQueueFull
	}
}

// Jobs exposes the receive-only channel for workers to consume from.
func (q *Queue) Jobs() <-chan store.Job {
	return q.jobs
}

func (q *Queue) Close() {
	close(q.jobs)
}

func (q *Queue) Depth() int {
	return len(q.jobs)
}

func (q *Queue) Capacity() int {
	return cap(q.jobs)
}
