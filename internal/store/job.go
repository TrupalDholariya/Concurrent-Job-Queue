package store

import "time"

type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

// Job represents a single unit of work submitted to the queue.
type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"` // e.g. a number to factorize, or text to process
	Status    Status    `json:"status"`
	Result    string    `json:"result,omitempty"`
	Error     string    `json:"error,omitempty"`
	SubmitAt  time.Time `json:"submit_at"`
	StartAt   time.Time `json:"start_at,omitempty"`
	FinishAt  time.Time `json:"finish_at,omitempty"`
	WorkerID  int       `json:"worker_id,omitempty"`
}

func (j Job) ProcessingDuration() time.Duration {
	if j.StartAt.IsZero() || j.FinishAt.IsZero() {
		return 0
	}
	return j.FinishAt.Sub(j.StartAt)
}
