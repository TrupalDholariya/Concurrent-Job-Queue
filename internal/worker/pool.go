package worker

import (
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"jobqueue/internal/queue"
	"jobqueue/internal/store"
)

// Pool manages a fixed number of worker goroutines that consume
// jobs from a shared queue and write results into a shared store.
type Pool struct {
	size      int
	q         *queue.Queue
	s         *store.Store
	wg        sync.WaitGroup
	processed atomic.Int64
}

func NewPool(size int, q *queue.Queue, s *store.Store) *Pool {
	return &Pool{size: size, q: q, s: s}
}

// Start launches `size` goroutines. Each pulls from the same channel,
// so Go's runtime guarantees a job is delivered to exactly one worker —
// no manual locking is needed for job distribution itself.
func (p *Pool) Start() {
	for i := 1; i <= p.size; i++ {
		p.wg.Add(1)
		go p.runWorker(i)
	}
}

func (p *Pool) runWorker(id int) {
	defer p.wg.Done()
	for job := range p.q.Jobs() {
		_ = p.s.Update(job.ID, func(j *store.Job) {
			j.Status = store.StatusRunning
			j.StartAt = time.Now()
			j.WorkerID = id
		})

		result, err := process(job.Payload)

		_ = p.s.Update(job.ID, func(j *store.Job) {
			j.FinishAt = time.Now()
			if err != nil {
				j.Status = store.StatusFailed
				j.Error = err.Error()
			} else {
				j.Status = store.StatusDone
				j.Result = result
			}
		})

		p.processed.Add(1)
	}
}

// Wait blocks until all workers have drained the queue and exited.
// Called after the queue channel is closed during shutdown.
func (p *Pool) Wait() {
	p.wg.Wait()
}

func (p *Pool) Processed() int64 {
	return p.processed.Load()
}

// process simulates CPU-bound work: prime factorization of the payload.
// This is intentionally non-trivial so timing differences across worker
// pool sizes are visible in the load test.
func process(payload string) (string, error) {
	n := new(big.Int)
	_, ok := n.SetString(payload, 10)
	if !ok {
		return "", fmt.Errorf("invalid payload, expected an integer string")
	}

	factors := factorize(n)
	return fmt.Sprintf("%v", factors), nil
}

func factorize(n *big.Int) []string {
	var factors []string
	num := new(big.Int).Set(n)
	two := big.NewInt(2)

	for num.Cmp(big.NewInt(1)) > 0 {
		divisor := new(big.Int).Set(two)
		mod := new(big.Int)
		divided := false
		for divisor.Cmp(num) <= 0 {
			mod.Mod(num, divisor)
			if mod.Sign() == 0 {
				factors = append(factors, divisor.String())
				num.Div(num, divisor)
				divided = true
				break
			}
			divisor.Add(divisor, big.NewInt(1))
		}
		if !divided {
			break
		}
	}
	if len(factors) == 0 {
		factors = append(factors, n.String())
	}
	return factors
}
