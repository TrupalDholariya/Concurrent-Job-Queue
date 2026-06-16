package store

import (
	"fmt"
	"sync"
)

// Store is a thread-safe in-memory store for jobs.
// Reads (status checks) use RLock so many clients can read concurrently.
// Writes (worker updates) use Lock to guarantee exclusive, safe mutation.
type Store struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewStore() *Store {
	return &Store{
		jobs: make(map[string]Job),
	}
}

func (s *Store) Put(job Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func (s *Store) Get(id string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

func (s *Store) Update(id string, mutate func(*Job)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %s not found", id)
	}
	mutate(&job)
	s.jobs[id] = job
	return nil
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.jobs)
}

func (s *Store) CountByStatus() map[Status]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := make(map[Status]int)
	for _, j := range s.jobs {
		counts[j.Status]++
	}
	return counts
}
