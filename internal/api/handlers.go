package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"jobqueue/internal/queue"
	"jobqueue/internal/store"
	"jobqueue/internal/worker"
)

// newJobID generates a short random hex ID without pulling in an
// external dependency, keeping the project stdlib-only.
func newJobID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type Server struct {
	queue     *queue.Queue
	store     *store.Store
	pool      *worker.Pool
	startTime time.Time
}

func NewServer(q *queue.Queue, s *store.Store, p *worker.Pool) *Server {
	return &Server{queue: q, store: s, pool: p, startTime: time.Now()}
}

func (srv *Server) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", srv.handleSubmit)
	mux.HandleFunc("GET /jobs/{id}", srv.handleGet)
	mux.HandleFunc("GET /metrics", srv.handleMetrics)
	return mux
}

type submitRequest struct {
	Payload string `json:"payload"`
}

func (srv *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	job := store.Job{
		ID:       newJobID(),
		Payload:  req.Payload,
		Status:   store.StatusPending,
		SubmitAt: time.Now(),
	}

	if err := srv.queue.Submit(job); err != nil {
		// Backpressure: queue is full, reject with 503 rather than
		// accepting unbounded work.
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	srv.store.Put(job)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"id": job.ID, "status": string(job.Status)})
}

func (srv *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, ok := srv.store.Get(id)
	if !ok {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (srv *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := map[string]any{
		"uptime_seconds":  time.Since(srv.startTime).Seconds(),
		"queue_depth":     srv.queue.Depth(),
		"queue_capacity":  srv.queue.Capacity(),
		"jobs_processed":  srv.pool.Processed(),
		"jobs_by_status":  srv.store.CountByStatus(),
		"total_jobs_seen": srv.store.Len(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}
