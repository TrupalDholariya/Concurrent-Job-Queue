# Concurrent Job Queue with Worker Pool

A backend service in Go that processes jobs concurrently using a bounded
queue and a fixed-size worker pool — built to demonstrate safe concurrent
state management, backpressure under load, and graceful shutdown, all
exposed over a REST API.

**Stack:** Go (stdlib only — no external dependencies) · goroutines & channels · `sync.RWMutex` · `net/http`

## The problem

Work doesn't arrive one task at a time in real systems — it arrives in
bursts, faster than a single thread can handle. This project models that
scenario end to end: HTTP requests enqueue jobs, a pool of worker goroutines
processes them in parallel, and a thread-safe store tracks results — without
race conditions, goroutine leaks, or unbounded memory growth under load.

## Architecture

```
Client --POST /jobs--> [Bounded Channel Queue] --> [Worker Pool: N goroutines] --> [RWMutex Result Store]
                              |                                                            |
                    backpressure: rejects (503)                                    GET /jobs/{id}
                    with explicit error when full                                  GET /metrics
```

| Component | File | Responsibility |
|---|---|---|
| Queue | `internal/queue/queue.go` | Buffered channel; non-blocking `Submit` rejects with `ErrQueueFull` (HTTP 503) instead of blocking or growing unbounded |
| Worker pool | `internal/worker/pool.go` | N goroutines consuming the same channel — Go's channel semantics guarantee each job is delivered to exactly one worker, no manual lock needed for distribution |
| Result store | `internal/store/store.go` | `map[string]Job` behind a `sync.RWMutex` — concurrent reads (status checks) don't block each other; writes (worker updates) are exclusive |
| Shutdown | `cmd/server/main.go` | On SIGINT/SIGTERM: stop accepting HTTP → close job channel → workers drain in-flight jobs → `WaitGroup.Wait()` before exit |

## Concurrency problems this solves, deliberately

| Problem | Mechanism |
|---|---|
| Concurrent map writes corrupting shared state | `sync.RWMutex` around the result store |
| Unbounded memory growth under burst load | Bounded channel; explicit rejection at capacity rather than infinite buffering |
| Goroutine leaks on shutdown | Closed-channel + `WaitGroup` drain pattern — no worker exits mid-job, none hang after shutdown |
| Duplicate job processing | Channel delivery guarantees exactly-once consumption per job across all workers |

Proof, not just claims — remove the mutex in `store.go` and run:

```bash
go run -race ./cmd/server
```

Go's race detector flags the concurrent map access immediately. Put the
lock back and the same test is clean.

## Verified test results

Tested live (Linux, Go 1.24.4):

```
$ curl -X POST localhost:8080/jobs -d '{"payload":"123456789"}'
{"id":"4ba5b673f5d9aeec","status":"pending"}

$ curl localhost:8080/jobs/4ba5b673f5d9aeec
{"id":"4ba5b673f5d9aeec","payload":"123456789","status":"done",
 "result":"[3 3 3607 3803]","worker_id":4, ...}
```
3 × 3 × 3607 × 3803 = 123,456,789 — correct factorization, processed by worker 4 in under a millisecond.

**Burst load test** (1000 submissions, 100 concurrent clients, default queue capacity 100, 10 workers):

```
submitted 1000 jobs in 155.6ms
  accepted: 397
  rejected (queue full): 603
  submission throughput: 6427.4 req/sec
```

This is the backpressure mechanism working as designed: when burst volume
(1000) exceeds queue capacity (100) faster than 10 workers can drain it, the
system rejects the overflow with a clear HTTP 503 instead of silently
queuing unbounded work or crashing.

**Worker-count scaling** (queue capacity raised to 2000 so bursts are fully
absorbed, isolating processing throughput from rejection rate):

| Worker pool size | Jobs/sec | Notes |
|---|---|---|
| 1  | _pending_ | baseline, fully serial |
| 5  | _pending_ | |
| 10 | _pending_ | |
| 50 | _pending_ | diminishing returns expected past available CPU cores |

*(Run `go run ./cmd/server -workers N -queue-capacity 2000`, then
`go run ./loadtest -n 1000 -concurrency 100` for each N, and fill in.)*

## Running it

```bash
go run ./cmd/server -workers 10 -queue-capacity 100 -addr :8080
```

```bash
curl -X POST localhost:8080/jobs -d '{"payload": "123456789"}'
curl localhost:8080/jobs/<id>
curl localhost:8080/metrics
```

The simulated work is prime factorization — CPU-bound, so processing time
and worker-count scaling are both visible and measurable.

## Load testing

```bash
go run ./loadtest -n 1000 -concurrency 100
```

## Project structure

```
cmd/server/main.go        entrypoint, wiring, graceful shutdown
internal/queue/queue.go   bounded channel queue with backpressure
internal/worker/pool.go   worker pool + CPU-bound job processing
internal/store/store.go   thread-safe result store (RWMutex)
internal/store/job.go     Job type definition
internal/api/handlers.go  REST API (submit, status, metrics)
loadtest/main.go          concurrent load generator for benchmarking
```

## Design decisions worth discussing

- **Channel vs. mutex** — channels for job *hand-off* (distribution across
  workers), mutex for shared *state* (the result store). Each is the right
  primitive for what it protects.
- **Reject vs. block at capacity** — the queue fails fast with a 503 rather
  than blocking the HTTP handler indefinitely, keeping the API responsive
  under overload instead of accumulating stuck requests.
- **Shutdown ordering** — stop HTTP → close queue → drain workers → exit.
  Reversing any step risks dropping in-flight jobs or leaking goroutines.

## Possible extensions

- Persistent queue (Redis/SQLite) so jobs survive a restart
- Retry logic with exponential backoff for failed jobs
- Horizontal scaling: multiple server instances pulling from a shared
  external queue (Redis/RabbitMQ) instead of an in-process channel
