# Concurrent Job Queue with Worker Pool

A small backend service in Go demonstrating concurrent job processing: a
bounded job queue, a fixed-size worker pool, thread-safe shared state, and
graceful shutdown — exposed over a REST API.

## Why this exists

Real systems receive work faster than a single thread can process it. This
project models that: jobs arrive via HTTP, get queued, and a pool of worker
goroutines processes them in parallel, safely, without corrupting shared
state or accepting unbounded load.

## Architecture

```
Client --POST /jobs--> [Bounded Channel Queue] --> [Worker Pool: N goroutines] --> [RWMutex-protected Result Store]
                              |                                                              |
                       (backpressure:                                                 GET /jobs/{id}
                        rejects when full)                                            GET /metrics
```

- **Queue** (`internal/queue`): a buffered Go channel. `Submit` is a
  non-blocking send — if the buffer is full, it returns `ErrQueueFull`
  immediately (HTTP 503) instead of blocking or growing unbounded. This is
  the backpressure mechanism.
- **Worker pool** (`internal/worker`): N goroutines all read from the same
  channel. Go's channel semantics guarantee each job is delivered to exactly
  one worker — no manual locking needed for job *distribution*.
- **Result store** (`internal/store`): a `map[string]Job` protected by a
  `sync.RWMutex`. Status reads (`GET /jobs/{id}`) take a read lock so many
  clients can check status concurrently; worker writes take an exclusive
  lock so updates never race.
- **Graceful shutdown** (`cmd/server/main.go`): on SIGINT/SIGTERM, the HTTP
  server stops accepting new requests, the job channel is closed, workers
  finish in-flight jobs and exit, and a `sync.WaitGroup` blocks the process
  from exiting until every worker has returned.

## Concurrency problems this deliberately demonstrates

| Problem | How it's handled |
|---|---|
| Concurrent map writes corrupting state | `sync.RWMutex` around the result store |
| Unbounded memory growth under load | Bounded channel + explicit rejection (503) when full |
| Goroutine leaks on shutdown | Closed-channel + `WaitGroup` drain pattern |
| Duplicate job processing | Channel delivery guarantees exactly-once consumption per job |

You can see the race-safety in action by removing the mutex in
`internal/store/store.go` and running:

```bash
go run -race ./cmd/server
```

— Go's race detector will immediately flag the concurrent map access.

## Running it

```bash
go run ./cmd/server -workers 10 -queue-capacity 100 -addr :8080
```

Submit a job:

```bash
curl -X POST localhost:8080/jobs -d '{"payload": "123456789"}'
# {"id":"a1b2c3d4e5f6...","status":"pending"}

curl localhost:8080/jobs/a1b2c3d4e5f6...
# {"id":"...","payload":"123456789","status":"done","result":"[3 3 3 3 3 ...]", ...}

curl localhost:8080/metrics
# {"queue_depth":0,"jobs_processed":1,"jobs_by_status":{"done":1}, ...}
```

The simulated work is prime factorization (CPU-bound, in
`internal/worker/pool.go`), chosen so processing time is visible and
worker-count scaling is measurable.

## Load testing

With the server running, in another terminal:

```bash
go run ./loadtest -n 1000 -concurrency 100
```

Re-run the server with different `-workers` values and record throughput:

| Worker pool size | Jobs/sec (factorization workload) | Notes |
|---|---|---|
| 1  | _fill in_ | baseline, fully serial |
| 5  | _fill in_ | |
| 10 | _fill in_ | |
| 50 | _fill in_ | diminishing returns expected past CPU core count |

Run this on your own machine and fill in the table before pushing — real
numbers from your hardware are far more credible in an interview than
invented ones.

## Project structure

```
cmd/server/main.go        — entrypoint, wiring, graceful shutdown
internal/queue/queue.go   — bounded channel queue with backpressure
internal/worker/pool.go   — worker pool + simulated CPU-bound processing
internal/store/store.go   — thread-safe result store (RWMutex)
internal/store/job.go     — Job type definition
internal/api/handlers.go  — REST API (submit, status, metrics)
loadtest/main.go          — concurrent load generator for benchmarking
```

## What I'd highlight in an interview

- Why a channel (not a mutex) is the right primitive for job *distribution*,
  but a mutex is the right primitive for the result *store* — channels for
  hand-off, mutexes for shared read/write state.
- The backpressure design: rejecting at the queue boundary rather than
  letting memory grow unbounded.
- The shutdown sequence and why ordering matters (stop HTTP → close queue →
  drain workers → exit) to avoid dropping in-flight work.
