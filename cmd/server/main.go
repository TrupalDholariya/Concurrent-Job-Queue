package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jobqueue/internal/api"
	"jobqueue/internal/queue"
	"jobqueue/internal/store"
	"jobqueue/internal/worker"
)

func main() {
	workers := flag.Int("workers", 10, "number of worker goroutines")
	queueCap := flag.Int("queue-capacity", 100, "max buffered jobs before backpressure kicks in")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	s := store.NewStore()
	q := queue.NewQueue(*queueCap)
	pool := worker.NewPool(*workers, q, s)
	pool.Start()

	server := api.NewServer(q, s, pool)
	httpServer := &http.Server{
		Addr:    *addr,
		Handler: server.Routes(),
	}

	go func() {
		log.Printf("listening on %s with %d workers, queue capacity %d", *addr, *workers, *queueCap)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown: wait for SIGINT/SIGTERM, stop accepting new
	// HTTP requests, close the job channel so workers drain in-flight
	// jobs and exit, then wait for them before terminating.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("shutdown signal received, draining...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(ctx)

	q.Close()
	pool.Wait()
	log.Printf("all workers drained, processed %d jobs total", pool.Processed())
}
