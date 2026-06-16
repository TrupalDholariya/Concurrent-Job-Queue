// loadtest fires N concurrent job submissions against a running server
// and reports throughput. Run the server separately first, e.g.:
//
//	go run ./cmd/server -workers 10
//	go run ./loadtest -n 1000 -concurrency 100
//
// Repeat with different -workers values on the server to compare
// throughput, and record results in the README table.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	n := flag.Int("n", 1000, "total number of jobs to submit")
	concurrency := flag.Int("concurrency", 100, "number of concurrent submitting clients")
	url := flag.String("url", "http://localhost:8080/jobs", "target endpoint")
	flag.Parse()

	var wg sync.WaitGroup
	var success, rejected, failed atomic.Int64

	sem := make(chan struct{}, *concurrency)
	start := time.Now()

	for i := 0; i < *n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			body, _ := json.Marshal(map[string]string{"payload": fmt.Sprintf("%d", 100000+i)})
			resp, err := http.Post(*url, "application/json", bytes.NewReader(body))
			if err != nil {
				failed.Add(1)
				return
			}
			defer resp.Body.Close()

			switch resp.StatusCode {
			case http.StatusAccepted:
				success.Add(1)
			case http.StatusServiceUnavailable:
				rejected.Add(1)
			default:
				failed.Add(1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("submitted %d jobs in %v\n", *n, elapsed)
	fmt.Printf("  accepted: %d\n", success.Load())
	fmt.Printf("  rejected (queue full): %d\n", rejected.Load())
	fmt.Printf("  failed: %d\n", failed.Load())
	fmt.Printf("  submission throughput: %.1f req/sec\n", float64(*n)/elapsed.Seconds())
}
