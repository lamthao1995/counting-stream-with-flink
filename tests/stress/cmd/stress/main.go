// stress drives load against a running ingestion service and prints a
// latency + throughput report. Safe to run from CI against an ephemeral
// docker-compose stack or against a staging environment.
//
// Example:
//
//	stress --target=http://localhost:8080 --rps=10000 --duration=60s --batch=10
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/airwallex/heartbeat/tests/stress/internal/load"
)

func main() {
	target := flag.String("target", "http://localhost:8080", "ingestion base URL")
	rps := flag.Int("rps", 5000, "aggregate target requests per second")
	dur := flag.Duration("duration", 30*time.Second, "test duration")
	workers := flag.Int("workers", 32, "concurrent workers")
	batch := flag.Int("batch", 1, "heartbeats per request")
	devices := flag.Int("devices", 10000, "unique devices to simulate")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	fmt.Printf("stress: target=%s rps=%d dur=%s workers=%d batch=%d devices=%d\n",
		*target, *rps, *dur, *workers, *batch, *devices)

	res, err := load.Run(ctx, load.Config{
		Target:     *target,
		RPS:        *rps,
		Duration:   *dur,
		Workers:    *workers,
		BatchSize:  *batch,
		DevicePool: *devices,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	fmt.Println("---- report ----")
	fmt.Printf("sent        : %d\n", res.Sent)
	fmt.Printf("accepted    : %d\n", res.Accepted)
	fmt.Printf("rejected    : %d\n", res.Rejected)
	fmt.Printf("net errors  : %d\n", res.NetworkErrs)
	fmt.Printf("avg latency : %s\n", res.Avg())
	fmt.Printf("p50 latency : %s\n", res.Percentile(0.50))
	fmt.Printf("p95 latency : %s\n", res.Percentile(0.95))
	fmt.Printf("p99 latency : %s\n", res.Percentile(0.99))
	if dur := res.Percentile(1.0); dur > 0 {
		fmt.Printf("max latency : %s\n", dur)
	}
}
