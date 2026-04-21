// ingestion is the heartbeat ingestion service entrypoint.
//
// Env:
//
//	INGESTION_ADDR        default :8080
//	KAFKA_BROKERS         comma separated,  default kafka:9092
//	KAFKA_TOPIC           default heartbeats
//	KAFKA_BATCH_SIZE      default 1000
//	KAFKA_BATCH_TIMEOUT   default 20ms
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/airwallex/heartbeat/pkg/kafkax"
	"github.com/airwallex/heartbeat/services/ingestion/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	addr := env("INGESTION_ADDR", ":8080")
	brokers := strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ",")
	topic := env("KAFKA_TOPIC", "heartbeats")
	batchSize, _ := strconv.Atoi(env("KAFKA_BATCH_SIZE", "1000"))
	batchTimeout, _ := time.ParseDuration(env("KAFKA_BATCH_TIMEOUT", "20ms"))

	prod := kafkax.NewProducer(kafkax.Config{
		Brokers:      brokers,
		Topic:        topic,
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
	})
	defer prod.Close()

	srv := server.New(server.Options{
		Producer: prod,
		Logger:   logger,
	})

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("ingestion listening", "addr", addr, "brokers", brokers, "topic", topic)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
