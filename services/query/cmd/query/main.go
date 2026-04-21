// query is the operator dashboard API entrypoint.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/airwallex/heartbeat/pkg/chx"
	"github.com/airwallex/heartbeat/services/query/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	addr := env("QUERY_ADDR", ":8081")
	chAddrs := strings.Split(env("CLICKHOUSE_ADDRS", "clickhouse:9000"), ",")
	chDB := env("CLICKHOUSE_DATABASE", "default")
	chUser := env("CLICKHOUSE_USER", "default")
	chPass := env("CLICKHOUSE_PASSWORD", "")

	client, err := chx.New(chx.Config{
		Addrs:    chAddrs,
		Database: chDB,
		Username: chUser,
		Password: chPass,
	})
	if err != nil {
		logger.Error("clickhouse", "err", err)
		os.Exit(1)
	}
	defer client.Close()

	srv := server.New(server.Options{Runner: client, Logger: logger})

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second, // aggregation queries can be slow
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("query listening", "addr", addr, "clickhouse", chAddrs)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
