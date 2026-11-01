package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dimonchik03/quotaware/bench/worker"
	"github.com/dimonchik03/quotaware/internal/queue"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	cfg := worker.DefaultConfig()
	redisAddress := flags.String("redis", "redis:6379", "redis address")
	metricsAddress := flags.String("metrics", ":9101", "address that serves /metrics")
	flags.StringVar(&cfg.List, "list", cfg.List, "job list")
	flags.StringVar(&cfg.ProxyURL, "proxy", cfg.ProxyURL, "quotaware proxy origin")
	flags.StringVar(&cfg.Model, "model", cfg.Model, "model to call")
	flags.StringVar(&cfg.APIKey, "api-key", os.Getenv("API_KEY"), "credential sent as a bearer token")
	flags.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "concurrent provider calls")
	flags.IntVar(&cfg.MaxAttempts, "max-attempts", cfg.MaxAttempts, "attempts per job before giving up")
	flags.DurationVar(&cfg.MaxBackoff, "max-backoff", cfg.MaxBackoff, "longest wait between attempts")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	registry := prometheus.NewRegistry()
	metrics := worker.NewMetrics(registry)
	redis := queue.NewRedis(*redisAddress)
	defer func() { _ = redis.Close() }()
	w, err := worker.New(cfg, redis, &http.Client{}, metrics, time.Now, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		return 2
	}

	server := &http.Server{Addr: *metricsAddress, Handler: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err.Error())
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("worker started", "proxy", cfg.ProxyURL, "model", cfg.Model, "concurrency", cfg.Concurrency, "list", cfg.List)
	w.Run(ctx)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		return 1
	}
	return 0
}
