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

	"github.com/dimonchik03/quotaware/bench/producer"
	"github.com/dimonchik03/quotaware/internal/queue"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("producer", flag.ContinueOnError)
	cfg := producer.DefaultConfig()
	redisAddress := flags.String("redis", "redis:6379", "redis address")
	metricsAddress := flags.String("metrics", ":9102", "address that serves /metrics")
	phases := flags.String("phases", "half:3m:0.5,full:3m:1,burst:4m:3,idle:4m:0", "schedule as name:duration:multiplier, comma separated")
	linger := flags.Duration("linger", 30*time.Second, "how long to keep serving metrics after the last phase")
	flags.StringVar(&cfg.List, "list", cfg.List, "job list")
	flags.Float64Var(&cfg.QuotaPerSecond, "quota-per-second", cfg.QuotaPerSecond, "provider quota the multipliers refer to")
	flags.Int64Var(&cfg.Seed, "seed", cfg.Seed, "random seed")
	flags.IntVar(&cfg.PromptTokens, "prompt-tokens", cfg.PromptTokens, "prompt size per job")
	flags.IntVar(&cfg.PromptJitter, "prompt-jitter", cfg.PromptJitter, "prompt size jitter per job")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	parsed, err := producer.ParsePhases(*phases)
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		return 2
	}
	cfg.Phases = parsed
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	registry := prometheus.NewRegistry()
	metrics := producer.NewMetrics(registry)
	redis := queue.NewRedis(*redisAddress)
	defer func() { _ = redis.Close() }()
	p, err := producer.New(cfg, redis, metrics, time.Now, sleepContext, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
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
	code := 0
	if err := p.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		code = 1
	} else {
		logger.Info("schedule finished", "linger", linger.String())
		sleepContext(ctx, *linger)
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		return 1
	}
	return code
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
