package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dimonchik03/quotaware/bench/fakeprovider"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("fakeprovider", flag.ContinueOnError)
	cfg := fakeprovider.DefaultConfig()
	listen := flags.String("listen", ":8081", "address to listen on")
	flags.IntVar(&cfg.RequestsPerMinute, "rpm", cfg.RequestsPerMinute, "requests per minute per model")
	flags.IntVar(&cfg.TokensPerMinute, "tpm", cfg.TokensPerMinute, "tokens per minute per model")
	flags.DurationVar(&cfg.Latency, "latency", cfg.Latency, "response latency")
	flags.DurationVar(&cfg.Jitter, "jitter", cfg.Jitter, "latency jitter")
	flags.IntVar(&cfg.OutputTokens, "output-tokens", cfg.OutputTokens, "output tokens per response")
	flags.Int64Var(&cfg.Seed, "seed", cfg.Seed, "random seed")
	flags.BoolVar(&cfg.Acceleration, "acceleration", cfg.Acceleration, "reject bursts above the ten-second share of the request limit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "fakeprovider: %v\n", err)
		return 2
	}

	server := &http.Server{
		Addr:              *listen,
		Handler:           fakeprovider.New(cfg, time.Now, time.Sleep),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopped := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopped <- server.Shutdown(shutdown)
	}()
	log.Printf("fakeprovider listening on %s (rpm=%d tpm=%d latency=%s seed=%d)", *listen, cfg.RequestsPerMinute, cfg.TokensPerMinute, cfg.Latency, cfg.Seed)
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		err = <-stopped
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeprovider: %v\n", err)
		return 1
	}
	return 0
}
