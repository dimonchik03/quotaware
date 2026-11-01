package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/dimonchik03/quotaware/internal/metrics"
	"github.com/dimonchik03/quotaware/internal/proxy"
	"github.com/dimonchik03/quotaware/internal/queue"
	"github.com/dimonchik03/quotaware/internal/scaler"
	"github.com/dimonchik03/quotaware/internal/state"
)

const (
	evictEvery = time.Minute
	evictAfter = 10 * time.Minute
)

type observerOptions struct {
	listen       string
	admin        string
	grpc         string
	alpha        float64
	pushInterval time.Duration
	proxy        proxy.Config
}

func parseObserverFlags(command string, args []string, stderr io.Writer, withScaler bool) (observerOptions, error) {
	flags := flag.NewFlagSet("quotaware "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	options := observerOptions{proxy: proxy.DefaultConfig(), pushInterval: scaler.DefaultConfig().PushInterval}
	openAI := flags.String("openai-upstream", "https://api.openai.com", "origin of the OpenAI API, without a path")
	anthropic := flags.String("anthropic-upstream", "https://api.anthropic.com", "origin of the Anthropic API, without a path")
	flags.StringVar(&options.listen, "listen", ":8080", "address the proxy listens on")
	flags.StringVar(&options.admin, "admin", ":9100", "address that serves /metrics and /healthz")
	flags.Float64Var(&options.alpha, "ewma-alpha", 0.2, "weight of the newest sample in the smoothed averages")
	flags.Int64Var(&options.proxy.MaxRequestBytes, "max-request-bytes", options.proxy.MaxRequestBytes, "largest request body accepted")
	flags.Int64Var(&options.proxy.MaxCaptureBytes, "max-capture-bytes", options.proxy.MaxCaptureBytes, "largest response body inspected for usage")
	if withScaler {
		flags.StringVar(&options.grpc, "grpc", ":9090", "address the KEDA external scaler listens on")
		flags.DurationVar(&options.pushInterval, "push-interval", options.pushInterval, "how often StreamIsActive re-evaluates a queue trigger")
	}
	if err := flags.Parse(args); err != nil {
		return observerOptions{}, err
	}
	addresses := map[string]string{options.listen: "listen", options.admin: "admin"}
	if len(addresses) < 2 {
		return observerOptions{}, errors.New("listen and admin addresses must differ")
	}
	if withScaler {
		if _, taken := addresses[options.grpc]; taken {
			return observerOptions{}, errors.New("grpc address must differ from listen and admin")
		}
	}
	var err error
	if options.proxy.OpenAI, err = parseOrigin(*openAI); err != nil {
		return observerOptions{}, fmt.Errorf("openai-upstream: %w", err)
	}
	if options.proxy.Anthropic, err = parseOrigin(*anthropic); err != nil {
		return observerOptions{}, fmt.Errorf("anthropic-upstream: %w", err)
	}
	return options, nil
}

func parseOrigin(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	switch {
	case err != nil:
		return nil, err
	case parsed.Scheme != "http" && parsed.Scheme != "https":
		return nil, errors.New("scheme must be http or https")
	case parsed.Host == "":
		return nil, errors.New("host is required")
	case parsed.Path != "" && parsed.Path != "/":
		return nil, errors.New("must be an origin without a path")
	}
	return parsed, nil
}

func runProxy(args []string, stderr io.Writer) int {
	return runObserver("proxy", args, stderr, false)
}

func runAll(args []string, stderr io.Writer) int {
	return runObserver("all", args, stderr, true)
}

func runObserver(command string, args []string, stderr io.Writer, withScaler bool) int {
	options, err := parseObserverFlags(command, args, stderr, withScaler)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 2
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	options.proxy.Logger = logger
	store, err := state.New(options.alpha)
	if err != nil {
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 2
	}
	handler, err := proxy.New(options.proxy, store, time.Now)
	if err != nil {
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 2
	}
	admin := http.NewServeMux()
	admin.Handle("GET /metrics", metrics.Handler(store, time.Now))
	admin.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	var rpc *grpc.Server
	if withScaler {
		cfg := scaler.DefaultConfig()
		cfg.PushInterval = options.pushInterval
		service, err := scaler.New(cfg, store, func(address string) scaler.QueueDepth { return queue.NewRedis(address) }, time.Now, logger)
		if err != nil {
			fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
			return 2
		}
		rpc = grpc.NewServer()
		service.Register(rpc)
		defer func() {
			if err := service.Close(); err != nil {
				logger.Warn("closing queue clients", "error", err.Error())
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go evictIdleBuckets(ctx, store)
	logger.Info("quotaware listening", "command", command, "listen", options.listen, "admin", options.admin, "grpc", options.grpc,
		"openai", options.proxy.OpenAI.String(), "anthropic", options.proxy.Anthropic.String())
	if err := serve(ctx, map[string]http.Handler{options.listen: handler, options.admin: admin}, rpc, options.grpc); err != nil {
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 1
	}
	return 0
}

func evictIdleBuckets(ctx context.Context, store *state.Store) {
	ticker := time.NewTicker(evictEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			store.Evict(now, evictAfter)
		}
	}
}

func serve(ctx context.Context, handlers map[string]http.Handler, rpc *grpc.Server, rpcAddress string) error {
	servers := make([]*http.Server, 0, len(handlers))
	failures := make(chan error, len(handlers)+1)
	for address, handler := range handlers {
		server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
		servers = append(servers, server)
		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				failures <- fmt.Errorf("listen on %s: %w", address, err)
			}
		}()
	}
	if rpc != nil {
		listener, err := net.Listen("tcp", rpcAddress)
		if err != nil {
			failures <- fmt.Errorf("listen on %s: %w", rpcAddress, err)
		} else {
			go func() {
				if err := rpc.Serve(listener); err != nil {
					failures <- fmt.Errorf("serve grpc on %s: %w", rpcAddress, err)
				}
			}()
		}
	}
	var failure error
	select {
	case <-ctx.Done():
	case failure = <-failures:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, server := range servers {
		failure = errors.Join(failure, server.Shutdown(shutdown))
	}
	if rpc != nil {
		stopped := make(chan struct{})
		go func() {
			rpc.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-shutdown.Done():
			rpc.Stop()
		}
	}
	return failure
}
