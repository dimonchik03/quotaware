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
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/dimonchik03/quotaware/internal/metrics"
	"github.com/dimonchik03/quotaware/internal/proxy"
	"github.com/dimonchik03/quotaware/internal/queue"
	"github.com/dimonchik03/quotaware/internal/scaler"
	"github.com/dimonchik03/quotaware/internal/state"
	"github.com/dimonchik03/quotaware/internal/usage"
)

const (
	evictEvery           = time.Minute
	evictAfter           = 10 * time.Minute
	observeTokenVariable = "QUOTAWARE_OBSERVE_TOKEN"
)

type observerOptions struct {
	listen          string
	admin           string
	grpc            string
	alpha           float64
	limits          state.Limits
	redisAddresses  []string
	pushInterval    time.Duration
	drainTimeout    time.Duration
	observeToken    string
	observeInsecure bool
	proxy           proxy.Config
}

func parseObserverFlags(command string, args []string, stderr io.Writer, withScaler bool) (observerOptions, error) {
	flags := flag.NewFlagSet("quotaware "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	options := observerOptions{proxy: proxy.DefaultConfig(), pushInterval: scaler.DefaultConfig().PushInterval, limits: state.DefaultLimits(), drainTimeout: 10 * time.Second}
	openAI := flags.String("openai-upstream", "https://api.openai.com", "origin of the OpenAI API, without a path")
	anthropic := flags.String("anthropic-upstream", "https://api.anthropic.com", "origin of the Anthropic API, without a path")
	flags.StringVar(&options.listen, "listen", ":8080", "address the proxy listens on")
	flags.StringVar(&options.admin, "admin", ":9100", "address that serves /metrics, /healthz and /v1/observe")
	flags.Float64Var(&options.alpha, "ewma-alpha", 0.2, "weight of the newest sample in the smoothed averages")
	flags.StringVar(&options.observeToken, "observe-token", "", "bearer token collectors must present on /v1/observe (falls back to $"+observeTokenVariable+")")
	flags.BoolVar(&options.observeInsecure, "observe-insecure", false, "accept observations on /v1/observe without a token")
	flags.IntVar(&options.limits.Buckets, "max-buckets", options.limits.Buckets, "most (provider, model, credential) buckets tracked at once")
	flags.IntVar(&options.limits.Consumers, "max-scaled-objects", options.limits.Consumers, "most (scaled object, trigger, credential) ceilings tracked at once")
	flags.Int64Var(&options.proxy.MaxRequestBytes, "max-request-bytes", options.proxy.MaxRequestBytes, "largest request body accepted")
	flags.Int64Var(&options.proxy.MaxCaptureBytes, "max-capture-bytes", options.proxy.MaxCaptureBytes, "largest response body inspected for usage")
	flags.DurationVar(&options.proxy.ReadBodyTimeout, "read-body-timeout", options.proxy.ReadBodyTimeout, "how long a client may take to send a request body")
	flags.DurationVar(&options.proxy.UpstreamHeaderTimeout, "upstream-header-timeout", options.proxy.UpstreamHeaderTimeout, "how long the provider may take to answer with headers before the call fails with 504")
	flags.IntVar(&options.proxy.MaxInFlight, "max-inflight", options.proxy.MaxInFlight, "most proxied calls open at once; calls beyond it are answered 503")
	flags.DurationVar(&options.drainTimeout, "drain-timeout", options.drainTimeout, "how long open calls may finish after a shutdown signal before they are closed")
	var redisAddresses string
	if withScaler {
		flags.StringVar(&options.grpc, "grpc", ":9090", "address the KEDA external scaler listens on")
		flags.DurationVar(&options.pushInterval, "push-interval", options.pushInterval, "how often StreamIsActive re-evaluates a queue trigger")
		flags.StringVar(&redisAddresses, "redis-address", "", "comma-separated redis addresses single-trigger mode may read queues from (empty disables queue mode)")
	}
	if err := flags.Parse(args); err != nil {
		return observerOptions{}, err
	}
	if !passed(flags, "observe-token") {
		options.observeToken = os.Getenv(observeTokenVariable)
	}
	options.redisAddresses = splitAddresses(redisAddresses)
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
	if options.observeToken == "" && !options.observeInsecure {
		return observerOptions{}, errors.New("observe token is required: set -observe-token or " + observeTokenVariable + ", or pass -observe-insecure to accept unauthenticated observations")
	}
	if options.drainTimeout <= 0 {
		return observerOptions{}, errors.New("drain timeout must be positive")
	}
	return options, nil
}

func passed(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(f *flag.Flag) {
		found = found || f.Name == name
	})
	return found
}

func splitAddresses(raw string) []string {
	var addresses []string
	for _, address := range strings.Split(raw, ",") {
		if address = strings.TrimSpace(address); address != "" {
			addresses = append(addresses, address)
		}
	}
	return addresses
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
	store, err := state.New(options.alpha, options.limits)
	if err != nil {
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 2
	}
	handler, err := proxy.New(options.proxy, store, time.Now)
	if err != nil {
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 2
	}
	observer, err := proxy.NewObserver(store, time.Now, 64<<10, options.observeToken, logger)
	if err != nil {
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 2
	}
	admin := http.NewServeMux()
	admin.Handle("GET /metrics", metrics.Handler(store, time.Now))
	admin.Handle("POST "+usage.ObservePath, observer)
	admin.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	var rpc *grpc.Server
	stopStreams := func() {}
	if withScaler {
		cfg := scaler.DefaultConfig()
		cfg.PushInterval = options.pushInterval
		cfg.AllowedQueues = options.redisAddresses
		var queues scaler.QueueFactory
		if len(options.redisAddresses) > 0 {
			queues = func(address string) scaler.QueueDepth { return queue.NewRedis(address) }
		}
		service, err := scaler.New(cfg, store, queues, time.Now, logger)
		if err != nil {
			fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
			return 2
		}
		rpc = scaler.NewServer(options.limits.Consumers)
		service.Register(rpc)
		stopStreams = service.Shutdown
		defer func() {
			if err := service.Close(); err != nil {
				logger.Warn("closing queue clients", "error", err.Error())
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go evictIdleBuckets(ctx, store)
	logger.Info("quotaware listening", listeningAttributes(command, options)...)
	endpoints := []endpoint{{address: options.listen, handler: handler}, {address: options.admin, handler: admin, readTimeout: options.proxy.ReadBodyTimeout}}
	if err := serve(ctx, endpoints, rpc, options.grpc, stopStreams, options.drainTimeout, logger); err != nil {
		fmt.Fprintf(stderr, "quotaware %s: %v\n", command, err)
		return 1
	}
	return 0
}

func listeningAttributes(command string, options observerOptions) []any {
	return []any{"command", command, "listen", options.listen, "admin", options.admin, "grpc", options.grpc,
		"openai", options.proxy.OpenAI.Redacted(), "anthropic", options.proxy.Anthropic.Redacted()}
}

type endpoint struct {
	address     string
	handler     http.Handler
	readTimeout time.Duration
}

func newServer(e endpoint) *http.Server {
	return &http.Server{Addr: e.address, Handler: e.handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: e.readTimeout, IdleTimeout: 2 * time.Minute}
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

func serve(ctx context.Context, endpoints []endpoint, rpc *grpc.Server, rpcAddress string, stopStreams func(), drain time.Duration, logger *slog.Logger) error {
	servers := make([]*http.Server, 0, len(endpoints))
	failures := make(chan error, len(endpoints)+1)
	for _, e := range endpoints {
		address := e.address
		server := newServer(e)
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
	shutdown, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	for _, server := range servers {
		switch err := server.Shutdown(shutdown); {
		case errors.Is(err, context.DeadlineExceeded):
			logger.Warn("drain deadline passed with connections still open, closing them", "address", server.Addr, "drain", drain)
			failure = errors.Join(failure, server.Close())
		case err != nil:
			failure = errors.Join(failure, err)
		}
	}
	if rpc != nil {
		stopStreams()
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
