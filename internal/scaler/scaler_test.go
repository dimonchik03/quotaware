package scaler_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dimonchik03/quotaware/internal/headroom"
	"github.com/dimonchik03/quotaware/internal/scaler"
	"github.com/dimonchik03/quotaware/internal/scaler/externalscaler"
	"github.com/dimonchik03/quotaware/internal/state"
)

var now = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

type fakeQueue struct {
	mu    sync.Mutex
	depth int64
	err   error
}

func (q *fakeQueue) Len(context.Context, string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.depth, q.err
}

func (q *fakeQueue) set(depth int64, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.depth, q.err = depth, err
}

type fixture struct {
	service *scaler.Service
	store   *state.Store
	queue   *fakeQueue
	logs    *strings.Builder
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	store, err := state.New(0.5)
	if err != nil {
		t.Fatal(err)
	}
	queue := &fakeQueue{}
	logs := &strings.Builder{}
	cfg := scaler.DefaultConfig()
	cfg.PushInterval = 10 * time.Millisecond
	service, err := scaler.New(cfg, store, func(string) scaler.QueueDepth { return queue }, func() time.Time { return now }, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{service: service, store: store, queue: queue, logs: logs}
}

func (f *fixture) observe(key state.Key, remainingRequests, remainingTokens float64) {
	f.store.Begin(key, now)
	f.store.End(key, state.Sample{
		Status:   http.StatusOK,
		Complete: true,
		Headers:  state.HeadersPresent,
		Quotas: state.Quotas{
			Requests: headroom.Quota{Limit: 1000, Remaining: remainingRequests, ResetAt: now.Add(30 * time.Second)},
			Tokens:   headroom.Quota{Limit: 600000, Remaining: remainingTokens, ResetAt: now.Add(30 * time.Second)},
		},
		HasUsage: true,
		Usage:    1000,
		Latency:  2 * time.Second,
		At:       now,
	})
}

func ref(metadata map[string]string) *externalscaler.ScaledObjectRef {
	return &externalscaler.ScaledObjectRef{Name: "llm-workers", Namespace: "jobs", ScalerMetadata: metadata}
}

func capMetadata() map[string]string {
	return map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "maxStepUp": "100", "defaultCeiling": "1"}
}

func metric(t *testing.T, f *fixture, metadata map[string]string) int64 {
	t.Helper()
	response, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(metadata), MetricName: scaler.MetricName})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetMetricValues()[0].GetMetricValue()
}

func code(err error) codes.Code {
	return status.Code(err)
}

func TestMetadata(t *testing.T) {
	cases := []struct {
		name     string
		metadata map[string]string
		want     codes.Code
	}{
		{"rejects metadata without a bucket", map[string]string{"concurrency": "4"}, codes.InvalidArgument},
		{"rejects a bucket that is not provider/model", map[string]string{"bucket": "gpt-4.1-mini", "concurrency": "4"}, codes.InvalidArgument},
		{"requires concurrency", map[string]string{"bucket": "openai/gpt-4.1-mini"}, codes.InvalidArgument},
		{"rejects a non-numeric concurrency", map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "four"}, codes.InvalidArgument},
		{"rejects a safety factor above one", map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "safety": "2"}, codes.InvalidArgument},
		{"rejects an unparseable duration", map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "staleAfter": "soon"}, codes.InvalidArgument},
		{"rejects queue mode without a list name", map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "redisAddress": "redis:6379"}, codes.InvalidArgument},
		{"rejects a non-positive list length", map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "redisAddress": "redis:6379", "listName": "jobs", "listLength": "0"}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)

			_, err := f.service.GetMetricSpec(context.Background(), ref(tc.metadata))

			if code(err) != tc.want {
				t.Fatalf("code = %v (%v), want %v", code(err), err, tc.want)
			}
		})
	}

	t.Run("refuses queue mode when no queue factory is configured", func(t *testing.T) {
		store, _ := state.New(0.5)
		service, err := scaler.New(scaler.DefaultConfig(), store, nil, time.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		metadata := capMetadata()
		metadata["redisAddress"], metadata["listName"] = "redis:6379", "jobs"

		_, err = service.GetMetricSpec(context.Background(), ref(metadata))

		if code(err) != codes.FailedPrecondition {
			t.Fatalf("code = %v, want FailedPrecondition", code(err))
		}
	})
}

func TestGetMetricSpec(t *testing.T) {
	f := newFixture(t)

	response, err := f.service.GetMetricSpec(context.Background(), ref(capMetadata()))

	if err != nil {
		t.Fatal(err)
	}
	spec := response.GetMetricSpecs()[0]
	if spec.GetMetricName() != scaler.MetricName || spec.GetTargetSize() != 1 {
		t.Fatalf("spec = %+v, want the ceiling metric with target size one", spec)
	}
}

func TestGetMetrics(t *testing.T) {
	t.Run("reports the default ceiling before any observation", func(t *testing.T) {
		f := newFixture(t)

		if got := metric(t, f, capMetadata()); got != 1 {
			t.Fatalf("ceiling = %d, want the default of 1", got)
		}
	})

	t.Run("reports the headroom ceiling of the observed bucket", func(t *testing.T) {
		f := newFixture(t)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}, 900, 500000)

		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want 4", got)
		}
	})

	t.Run("sums the ceilings of credentials that share a model", func(t *testing.T) {
		f := newFixture(t)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "a"}, 900, 500000)

		if got := metric(t, f, capMetadata()); got != 8 {
			t.Fatalf("ceiling = %d, want 4 + 4 for the two credentials of the model", got)
		}
	})

	t.Run("narrows to one credential when the metadata names it", func(t *testing.T) {
		f := newFixture(t)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		metadata := capMetadata()
		metadata["credential"] = "b"

		if got := metric(t, f, capMetadata()); got != 8 {
			t.Fatalf("ceiling = %d, want 8 without a credential filter", got)
		}
		if got := metric(t, f, metadata); got != 4 {
			t.Fatalf("ceiling = %d, want 4 for one credential", got)
		}
	})

	t.Run("applies the step-up cap between consecutive polls and records the ceiling", func(t *testing.T) {
		f := newFixture(t)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}, 900, 500000)
		metadata := capMetadata()
		metadata["maxStepUp"] = "1"
		metadata["defaultCeiling"] = "1"

		first := metric(t, f, metadata)
		second := metric(t, f, metadata)

		if first != 4 || second != 4 {
			t.Fatalf("ceilings = %d, %d, want 4 on the first poll without a previous and 4 after", first, second)
		}
		ceilings := f.store.Ceilings()
		if len(ceilings) != 1 || ceilings[0].Value != 4 || ceilings[0].Consumer.ScaledObject != "llm-workers" {
			t.Fatalf("recorded ceilings = %+v, want one for llm-workers at 4", ceilings)
		}
		if !strings.Contains(f.logs.String(), "ceiling changed") {
			t.Fatalf("logs = %q, want the first ceiling logged", f.logs.String())
		}
	})

	t.Run("steps up from a ceiling it reported earlier", func(t *testing.T) {
		f := newFixture(t)
		key := state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}
		f.observe(key, 0, 0)
		metadata := capMetadata()
		metadata["maxStepUp"] = "1"

		exhausted := metric(t, f, metadata)
		f.observe(key, 900, 500000)
		recovering := metric(t, f, metadata)

		if exhausted != 0 || recovering != 1 {
			t.Fatalf("ceilings = %d then %d, want 0 while exhausted and 1 after one step up", exhausted, recovering)
		}
	})

	t.Run("queue mode returns the smaller of demand and ceiling", func(t *testing.T) {
		f := newFixture(t)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}, 900, 500000)
		metadata := capMetadata()
		metadata["redisAddress"], metadata["listName"], metadata["listLength"] = "redis:6379", "jobs", "10"
		f.queue.set(25, nil)

		if got := metric(t, f, metadata); got != 3 {
			t.Fatalf("metric = %d, want demand ceil(25/10) = 3 below the ceiling of 4", got)
		}
		f.queue.set(95, nil)
		if got := metric(t, f, metadata); got != 4 {
			t.Fatalf("metric = %d, want the ceiling of 4 below a demand of 10", got)
		}
	})

	t.Run("queue mode fails loudly when the queue cannot be read", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["redisAddress"], metadata["listName"] = "redis:6379", "jobs"
		f.queue.set(0, errors.New("connection refused"))

		_, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(metadata)})

		if code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", code(err))
		}
	})
}

func TestIsActive(t *testing.T) {
	t.Run("a cap trigger never activates a scaled object on its own", func(t *testing.T) {
		f := newFixture(t)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}, 900, 500000)

		response, err := f.service.IsActive(context.Background(), ref(capMetadata()))

		if err != nil || response.GetResult() {
			t.Fatalf("active = %v err = %v, want inactive", response.GetResult(), err)
		}
	})

	t.Run("a queue trigger is active while the list has items", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["redisAddress"], metadata["listName"] = "redis:6379", "jobs"
		f.queue.set(1, nil)

		response, err := f.service.IsActive(context.Background(), ref(metadata))

		if err != nil || !response.GetResult() {
			t.Fatalf("active = %v err = %v, want active", response.GetResult(), err)
		}
	})
}

func TestOverGRPC(t *testing.T) {
	f := newFixture(t)
	f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}, 900, 500000)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	f.service.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := externalscaler.NewExternalScalerClient(conn)

	t.Run("serves metrics over the wire", func(t *testing.T) {
		response, err := client.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(capMetadata())})

		if err != nil || response.GetMetricValues()[0].GetMetricValue() != 4 {
			t.Fatalf("response = %v err = %v, want a ceiling of 4", response, err)
		}
	})

	t.Run("streams the activity state and pushes a change", func(t *testing.T) {
		metadata := capMetadata()
		metadata["redisAddress"], metadata["listName"] = "redis:6379", "jobs"
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stream, err := client.StreamIsActive(ctx, ref(metadata))
		if err != nil {
			t.Fatal(err)
		}

		first, err := stream.Recv()
		if err != nil || first.GetResult() {
			t.Fatalf("first = %v err = %v, want inactive with an empty queue", first.GetResult(), err)
		}
		f.queue.set(3, nil)
		second, err := stream.Recv()
		if err != nil || !second.GetResult() {
			t.Fatalf("second = %v err = %v, want the change to active pushed", second.GetResult(), err)
		}
	})

	t.Run("leaves StreamMetricSpec unimplemented so KEDA falls back to polling", func(t *testing.T) {
		stream, err := client.StreamMetricSpec(context.Background(), ref(capMetadata()))
		if err != nil {
			t.Fatal(err)
		}

		_, err = stream.Recv()

		if code(err) != codes.Unimplemented {
			t.Fatalf("code = %v, want Unimplemented", code(err))
		}
	})
}
