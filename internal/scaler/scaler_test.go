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
	mu     sync.Mutex
	depth  int64
	err    error
	closed bool
}

func (q *fakeQueue) Len(context.Context, string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.depth, q.err
}

func (q *fakeQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return nil
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
	clock   *clock
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixtureWithLimits(t, state.DefaultLimits())
}

func newFixtureWithLimits(t *testing.T, limits state.Limits) *fixture {
	t.Helper()
	store, err := state.NewWithLimits(0.5, limits)
	if err != nil {
		t.Fatal(err)
	}
	queue := &fakeQueue{}
	logs := &strings.Builder{}
	c := &clock{now: now}
	cfg := scaler.DefaultConfig()
	cfg.PushInterval = 10 * time.Millisecond
	service, err := scaler.New(cfg, store, func(string) scaler.QueueDepth { return queue }, c.Now, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{service: service, store: store, queue: queue, logs: logs, clock: c}
}

func (f *fixture) observe(key state.Key, remainingRequests, remainingTokens float64) {
	at := f.clock.Now()
	f.store.Begin(key, at)
	f.store.End(key, state.Sample{
		Status:   http.StatusOK,
		Complete: true,
		Headers:  state.HeadersPresent,
		Quotas: state.Quotas{
			Requests: headroom.Quota{Limit: 1000, Remaining: remainingRequests, ResetAt: at.Add(30 * time.Second)},
			Tokens:   headroom.Quota{Limit: 600000, Remaining: remainingTokens, ResetAt: at.Add(30 * time.Second)},
		},
		HasUsage: true,
		Usage:    1000,
		Latency:  2 * time.Second,
		At:       at,
	})
}

func (f *fixture) respond(key state.Key, status int, headersStatus state.HeadersStatus, retryAfter time.Duration) {
	at := f.clock.Now()
	f.store.Begin(key, at)
	f.store.End(key, state.Sample{Status: status, Complete: true, Headers: headersStatus, RetryAfter: retryAfter, Latency: time.Second, At: at})
}

func ref(metadata map[string]string) *externalscaler.ScaledObjectRef {
	return &externalscaler.ScaledObjectRef{Name: "llm-workers", Namespace: "jobs", ScalerMetadata: metadata}
}

func capMetadata() map[string]string {
	return map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "maxStepUp": "100", "defaultCeiling": "1"}
}

func queueMetadata() map[string]string {
	metadata := capMetadata()
	metadata["redisAddress"], metadata["listName"], metadata["listLength"] = "redis:6379", "jobs", "10"
	return metadata
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

var bucketA = state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "a"}

func TestNew(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, _ := state.New(0.5)
	cases := []struct {
		name  string
		store *state.Store
		now   func() time.Time
		cfg   scaler.Config
	}{
		{"rejects a nil store", nil, time.Now, scaler.DefaultConfig()},
		{"rejects a nil clock", store, nil, scaler.DefaultConfig()},
		{"rejects a zero push interval", store, time.Now, scaler.Config{Defaults: headroom.DefaultConfig()}},
		{"rejects invalid defaults", store, time.Now, scaler.Config{PushInterval: time.Second}},
		{"rejects invalid aimd defaults", store, time.Now, scaler.Config{Defaults: headroom.DefaultConfig(), PushInterval: time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := scaler.New(tc.cfg, tc.store, nil, tc.now, logger); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
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

		_, err = service.GetMetricSpec(context.Background(), ref(queueMetadata()))

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
	if spec.GetMetricName() != scaler.MetricName || spec.GetTargetSize() != 1 || spec.GetTargetSizeFloat() != 1 {
		t.Fatalf("spec = %+v, want the ceiling metric with target size one in both fields", spec)
	}
}

func TestGetMetrics(t *testing.T) {
	t.Run("reports the default ceiling before any observation and records it", func(t *testing.T) {
		f := newFixture(t)

		if got := metric(t, f, capMetadata()); got != 1 {
			t.Fatalf("ceiling = %d, want the default of 1", got)
		}
		if ceilings := f.store.Ceilings(); len(ceilings) != 1 || ceilings[0].Value != 1 || ceilings[0].Consumer.Bucket.Model != "gpt-4.1-mini" {
			t.Fatalf("recorded = %+v, want the default ceiling recorded for the consumer", ceilings)
		}
	})

	t.Run("answers under the metric name KEDA asked for, in both value fields", func(t *testing.T) {
		f := newFixture(t)

		response, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(capMetadata()), MetricName: "s1-quotaware-ceiling"})

		if err != nil {
			t.Fatal(err)
		}
		value := response.GetMetricValues()[0]
		if value.GetMetricName() != "s1-quotaware-ceiling" || value.GetMetricValue() != 1 || value.GetMetricValueFloat() != 1 {
			t.Fatalf("value = %+v, want the requested name echoed with both fields set", value)
		}
	})

	t.Run("reports the headroom ceiling of the observed bucket", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)

		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want 4", got)
		}
	})

	t.Run("sums the ceilings of credentials that share a model", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "a"}, 900, 500000)

		if got := metric(t, f, capMetadata()); got != 8 {
			t.Fatalf("ceiling = %d, want 4 + 4 for the two credentials of the model", got)
		}
	})

	t.Run("clamps the sum across credentials to maxReplicas", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		metadata := capMetadata()
		metadata["maxReplicas"] = "6"

		if got := metric(t, f, metadata); got != 6 {
			t.Fatalf("ceiling = %d, want the sum clamped to 6", got)
		}
	})

	t.Run("narrows to one credential when the metadata names it", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		metadata := capMetadata()
		metadata["credential"] = "b"

		if got := metric(t, f, metadata); got != 4 {
			t.Fatalf("ceiling = %d, want 4 for one credential", got)
		}
	})

	t.Run("records the ceiling per scaled object and logs the change", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)

		metric(t, f, capMetadata())

		ceilings := f.store.Ceilings()
		if len(ceilings) != 1 || ceilings[0].Value != 4 || ceilings[0].Consumer.ScaledObject != "llm-workers" || !ceilings[0].ChangedAt.Equal(now) {
			t.Fatalf("recorded ceilings = %+v, want one for llm-workers at 4 changed now", ceilings)
		}
		if !strings.Contains(f.logs.String(), "ceiling changed") {
			t.Fatalf("logs = %q, want the first ceiling logged", f.logs.String())
		}
	})

	t.Run("steps up from a ceiling it reported earlier", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 0, 0)
		metadata := capMetadata()
		metadata["maxStepUp"] = "1"

		exhausted := metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		f.observe(bucketA, 900, 500000)
		recovering := metric(t, f, metadata)

		if exhausted != 1 || recovering != 2 {
			t.Fatalf("ceilings = %d then %d, want the minimum of 1 while exhausted and one step up after", exhausted, recovering)
		}
	})

	t.Run("does not raise twice within one step interval however often KEDA asks", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 0, 0)
		metadata := capMetadata()
		metadata["maxStepUp"] = "1"
		metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		f.observe(bucketA, 900, 500000)

		first := metric(t, f, metadata)
		second := metric(t, f, metadata)
		third := metric(t, f, metadata)

		if first != 2 || second != 2 || third != 2 {
			t.Fatalf("ceilings = %d, %d, %d, want one raise held for the step interval", first, second, third)
		}
	})

	t.Run("queue mode returns the demand when it is below the ceiling", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.queue.set(25, nil)

		if got := metric(t, f, queueMetadata()); got != 3 {
			t.Fatalf("metric = %d, want demand ceil(25/10) = 3 below the ceiling of 4", got)
		}
	})

	t.Run("queue mode returns the ceiling when demand exceeds it", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.queue.set(95, nil)

		if got := metric(t, f, queueMetadata()); got != 4 {
			t.Fatalf("metric = %d, want the ceiling of 4 below a demand of 10", got)
		}
	})

	t.Run("queue mode defaults the list length to one", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.queue.set(3, nil)
		metadata := queueMetadata()
		delete(metadata, "listLength")

		if got := metric(t, f, metadata); got != 3 {
			t.Fatalf("metric = %d, want one replica per queued item", got)
		}
	})

	t.Run("answers ResourceExhausted once too many scaled objects track a ceiling", func(t *testing.T) {
		f := newFixtureWithLimits(t, state.Limits{Buckets: state.DefaultMaxBuckets, Consumers: 1})
		f.observe(bucketA, 900, 500000)
		if _, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(capMetadata())}); err != nil {
			t.Fatal(err)
		}
		other := ref(capMetadata())
		other.Name = "another-workers"

		_, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: other})

		if code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v, want ResourceExhausted", code(err))
		}
	})

	t.Run("queue mode fails loudly when the queue cannot be read", func(t *testing.T) {
		f := newFixture(t)
		f.queue.set(0, errors.New("connection refused"))

		_, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(queueMetadata())})

		if code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", code(err))
		}
	})
}

func TestAIMD(t *testing.T) {
	t.Run("grows additively from the default when the provider sends no headers", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["defaultCeiling"] = "2"
		for range 5 {
			f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		}

		first := metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		for range 5 {
			f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		}
		second := metric(t, f, metadata)

		if first != 2 || second != 3 {
			t.Fatalf("ceilings = %d then %d, want the default and one additive increase", first, second)
		}
	})

	t.Run("halves on 429s without headers and holds for the retry-after", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["defaultCeiling"] = "8"
		f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		for range 4 {
			f.respond(bucketA, http.StatusTooManyRequests, state.HeadersMissing, 90*time.Second)
		}

		halved := metric(t, f, metadata)
		f.clock.Advance(70 * time.Second)
		for range 4 {
			f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		}
		held := metric(t, f, metadata)
		f.clock.Advance(30 * time.Second)
		for range 4 {
			f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		}
		resumed := metric(t, f, metadata)

		if halved != 4 || held != 4 || resumed != 5 {
			t.Fatalf("ceilings = %d, %d, %d, want 8 halved to 4, held through the retry-after, then one increase", halved, held, resumed)
		}
	})

	t.Run("does not halve again after the hold when the burst stopped", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["defaultCeiling"] = "8"
		f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		for range 4 {
			f.respond(bucketA, http.StatusTooManyRequests, state.HeadersMissing, 0)
		}
		metric(t, f, metadata)
		f.clock.Advance(61 * time.Second)

		if got := metric(t, f, metadata); got != 4 {
			t.Fatalf("ceiling = %d, want the halved value kept once the old 429s left the window", got)
		}
	})

	t.Run("reads the aimd knobs from the metadata", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["defaultCeiling"], metadata["aimdThreshold"], metadata["aimdIncrease"] = "8", "0.9", "3"
		f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		f.respond(bucketA, http.StatusTooManyRequests, state.HeadersMissing, 0)
		f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)

		if got := metric(t, f, metadata); got != 11 {
			t.Fatalf("ceiling = %d, want 8 + 3 because half of the responses is below a 0.9 threshold", got)
		}
	})

	t.Run("falls back to the default ceiling when the headers are older than staleHard", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["defaultCeiling"] = "2"
		f.observe(bucketA, 900, 500000)
		if got := metric(t, f, metadata); got != 4 {
			t.Fatalf("ceiling = %d, want 4 from the headers", got)
		}
		f.clock.Advance(11 * time.Minute)
		f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)

		if got := metric(t, f, metadata); got != 2 {
			t.Fatalf("ceiling = %d, want the default once the quota is stale", got)
		}
	})

	t.Run("guards a header ceiling while 429s exceed the threshold", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want 4 from the headers", got)
		}
		f.clock.Advance(20 * time.Second)
		f.observe(bucketA, 900, 500000)
		for range 3 {
			f.respond(bucketA, http.StatusTooManyRequests, state.HeadersPresent, 30*time.Second)
		}

		if got := metric(t, f, capMetadata()); got != 2 {
			t.Fatalf("ceiling = %d, want the header ceiling halved by the guard", got)
		}
	})

	t.Run("keeps the guarded value while the drain floor is above it", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		metric(t, f, capMetadata())
		f.clock.Advance(20 * time.Second)
		f.observe(bucketA, 900, 500000)
		for range 3 {
			f.respond(bucketA, http.StatusTooManyRequests, state.HeadersPresent, 0)
		}
		for range 40 {
			f.store.Begin(bucketA, f.clock.Now())
		}

		inflated := metric(t, f, capMetadata())
		f.clock.Advance(20 * time.Second)
		f.observe(bucketA, 900, 500000)
		stillHeld := metric(t, f, capMetadata())

		if inflated != 10 || stillHeld != 10 {
			t.Fatalf("ceilings = %d then %d, want the drain floor of 10 reported while the guarded bound of 2 stays recorded", inflated, stillHeld)
		}
		if ceiling, _ := f.store.Ceiling(state.Consumer{Namespace: "jobs", ScaledObject: "llm-workers", Bucket: bucketA}); ceiling.Bound != 2 {
			t.Fatalf("bound = %d, want the halved value kept apart from the reported value", ceiling.Bound)
		}
	})

	t.Run("recovers from a guard by at most maxStepUp per interval", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["maxStepUp"] = "1"
		f.observe(bucketA, 900, 500000)
		metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		f.observe(bucketA, 900, 500000)
		for range 3 {
			f.respond(bucketA, http.StatusTooManyRequests, state.HeadersPresent, 0)
		}
		if got := metric(t, f, metadata); got != 2 {
			t.Fatalf("ceiling = %d, want the guard to halve 4 to 2", got)
		}
		f.clock.Advance(61 * time.Second)
		f.observe(bucketA, 900, 500000)

		if got := metric(t, f, metadata); got != 3 {
			t.Fatalf("ceiling = %d, want one step up from the guarded value after the hold", got)
		}
	})

	t.Run("rejects an invalid aimd knob", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["aimdDecrease"] = "1.5"

		_, err := f.service.GetMetricSpec(context.Background(), ref(metadata))

		if code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument", code(err))
		}
	})
}

func TestIsActive(t *testing.T) {
	t.Run("a cap trigger never activates a scaled object on its own", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)

		response, err := f.service.IsActive(context.Background(), ref(capMetadata()))

		if err != nil || response.GetResult() {
			t.Fatalf("active = %v err = %v, want inactive", response.GetResult(), err)
		}
	})

	t.Run("a queue trigger is active while the list has items", func(t *testing.T) {
		f := newFixture(t)
		f.queue.set(1, nil)

		response, err := f.service.IsActive(context.Background(), ref(queueMetadata()))

		if err != nil || !response.GetResult() {
			t.Fatalf("active = %v err = %v, want active", response.GetResult(), err)
		}
	})
}

func TestClose(t *testing.T) {
	f := newFixture(t)
	f.queue.set(1, nil)
	metric(t, f, queueMetadata())

	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}

	if !f.queue.closed {
		t.Fatal("the cached queue client was not closed")
	}
}

func TestOverGRPC(t *testing.T) {
	f := newFixture(t)
	f.observe(bucketA, 900, 500000)
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		f.queue.set(0, nil)
		stream, err := client.StreamIsActive(ctx, ref(queueMetadata()))
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

	t.Run("ends the stream with Unavailable when the queue fails", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		f.queue.set(0, nil)
		stream, err := client.StreamIsActive(ctx, ref(queueMetadata()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}

		f.queue.set(0, errors.New("connection reset"))
		_, err = stream.Recv()

		if code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", code(err))
		}
	})

	t.Run("keeps a cap trigger stream open without sending again", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		stream, err := client.StreamIsActive(ctx, ref(capMetadata()))
		if err != nil {
			t.Fatal(err)
		}
		first, err := stream.Recv()
		if err != nil || first.GetResult() {
			t.Fatalf("first = %v err = %v, want a single inactive state", first.GetResult(), err)
		}

		_, err = stream.Recv()

		if code(err) != codes.DeadlineExceeded {
			t.Fatalf("code = %v, want the stream to stay open until the client leaves", code(err))
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
