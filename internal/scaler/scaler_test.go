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
	cfg.AllowedQueues = []string{"redis:6379"}
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
		HasUsage:  true,
		Usage:     1000,
		Latency:   2 * time.Second,
		HeadersAt: at,
		At:        at,
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

func bucketCeilings(f *fixture) []state.Ceiling {
	var buckets []state.Ceiling
	for _, c := range f.store.Ceilings() {
		if c.Consumer.Bucket.Key != scaler.CombinedKey {
			buckets = append(buckets, c)
		}
	}
	return buckets
}

func combinedCeilings(f *fixture) []state.Ceiling {
	var combined []state.Ceiling
	for _, c := range f.store.Ceilings() {
		if c.Consumer.Bucket.Key == scaler.CombinedKey {
			combined = append(combined, c)
		}
	}
	return combined
}

func perCredentialMetadata() map[string]string {
	metadata := capMetadata()
	metadata["quotaScope"] = "credential"
	return metadata
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
		{"rejects a provider quotaware does not parse", map[string]string{"bucket": "gemini/gemini-2.5-pro", "concurrency": "4"}, codes.InvalidArgument},
		{"rejects a model longer than the label limit", map[string]string{"bucket": "openai/" + strings.Repeat("m", 257), "concurrency": "4"}, codes.InvalidArgument},
		{"rejects a credential that is not the hash of a key", map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "credential": "sk-live-not-a-hash"}, codes.InvalidArgument},
		{"rejects an unknown quota scope", map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "quotaScope": "organisation"}, codes.InvalidArgument},
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
		if ceilings := bucketCeilings(f); len(ceilings) != 1 || ceilings[0].Value != 1 || ceilings[0].Consumer.Bucket.Model != "gpt-4.1-mini" {
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

	t.Run("hands a bucket with token windows but no usage yet to AIMD instead of the default", func(t *testing.T) {
		f := newFixture(t)
		tokensOnly := func() {
			at := f.clock.Now()
			f.store.Begin(bucketA, at)
			f.store.End(bucketA, state.Sample{
				Status:    http.StatusOK,
				Complete:  true,
				Headers:   state.HeadersPresent,
				Quotas:    state.Quotas{Tokens: headroom.Quota{Limit: 600000, Remaining: 500000, ResetAt: at.Add(30 * time.Second)}},
				Latency:   time.Second,
				HeadersAt: at,
				At:        at,
			})
		}
		tokensOnly()
		started := metric(t, f, capMetadata())
		f.clock.Advance(20 * time.Second)
		tokensOnly()

		increased := metric(t, f, capMetadata())

		if started != 1 || increased != 2 {
			t.Fatalf("ceilings = %d then %d, want AIMD to start at 1 and add one after a clean interval", started, increased)
		}
	})

	t.Run("takes one ceiling for credentials that share the model's quota", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "a"}, 900, 500000)

		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want 4 for one quota seen through two credentials", got)
		}
	})

	t.Run("follows the freshest headers of a quota that several credentials share", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.clock.Advance(30 * time.Second)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 0, 0)

		if got := metric(t, f, capMetadata()); got != 1 {
			t.Fatalf("ceiling = %d, want 1 from the exhausted quota the newer headers report", got)
		}
	})

	t.Run("limits a raise of the value KEDA receives when a new credential reports more headroom", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["maxStepUp"] = "2"
		f.observe(bucketA, 0, 0)
		exhausted := metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)

		recovering := metric(t, f, metadata)

		if exhausted != 1 || recovering != 3 {
			t.Fatalf("ceilings = %d then %d, want 1 and then one step of 2, not the new credential's 4", exhausted, recovering)
		}
	})

	t.Run("never raises twice when KEDA polls in parallel", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["maxStepUp"] = "2"
		f.observe(bucketA, 0, 0)
		metric(t, f, metadata)
		f.clock.Advance(20 * time.Second)
		f.observe(bucketA, 900, 500000)
		results := make(chan *externalscaler.GetMetricsResponse, 16)
		failures := make(chan error, 16)
		var wg sync.WaitGroup

		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				response, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(metadata), MetricName: scaler.MetricName})
				if err != nil {
					failures <- err
					return
				}
				results <- response
			}()
		}
		wg.Wait()
		close(results)
		close(failures)

		for err := range failures {
			t.Fatal(err)
		}
		for response := range results {
			if got := response.GetMetricValues()[0].GetMetricValue(); got != 3 {
				t.Fatalf("ceiling = %d, want every parallel poll to see the one raise from 1 to 3", got)
			}
		}
	})

	t.Run("records the value KEDA receives under the combined key", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)

		metric(t, f, capMetadata())

		combined := combinedCeilings(f)
		if len(combined) != 1 || combined[0].Value != 4 || len(bucketCeilings(f)) != 2 {
			t.Fatalf("combined = %+v buckets = %+v, want one combined ceiling of 4 beside the two bucket ceilings", combined, bucketCeilings(f))
		}
	})

	t.Run("sums the ceilings of credentials when each has a quota of its own", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "a"}, 900, 500000)

		if got := metric(t, f, perCredentialMetadata()); got != 8 {
			t.Fatalf("ceiling = %d, want 4 + 4 for the two credentials of the model", got)
		}
	})

	t.Run("keeps pods for every call open across the credentials that share the quota", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		other := state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}
		f.observe(other, 900, 500000)
		for range 20 {
			f.store.Begin(bucketA, f.clock.Now())
			f.store.Begin(other, f.clock.Now())
		}

		if got := metric(t, f, capMetadata()); got != 10 {
			t.Fatalf("ceiling = %d, want ceil(40/4) = 10 pods for the calls open on both credentials", got)
		}
	})

	t.Run("leaves a credential the provider never accepted out of the sum", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		for range 3 {
			f.respond(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, http.StatusUnauthorized, state.HeadersMissing, 0)
		}

		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want 4 with the refused credential ignored", got)
		}
		if ceilings := bucketCeilings(f); len(ceilings) != 1 {
			t.Fatalf("recorded = %+v, want no ceiling tracked for the refused credential", ceilings)
		}
	})

	t.Run("keeps the drain floor of calls open on a credential the provider has not answered yet", func(t *testing.T) {
		f := newFixture(t)
		for range 40 {
			f.store.Begin(bucketA, f.clock.Now())
		}

		if got := metric(t, f, capMetadata()); got != 10 {
			t.Fatalf("ceiling = %d, want ceil(40/4) = 10 pods for the open calls", got)
		}
	})

	t.Run("raises the shared ceiling to the drain floor of calls open on an unanswered credential", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		for range 40 {
			f.store.Begin(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, f.clock.Now())
		}

		if got := metric(t, f, capMetadata()); got != 10 {
			t.Fatalf("ceiling = %d, want ceil(40/4) = 10 above the headroom ceiling of 4", got)
		}
	})

	t.Run("counts the calls open on a credential whose first answer was a 5xx", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		failing := state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}
		f.respond(failing, http.StatusServiceUnavailable, state.HeadersMissing, 0)
		for range 40 {
			f.store.Begin(failing, f.clock.Now())
		}

		if got := metric(t, f, capMetadata()); got != 10 {
			t.Fatalf("ceiling = %d, want ceil(40/4) = 10 for the open calls of a credential the provider did not refuse", got)
		}
	})

	t.Run("adds the drain floor of an unanswered credential to the summed ceilings", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		for range 40 {
			f.store.Begin(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, f.clock.Now())
		}

		if got := metric(t, f, perCredentialMetadata()); got != 14 {
			t.Fatalf("ceiling = %d, want 4 from the headers plus 10 for the open calls", got)
		}
	})

	t.Run("leaves the open calls of a credential the provider refused out of the drain floor", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		refused := state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}
		f.respond(refused, http.StatusUnauthorized, state.HeadersMissing, 0)
		for range 40 {
			f.store.Begin(refused, f.clock.Now())
		}

		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want 4 with the refused credential's open calls ignored", got)
		}
	})

	t.Run("rounds the drain floor of unanswered credentials once across them", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		for _, credential := range []string{"b", "c", "d", "e"} {
			f.store.Begin(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: credential}, f.clock.Now())
		}

		if got := metric(t, f, perCredentialMetadata()); got != 5 {
			t.Fatalf("ceiling = %d, want 4 plus ceil(4/4) = 1 for four single open calls", got)
		}
	})

	t.Run("counts a credential that only saw 429s", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.respond(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, http.StatusTooManyRequests, state.HeadersMissing, 0)

		if got := metric(t, f, perCredentialMetadata()); got != 5 {
			t.Fatalf("ceiling = %d, want 4 plus the default of the rate-limited credential", got)
		}
	})

	t.Run("clamps the sum across credentials to maxReplicas", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "b"}, 900, 500000)
		metadata := perCredentialMetadata()
		metadata["maxReplicas"], metadata["maxStepUp"] = "6", "6"

		if got := metric(t, f, metadata); got != 6 {
			t.Fatalf("ceiling = %d, want the sum clamped to 6", got)
		}
	})

	t.Run("narrows to one credential when the metadata names it", func(t *testing.T) {
		f := newFixture(t)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "0123456789ab"}, 900, 500000)
		f.clock.Advance(time.Second)
		f.observe(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "ba9876543210"}, 0, 0)
		metadata := capMetadata()
		metadata["credential"] = "0123456789ab"

		if got := metric(t, f, metadata); got != 4 {
			t.Fatalf("ceiling = %d, want 4 from the named credential, not 1 from the newer exhausted one", got)
		}
	})

	t.Run("records the ceiling per scaled object and logs the change", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)

		metric(t, f, capMetadata())

		ceilings := bucketCeilings(f)
		if len(ceilings) != 1 || ceilings[0].Value != 4 || ceilings[0].Consumer.ScaledObject != "llm-workers" || !ceilings[0].BoundChangedAt.Equal(now) {
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

	t.Run("measures the step interval from the last change of the bound, not from a drain floor that moved", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 0, 0)
		metric(t, f, capMetadata())
		f.clock.Advance(20 * time.Second)
		for range 8 {
			f.store.Begin(bucketA, f.clock.Now())
		}
		draining := metric(t, f, capMetadata())
		f.clock.Advance(5 * time.Second)
		f.observe(bucketA, 900, 500000)

		recovered := metric(t, f, capMetadata())

		if draining != 2 || recovered != 4 {
			t.Fatalf("ceilings = %d then %d, want the drain floor of 2 and then the raise to 4, since the bound last changed 25 s ago", draining, recovered)
		}
	})

	t.Run("keeps the ceiling of a trigger apart from a caller that sends other metadata for the same scaled object", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want 4 before the other caller", got)
		}
		other := capMetadata()
		other["maxReplicas"], other["maxStepUp"] = "1", "1"

		if got := metric(t, f, other); got != 1 {
			t.Fatalf("ceiling = %d, want the other caller clamped to its own maxReplicas", got)
		}
		if got := metric(t, f, capMetadata()); got != 4 {
			t.Fatalf("ceiling = %d, want the trigger's own ceiling untouched by the other caller", got)
		}
		if ceilings := bucketCeilings(f); len(ceilings) != 2 || ceilings[0].Consumer.Trigger == ceilings[1].Consumer.Trigger {
			t.Fatalf("recorded = %+v, want one entry per trigger", ceilings)
		}
	})

	t.Run("keeps a trigger apart from a caller that moves its fields across keys", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		metric(t, f, capMetadata())
		resplit := map[string]string{"bucket": "openai/gpt-4.1-mini", "concurrency": "4", "defaultCeiling\x001\x00maxStepUp": "100"}

		metric(t, f, resplit)

		if ceilings := bucketCeilings(f); len(ceilings) != 2 {
			t.Fatalf("recorded = %+v, want the re-split metadata kept in its own entry", ceilings)
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

	t.Run("queue mode never reports fewer pods than the calls still open need", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		for range 40 {
			f.store.Begin(bucketA, f.clock.Now())
		}
		f.queue.set(0, nil)

		if got := metric(t, f, queueMetadata()); got != 10 {
			t.Fatalf("metric = %d, want ceil(40/4) = 10 pods for the open calls although the queue is empty", got)
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
		f := newFixtureWithLimits(t, state.Limits{Buckets: state.DefaultMaxBuckets, Consumers: 2})
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

	t.Run("refuses a redis address that is not on the allow list", func(t *testing.T) {
		f := newFixture(t)
		metadata := queueMetadata()
		metadata["redisAddress"] = "10.0.0.5:6379"

		_, err := f.service.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(metadata)})

		if code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "10.0.0.5:6379") {
			t.Fatalf("err = %v, want PermissionDenied naming the address", err)
		}
	})

	t.Run("rejects a scaled object name longer than a Kubernetes name", func(t *testing.T) {
		f := newFixture(t)
		long := ref(capMetadata())
		long.Name = strings.Repeat("n", 254)

		_, err := f.service.GetMetricSpec(context.Background(), long)

		if code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument", code(err))
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

	t.Run("holds the ceiling while the provider answers 5xx instead of counting them as clean traffic", func(t *testing.T) {
		f := newFixture(t)
		metadata := capMetadata()
		metadata["defaultCeiling"] = "2"
		for range 5 {
			f.respond(bucketA, http.StatusOK, state.HeadersMissing, 0)
		}
		if got := metric(t, f, metadata); got != 2 {
			t.Fatalf("ceiling = %d, want the default of 2", got)
		}
		f.clock.Advance(70 * time.Second)
		for range 5 {
			f.respond(bucketA, http.StatusServiceUnavailable, state.HeadersMissing, 0)
		}

		if got := metric(t, f, metadata); got != 2 {
			t.Fatalf("ceiling = %d, want the ceiling held through an outage", got)
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
		if ceilings := bucketCeilings(f); len(ceilings) != 1 || ceilings[0].Bound != 2 {
			t.Fatalf("ceilings = %+v, want the halved value kept apart from the reported value", ceilings)
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

	t.Run("a queue trigger stays active while calls are open on an empty list", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.store.Begin(bucketA, f.clock.Now())
		f.queue.set(0, nil)

		response, err := f.service.IsActive(context.Background(), ref(queueMetadata()))

		if err != nil || !response.GetResult() {
			t.Fatalf("active = %v err = %v, want active until the open call ends", response.GetResult(), err)
		}
	})

	t.Run("a queue trigger is inactive on an empty list without open calls", func(t *testing.T) {
		f := newFixture(t)
		f.observe(bucketA, 900, 500000)
		f.queue.set(0, nil)

		response, err := f.service.IsActive(context.Background(), ref(queueMetadata()))

		if err != nil || response.GetResult() {
			t.Fatalf("active = %v err = %v, want inactive", response.GetResult(), err)
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
	const servedObjects = 2
	server := scaler.NewServer(servedObjects)
	f.service.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	client := dial(t, listener)

	t.Run("refuses a request larger than any trigger needs", func(t *testing.T) {
		metadata := capMetadata()
		metadata["padding"] = strings.Repeat("x", 256<<10)

		_, err := client.GetMetrics(context.Background(), &externalscaler.GetMetricsRequest{ScaledObjectRef: ref(metadata)})

		if code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v (%v), want ResourceExhausted for a 256 KiB request", code(err), err)
		}
	})

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

	t.Run("holds a stream beyond two per scaled object it serves on one connection", func(t *testing.T) {
		own := dial(t, listener)
		parked, release := context.WithCancel(context.Background())
		defer release()
		park(t, own, parked, 2*servedObjects)
		waiting, stopWaiting := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer stopWaiting()

		extra, err := own.StreamIsActive(waiting, ref(capMetadata()))
		if err == nil {
			_, err = extra.Recv()
		}

		if code(err) != codes.DeadlineExceeded {
			t.Fatalf("code = %v, want the stream beyond the limit to wait", code(err))
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

func TestNewServerSaturatesTheStreamCapOfAHugeLimit(t *testing.T) {
	f := newFixture(t)
	listener := bufconn.Listen(1 << 20)
	server := scaler.NewServer(1<<31 + 1)
	f.service.Register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	parked, release := context.WithTimeout(context.Background(), 5*time.Second)
	defer release()

	park(t, dial(t, listener), parked, 3)
}

func dial(t *testing.T, listener *bufconn.Listener) externalscaler.ExternalScalerClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return externalscaler.NewExternalScalerClient(conn)
}

func park(t *testing.T, client externalscaler.ExternalScalerClient, ctx context.Context, streams int) {
	t.Helper()
	for range streams {
		stream, err := client.StreamIsActive(ctx, ref(capMetadata()))
		if err == nil {
			_, err = stream.Recv()
		}
		if err != nil {
			t.Fatalf("stream did not open: %v", err)
		}
	}
}
