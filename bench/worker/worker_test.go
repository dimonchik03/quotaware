package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dimonchik03/quotaware/bench/worker"
)

var now = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

type fakeQueue struct {
	mu    sync.Mutex
	items []string
}

func (q *fakeQueue) Pop(ctx context.Context, _ string, timeout time.Duration) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
			return "", nil
		}
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, nil
}

func (q *fakeQueue) push(t *testing.T, job worker.Job) {
	t.Helper()
	payload, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, string(payload))
}

type fixture struct {
	queue    *fakeQueue
	metrics  *worker.Metrics
	registry *prometheus.Registry
	statuses []int
	calls    atomic.Int64
	server   *httptest.Server
}

func newFixture(t *testing.T, statuses ...int) *fixture {
	t.Helper()
	registry := prometheus.NewRegistry()
	f := &fixture{queue: &fakeQueue{}, metrics: worker.NewMetrics(registry), registry: registry, statuses: statuses}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := int(f.calls.Add(1)) - 1
		status := http.StatusOK
		if index < len(f.statuses) {
			status = f.statuses[index]
		}
		if r.Header.Get("Authorization") != "Bearer sk-bench" {
			status = http.StatusUnauthorized
		}
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "0")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fixture) run(t *testing.T, cfg worker.Config, until func() bool) {
	t.Helper()
	cfg.ProxyURL = f.server.URL
	cfg.APIKey = "sk-bench"
	w, err := worker.New(cfg, f.queue, f.server.Client(), f.metrics, time.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !until() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if !until() {
		t.Fatal("the expected outcome was not reached in time")
	}
}

func fastConfig() worker.Config {
	cfg := worker.DefaultConfig()
	cfg.Concurrency = 1
	cfg.BaseBackoff = time.Millisecond
	cfg.MaxBackoff = 2 * time.Millisecond
	cfg.PopTimeout = 10 * time.Millisecond
	return cfg
}

func counter(c prometheus.Collector) float64 {
	return testutil.ToFloat64(c)
}

func TestWorker(t *testing.T) {
	t.Run("completes a job on a successful call and records its latency from enqueue", func(t *testing.T) {
		f := newFixture(t)
		f.queue.push(t, worker.Job{ID: 1, EnqueuedAt: time.Now().Add(-3 * time.Second), PromptTokens: 20})

		f.run(t, fastConfig(), func() bool { return counter(f.metrics.Completed) == 1 })

		if counter(f.metrics.Calls.WithLabelValues("200")) != 1 || counter(f.metrics.Retries) != 0 {
			t.Fatalf("calls = %v retries = %v, want one successful call", counter(f.metrics.Calls.WithLabelValues("200")), counter(f.metrics.Retries))
		}
		if count := testutil.CollectAndCount(f.metrics.JobLatency); count != 1 {
			t.Fatalf("latency histogram samples = %d, want 1", count)
		}
	})

	t.Run("retries a 429 after the retry-after and counts the wait", func(t *testing.T) {
		f := newFixture(t, http.StatusTooManyRequests, http.StatusTooManyRequests, http.StatusOK)
		f.queue.push(t, worker.Job{ID: 2, EnqueuedAt: time.Now(), PromptTokens: 20})

		f.run(t, fastConfig(), func() bool { return counter(f.metrics.Completed) == 1 })

		if counter(f.metrics.Calls.WithLabelValues("429")) != 2 || counter(f.metrics.Retries) != 2 {
			t.Fatalf("429s = %v retries = %v, want two of each", counter(f.metrics.Calls.WithLabelValues("429")), counter(f.metrics.Retries))
		}
		if counter(f.metrics.Backoff) <= 0 {
			t.Fatal("backoff seconds were not accumulated")
		}
	})

	t.Run("gives up after the last attempt and counts the job as failed", func(t *testing.T) {
		f := newFixture(t, 503, 503, 503)
		f.queue.push(t, worker.Job{ID: 3, EnqueuedAt: time.Now(), PromptTokens: 20})
		cfg := fastConfig()
		cfg.MaxAttempts = 3

		f.run(t, cfg, func() bool { return counter(f.metrics.Failed) == 1 })

		if counter(f.metrics.Calls.WithLabelValues("503")) != 3 || counter(f.metrics.Completed) != 0 {
			t.Fatalf("503s = %v completed = %v, want three attempts and no completion", counter(f.metrics.Calls.WithLabelValues("503")), counter(f.metrics.Completed))
		}
	})

	t.Run("does not retry a client error", func(t *testing.T) {
		f := newFixture(t, http.StatusBadRequest)
		f.queue.push(t, worker.Job{ID: 4, EnqueuedAt: time.Now(), PromptTokens: 20})

		f.run(t, fastConfig(), func() bool { return counter(f.metrics.Failed) == 1 })

		if f.calls.Load() != 1 {
			t.Fatalf("calls = %d, want exactly one", f.calls.Load())
		}
	})

	t.Run("counts an unreadable payload as failed and keeps going", func(t *testing.T) {
		f := newFixture(t)
		f.queue.mu.Lock()
		f.queue.items = append(f.queue.items, "not json")
		f.queue.mu.Unlock()
		f.queue.push(t, worker.Job{ID: 5, EnqueuedAt: time.Now(), PromptTokens: 20})

		f.run(t, fastConfig(), func() bool { return counter(f.metrics.Completed) == 1 })

		if counter(f.metrics.Failed) != 1 {
			t.Fatalf("failed = %v, want the unreadable payload counted", counter(f.metrics.Failed))
		}
	})

	t.Run("runs the configured number of concurrent loops", func(t *testing.T) {
		f := newFixture(t)
		for i := range 6 {
			f.queue.push(t, worker.Job{ID: int64(i), EnqueuedAt: time.Now(), PromptTokens: 20})
		}
		cfg := fastConfig()
		cfg.Concurrency = 3

		f.run(t, cfg, func() bool { return counter(f.metrics.Completed) == 6 })
	})

	t.Run("rejects an invalid configuration", func(t *testing.T) {
		cfg := worker.DefaultConfig()
		cfg.Concurrency = 0

		if _, err := worker.New(cfg, &fakeQueue{}, http.DefaultClient, worker.NewMetrics(prometheus.NewRegistry()), time.Now, slog.Default()); err == nil {
			t.Fatal("expected a validation error")
		}
	})
}
