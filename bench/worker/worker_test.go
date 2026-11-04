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
	queue      *fakeQueue
	metrics    *worker.Metrics
	registry   *prometheus.Registry
	statuses   []int
	retryAfter string
	breakFirst bool
	hold       time.Duration
	calls      atomic.Int64
	open       atomic.Int64
	peak       atomic.Int64
	server     *httptest.Server
}

func (f *fixture) enter() {
	open := f.open.Add(1)
	for {
		peak := f.peak.Load()
		if open <= peak || f.peak.CompareAndSwap(peak, open) {
			return
		}
	}
}

func newFixture(t *testing.T, statuses ...int) *fixture {
	t.Helper()
	registry := prometheus.NewRegistry()
	f := &fixture{queue: &fakeQueue{}, metrics: worker.NewMetrics(registry), registry: registry, statuses: statuses, retryAfter: "0"}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.enter()
		defer f.open.Add(-1)
		time.Sleep(f.hold)
		index := int(f.calls.Add(1)) - 1
		if index == 0 && f.breakFirst {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"usage":`)
			return
		}
		status := http.StatusOK
		if index < len(f.statuses) {
			status = f.statuses[index]
		}
		if r.Header.Get("Authorization") != "Bearer sk-bench" {
			status = http.StatusUnauthorized
		}
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", f.retryAfter)
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

	t.Run("never retries before the retry-after, whatever the jitter", func(t *testing.T) {
		f := newFixture(t, http.StatusTooManyRequests, http.StatusOK)
		f.retryAfter = "1"
		f.queue.push(t, worker.Job{ID: 7, EnqueuedAt: time.Now(), PromptTokens: 20})
		started := time.Now()

		f.run(t, fastConfig(), func() bool { return counter(f.metrics.Completed) == 1 })

		if elapsed := time.Since(started); elapsed < time.Second {
			t.Fatalf("retried after %v, want at least the retry-after of 1s", elapsed)
		}
		if counter(f.metrics.Backoff) < 1 {
			t.Fatalf("backoff = %v s, want at least the retry-after", counter(f.metrics.Backoff))
		}
	})

	t.Run("counts a job abandoned on shutdown while it waits to retry", func(t *testing.T) {
		f := newFixture(t, http.StatusTooManyRequests)
		f.retryAfter = "60"
		f.queue.push(t, worker.Job{ID: 8, EnqueuedAt: time.Now(), PromptTokens: 20})

		f.run(t, fastConfig(), func() bool { return counter(f.metrics.Retries) == 1 })

		if counter(f.metrics.Abandoned) != 1 || counter(f.metrics.Completed) != 0 || counter(f.metrics.Failed) != 0 {
			t.Fatalf("abandoned = %v completed = %v failed = %v, want the job counted as abandoned only", counter(f.metrics.Abandoned), counter(f.metrics.Completed), counter(f.metrics.Failed))
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

	t.Run("retries a success whose body broke before the end instead of counting the job done", func(t *testing.T) {
		f := newFixture(t)
		f.breakFirst = true
		f.queue.push(t, worker.Job{ID: 5, EnqueuedAt: time.Now(), PromptTokens: 20})

		f.run(t, fastConfig(), func() bool { return counter(f.metrics.Completed) == 1 })

		if f.calls.Load() != 2 || counter(f.metrics.Retries) != 1 {
			t.Fatalf("calls = %d retries = %v, want the broken body retried once", f.calls.Load(), counter(f.metrics.Retries))
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
		f.hold = 50 * time.Millisecond
		cfg := fastConfig()
		cfg.Concurrency = 3

		f.run(t, cfg, func() bool { return counter(f.metrics.Completed) == 6 })

		if peak := f.peak.Load(); peak != 3 {
			t.Fatalf("peak calls in flight = %d, want 3", peak)
		}
	})

	t.Run("exports the call counters at zero before the first call so a rate sees the first 429", func(t *testing.T) {
		registry := prometheus.NewRegistry()

		metrics := worker.NewMetrics(registry)

		if calls := testutil.CollectAndCount(metrics.Calls); calls != 3 {
			t.Fatalf("call series = %d, want 200, 429 and error exported before any call", calls)
		}
	})

	t.Run("rejects an invalid configuration", func(t *testing.T) {
		cfg := worker.DefaultConfig()
		cfg.Concurrency = 0

		if _, err := worker.New(cfg, &fakeQueue{}, http.DefaultClient, worker.NewMetrics(prometheus.NewRegistry()), time.Now, slog.Default()); err == nil {
			t.Fatal("expected a validation error")
		}
	})
}
