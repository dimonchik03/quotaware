package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	List        string
	ProxyURL    string
	Model       string
	APIKey      string
	Concurrency int
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	PopTimeout  time.Duration
	CallTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		List:        "jobs",
		ProxyURL:    "http://quotaware:8080",
		Model:       "gpt-4.1-mini",
		Concurrency: 2,
		MaxAttempts: 6,
		BaseBackoff: time.Second,
		MaxBackoff:  60 * time.Second,
		PopTimeout:  time.Second,
		CallTimeout: 2 * time.Minute,
	}
}

func (c Config) Validate() error {
	switch {
	case c.List == "" || c.ProxyURL == "" || c.Model == "":
		return errors.New("list, proxy url and model are required")
	case c.Concurrency <= 0:
		return errors.New("concurrency must be positive")
	case c.MaxAttempts <= 0:
		return errors.New("max attempts must be positive")
	case c.BaseBackoff <= 0 || c.MaxBackoff < c.BaseBackoff:
		return errors.New("base backoff must be positive and max backoff at least base backoff")
	case c.PopTimeout <= 0 || c.CallTimeout <= 0:
		return errors.New("pop and call timeouts must be positive")
	}
	return nil
}

type Job struct {
	ID           int64     `json:"id"`
	EnqueuedAt   time.Time `json:"enqueued_at"`
	PromptTokens int       `json:"prompt_tokens"`
}

type Queue interface {
	Pop(ctx context.Context, name string, timeout time.Duration) (string, error)
}

type Metrics struct {
	Completed   prometheus.Counter
	Failed      prometheus.Counter
	Abandoned   prometheus.Counter
	Calls       *prometheus.CounterVec
	Retries     prometheus.Counter
	Backoff     prometheus.Counter
	InFlight    prometheus.Gauge
	JobLatency  prometheus.Histogram
	CallLatency prometheus.Histogram
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		Completed:   prometheus.NewCounter(prometheus.CounterOpts{Name: "worker_jobs_completed_total", Help: "Jobs finished with a successful provider call."}),
		Failed:      prometheus.NewCounter(prometheus.CounterOpts{Name: "worker_jobs_failed_total", Help: "Jobs given up after the last attempt or with an unreadable payload."}),
		Abandoned:   prometheus.NewCounter(prometheus.CounterOpts{Name: "worker_jobs_abandoned_total", Help: "Jobs popped from the queue but dropped because the worker was shutting down."}),
		Calls:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "worker_calls_total", Help: "Provider calls by HTTP status, or error for transport failures."}, []string{"status"}),
		Retries:     prometheus.NewCounter(prometheus.CounterOpts{Name: "worker_retries_total", Help: "Calls repeated after a 429, a 5xx or a transport failure."}),
		Backoff:     prometheus.NewCounter(prometheus.CounterOpts{Name: "worker_backoff_seconds_total", Help: "Seconds spent waiting before a retry."}),
		InFlight:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "worker_calls_inflight", Help: "Provider calls currently open."}),
		JobLatency:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "worker_job_latency_seconds", Help: "Seconds from enqueue to completion.", Buckets: prometheus.ExponentialBuckets(0.5, 1.5, 24)}),
		CallLatency: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "worker_call_latency_seconds", Help: "Seconds per provider call attempt.", Buckets: prometheus.ExponentialBuckets(0.1, 1.5, 20)}),
	}
	registry.MustRegister(m.Completed, m.Failed, m.Abandoned, m.Calls, m.Retries, m.Backoff, m.InFlight, m.JobLatency, m.CallLatency)
	for _, status := range []string{"200", "429", "error"} {
		m.Calls.WithLabelValues(status)
	}
	return m
}

type Worker struct {
	cfg     Config
	queue   Queue
	client  *http.Client
	metrics *Metrics
	now     func() time.Time
	sleep   func(context.Context, time.Duration) bool
	logger  *slog.Logger
}

func New(cfg Config, queue Queue, client *http.Client, metrics *Metrics, now func() time.Time, logger *slog.Logger) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch {
	case queue == nil || client == nil || metrics == nil || now == nil || logger == nil:
		return nil, errors.New("queue, client, metrics, clock and logger are required")
	}
	return &Worker{cfg: cfg, queue: queue, client: client, metrics: metrics, now: now, sleep: sleepContext, logger: logger}, nil
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

func (w *Worker) Run(ctx context.Context) {
	var group sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			w.loop(ctx)
		}()
	}
	group.Wait()
}

func (w *Worker) loop(ctx context.Context) {
	for ctx.Err() == nil {
		payload, err := w.queue.Pop(ctx, w.cfg.List, w.cfg.PopTimeout)
		if ctx.Err() != nil {
			if payload != "" {
				w.metrics.Abandoned.Inc()
			}
			return
		}
		if err != nil {
			w.logger.Warn("queue pop failed", "error", err.Error())
			w.sleep(ctx, w.cfg.BaseBackoff)
			continue
		}
		if payload == "" {
			continue
		}
		var job Job
		if err := json.Unmarshal([]byte(payload), &job); err != nil {
			w.metrics.Failed.Inc()
			w.logger.Warn("unreadable job", "error", err.Error())
			continue
		}
		w.process(ctx, job)
	}
}

func (w *Worker) process(ctx context.Context, job Job) {
	for attempt := 1; attempt <= w.cfg.MaxAttempts; attempt++ {
		outcome := w.call(job)
		if outcome.done {
			w.metrics.Completed.Inc()
			w.metrics.JobLatency.Observe(w.now().Sub(job.EnqueuedAt).Seconds())
			return
		}
		if !outcome.retryable || attempt == w.cfg.MaxAttempts {
			w.metrics.Failed.Inc()
			w.logger.Warn("job failed", "id", job.ID, "attempt", attempt, "status", outcome.status)
			return
		}
		wait := w.backoff(attempt, outcome.retryAfter)
		w.metrics.Retries.Inc()
		w.metrics.Backoff.Add(wait.Seconds())
		if !w.sleep(ctx, wait) {
			w.metrics.Abandoned.Inc()
			w.logger.Info("job abandoned on shutdown", "id", job.ID)
			return
		}
	}
}

type outcome struct {
	done       bool
	retryable  bool
	status     string
	retryAfter time.Duration
}

func (w *Worker) call(job Job) outcome {
	body, err := json.Marshal(map[string]any{
		"model":    w.cfg.Model,
		"messages": []map[string]string{{"role": "user", "content": prompt(job.PromptTokens)}},
	})
	if err != nil {
		return outcome{status: "encode"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.CallTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(w.cfg.ProxyURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return outcome{status: "request"}
	}
	request.Header.Set("Content-Type", "application/json")
	if w.cfg.APIKey != "" {
		request.Header.Set("Authorization", "Bearer "+w.cfg.APIKey)
	}
	started := w.now()
	w.metrics.InFlight.Inc()
	response, err := w.client.Do(request)
	w.metrics.InFlight.Dec()
	w.metrics.CallLatency.Observe(w.now().Sub(started).Seconds())
	if err != nil {
		w.metrics.Calls.WithLabelValues("error").Inc()
		return outcome{retryable: true, status: "error"}
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	status := strconv.Itoa(response.StatusCode)
	w.metrics.Calls.WithLabelValues(status).Inc()
	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		return outcome{done: true, status: status}
	case response.StatusCode == http.StatusTooManyRequests:
		return outcome{retryable: true, status: status, retryAfter: retryAfter(response.Header)}
	case response.StatusCode >= 500:
		return outcome{retryable: true, status: status}
	default:
		return outcome{status: status}
	}
}

func retryAfter(h http.Header) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After")))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func (w *Worker) backoff(attempt int, retryAfter time.Duration) time.Duration {
	wait := time.Duration(float64(w.cfg.BaseBackoff) * math.Pow(2, float64(attempt-1)))
	wait = min(wait, w.cfg.MaxBackoff)
	jitter := 1 + (rand.Float64()*0.5 - 0.25)
	wait = time.Duration(float64(wait) * jitter)
	return max(wait, retryAfter)
}

func prompt(tokens int) string {
	if tokens <= 0 {
		tokens = 1
	}
	return strings.TrimSpace(strings.Repeat("job ", tokens))
}
