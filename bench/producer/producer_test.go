package producer_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dimonchik03/quotaware/bench/producer"
)

var start = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

type fakeQueue struct {
	items    []string
	pushed   []string
	cleared  int
	failing  error
	drainPer int
}

func (q *fakeQueue) Push(_ context.Context, _ string, payloads ...string) error {
	if q.failing != nil {
		return q.failing
	}
	q.items = append(q.items, payloads...)
	q.pushed = append(q.pushed, payloads...)
	return nil
}

func (q *fakeQueue) Len(context.Context, string) (int64, error) {
	length := int64(len(q.items))
	if q.drainPer > 0 {
		q.items = q.items[min(q.drainPer, len(q.items)):]
	}
	return length, nil
}

func (q *fakeQueue) Clear(context.Context, string) error {
	q.cleared++
	q.items = nil
	return nil
}

type clock struct {
	now time.Time
}

func (c *clock) Now() time.Time {
	return c.now
}

func (c *clock) sleep(_ context.Context, d time.Duration) bool {
	c.now = c.now.Add(d)
	return true
}

func runProducer(cfg producer.Config, queue *fakeQueue) (*producer.Metrics, error) {
	c := &clock{now: start}
	metrics := producer.NewMetrics(prometheus.NewRegistry())
	p, err := producer.New(cfg, queue, metrics, c.Now, c.sleep, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		return nil, err
	}
	return metrics, p.Run(context.Background())
}

func run(t *testing.T, cfg producer.Config, queue *fakeQueue) *producer.Metrics {
	t.Helper()
	queue.drainPer = max(queue.drainPer, 40)
	metrics, err := runProducer(cfg, queue)
	if err != nil {
		t.Fatal(err)
	}
	return metrics
}

func config() producer.Config {
	cfg := producer.DefaultConfig()
	cfg.QuotaPerSecond = 10
	cfg.Phases = []producer.Phase{
		{Name: "half", Duration: 10 * time.Second, Multiplier: 0.5},
		{Name: "burst", Duration: 10 * time.Second, Multiplier: 3},
		{Name: "idle", Duration: 5 * time.Second, Multiplier: 0},
	}
	cfg.Tick = 100 * time.Millisecond
	cfg.DrainTimeout = time.Minute
	return cfg
}

func TestProducer(t *testing.T) {
	t.Run("pushes the quota multiple per phase and nothing while idle", func(t *testing.T) {
		queue := &fakeQueue{}
		cfg := config()
		cfg.DrainTimeout = 0

		metrics, err := runProducer(cfg, queue)

		if err == nil || testutil.ToFloat64(metrics.Jobs) != 350 || len(queue.pushed) != 350 {
			t.Fatalf("jobs = %d err = %v, want 350 pushed and a drain timeout with nobody consuming", len(queue.pushed), err)
		}
		if queue.cleared != 1 {
			t.Fatalf("cleared = %d, want the list cleared once before the run", queue.cleared)
		}
	})

	t.Run("returns once the queue has drained after the schedule", func(t *testing.T) {
		queue := &fakeQueue{drainPer: 40}

		metrics := run(t, config(), queue)

		if len(queue.items) != 0 || testutil.ToFloat64(metrics.QueueLength) != 0 {
			t.Fatalf("queue = %d items, gauge = %v, want both at zero", len(queue.items), testutil.ToFloat64(metrics.QueueLength))
		}
	})

	t.Run("stamps jobs with sequential ids and the enqueue time", func(t *testing.T) {
		queue := &fakeQueue{}

		run(t, config(), queue)

		var first, last producer.Job
		if err := json.Unmarshal([]byte(queue.pushed[0]), &first); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(queue.pushed[len(queue.pushed)-1]), &last); err != nil {
			t.Fatal(err)
		}
		if first.ID != 0 || last.ID != 349 || first.EnqueuedAt.Before(start) || !last.EnqueuedAt.After(first.EnqueuedAt) {
			t.Fatalf("first = %+v last = %+v, want ids 0..349 with increasing timestamps", first, last)
		}
	})

	t.Run("is deterministic for a seed", func(t *testing.T) {
		a, b := &fakeQueue{}, &fakeQueue{}

		run(t, config(), a)
		run(t, config(), b)

		for i := range a.pushed {
			if a.pushed[i] != b.pushed[i] {
				t.Fatalf("item %d differs: %s vs %s", i, a.pushed[i], b.pushed[i])
			}
		}
	})

	t.Run("varies prompt sizes within the jitter", func(t *testing.T) {
		queue := &fakeQueue{}
		cfg := config()
		cfg.PromptTokens, cfg.PromptJitter = 200, 50

		run(t, cfg, queue)

		sizes := map[int]bool{}
		for _, item := range queue.pushed {
			var job producer.Job
			if err := json.Unmarshal([]byte(item), &job); err != nil {
				t.Fatal(err)
			}
			if job.PromptTokens < 150 || job.PromptTokens > 250 {
				t.Fatalf("prompt tokens = %d, want within 150..250", job.PromptTokens)
			}
			sizes[job.PromptTokens] = true
		}
		if len(sizes) < 10 {
			t.Fatalf("distinct sizes = %d, want the jitter applied", len(sizes))
		}
	})

	t.Run("stops with the queue error when a push fails", func(t *testing.T) {
		queue := &fakeQueue{failing: errors.New("connection refused")}
		c := &clock{now: start}
		p, err := producer.New(config(), queue, producer.NewMetrics(prometheus.NewRegistry()), c.Now, c.sleep, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}

		if err := p.Run(context.Background()); err == nil {
			t.Fatal("expected the push error")
		}
	})

	t.Run("parses a phase specification", func(t *testing.T) {
		phases, err := producer.ParsePhases("half:3m:0.5, full:3m:1,burst:4m:3,idle:4m:0")

		if err != nil {
			t.Fatal(err)
		}
		if len(phases) != 4 || phases[2].Name != "burst" || phases[2].Duration != 4*time.Minute || phases[2].Multiplier != 3 {
			t.Fatalf("phases = %+v", phases)
		}
	})

	t.Run("rejects a malformed phase specification", func(t *testing.T) {
		if _, err := producer.ParsePhases("half:3m"); err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("rejects an invalid configuration", func(t *testing.T) {
		cfg := config()
		cfg.PromptJitter = cfg.PromptTokens

		if err := cfg.Validate(); err == nil {
			t.Fatal("expected a validation error")
		}
	})
}
