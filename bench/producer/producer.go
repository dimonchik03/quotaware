package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Phase struct {
	Name       string
	Duration   time.Duration
	Multiplier float64
}

type Config struct {
	List           string
	QuotaPerSecond float64
	Phases         []Phase
	Seed           int64
	PromptTokens   int
	PromptJitter   int
	Tick           time.Duration
}

func DefaultPhases() []Phase {
	return []Phase{
		{Name: "half", Duration: 3 * time.Minute, Multiplier: 0.5},
		{Name: "full", Duration: 3 * time.Minute, Multiplier: 1},
		{Name: "burst", Duration: 4 * time.Minute, Multiplier: 3},
		{Name: "idle", Duration: 4 * time.Minute, Multiplier: 0},
	}
}

func DefaultConfig() Config {
	return Config{List: "jobs", QuotaPerSecond: 10, Phases: DefaultPhases(), Seed: 1, PromptTokens: 200, PromptJitter: 50, Tick: 100 * time.Millisecond}
}

func (c Config) Validate() error {
	switch {
	case c.List == "":
		return errors.New("list is required")
	case c.QuotaPerSecond <= 0:
		return errors.New("quota per second must be positive")
	case len(c.Phases) == 0:
		return errors.New("at least one phase is required")
	case c.PromptTokens <= 0 || c.PromptJitter < 0 || c.PromptJitter >= c.PromptTokens:
		return errors.New("prompt tokens must be positive and the jitter smaller than them")
	case c.Tick <= 0:
		return errors.New("tick must be positive")
	}
	for _, phase := range c.Phases {
		if phase.Name == "" || phase.Duration <= 0 || phase.Multiplier < 0 {
			return fmt.Errorf("phase %q must have a name, a positive duration and a non-negative multiplier", phase.Name)
		}
	}
	return nil
}

func ParsePhases(spec string) ([]Phase, error) {
	var phases []Phase
	for _, item := range strings.Split(spec, ",") {
		parts := strings.Split(strings.TrimSpace(item), ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("phase %q must be name:duration:multiplier", item)
		}
		duration, err := time.ParseDuration(parts[1])
		if err != nil {
			return nil, fmt.Errorf("phase %q: %w", item, err)
		}
		multiplier, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			return nil, fmt.Errorf("phase %q: %w", item, err)
		}
		phases = append(phases, Phase{Name: parts[0], Duration: duration, Multiplier: multiplier})
	}
	return phases, nil
}

type Job struct {
	ID           int64     `json:"id"`
	EnqueuedAt   time.Time `json:"enqueued_at"`
	PromptTokens int       `json:"prompt_tokens"`
}

type Queue interface {
	Push(ctx context.Context, name string, payloads ...string) error
	Len(ctx context.Context, name string) (int64, error)
	Clear(ctx context.Context, name string) error
}

type Metrics struct {
	Jobs        prometheus.Counter
	QueueLength prometheus.Gauge
	Multiplier  prometheus.Gauge
	Phase       *prometheus.GaugeVec
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	m := &Metrics{
		Jobs:        prometheus.NewCounter(prometheus.CounterOpts{Name: "producer_jobs_total", Help: "Jobs pushed to the queue."}),
		QueueLength: prometheus.NewGauge(prometheus.GaugeOpts{Name: "producer_queue_length", Help: "Length of the job list as last observed."}),
		Multiplier:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "producer_load_multiplier", Help: "Current job rate as a multiple of the provider quota."}),
		Phase:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "producer_phase", Help: "One for the phase in progress."}, []string{"phase"}),
	}
	registry.MustRegister(m.Jobs, m.QueueLength, m.Multiplier, m.Phase)
	return m
}

type Producer struct {
	cfg     Config
	queue   Queue
	metrics *Metrics
	now     func() time.Time
	sleep   func(context.Context, time.Duration) bool
	logger  *slog.Logger
}

func New(cfg Config, queue Queue, metrics *Metrics, now func() time.Time, sleep func(context.Context, time.Duration) bool, logger *slog.Logger) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if queue == nil || metrics == nil || now == nil || sleep == nil || logger == nil {
		return nil, errors.New("queue, metrics, clock, sleep and logger are required")
	}
	return &Producer{cfg: cfg, queue: queue, metrics: metrics, now: now, sleep: sleep, logger: logger}, nil
}

func (p *Producer) Run(ctx context.Context) error {
	if err := p.queue.Clear(ctx, p.cfg.List); err != nil {
		return fmt.Errorf("clear %s: %w", p.cfg.List, err)
	}
	random := rand.New(rand.NewSource(p.cfg.Seed))
	var next int64
	for _, phase := range p.cfg.Phases {
		p.metrics.Phase.Reset()
		p.metrics.Phase.WithLabelValues(phase.Name).Set(1)
		p.metrics.Multiplier.Set(phase.Multiplier)
		p.logger.Info("phase started", "phase", phase.Name, "multiplier", phase.Multiplier, "duration", phase.Duration.String())
		rate := phase.Multiplier * p.cfg.QuotaPerSecond
		started := p.now()
		pushed := int64(0)
		for {
			elapsed := min(p.now().Sub(started), phase.Duration)
			due := int64(math.Floor(rate * elapsed.Seconds()))
			if due > pushed {
				if err := p.push(ctx, random, next, due-pushed); err != nil {
					return err
				}
				next += due - pushed
				pushed = due
			}
			if err := p.observe(ctx); err != nil {
				return err
			}
			if elapsed >= phase.Duration {
				break
			}
			if !p.sleep(ctx, p.cfg.Tick) {
				return ctx.Err()
			}
		}
	}
	p.metrics.Phase.Reset()
	p.metrics.Multiplier.Set(0)
	return nil
}

func (p *Producer) push(ctx context.Context, random *rand.Rand, first, count int64) error {
	payloads := make([]string, 0, count)
	at := p.now()
	for i := int64(0); i < count; i++ {
		tokens := p.cfg.PromptTokens
		if p.cfg.PromptJitter > 0 {
			tokens += random.Intn(2*p.cfg.PromptJitter+1) - p.cfg.PromptJitter
		}
		payload, err := json.Marshal(Job{ID: first + i, EnqueuedAt: at, PromptTokens: tokens})
		if err != nil {
			return err
		}
		payloads = append(payloads, string(payload))
	}
	if err := p.queue.Push(ctx, p.cfg.List, payloads...); err != nil {
		return fmt.Errorf("push to %s: %w", p.cfg.List, err)
	}
	p.metrics.Jobs.Add(float64(count))
	return nil
}

func (p *Producer) observe(ctx context.Context) error {
	length, err := p.queue.Len(ctx, p.cfg.List)
	if err != nil {
		return fmt.Errorf("len %s: %w", p.cfg.List, err)
	}
	p.metrics.QueueLength.Set(float64(length))
	return nil
}
