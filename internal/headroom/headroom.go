package headroom

import (
	"errors"
	"math"
	"time"
)

type Quota struct {
	Limit      float64
	Remaining  float64
	ResetAt    time.Time
	ReportedAt time.Time
}

func (q Quota) Known() bool {
	return q.Limit > 0
}

func (q Quota) current(cfg Config, now time.Time) bool {
	return q.Known() && (q.ReportedAt.IsZero() || now.Sub(q.ReportedAt) <= cfg.StaleAfter)
}

type Observation struct {
	Requests      Quota
	Tokens        Quota
	ProjectTokens Quota
	TokensPerCall float64
	Latency       time.Duration
	InFlight      int
	ObservedAt    time.Time
}

type Config struct {
	Concurrency    int
	Safety         float64
	Share          float64
	Window         time.Duration
	MinReplicas    int
	MaxReplicas    int
	MaxStepUp      int
	StepInterval   time.Duration
	StaleAfter     time.Duration
	StaleHard      time.Duration
	DefaultCeiling int
}

func DefaultConfig() Config {
	return Config{
		Concurrency:    1,
		Safety:         0.85,
		Share:          1,
		Window:         time.Minute,
		MinReplicas:    1,
		MaxReplicas:    100,
		MaxStepUp:      2,
		StepInterval:   15 * time.Second,
		StaleAfter:     2 * time.Minute,
		StaleHard:      10 * time.Minute,
		DefaultCeiling: 1,
	}
}

func (c Config) Validate() error {
	switch {
	case c.Concurrency <= 0:
		return errors.New("concurrency must be positive")
	case math.IsNaN(c.Safety) || c.Safety <= 0 || c.Safety > 1:
		return errors.New("safety must be within (0, 1]")
	case math.IsNaN(c.Share) || c.Share <= 0 || c.Share > 1:
		return errors.New("share must be within (0, 1]")
	case c.Window <= 0:
		return errors.New("window must be positive")
	case c.MinReplicas < 0:
		return errors.New("min replicas must not be negative")
	case c.MaxReplicas <= 0 || c.MaxReplicas < c.MinReplicas:
		return errors.New("max replicas must be positive and at least min replicas")
	case c.MaxStepUp <= 0:
		return errors.New("max step up must be positive")
	case c.StepInterval <= 0:
		return errors.New("step interval must be positive")
	case c.StaleAfter <= 0 || c.StaleHard < c.StaleAfter:
		return errors.New("stale_after must be positive and stale_hard at least stale_after")
	case c.DefaultCeiling < c.MinReplicas || c.DefaultCeiling > c.MaxReplicas:
		return errors.New("default ceiling must be within [min replicas, max replicas]")
	}
	return nil
}

type Previous struct {
	Ceiling int
	Known   bool
	At      time.Time
}

type Reason string

const (
	ReasonHeadroom Reason = "headroom"
	ReasonClamped  Reason = "clamped"
	ReasonStepUp   Reason = "step_up"
	ReasonDrain    Reason = "drain"
	ReasonFrozen   Reason = "frozen"
	ReasonDefault  Reason = "default"
)

type Result struct {
	Ceiling     int
	Bound       int
	Reason      Reason
	AllowedRate float64
	PodRate     float64
}

func Compute(cfg Config, obs Observation, previous Previous, now time.Time) Result {
	age := now.Sub(obs.ObservedAt)
	switch {
	case obs.ObservedAt.IsZero() || age > cfg.StaleHard:
		return guard(cfg, obs, defaultResult(cfg))
	case age > cfg.StaleAfter && previous.Known:
		return guard(cfg, obs, Result{Ceiling: previous.Ceiling, Reason: ReasonFrozen})
	case age > cfg.StaleAfter:
		return guard(cfg, obs, defaultResult(cfg))
	}

	perPod := podRate(cfg, obs)
	allowed, ok := allowedRate(cfg, obs, now)
	if perPod <= 0 || !ok {
		return guard(cfg, obs, defaultResult(cfg))
	}

	raw := math.Floor(allowed / perPod)
	result := Result{
		Ceiling:     int(math.Max(math.Min(raw, float64(cfg.MaxReplicas)), float64(cfg.MinReplicas))),
		Reason:      ReasonHeadroom,
		AllowedRate: allowed,
		PodRate:     perPod,
	}
	if float64(result.Ceiling) != raw {
		result.Reason = ReasonClamped
	}
	if previous.Known && result.Ceiling > previous.Ceiling {
		switch {
		case now.Sub(previous.At) < cfg.StepInterval:
			result.Ceiling = max(previous.Ceiling, cfg.MinReplicas)
			result.Reason = ReasonStepUp
		case result.Ceiling > previous.Ceiling+cfg.MaxStepUp:
			result.Ceiling = max(previous.Ceiling+cfg.MaxStepUp, cfg.MinReplicas)
			result.Reason = ReasonStepUp
		}
	}
	return guard(cfg, obs, result)
}

func defaultResult(cfg Config) Result {
	return Result{Ceiling: cfg.DefaultCeiling, Reason: ReasonDefault}
}

func guard(cfg Config, obs Observation, result Result) Result {
	result.Bound = result.Ceiling
	drainFloor := DrainFloor(cfg, obs)
	if drainFloor > result.Ceiling {
		result.Ceiling = drainFloor
		result.Reason = ReasonDrain
	}
	return result
}

func DrainFloor(cfg Config, obs Observation) int {
	return min(ceilDiv(obs.InFlight, cfg.Concurrency), cfg.MaxReplicas)
}

func podRate(cfg Config, obs Observation) float64 {
	if obs.Latency <= 0 {
		return 0
	}
	return float64(cfg.Concurrency) / obs.Latency.Seconds()
}

func allowedRate(cfg Config, obs Observation, now time.Time) (float64, bool) {
	rate, ok := steadyRate(cfg, obs, now)
	if !ok {
		return 0, false
	}
	if instant, ok := instantRate(cfg, obs, now); ok {
		rate = min(rate, instant)
	}
	return rate * cfg.Safety * cfg.Share, true
}

func steadyRate(cfg Config, obs Observation, now time.Time) (float64, bool) {
	window := cfg.Window.Seconds()
	rate := math.Inf(1)
	if obs.Requests.current(cfg, now) {
		rate = min(rate, obs.Requests.Limit/window)
	}
	for _, tokens := range []Quota{obs.Tokens, obs.ProjectTokens} {
		if tokens.current(cfg, now) && obs.TokensPerCall > 0 {
			rate = min(rate, tokens.Limit/obs.TokensPerCall/window)
		}
	}
	return rate, !math.IsInf(rate, 1)
}

func instantRate(cfg Config, obs Observation, now time.Time) (float64, bool) {
	rate := math.Inf(1)
	if requestRate, ok := quotaRate(cfg, obs.Requests, 1, now); ok {
		rate = min(rate, requestRate)
	}
	for _, tokens := range []Quota{obs.Tokens, obs.ProjectTokens} {
		if tokenRate, ok := quotaRate(cfg, tokens, obs.TokensPerCall, now); ok {
			rate = min(rate, tokenRate)
		}
	}
	return rate, !math.IsInf(rate, 1)
}

func quotaRate(cfg Config, q Quota, perCall float64, now time.Time) (float64, bool) {
	if !q.current(cfg, now) || perCall <= 0 || q.ResetAt.IsZero() {
		return 0, false
	}
	untilReset := q.ResetAt.Sub(now).Seconds()
	if untilReset <= 0 {
		return 0, false
	}
	return max(q.Remaining, 0) / perCall / untilReset, true
}

func ceilDiv(numerator, denominator int) int {
	if denominator <= 0 || numerator <= 0 {
		return 0
	}
	return (numerator + denominator - 1) / denominator
}
