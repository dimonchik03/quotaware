package headroom

import (
	"errors"
	"math"
	"time"
)

type Quota struct {
	Limit     float64
	Remaining float64
	ResetAt   time.Time
}

func (q Quota) Known() bool {
	return q.Limit > 0
}

type Observation struct {
	Requests      Quota
	Tokens        Quota
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
		MaxReplicas:    10,
		MaxStepUp:      2,
		StaleAfter:     2 * time.Minute,
		StaleHard:      10 * time.Minute,
		DefaultCeiling: 1,
	}
}

func (c Config) Validate() error {
	switch {
	case c.Concurrency <= 0:
		return errors.New("concurrency must be positive")
	case c.Safety <= 0 || c.Safety > 1:
		return errors.New("safety must be within (0, 1]")
	case c.Share <= 0 || c.Share > 1:
		return errors.New("share must be within (0, 1]")
	case c.Window <= 0:
		return errors.New("window must be positive")
	case c.MinReplicas < 0:
		return errors.New("min replicas must not be negative")
	case c.MaxReplicas <= 0 || c.MaxReplicas < c.MinReplicas:
		return errors.New("max replicas must be positive and at least min replicas")
	case c.MaxStepUp < 0:
		return errors.New("max step up must not be negative")
	case c.StaleAfter <= 0 || c.StaleHard < c.StaleAfter:
		return errors.New("stale_after must be positive and stale_hard at least stale_after")
	case c.DefaultCeiling < 0:
		return errors.New("default ceiling must not be negative")
	}
	return nil
}

type Reason string

const (
	ReasonHeadroom Reason = "headroom"
	ReasonStepUp   Reason = "step_up"
	ReasonDrain    Reason = "drain"
	ReasonFrozen   Reason = "frozen"
	ReasonDefault  Reason = "default"
)

type Result struct {
	Ceiling     int
	Reason      Reason
	AllowedRate float64
	PodRate     float64
}

func Compute(cfg Config, obs Observation, previous int, now time.Time) Result {
	age := now.Sub(obs.ObservedAt)
	switch {
	case obs.ObservedAt.IsZero() || age > cfg.StaleHard:
		return guard(cfg, obs, Result{Ceiling: cfg.DefaultCeiling, Reason: ReasonDefault})
	case age > cfg.StaleAfter && previous > 0:
		return guard(cfg, obs, Result{Ceiling: previous, Reason: ReasonFrozen})
	case age > cfg.StaleAfter:
		return guard(cfg, obs, Result{Ceiling: cfg.DefaultCeiling, Reason: ReasonDefault})
	}

	perPod := podRate(cfg, obs)
	allowed, ok := allowedRate(cfg, obs, now)
	if perPod <= 0 || !ok {
		return guard(cfg, obs, Result{Ceiling: cfg.DefaultCeiling, Reason: ReasonDefault})
	}

	result := Result{
		Ceiling:     int(math.Floor(allowed / perPod)),
		Reason:      ReasonHeadroom,
		AllowedRate: allowed,
		PodRate:     perPod,
	}
	result.Ceiling = min(max(result.Ceiling, cfg.MinReplicas), cfg.MaxReplicas)
	if previous > 0 && result.Ceiling > previous+cfg.MaxStepUp {
		result.Ceiling = previous + cfg.MaxStepUp
		result.Reason = ReasonStepUp
	}
	return guard(cfg, obs, result)
}

func guard(cfg Config, obs Observation, result Result) Result {
	floor := ceilDiv(obs.InFlight, cfg.Concurrency)
	if floor > result.Ceiling {
		result.Ceiling = min(floor, cfg.MaxReplicas)
		result.Reason = ReasonDrain
	}
	return result
}

func podRate(cfg Config, obs Observation) float64 {
	if obs.Latency <= 0 {
		return 0
	}
	return float64(cfg.Concurrency) / obs.Latency.Seconds()
}

func allowedRate(cfg Config, obs Observation, now time.Time) (float64, bool) {
	rate, ok := steadyRate(cfg, obs)
	if !ok {
		return 0, false
	}
	if instant, ok := instantRate(obs, now); ok {
		rate = min(rate, instant)
	}
	return rate * cfg.Safety * cfg.Share, true
}

func steadyRate(cfg Config, obs Observation) (float64, bool) {
	window := cfg.Window.Seconds()
	rate := math.Inf(1)
	if obs.Requests.Known() {
		rate = min(rate, obs.Requests.Limit/window)
	}
	if obs.Tokens.Known() && obs.TokensPerCall > 0 {
		rate = min(rate, obs.Tokens.Limit/obs.TokensPerCall/window)
	}
	return rate, !math.IsInf(rate, 1)
}

func instantRate(obs Observation, now time.Time) (float64, bool) {
	rate := math.Inf(1)
	if r, ok := quotaRate(obs.Requests, 1, now); ok {
		rate = min(rate, r)
	}
	if r, ok := quotaRate(obs.Tokens, obs.TokensPerCall, now); ok {
		rate = min(rate, r)
	}
	return rate, !math.IsInf(rate, 1)
}

func quotaRate(q Quota, perCall float64, now time.Time) (float64, bool) {
	if !q.Known() || perCall <= 0 || q.ResetAt.IsZero() {
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
