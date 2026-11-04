package aimd

import (
	"errors"
	"math"
	"time"
)

type Config struct {
	Threshold float64
	Decrease  float64
	Increase  int
	Interval  time.Duration
	Window    time.Duration
	MaxHold   time.Duration
	Start     int
	Min       int
	Max       int
}

func DefaultConfig() Config {
	return Config{Threshold: 0.02, Decrease: 0.5, Increase: 1, Interval: 15 * time.Second, Window: time.Minute, MaxHold: 10 * time.Minute, Start: 1, Min: 1, Max: 100}
}

func (c Config) Validate() error {
	switch {
	case math.IsNaN(c.Threshold) || c.Threshold < 0 || c.Threshold >= 1:
		return errors.New("threshold must be within [0, 1)")
	case math.IsNaN(c.Decrease) || c.Decrease <= 0 || c.Decrease >= 1:
		return errors.New("decrease must be within (0, 1)")
	case c.Increase <= 0 || c.Increase > c.Max:
		return errors.New("increase must be positive and at most max")
	case c.Interval <= 0 || c.Window <= 0:
		return errors.New("interval and window must be positive")
	case c.MaxHold < c.Window:
		return errors.New("max hold must be at least the window")
	case c.Min < 0 || c.Max <= 0 || c.Max < c.Min:
		return errors.New("min must not be negative and max must be positive and at least min")
	case c.Start < c.Min || c.Start > c.Max:
		return errors.New("start must lie between min and max")
	}
	return nil
}

type State struct {
	Ceiling   int
	Known     bool
	ChangedAt time.Time
	HoldUntil time.Time
}

type Signal struct {
	Responses   int64
	RateLimited int64
	RetryAfter  time.Duration
}

func (s Signal) Ratio() float64 {
	if s.Responses == 0 {
		return 0
	}
	return float64(s.RateLimited) / float64(s.Responses)
}

type Action string

const (
	ActionStart    Action = "start"
	ActionDecrease Action = "decrease"
	ActionIncrease Action = "increase"
	ActionHold     Action = "hold"
)

type Result struct {
	Ceiling   int
	Action    Action
	HoldUntil time.Time
}

func Step(cfg Config, state State, signal Signal, now time.Time) Result {
	if !state.Known {
		return Result{Ceiling: clamp(cfg, cfg.Start), Action: ActionStart}
	}
	switch {
	case now.Before(state.HoldUntil):
		return Result{Ceiling: clamp(cfg, state.Ceiling), Action: ActionHold, HoldUntil: state.HoldUntil}
	case signal.Ratio() > cfg.Threshold:
		return Result{
			Ceiling:   clamp(cfg, int(math.Floor(float64(state.Ceiling)*cfg.Decrease))),
			Action:    ActionDecrease,
			HoldUntil: now.Add(hold(cfg, signal)),
		}
	case now.Sub(state.ChangedAt) < cfg.Interval || signal.Responses == 0:
		return Result{Ceiling: clamp(cfg, state.Ceiling), Action: ActionHold}
	default:
		return Result{Ceiling: clamp(cfg, state.Ceiling+cfg.Increase), Action: ActionIncrease}
	}
}

func Guard(cfg Config, ceiling int, state State, signal Signal, now time.Time) (int, bool, time.Time) {
	if state.Known && now.Before(state.HoldUntil) {
		return min(ceiling, state.Ceiling), true, state.HoldUntil
	}
	if signal.Ratio() > cfg.Threshold {
		return clamp(cfg, int(math.Floor(float64(ceiling)*cfg.Decrease))), true, now.Add(hold(cfg, signal))
	}
	return ceiling, false, time.Time{}
}

func hold(cfg Config, signal Signal) time.Duration {
	return min(max(signal.RetryAfter, cfg.Interval, cfg.Window), cfg.MaxHold)
}

func clamp(cfg Config, v int) int {
	return min(max(v, cfg.Min), cfg.Max)
}
