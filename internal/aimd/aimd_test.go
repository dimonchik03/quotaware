package aimd_test

import (
	"math"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/internal/aimd"
)

var now = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

func config() aimd.Config {
	return aimd.Config{Threshold: 0.02, Decrease: 0.5, Increase: 1, Interval: 15 * time.Second, Window: time.Minute, MaxHold: 10 * time.Minute, Start: 4, Min: 1, Max: 20}
}

func settled(ceiling int) aimd.State {
	return aimd.State{Ceiling: ceiling, Known: true, ChangedAt: now.Add(-time.Minute)}
}

func TestStep(t *testing.T) {
	cases := []struct {
		name        string
		state       aimd.State
		signal      aimd.Signal
		wantCeiling int
		wantAction  aimd.Action
	}{
		{"starts at the configured start value", aimd.State{}, aimd.Signal{}, 4, aimd.ActionStart},
		{"halves the ceiling when the 429 ratio exceeds the threshold", settled(10), aimd.Signal{Responses: 100, RateLimited: 5}, 5, aimd.ActionDecrease},
		{"never decreases below min", settled(1), aimd.Signal{Responses: 100, RateLimited: 50}, 1, aimd.ActionDecrease},
		{"adds the increase after an interval of clean traffic", settled(10), aimd.Signal{Responses: 100}, 11, aimd.ActionIncrease},
		{"never increases above max", settled(20), aimd.Signal{Responses: 100}, 20, aimd.ActionIncrease},
		{"holds while the retry-after has not elapsed", aimd.State{Ceiling: 5, Known: true, ChangedAt: now.Add(-time.Minute), HoldUntil: now.Add(10 * time.Second)}, aimd.Signal{Responses: 100}, 5, aimd.ActionHold},
		{"does not halve twice on the same burst while holding", aimd.State{Ceiling: 5, Known: true, ChangedAt: now.Add(-20 * time.Second), HoldUntil: now.Add(40 * time.Second)}, aimd.Signal{Responses: 100, RateLimited: 50}, 5, aimd.ActionHold},
		{"holds within an interval of the last change", aimd.State{Ceiling: 5, Known: true, ChangedAt: now.Add(-5 * time.Second)}, aimd.Signal{Responses: 100}, 5, aimd.ActionHold},
		{"holds without any traffic to learn from", settled(5), aimd.Signal{}, 5, aimd.ActionHold},
		{"a ratio exactly at the threshold does not decrease", settled(10), aimd.Signal{Responses: 100, RateLimited: 2}, 11, aimd.ActionIncrease},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := aimd.Step(config(), tc.state, tc.signal, now)

			if result.Ceiling != tc.wantCeiling || result.Action != tc.wantAction {
				t.Fatalf("result = %+v, want %d %s", result, tc.wantCeiling, tc.wantAction)
			}
		})
	}

	t.Run("a decrease holds for the longest retry-after", func(t *testing.T) {
		result := aimd.Step(config(), settled(10), aimd.Signal{Responses: 100, RateLimited: 10, RetryAfter: 90 * time.Second}, now)

		if !result.HoldUntil.Equal(now.Add(90 * time.Second)) {
			t.Fatalf("hold until = %v, want the retry-after honoured", result.HoldUntil)
		}
	})

	t.Run("a decrease never holds longer than max_hold", func(t *testing.T) {
		result := aimd.Step(config(), settled(10), aimd.Signal{Responses: 100, RateLimited: 10, RetryAfter: 6 * time.Hour}, now)

		if !result.HoldUntil.Equal(now.Add(10 * time.Minute)) {
			t.Fatalf("hold until = %v, want the hold capped at ten minutes", result.HoldUntil)
		}
	})

	t.Run("a decrease holds at least one observation window", func(t *testing.T) {
		result := aimd.Step(config(), settled(10), aimd.Signal{Responses: 100, RateLimited: 10, RetryAfter: 2 * time.Second}, now)

		if !result.HoldUntil.Equal(now.Add(time.Minute)) {
			t.Fatalf("hold until = %v, want the window so one burst is not halved twice", result.HoldUntil)
		}
	})
}

func TestGuard(t *testing.T) {
	t.Run("leaves a clean header ceiling alone", func(t *testing.T) {
		ceiling, guarded, _ := aimd.Guard(config(), 8, aimd.State{}, aimd.Signal{Responses: 50}, now)

		if ceiling != 8 || guarded {
			t.Fatalf("ceiling = %d guarded = %v, want 8 untouched", ceiling, guarded)
		}
	})

	t.Run("halves a header ceiling while 429s exceed the threshold", func(t *testing.T) {
		ceiling, guarded, holdUntil := aimd.Guard(config(), 8, aimd.State{}, aimd.Signal{Responses: 50, RateLimited: 5, RetryAfter: 20 * time.Second}, now)

		if ceiling != 4 || !guarded || !holdUntil.Equal(now.Add(time.Minute)) {
			t.Fatalf("ceiling = %d guarded = %v hold = %v, want 4 held for the window", ceiling, guarded, holdUntil)
		}
	})

	t.Run("keeps the guarded value while the hold lasts even when headers allow more", func(t *testing.T) {
		state := aimd.State{Ceiling: 4, Known: true, HoldUntil: now.Add(10 * time.Second)}

		ceiling, guarded, _ := aimd.Guard(config(), 8, state, aimd.Signal{Responses: 50}, now)

		if ceiling != 4 || !guarded {
			t.Fatalf("ceiling = %d guarded = %v, want the hold respected", ceiling, guarded)
		}
	})

	t.Run("releases the hold once it expires", func(t *testing.T) {
		state := aimd.State{Ceiling: 4, Known: true, HoldUntil: now.Add(-time.Second)}

		ceiling, guarded, _ := aimd.Guard(config(), 8, state, aimd.Signal{Responses: 50}, now)

		if ceiling != 8 || guarded {
			t.Fatalf("ceiling = %d guarded = %v, want the header ceiling back", ceiling, guarded)
		}
	})
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*aimd.Config)
	}{
		{"rejects a threshold of one", func(c *aimd.Config) { c.Threshold = 1 }},
		{"rejects a threshold that is not a number", func(c *aimd.Config) { c.Threshold = math.NaN() }},
		{"rejects a decrease that is not a number", func(c *aimd.Config) { c.Decrease = math.NaN() }},
		{"rejects a decrease of one", func(c *aimd.Config) { c.Decrease = 1 }},
		{"rejects a zero increase", func(c *aimd.Config) { c.Increase = 0 }},
		{"rejects a zero interval", func(c *aimd.Config) { c.Interval = 0 }},
		{"rejects a zero window", func(c *aimd.Config) { c.Window = 0 }},
		{"rejects a max hold shorter than the window", func(c *aimd.Config) { c.MaxHold = 30 * time.Second }},
		{"rejects max below min", func(c *aimd.Config) { c.Min, c.Max = 5, 3 }},
		{"rejects a start outside the range", func(c *aimd.Config) { c.Start = 50 }},
	}
	if err := aimd.DefaultConfig().Validate(); err != nil {
		t.Fatalf("default config rejected: %v", err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := config()
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}
