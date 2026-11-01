package headroom_test

import (
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/internal/headroom"
)

var now = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

func config() headroom.Config {
	return headroom.Config{
		Concurrency:    4,
		Safety:         0.85,
		Share:          1,
		Window:         time.Minute,
		MinReplicas:    1,
		MaxReplicas:    40,
		MaxStepUp:      2,
		StaleAfter:     2 * time.Minute,
		StaleHard:      10 * time.Minute,
		DefaultCeiling: 1,
	}
}

func observation(remainingRequests, remainingTokens float64, resetAt time.Time) headroom.Observation {
	return headroom.Observation{
		Requests:      headroom.Quota{Limit: 1000, Remaining: remainingRequests, ResetAt: resetAt},
		Tokens:        headroom.Quota{Limit: 600000, Remaining: remainingTokens, ResetAt: resetAt},
		TokensPerCall: 1000,
		Latency:       2 * time.Second,
		ObservedAt:    now,
	}
}

func known(ceiling int) headroom.Previous {
	return headroom.Previous{Ceiling: ceiling, Known: true}
}

func TestCompute(t *testing.T) {
	midWindow := now.Add(30 * time.Second)
	endOfWindow := now.Add(time.Second)

	cases := []struct {
		name        string
		config      headroom.Config
		observation headroom.Observation
		previous    headroom.Previous
		now         time.Time
		wantCeiling int
		wantReason  headroom.Reason
	}{
		{
			name:        "under quota the ceiling follows the steady-state rate",
			config:      config(),
			observation: observation(900, 500000, midWindow),
			now:         now,
			wantCeiling: 4,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "at quota the ceiling is clamped to min_replicas",
			config:      config(),
			observation: observation(0, 0, midWindow),
			previous:    known(4),
			now:         now,
			wantCeiling: 1,
			wantReason:  headroom.ReasonClamped,
		},
		{
			name:        "a short time to reset does not inflate the ceiling at the end of a window",
			config:      config(),
			observation: observation(100, 50000, endOfWindow),
			previous:    known(4),
			now:         now,
			wantCeiling: 4,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "a window that already reset is judged by the steady-state rate alone",
			config:      config(),
			observation: observation(0, 0, now.Add(-5*time.Second)),
			previous:    known(4),
			now:         now,
			wantCeiling: 4,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "an observation older than stale_after freezes the previous ceiling",
			config:      config(),
			observation: observation(900, 500000, midWindow),
			previous:    known(6),
			now:         now.Add(3 * time.Minute),
			wantCeiling: 6,
			wantReason:  headroom.ReasonFrozen,
		},
		{
			name:        "a stale observation without a previous ceiling uses the default",
			config:      config(),
			observation: observation(900, 500000, midWindow),
			now:         now.Add(3 * time.Minute),
			wantCeiling: 1,
			wantReason:  headroom.ReasonDefault,
		},
		{
			name:        "an observation older than stale_hard falls back to the default ceiling",
			config:      config(),
			observation: observation(900, 500000, midWindow),
			previous:    known(6),
			now:         now.Add(11 * time.Minute),
			wantCeiling: 1,
			wantReason:  headroom.ReasonDefault,
		},
		{
			name:        "the drain guard never reports fewer pods than the calls in flight need",
			config:      config(),
			observation: withInFlight(observation(0, 0, midWindow), 10),
			previous:    known(4),
			now:         now,
			wantCeiling: 3,
			wantReason:  headroom.ReasonDrain,
		},
		{
			name:        "a share below one scales the allowed rate down",
			config:      withShare(config(), 0.5),
			observation: observation(900, 500000, midWindow),
			now:         now,
			wantCeiling: 2,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "the ceiling rises by at most max_step_up per evaluation",
			config:      config(),
			observation: observation(900, 500000, midWindow),
			previous:    known(1),
			now:         now,
			wantCeiling: 3,
			wantReason:  headroom.ReasonStepUp,
		},
		{
			name:        "a raise that lands exactly on the step-up cap is not reported as capped",
			config:      config(),
			observation: observation(900, 500000, midWindow),
			previous:    known(2),
			now:         now,
			wantCeiling: 4,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "after a ceiling of zero the step-up cap still applies",
			config:      withMinReplicas(config(), 0),
			observation: observation(900, 500000, midWindow),
			previous:    known(0),
			now:         now,
			wantCeiling: 2,
			wantReason:  headroom.ReasonStepUp,
		},
		{
			name:        "the step-up cap never lands below min_replicas",
			config:      withMinReplicas(config(), 3),
			observation: observation(900, 500000, midWindow),
			previous:    known(0),
			now:         now,
			wantCeiling: 3,
			wantReason:  headroom.ReasonStepUp,
		},
		{
			name:        "the ceiling lowers immediately",
			config:      config(),
			observation: observation(900, 500000, midWindow),
			previous:    known(10),
			now:         now,
			wantCeiling: 4,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "the ceiling is clamped to max_replicas",
			config:      config(),
			observation: withLimits(observation(99000, 59000000, midWindow), 100000, 60000000),
			now:         now,
			wantCeiling: 40,
			wantReason:  headroom.ReasonClamped,
		},
		{
			name:        "an astronomically large quota clamps to max_replicas instead of overflowing",
			config:      config(),
			observation: withLimits(observation(1e30, 1e30, midWindow), 1e30, 1e30),
			now:         now,
			wantCeiling: 40,
			wantReason:  headroom.ReasonClamped,
		},
		{
			name:        "a tighter project token window bounds the ceiling",
			config:      config(),
			observation: withProjectTokens(observation(900, 500000, midWindow), 300000, 300000, midWindow),
			now:         now,
			wantCeiling: 2,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "a depleted project token window lowers the ceiling before the organization window does",
			config:      config(),
			observation: withProjectTokens(observation(900, 500000, midWindow), 600000, 240000, midWindow),
			now:         now,
			wantCeiling: 3,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "the token constraint is ignored until tokens per call are observed",
			config:      config(),
			observation: withTokensPerCall(observation(900, 500000, midWindow), 0),
			now:         now,
			wantCeiling: 7,
			wantReason:  headroom.ReasonHeadroom,
		},
		{
			name:        "without any known quota the default ceiling is reported",
			config:      config(),
			observation: headroom.Observation{TokensPerCall: 1000, Latency: 2 * time.Second, ObservedAt: now},
			now:         now,
			wantCeiling: 1,
			wantReason:  headroom.ReasonDefault,
		},
		{
			name:        "without an observed latency the default ceiling is reported",
			config:      config(),
			observation: withLatency(observation(900, 500000, midWindow), 0),
			now:         now,
			wantCeiling: 1,
			wantReason:  headroom.ReasonDefault,
		},
		{
			name:        "the drain guard also applies to the default ceiling",
			config:      config(),
			observation: withInFlight(headroom.Observation{ObservedAt: now, Latency: 2 * time.Second}, 8),
			now:         now,
			wantCeiling: 2,
			wantReason:  headroom.ReasonDrain,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := headroom.Compute(tc.config, tc.observation, tc.previous, tc.now)

			if result.Ceiling != tc.wantCeiling {
				t.Fatalf("ceiling = %d (%s), want %d", result.Ceiling, result.Reason, tc.wantCeiling)
			}
			if result.Reason != tc.wantReason {
				t.Fatalf("reason = %s, want %s", result.Reason, tc.wantReason)
			}
		})
	}
}

func TestComputeReportsTheRatesBehindTheCeiling(t *testing.T) {
	result := headroom.Compute(config(), observation(900, 500000, now.Add(30*time.Second)), headroom.Previous{}, now)

	if result.PodRate != 2 {
		t.Fatalf("pod rate = %v calls/s, want 2", result.PodRate)
	}
	if result.AllowedRate != 8.5 {
		t.Fatalf("allowed rate = %v calls/s, want 8.5", result.AllowedRate)
	}
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*headroom.Config)
	}{
		{"rejects a non-positive concurrency", func(c *headroom.Config) { c.Concurrency = 0 }},
		{"rejects a zero safety factor", func(c *headroom.Config) { c.Safety = 0 }},
		{"rejects a safety factor above one", func(c *headroom.Config) { c.Safety = 1.5 }},
		{"rejects a zero share", func(c *headroom.Config) { c.Share = 0 }},
		{"rejects a zero window", func(c *headroom.Config) { c.Window = 0 }},
		{"rejects negative min replicas", func(c *headroom.Config) { c.MinReplicas = -1 }},
		{"rejects zero max replicas", func(c *headroom.Config) { c.MaxReplicas = 0 }},
		{"rejects max replicas below min replicas", func(c *headroom.Config) { c.MinReplicas, c.MaxReplicas = 5, 3 }},
		{"rejects a zero step up", func(c *headroom.Config) { c.MaxStepUp = 0 }},
		{"rejects a zero stale_after", func(c *headroom.Config) { c.StaleAfter = 0 }},
		{"rejects stale_hard below stale_after", func(c *headroom.Config) { c.StaleHard = time.Minute }},
		{"rejects a negative default ceiling", func(c *headroom.Config) { c.DefaultCeiling = -1 }},
	}

	if err := config().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := headroom.DefaultConfig().Validate(); err != nil {
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

func withInFlight(o headroom.Observation, inFlight int) headroom.Observation {
	o.InFlight = inFlight
	return o
}

func withTokensPerCall(o headroom.Observation, tokensPerCall float64) headroom.Observation {
	o.TokensPerCall = tokensPerCall
	return o
}

func withLatency(o headroom.Observation, latency time.Duration) headroom.Observation {
	o.Latency = latency
	return o
}

func withLimits(o headroom.Observation, requests, tokens float64) headroom.Observation {
	o.Requests.Limit = requests
	o.Tokens.Limit = tokens
	return o
}

func withProjectTokens(o headroom.Observation, limit, remaining float64, resetAt time.Time) headroom.Observation {
	o.ProjectTokens = headroom.Quota{Limit: limit, Remaining: remaining, ResetAt: resetAt}
	return o
}

func withShare(c headroom.Config, share float64) headroom.Config {
	c.Share = share
	return c
}

func withMinReplicas(c headroom.Config, minReplicas int) headroom.Config {
	c.MinReplicas = minReplicas
	return c
}
