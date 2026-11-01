package state_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/internal/headroom"
	"github.com/dimonchik03/quotaware/internal/state"
)

var (
	now = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)
	key = state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "ab12"}
)

func store(t *testing.T, alpha float64) *state.Store {
	t.Helper()
	s, err := state.New(alpha)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func success(usage float64, latency time.Duration) state.Sample {
	return state.Sample{
		Status:    http.StatusOK,
		HasLimits: true,
		Requests:  headroom.Quota{Limit: 1000, Remaining: 990, ResetAt: now.Add(time.Minute)},
		Tokens:    headroom.Quota{Limit: 600000, Remaining: 590000, ResetAt: now.Add(time.Minute)},
		HasUsage:  true,
		Usage:     usage,
		Latency:   latency,
		At:        now,
	}
}

func bucket(t *testing.T, s *state.Store) state.Bucket {
	t.Helper()
	b, ok := s.Bucket(key)
	if !ok {
		t.Fatal("bucket not found")
	}
	return b
}

func TestStore(t *testing.T) {
	t.Run("begin and end keep the in-flight count balanced", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key)
		s.Begin(key)
		if got := bucket(t, s).InFlight; got != 2 {
			t.Fatalf("in flight = %d, want 2", got)
		}

		s.End(key, success(1000, time.Second))

		if got := bucket(t, s).InFlight; got != 1 {
			t.Fatalf("in flight = %d, want 1", got)
		}
	})

	t.Run("end without begin never drives in-flight below zero", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, success(1000, time.Second))

		if got := bucket(t, s).InFlight; got != 0 {
			t.Fatalf("in flight = %d, want 0", got)
		}
	})

	t.Run("the first success sets tokens per call and latency directly", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, success(1000, 2*time.Second))

		b := bucket(t, s)
		if b.TokensPerCall != 1000 || b.Latency != 2*time.Second {
			t.Fatalf("tokens per call = %v, latency = %v, want 1000 and 2s", b.TokensPerCall, b.Latency)
		}
		if !b.ObservedAt.Equal(now) || b.Requests.Remaining != 990 {
			t.Fatalf("bucket = %+v, want quotas observed at now", b)
		}
	})

	t.Run("later successes are smoothed with the alpha", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second))

		s.End(key, success(2000, 4*time.Second))

		b := bucket(t, s)
		if b.TokensPerCall != 1500 || b.Latency != 3*time.Second {
			t.Fatalf("tokens per call = %v, latency = %v, want 1500 and 3s", b.TokensPerCall, b.Latency)
		}
	})

	t.Run("a 429 updates the quotas and retry-after but not the averages", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second))
		limited := success(0, 50*time.Millisecond)
		limited.Status = http.StatusTooManyRequests
		limited.HasUsage = false
		limited.Requests.Remaining = 0
		limited.RetryAfter = 20 * time.Second
		limited.At = now.Add(time.Second)

		s.End(key, limited)

		b := bucket(t, s)
		if b.RateLimited != 1 || b.RetryAfter != 20*time.Second {
			t.Fatalf("rate limited = %d, retry-after = %v, want 1 and 20s", b.RateLimited, b.RetryAfter)
		}
		if b.Requests.Remaining != 0 || !b.ObservedAt.Equal(now.Add(time.Second)) {
			t.Fatalf("quotas = %+v observed at %v, want the 429 headers applied", b.Requests, b.ObservedAt)
		}
		if b.TokensPerCall != 1000 || b.Latency != 2*time.Second {
			t.Fatalf("tokens per call = %v, latency = %v, want untouched", b.TokensPerCall, b.Latency)
		}
		if b.UsageMissing != 0 {
			t.Fatalf("usage missing = %d, want 0 for a 429", b.UsageMissing)
		}
	})

	t.Run("a success without limits counts as headers missing and keeps the last quotas", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second))
		bare := success(1200, 2*time.Second)
		bare.HasLimits = false
		bare.At = now.Add(time.Second)

		s.End(key, bare)

		b := bucket(t, s)
		if b.HeadersMissing != 1 {
			t.Fatalf("headers missing = %d, want 1", b.HeadersMissing)
		}
		if b.Requests.Remaining != 990 || !b.ObservedAt.Equal(now) {
			t.Fatalf("quotas = %+v observed at %v, want the previous observation kept", b.Requests, b.ObservedAt)
		}
		if b.TokensPerCall != 1100 {
			t.Fatalf("tokens per call = %v, want 1100 smoothed from the usage that was present", b.TokensPerCall)
		}
	})

	t.Run("a success without usage counts as usage missing", func(t *testing.T) {
		s := store(t, 0.5)
		noUsage := success(0, time.Second)
		noUsage.HasUsage = false

		s.End(key, noUsage)

		b := bucket(t, s)
		if b.UsageMissing != 1 || b.TokensPerCall != 0 {
			t.Fatalf("usage missing = %d, tokens per call = %v, want 1 and 0", b.UsageMissing, b.TokensPerCall)
		}
	})

	t.Run("a transport failure only releases the in-flight slot", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key)

		s.End(key, state.Sample{Status: 0})

		b := bucket(t, s)
		if b.InFlight != 0 || b.Responses != 0 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want nothing recorded but in-flight released", b)
		}
	})

	t.Run("a 5xx without limits is not counted as headers missing", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, state.Sample{Status: http.StatusBadGateway, At: now})

		b := bucket(t, s)
		if b.Responses != 1 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want one response and no missing headers", b)
		}
	})

	t.Run("buckets are listed in a stable order", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "b"})
		s.Begin(state.Key{Provider: "anthropic", Model: "claude-sonnet-4-5", Key: "a"})
		s.Begin(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "a"})

		all := s.Buckets()

		want := []state.Key{
			{Provider: "anthropic", Model: "claude-sonnet-4-5", Key: "a"},
			{Provider: "openai", Model: "gpt-4.1", Key: "a"},
			{Provider: "openai", Model: "gpt-4.1", Key: "b"},
		}
		for i, b := range all {
			if b.Key != want[i] {
				t.Fatalf("bucket %d = %+v, want %+v", i, b.Key, want[i])
			}
		}
	})

	t.Run("a bucket converts to a headroom observation", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key)
		s.Begin(key)
		s.End(key, success(1000, 2*time.Second))

		obs := bucket(t, s).Observation()

		if obs.InFlight != 1 || obs.TokensPerCall != 1000 || obs.Latency != 2*time.Second || !obs.ObservedAt.Equal(now) {
			t.Fatalf("observation = %+v, want in-flight 1, 1000 tokens per call, 2s latency, observed at now", obs)
		}
		if obs.Requests.Limit != 1000 || obs.Tokens.Limit != 600000 {
			t.Fatalf("observation quotas = %+v / %+v, want the sample quotas", obs.Requests, obs.Tokens)
		}
	})

	t.Run("rejects an alpha outside the unit interval", func(t *testing.T) {
		for _, alpha := range []float64{0, -0.1, 1.5} {
			if _, err := state.New(alpha); err == nil {
				t.Fatalf("alpha %v accepted", alpha)
			}
		}
	})
}
