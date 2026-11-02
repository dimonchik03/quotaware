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

func quotas() state.Quotas {
	return state.Quotas{
		Requests: headroom.Quota{Limit: 1000, Remaining: 990, ResetAt: now.Add(time.Minute)},
		Tokens:   headroom.Quota{Limit: 600000, Remaining: 590000, ResetAt: now.Add(time.Minute)},
	}
}

func success(usage float64, latency time.Duration) state.Sample {
	return state.Sample{
		Status:   http.StatusOK,
		Complete: true,
		Headers:  state.HeadersPresent,
		Quotas:   quotas(),
		HasUsage: true,
		Usage:    usage,
		Latency:  latency,
		At:       now,
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
		s.Begin(key, now)
		s.Begin(key, now)
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
		if !b.ObservedAt.Equal(now) || b.Quotas.Requests.Remaining != 990 {
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
		limited.Quotas.Requests.Remaining = 0
		limited.RetryAfter = 20 * time.Second
		limited.At = now.Add(time.Second)

		s.End(key, limited)

		b := bucket(t, s)
		if b.RateLimited != 1 || b.RetryAfter != 20*time.Second {
			t.Fatalf("rate limited = %d, retry-after = %v, want 1 and 20s", b.RateLimited, b.RetryAfter)
		}
		if b.Quotas.Requests.Remaining != 0 || !b.ObservedAt.Equal(now.Add(time.Second)) {
			t.Fatalf("quotas = %+v observed at %v, want the 429 headers applied", b.Quotas.Requests, b.ObservedAt)
		}
		if b.TokensPerCall != 1000 || b.Latency != 2*time.Second || b.UsageMissing != 0 {
			t.Fatalf("bucket = %+v, want the averages and usage counter untouched", b)
		}
	})

	t.Run("a window the new sample does not know keeps its last value", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second))
		partial := success(1000, 2*time.Second)
		partial.Quotas = state.Quotas{Requests: headroom.Quota{Limit: 1000, Remaining: 500, ResetAt: now.Add(time.Minute)}}

		s.End(key, partial)

		b := bucket(t, s)
		if b.Quotas.Requests.Remaining != 500 || b.Quotas.Tokens.Remaining != 590000 {
			t.Fatalf("quotas = %+v, want requests updated and tokens kept", b.Quotas)
		}
	})

	t.Run("a success without limits counts as headers missing and keeps the last quotas", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second))
		bare := success(1200, 2*time.Second)
		bare.Headers = state.HeadersMissing
		bare.Quotas = state.Quotas{}
		bare.At = now.Add(time.Second)

		s.End(key, bare)

		b := bucket(t, s)
		if b.HeadersMissing != 1 || b.Quotas.Requests.Remaining != 990 || !b.ObservedAt.Equal(now) {
			t.Fatalf("bucket = %+v, want one missing header and the previous observation kept", b)
		}
		if b.TokensPerCall != 1100 {
			t.Fatalf("tokens per call = %v, want 1100 smoothed from the usage that was present", b.TokensPerCall)
		}
	})

	t.Run("malformed headers are counted apart from missing ones", func(t *testing.T) {
		s := store(t, 0.5)
		broken := success(1000, time.Second)
		broken.Headers = state.HeadersMalformed
		broken.Quotas = state.Quotas{}

		s.End(key, broken)

		b := bucket(t, s)
		if b.HeadersMalformed != 1 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want one malformed and no missing", b)
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

	t.Run("an aborted response keeps its quotas but skips latency and usage", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key, now)
		aborted := success(0, 400*time.Millisecond)
		aborted.Complete = false
		aborted.HasUsage = false

		s.End(key, aborted)

		b := bucket(t, s)
		if b.InFlight != 0 || b.Aborted != 1 || b.Responses != 1 {
			t.Fatalf("bucket = %+v, want the slot released and one abort counted", b)
		}
		if b.Latency != 0 || b.UsageMissing != 0 || b.Quotas.Requests.Remaining != 990 {
			t.Fatalf("bucket = %+v, want no latency or usage update but the quotas applied", b)
		}
	})

	t.Run("an upstream failure only releases the slot and counts the error", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key, now)

		s.Fail(key, now.Add(time.Second))

		b := bucket(t, s)
		if b.InFlight != 0 || b.UpstreamErrors != 1 || b.Responses != 0 || !b.LastSeen.Equal(now.Add(time.Second)) {
			t.Fatalf("bucket = %+v, want one upstream error, nothing observed, last seen updated", b)
		}
	})

	t.Run("a 5xx without limits is not counted as headers missing", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, state.Sample{Status: http.StatusBadGateway, Complete: true, Headers: state.HeadersMissing, At: now})

		b := bucket(t, s)
		if b.Responses != 1 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want one response and no missing headers", b)
		}
	})

	t.Run("idle buckets are evicted while busy or recent ones stay", func(t *testing.T) {
		s := store(t, 0.5)
		idle := state.Key{Provider: "openai", Model: "typo", Key: "a"}
		busy := state.Key{Provider: "openai", Model: "busy", Key: "a"}
		s.Begin(idle, now)
		s.End(idle, success(10, time.Second))
		s.Begin(busy, now)
		s.Begin(key, now.Add(9*time.Minute))
		s.End(key, success(10, time.Second))

		evicted := s.Evict(now.Add(11*time.Minute), 10*time.Minute)

		if evicted != 1 {
			t.Fatalf("evicted = %d, want 1", evicted)
		}
		if _, ok := s.Bucket(idle); ok {
			t.Fatal("idle bucket still present")
		}
		if _, ok := s.Bucket(busy); !ok {
			t.Fatal("bucket with a call in flight was evicted")
		}
		if _, ok := s.Bucket(key); !ok {
			t.Fatal("recently seen bucket was evicted")
		}
	})

	t.Run("ceilings are updated atomically per consumer and dropped once nobody polls them", func(t *testing.T) {
		s := store(t, 0.5)
		consumer := state.Consumer{Namespace: "jobs", ScaledObject: "llm-workers", Bucket: key}
		first := s.UpdateCeiling(consumer, func(previous state.Ceiling, known bool) state.Ceiling {
			if known {
				t.Fatal("a previous ceiling was reported before any update")
			}
			return state.Ceiling{Value: 4, ChangedAt: now, Seen: now}
		})
		second := s.UpdateCeiling(consumer, func(previous state.Ceiling, known bool) state.Ceiling {
			if !known || previous.Value != 4 {
				t.Fatalf("previous = %+v known = %v, want the first update", previous, known)
			}
			return state.Ceiling{Value: 6, ChangedAt: now.Add(time.Minute), Seen: now.Add(time.Minute)}
		})

		if first.Value != 4 || second.Value != 6 || second.Consumer != consumer || len(s.Ceilings()) != 1 {
			t.Fatalf("updates = %+v / %+v, want both applied to one consumer", first, second)
		}

		s.Evict(now.Add(12*time.Minute), 10*time.Minute)

		if _, ok := s.Ceiling(consumer); ok || len(s.Ceilings()) != 0 {
			t.Fatal("ceiling survived although its consumer stopped polling")
		}
	})

	t.Run("buckets are listed in a stable order", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "b"}, now)
		s.Begin(state.Key{Provider: "anthropic", Model: "claude-sonnet-4-5", Key: "a"}, now)
		s.Begin(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "a"}, now)

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
		s.Begin(key, now)
		s.Begin(key, now)
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
