package state_test

import (
	"errors"
	"math"
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
	s, err := state.New(alpha, state.DefaultLimits())
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
		Status:    http.StatusOK,
		Complete:  true,
		Headers:   state.HeadersPresent,
		Quotas:    quotas(),
		HasUsage:  true,
		Usage:     usage,
		Latency:   latency,
		HeadersAt: now,
		At:        now,
	}
}

func updateCeiling(t *testing.T, s *state.Store, consumer state.Consumer, update func(state.Ceiling, bool) state.Ceiling) state.Ceiling {
	t.Helper()
	c, err := s.UpdateCeiling(consumer, update)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func bucket(t *testing.T, s *state.Store) state.Bucket {
	t.Helper()
	return bucketAt(t, s, now.Add(time.Minute))
}

func bucketAt(t *testing.T, s *state.Store, at time.Time) state.Bucket {
	t.Helper()
	for _, b := range s.Buckets(at) {
		if b.Key == key {
			return b
		}
	}
	t.Fatal("bucket not found")
	return state.Bucket{}
}

func TestStore(t *testing.T) {
	t.Run("begin and end keep the in-flight count balanced", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key, now, "")
		s.Begin(key, now, "")
		if got := bucket(t, s).InFlight; got != 2 {
			t.Fatalf("in flight = %d, want 2", got)
		}

		s.End(key, success(1000, time.Second), "")

		if got := bucket(t, s).InFlight; got != 1 {
			t.Fatalf("in flight = %d, want 1", got)
		}
	})

	t.Run("an observation applies its sample without a call in flight", func(t *testing.T) {
		s := store(t, 0.5)

		s.Observe(key, success(100, time.Second))

		b := bucket(t, s)
		if b.InFlight != 0 || b.Responses != 1 || b.TokensPerCall != 100 {
			t.Fatalf("bucket = %+v, want the sample applied and nothing in flight", b)
		}
	})

	t.Run("end without begin never drives in-flight below zero", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, success(1000, time.Second), "")

		if got := bucket(t, s).InFlight; got != 0 {
			t.Fatalf("in flight = %d, want 0", got)
		}
	})

	t.Run("the first success sets tokens per call and latency directly", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, success(1000, 2*time.Second), "")

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
		s.End(key, success(1000, 2*time.Second), "")

		s.End(key, success(2000, 4*time.Second), "")

		b := bucket(t, s)
		if b.TokensPerCall != 1500 || b.Latency != 3*time.Second {
			t.Fatalf("tokens per call = %v, latency = %v, want 1500 and 3s", b.TokensPerCall, b.Latency)
		}
	})

	t.Run("a 429 updates the quotas and retry-after but not the averages", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second), "")
		limited := success(0, 50*time.Millisecond)
		limited.Status = http.StatusTooManyRequests
		limited.HasUsage = false
		limited.Quotas.Requests.Remaining = 0
		limited.RetryAfter = 20 * time.Second
		limited.HeadersAt, limited.At = now.Add(time.Second), now.Add(time.Second)

		s.End(key, limited, "")

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

	t.Run("keeps the 429 ratio and the longest retry-after over the last minute", func(t *testing.T) {
		s := minuteOfTraffic(t)

		recent := bucketAt(t, s, now.Add(50*time.Second)).Recent

		if recent.Responses != 12 || recent.RateLimited != 2 || recent.RetryAfter != 20*time.Second {
			t.Fatalf("recent = %+v, want 12 responses, 2 limited, retry-after 20s", recent)
		}
	})

	t.Run("empties the 429 window a minute after the last response", func(t *testing.T) {
		s := minuteOfTraffic(t)

		recent := bucketAt(t, s, now.Add(2*time.Minute)).Recent

		if recent.Responses != 0 || recent.RateLimited != 0 {
			t.Fatalf("recent = %+v, want the window empty", recent)
		}
	})

	t.Run("counts only the responses inside the last minute", func(t *testing.T) {
		s := minuteOfTraffic(t)
		late := success(1000, time.Second)
		late.At = now.Add(2 * time.Minute)

		s.End(key, late, "")

		if recent := bucketAt(t, s, now.Add(2*time.Minute)).Recent; recent.Responses != 1 || recent.RateLimited != 0 {
			t.Fatalf("recent = %+v, want only the sample inside the last minute", recent)
		}
	})

	t.Run("a response dated a window back never wipes the newer bin it maps to", func(t *testing.T) {
		s := store(t, 0.5)
		limited := success(0, time.Second)
		limited.Status = http.StatusTooManyRequests
		s.End(key, limited, "")
		late := success(100, time.Second)
		late.At = now.Add(-time.Minute)

		s.End(key, late, "")

		if b := bucketAt(t, s, now); b.Recent.Responses != 1 || b.Recent.RateLimited != 1 {
			t.Fatalf("recent = %+v, want the 429 kept and the response from a window back ignored", b.Recent)
		}
	})

	t.Run("records a response dated before 1970 without failing", func(t *testing.T) {
		s := store(t, 0.5)
		old := success(100, time.Second)
		old.At = time.Unix(-7, 0)

		s.End(key, old, "")

		if b := bucket(t, s); b.Responses != 1 {
			t.Fatalf("responses = %d, want the response counted", b.Responses)
		}
	})

	t.Run("the 429 window ignores responses the provider did not accept", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, state.Sample{Status: http.StatusUnauthorized, Complete: true, Headers: state.HeadersMissing, At: now}, "")
		s.End(key, state.Sample{Status: http.StatusServiceUnavailable, Complete: true, Headers: state.HeadersMissing, At: now}, "")

		b := bucketAt(t, s, now)
		if b.Recent.Responses != 0 || b.Recent.RateLimited != 0 {
			t.Fatalf("recent = %+v, want an empty window after a 401 and a 503", b.Recent)
		}
		if b.Responses != 2 {
			t.Fatalf("responses = %d, want both still counted as responses", b.Responses)
		}
	})

	t.Run("headers older than the ones applied never replace them", func(t *testing.T) {
		s := store(t, 0.5)
		exhausted := success(100, time.Second)
		exhausted.Quotas.Requests.Remaining = 0
		exhausted.HeadersAt, exhausted.At = now.Add(50*time.Second), now.Add(51*time.Second)
		older := success(100, time.Second)
		older.At = now.Add(60 * time.Second)
		s.Begin(key, now, "")
		s.Begin(key, now, "")

		s.End(key, exhausted, "")
		s.End(key, older, "")

		b := bucketAt(t, s, now.Add(60*time.Second))
		if b.Quotas.Requests.Remaining != 0 || !b.ObservedAt.Equal(now.Add(50*time.Second)) {
			t.Fatalf("remaining = %v observed at %v, want the newer headers kept and dated when they arrived", b.Quotas.Requests.Remaining, b.ObservedAt)
		}
	})

	t.Run("dates each window by the headers that reported it", func(t *testing.T) {
		s := store(t, 0.5)
		sample := success(100, time.Second)
		sample.HeadersAt = now.Add(-3 * time.Second)

		s.End(key, sample, "")

		b := bucket(t, s)
		if !b.Quotas.Requests.ReportedAt.Equal(now.Add(-3*time.Second)) || !b.Quotas.Tokens.ReportedAt.Equal(now.Add(-3*time.Second)) || !b.Quotas.ProjectTokens.ReportedAt.IsZero() {
			t.Fatalf("quotas = %+v, want the reported windows dated by their headers and the absent one left undated", b.Quotas)
		}
	})

	t.Run("a window the new sample does not know keeps its last value", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second), "")
		partial := success(1000, 2*time.Second)
		partial.Quotas = state.Quotas{Requests: headroom.Quota{Limit: 1000, Remaining: 500, ResetAt: now.Add(time.Minute)}}

		s.End(key, partial, "")

		b := bucket(t, s)
		if b.Quotas.Requests.Remaining != 500 || b.Quotas.Tokens.Remaining != 590000 {
			t.Fatalf("quotas = %+v, want requests updated and tokens kept", b.Quotas)
		}
	})

	t.Run("a success without limits counts as headers missing and keeps the last quotas", func(t *testing.T) {
		s := store(t, 0.5)
		s.End(key, success(1000, 2*time.Second), "")
		bare := success(1200, 2*time.Second)
		bare.Headers = state.HeadersMissing
		bare.Quotas = state.Quotas{}
		bare.At = now.Add(time.Second)

		s.End(key, bare, "")

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

		s.End(key, broken, "")

		b := bucket(t, s)
		if b.HeadersMalformed != 1 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want one malformed and no missing", b)
		}
		if b.ObservedAt.Equal(now) {
			t.Fatalf("observed at = %v, want untouched when nothing usable was parsed", b.ObservedAt)
		}
	})

	t.Run("malformed headers still apply the windows that parsed", func(t *testing.T) {
		s := store(t, 0.5)
		partial := success(1000, time.Second)
		partial.Headers = state.HeadersMalformed
		partial.Quotas = state.Quotas{Requests: headroom.Quota{Limit: 60, Remaining: 59}}

		s.End(key, partial, "")

		b := bucket(t, s)
		if b.HeadersMalformed != 1 || b.Quotas.Requests.Remaining != 59 || !b.ObservedAt.Equal(now) {
			t.Fatalf("bucket = %+v, want the malformed count and the parsed window applied", b)
		}
	})

	t.Run("a body that failed on the upstream side counts as an upstream error, not an abort", func(t *testing.T) {
		s := store(t, 0.5)
		broken := success(1000, time.Second)
		broken.Complete, broken.Failed = false, true

		s.End(key, broken, "")

		b := bucket(t, s)
		if b.UpstreamErrors != 1 || b.Aborted != 0 || b.Latency != 0 {
			t.Fatalf("bucket = %+v, want one upstream error and no abort or latency", b)
		}
	})

	t.Run("an abort before any response releases the slot and counts", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key, now, "")

		s.Abort(key, now, "")

		b := bucket(t, s)
		if b.InFlight != 0 || b.Aborted != 1 || b.Responses != 0 || b.UpstreamErrors != 0 {
			t.Fatalf("bucket = %+v, want the slot released, one abort and no response", b)
		}
	})

	t.Run("refuses a new bucket beyond the limit and counts the rejection", func(t *testing.T) {
		s, err := state.New(0.5, state.Limits{Buckets: 1, Consumers: 2})
		if err != nil {
			t.Fatal(err)
		}
		other := state.Key{Provider: "openai", Model: "gpt-4.1", Key: "cd34"}

		s.Begin(key, now, "")
		s.Begin(other, now, "")
		s.End(other, success(1000, time.Second), "")

		if _, ok := lookup(s, other, now); ok {
			t.Fatal("bucket beyond the limit was created")
		}
		if stats := s.Stats(); stats.Buckets != 1 || stats.RejectedBuckets != 2 {
			t.Fatalf("stats = %+v, want one bucket and two rejected touches", stats)
		}
		s.End(key, success(1000, time.Second), "")
		if b := bucket(t, s); b.Responses != 1 || b.InFlight != 0 {
			t.Fatalf("bucket = %+v, want the known bucket still updated at the limit", b)
		}
	})

	t.Run("refuses a new scaled object beyond the limit", func(t *testing.T) {
		s, err := state.New(0.5, state.Limits{Buckets: 1, Consumers: 2})
		if err != nil {
			t.Fatal(err)
		}
		keep := func(state.Ceiling, bool) state.Ceiling { return state.Ceiling{Value: 1, Seen: now} }
		first := state.Consumer{Namespace: "jobs", ScaledObject: "a", Bucket: key}
		second := state.Consumer{Namespace: "jobs", ScaledObject: "b", Bucket: key}

		if _, err := s.UpdateCeiling(first, keep); err != nil {
			t.Fatal(err)
		}
		if _, err := s.UpdateCeiling(state.Consumer{Namespace: "jobs", ScaledObject: "c"}, keep); err != nil {
			t.Fatal(err)
		}
		_, err = s.UpdateCeiling(second, keep)

		if !errors.Is(err, state.ErrTooManyConsumers) {
			t.Fatalf("err = %v, want ErrTooManyConsumers", err)
		}
		if _, err := s.UpdateCeiling(first, keep); err != nil {
			t.Fatalf("known scaled object rejected at the limit: %v", err)
		}
		if stats := s.Stats(); stats.Consumers != 2 || stats.RejectedConsumers != 1 {
			t.Fatalf("stats = %+v, want one scaled object and one rejection", stats)
		}
	})

	t.Run("rejects non-positive limits", func(t *testing.T) {
		if _, err := state.New(0.5, state.Limits{Buckets: 0, Consumers: 1}); err == nil {
			t.Fatal("expected an error for a zero bucket limit")
		}
	})

	t.Run("a success without usage counts as usage missing", func(t *testing.T) {
		s := store(t, 0.5)
		noUsage := success(0, time.Second)
		noUsage.HasUsage = false

		s.End(key, noUsage, "")

		b := bucket(t, s)
		if b.UsageMissing != 1 || b.TokensPerCall != 0 {
			t.Fatalf("usage missing = %d, tokens per call = %v, want 1 and 0", b.UsageMissing, b.TokensPerCall)
		}
	})

	t.Run("an aborted response keeps its quotas but skips latency and usage", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key, now, "")
		aborted := success(0, 400*time.Millisecond)
		aborted.Complete = false
		aborted.HasUsage = false

		s.End(key, aborted, "")

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
		s.Begin(key, now, "")

		s.Fail(key, now.Add(time.Second), "")

		b := bucket(t, s)
		if b.InFlight != 0 || b.UpstreamErrors != 1 || b.Responses != 0 || !b.LastSeen.Equal(now.Add(time.Second)) {
			t.Fatalf("bucket = %+v, want one upstream error, nothing observed, last seen updated", b)
		}
	})

	t.Run("a 5xx without limits is not counted as headers missing", func(t *testing.T) {
		s := store(t, 0.5)

		s.End(key, state.Sample{Status: http.StatusBadGateway, Complete: true, Headers: state.HeadersMissing, At: now}, "")

		b := bucket(t, s)
		if b.Responses != 1 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want one response and no missing headers", b)
		}
	})

	t.Run("idle buckets are evicted while busy or recent ones stay", func(t *testing.T) {
		s := store(t, 0.5)
		idle := state.Key{Provider: "openai", Model: "typo", Key: "a"}
		busy := state.Key{Provider: "openai", Model: "busy", Key: "a"}
		s.Begin(idle, now, "")
		s.End(idle, success(10, time.Second), "")
		s.Begin(busy, now, "")
		s.Begin(key, now.Add(9*time.Minute), "")
		s.End(key, success(10, time.Second), "")

		evicted := s.Evict(now.Add(11*time.Minute), 10*time.Minute)

		if evicted != 1 {
			t.Fatalf("evicted = %d, want 1", evicted)
		}
		if _, ok := lookup(s, idle, now); ok {
			t.Fatal("idle bucket still present")
		}
		if _, ok := lookup(s, busy, now); !ok {
			t.Fatal("bucket with a call in flight was evicted")
		}
		if _, ok := lookup(s, key, now); !ok {
			t.Fatal("recently seen bucket was evicted")
		}
	})

	t.Run("ceilings are updated atomically per consumer and dropped once nobody polls them", func(t *testing.T) {
		s := store(t, 0.5)
		consumer := state.Consumer{Namespace: "jobs", ScaledObject: "llm-workers", Bucket: key}
		first := updateCeiling(t, s, consumer, func(previous state.Ceiling, known bool) state.Ceiling {
			if known {
				t.Fatal("a previous ceiling was reported before any update")
			}
			return state.Ceiling{Value: 4, BoundChangedAt: now, Seen: now}
		})
		second := updateCeiling(t, s, consumer, func(previous state.Ceiling, known bool) state.Ceiling {
			if !known || previous.Value != 4 {
				t.Fatalf("previous = %+v known = %v, want the first update", previous, known)
			}
			return state.Ceiling{Value: 6, BoundChangedAt: now.Add(time.Minute), Seen: now.Add(time.Minute)}
		})

		if first.Value != 4 || second.Value != 6 || second.Consumer != consumer || len(s.Ceilings()) != 1 {
			t.Fatalf("updates = %+v / %+v, want both applied to one consumer", first, second)
		}

		s.Evict(now.Add(12*time.Minute), 10*time.Minute)

		if len(s.Ceilings()) != 0 {
			t.Fatal("ceiling survived although its consumer stopped polling")
		}
	})

	t.Run("a scaled object keeps one ceiling per trigger", func(t *testing.T) {
		s := store(t, 0.5)
		keep := func(value int) func(state.Ceiling, bool) state.Ceiling {
			return func(state.Ceiling, bool) state.Ceiling { return state.Ceiling{Value: value, Seen: now} }
		}
		updateCeiling(t, s, state.Consumer{Namespace: "jobs", ScaledObject: "llm-workers", Trigger: "b", Bucket: key}, keep(4))
		updateCeiling(t, s, state.Consumer{Namespace: "jobs", ScaledObject: "llm-workers", Trigger: "a", Bucket: key}, keep(1))

		all := s.Ceilings()

		if len(all) != 2 || all[0].Consumer.Trigger != "a" || all[0].Value != 1 || all[1].Consumer.Trigger != "b" || all[1].Value != 4 {
			t.Fatalf("ceilings = %+v, want both triggers kept and listed by trigger", all)
		}
	})

	t.Run("buckets are listed in a stable order", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "b"}, now, "")
		s.Begin(state.Key{Provider: "anthropic", Model: "claude-sonnet-4-5", Key: "a"}, now, "")
		s.Begin(state.Key{Provider: "openai", Model: "gpt-4.1", Key: "a"}, now, "")

		all := s.Buckets(now)

		want := []state.Key{
			{Provider: "anthropic", Model: "claude-sonnet-4-5", Key: "a"},
			{Provider: "openai", Model: "gpt-4.1", Key: "a"},
			{Provider: "openai", Model: "gpt-4.1", Key: "b"},
		}
		if len(all) != len(want) {
			t.Fatalf("buckets = %d, want %d", len(all), len(want))
		}
		for i, b := range all {
			if b.Key != want[i] {
				t.Fatalf("bucket %d = %+v, want %+v", i, b.Key, want[i])
			}
		}
	})

	t.Run("a bucket converts to a headroom observation", func(t *testing.T) {
		s := store(t, 0.5)
		s.Begin(key, now, "")
		s.Begin(key, now, "")
		s.End(key, success(1000, 2*time.Second), "")

		obs := bucket(t, s).Observation()

		if obs.InFlight != 1 || obs.TokensPerCall != 1000 || obs.Latency != 2*time.Second || !obs.ObservedAt.Equal(now) {
			t.Fatalf("observation = %+v, want in-flight 1, 1000 tokens per call, 2s latency, observed at now", obs)
		}
		if obs.Requests.Limit != 1000 || obs.Tokens.Limit != 600000 {
			t.Fatalf("observation quotas = %+v / %+v, want the sample quotas", obs.Requests, obs.Tokens)
		}
	})

	t.Run("rejects an alpha outside the unit interval", func(t *testing.T) {
		for _, alpha := range []float64{0, -0.1, 1.5, math.NaN()} {
			if _, err := state.New(alpha, state.DefaultLimits()); err == nil {
				t.Fatalf("alpha %v accepted", alpha)
			}
		}
	})
}

func TestBucketAccepted(t *testing.T) {
	cases := []struct {
		name     string
		statuses []int
		want     bool
	}{
		{"a bucket the provider only refused is not accepted", []int{http.StatusUnauthorized, http.StatusForbidden}, false},
		{"a bucket with only upstream failures is not accepted", []int{http.StatusServiceUnavailable}, false},
		{"a success accepts the bucket", []int{http.StatusUnauthorized, http.StatusOK}, true},
		{"a 429 alone accepts the bucket", []int{http.StatusTooManyRequests}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := store(t, 0.5)
			for _, status := range tc.statuses {
				s.End(key, state.Sample{Status: status, Complete: true, Headers: state.HeadersMissing, At: now}, "")
			}

			got := bucketAt(t, s, now).Accepted()

			if got != tc.want {
				t.Fatalf("accepted = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBucketRefusedOnly(t *testing.T) {
	cases := []struct {
		name     string
		statuses []int
		want     bool
	}{
		{"a bucket answered only with 401 and 403 is refused", []int{http.StatusUnauthorized, http.StatusForbidden}, true},
		{"a bucket answered only with a 5xx is not refused", []int{http.StatusServiceUnavailable}, false},
		{"a bucket refused once and then accepted is not refused", []int{http.StatusUnauthorized, http.StatusOK}, false},
		{"a bucket without any answer is not refused", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := store(t, 0.5)
			s.Begin(key, now, "")
			for _, status := range tc.statuses {
				s.End(key, state.Sample{Status: status, Complete: true, Headers: state.HeadersMissing, At: now}, "")
			}

			got := bucketAt(t, s, now).RefusedOnly()

			if got != tc.want {
				t.Fatalf("refused only = %v, want %v", got, tc.want)
			}
		})
	}
}

func minuteOfTraffic(t *testing.T) *state.Store {
	t.Helper()
	s := store(t, 0.5)
	for i := range 10 {
		sample := success(1000, time.Second)
		sample.At = now.Add(time.Duration(i) * 4 * time.Second)
		s.End(key, sample, "")
	}
	limited := success(0, 50*time.Millisecond)
	limited.Status, limited.HasUsage = http.StatusTooManyRequests, false
	limited.RetryAfter, limited.At = 20*time.Second, now.Add(45*time.Second)
	s.End(key, limited, "")
	limited.RetryAfter, limited.At = 5*time.Second, now.Add(50*time.Second)
	s.End(key, limited, "")
	return s
}

func lookup(s *state.Store, wanted state.Key, at time.Time) (state.Bucket, bool) {
	for _, b := range s.Buckets(at) {
		if b.Key == wanted {
			return b, true
		}
	}
	return state.Bucket{}, false
}

func TestRefusesASlotBudgetThatCannotServeOneTrigger(t *testing.T) {
	if _, err := state.New(.5, state.Limits{Buckets: 10, Consumers: 1}); err == nil {
		t.Fatal("one slot accepted")
	}
}

func TestFinishingACallReleasesOnlyItsWorkload(t *testing.T) {
	cases := []struct {
		name   string
		finish func(*state.Store)
	}{
		{"response", func(s *state.Store) { s.End(key, success(100, time.Second), "indexer") }},
		{"abort", func(s *state.Store) { s.Abort(key, now, "indexer") }},
		{"transport failure", func(s *state.Store) { s.Fail(key, now, "indexer") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := store(t, .5)
			s.Begin(key, now, "indexer")
			s.Begin(key, now, "summarizer")
			before := s.Buckets(now)[0]
			tc.finish(s)
			after := s.Buckets(now)[0]
			if before.OpenCalls("indexer") != 1 || after.OpenCalls("indexer") != 0 || after.OpenCalls("summarizer") != 1 || after.InFlight != 1 {
				t.Fatalf("before=%+v after=%+v", before, after)
			}
		})
	}
}

func rateLimited(quotas state.Quotas) state.Sample {
	sample := success(0, 50*time.Millisecond)
	sample.Status, sample.HasUsage, sample.Quotas = http.StatusTooManyRequests, false, quotas
	sample.RetryAfter, sample.HeadersAt, sample.At = 30*time.Second, now.Add(time.Second), now.Add(time.Second)
	return sample
}

func TestUnexplainedRateLimits(t *testing.T) {
	spentRequests := quotas()
	spentRequests.Requests.Remaining = 0
	spentTokens := quotas()
	spentTokens.Tokens.Remaining = 400
	spentProjectTokens := quotas()
	spentProjectTokens.ProjectTokens = headroom.Quota{Limit: 300000, Remaining: 0, ResetAt: now.Add(time.Minute)}
	cases := []struct {
		name            string
		quotas          state.Quotas
		wantUnexplained int64
	}{
		{"a 429 whose headers show no request remaining is explained", spentRequests, 0},
		{"a 429 whose headers show fewer tokens than a call uses is explained", spentTokens, 0},
		{"a 429 whose project window is spent is explained", spentProjectTokens, 0},
		{"a 429 whose headers still show quota for another call is unexplained", quotas(), 1},
		{"a 429 without headers is unexplained", state.Quotas{}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := store(t, 0.5)
			s.End(key, success(1000, 2*time.Second), "")

			s.End(key, rateLimited(tc.quotas), "")

			b := bucketAt(t, s, now.Add(2*time.Second))
			if b.RateLimited != 1 || b.Unexplained != tc.wantUnexplained {
				t.Fatalf("rate limited = %d, unexplained = %d, want 1 and %d", b.RateLimited, b.Unexplained, tc.wantUnexplained)
			}
			if b.Recent.RateLimited != 1 || b.Recent.Unexplained != tc.wantUnexplained {
				t.Fatalf("recent = %+v, want 1 limited and %d unexplained in the window", b.Recent, tc.wantUnexplained)
			}
		})
	}
}
