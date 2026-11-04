package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dimonchik03/quotaware/internal/headroom"
	"github.com/dimonchik03/quotaware/internal/metrics"
	"github.com/dimonchik03/quotaware/internal/state"
)

var now = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

func populated(t *testing.T) *state.Store {
	t.Helper()
	s, err := state.New(0.5)
	if err != nil {
		t.Fatal(err)
	}
	key := state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "ab12"}
	quotas := state.Quotas{
		Requests:      headroom.Quota{Limit: 1000, Remaining: 990, ResetAt: now.Add(30 * time.Second)},
		Tokens:        headroom.Quota{Limit: 600000, Remaining: 590000, ResetAt: now.Add(45 * time.Second)},
		ProjectTokens: headroom.Quota{Limit: 100000, Remaining: 20000, ResetAt: now.Add(48 * time.Second)},
	}
	s.Begin(key, now)
	s.Begin(key, now)
	s.End(key, state.Sample{
		Status:   http.StatusOK,
		Complete: true,
		Headers:  state.HeadersPresent,
		Quotas:   quotas,
		HasUsage: true,
		Usage:    1200,
		Latency:  1500 * time.Millisecond,
		At:       now,
	})
	quotas.Requests.Remaining = 0
	quotas.Tokens.Remaining = 0
	s.Begin(key, now)
	s.End(key, state.Sample{Status: http.StatusTooManyRequests, Complete: true, Headers: state.HeadersPresent, Quotas: quotas, RetryAfter: 20 * time.Second, At: now})
	s.Begin(key, now)
	s.Fail(key, now)
	_, err = s.UpdateCeiling(state.Consumer{Namespace: "jobs", ScaledObject: "llm-workers", Trigger: "7c1d", Bucket: key}, func(state.Ceiling, bool) state.Ceiling {
		return state.Ceiling{Value: 4, Bound: 4, Mode: state.ModeHeadroom, ChangedAt: now, Seen: now}
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCollector(t *testing.T) {
	t.Run("exposes how many buckets and scaled objects are tracked and how many were refused", func(t *testing.T) {
		s, err := state.NewWithLimits(0.5, state.Limits{Buckets: 1, Consumers: 1})
		if err != nil {
			t.Fatal(err)
		}
		s.Begin(state.Key{Provider: "openai", Model: "a", Key: "k"}, now)
		s.Begin(state.Key{Provider: "openai", Model: "b", Key: "k"}, now)
		keep := func(state.Ceiling, bool) state.Ceiling { return state.Ceiling{Value: 1, Seen: now} }
		if _, err := s.UpdateCeiling(state.Consumer{Namespace: "jobs", ScaledObject: "a"}, keep); err != nil {
			t.Fatal(err)
		}
		_, _ = s.UpdateCeiling(state.Consumer{Namespace: "jobs", ScaledObject: "b"}, keep)
		collector := metrics.NewCollector(s, func() time.Time { return now })
		expected := `
# HELP quotaware_buckets Buckets currently tracked.
# TYPE quotaware_buckets gauge
quotaware_buckets 1
# HELP quotaware_buckets_rejected_total Calls on new buckets refused because the bucket limit was reached.
# TYPE quotaware_buckets_rejected_total counter
quotaware_buckets_rejected_total 1
# HELP quotaware_scaled_objects Scaled objects currently tracking a ceiling.
# TYPE quotaware_scaled_objects gauge
quotaware_scaled_objects 1
# HELP quotaware_scaled_objects_rejected_total Ceiling evaluations refused because the scaled object limit was reached.
# TYPE quotaware_scaled_objects_rejected_total counter
quotaware_scaled_objects_rejected_total 1
`

		if err := testutil.CollectAndCompare(collector, strings.NewReader(expected),
			"quotaware_buckets", "quotaware_buckets_rejected_total", "quotaware_scaled_objects", "quotaware_scaled_objects_rejected_total"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("exposes every bucket metric with provider, model and key labels", func(t *testing.T) {
		collector := metrics.NewCollector(populated(t), func() time.Time { return now })
		expected := `
# HELP quotaware_429_total Responses with status 429.
# TYPE quotaware_429_total counter
quotaware_429_total{key="ab12",model="gpt-4.1-mini",provider="openai"} 1
# HELP quotaware_ceiling Replica ceiling of one bucket for a scaled object and trigger, before the sum KEDA receives.
# TYPE quotaware_ceiling gauge
quotaware_ceiling{key="ab12",model="gpt-4.1-mini",namespace="jobs",provider="openai",scaledobject="llm-workers",trigger="7c1d"} 4
# HELP quotaware_inflight Calls currently open through the proxy.
# TYPE quotaware_inflight gauge
quotaware_inflight{key="ab12",model="gpt-4.1-mini",provider="openai"} 1
# HELP quotaware_limit_requests Request limit of the window as reported by the provider.
# TYPE quotaware_limit_requests gauge
quotaware_limit_requests{key="ab12",model="gpt-4.1-mini",provider="openai"} 1000
# HELP quotaware_limit_tokens Token limit of the window as reported by the provider.
# TYPE quotaware_limit_tokens gauge
quotaware_limit_tokens{key="ab12",model="gpt-4.1-mini",provider="openai",window="project_tokens"} 100000
quotaware_limit_tokens{key="ab12",model="gpt-4.1-mini",provider="openai",window="tokens"} 600000
# HELP quotaware_rate_limited_ratio Share of 429 among the 2xx and 429 responses of the last minute.
# TYPE quotaware_rate_limited_ratio gauge
quotaware_rate_limited_ratio{key="ab12",model="gpt-4.1-mini",provider="openai"} 0.5
# HELP quotaware_remaining_requests Requests remaining in the current window.
# TYPE quotaware_remaining_requests gauge
quotaware_remaining_requests{key="ab12",model="gpt-4.1-mini",provider="openai"} 0
# HELP quotaware_remaining_tokens Tokens remaining in the current window.
# TYPE quotaware_remaining_tokens gauge
quotaware_remaining_tokens{key="ab12",model="gpt-4.1-mini",provider="openai",window="project_tokens"} 20000
quotaware_remaining_tokens{key="ab12",model="gpt-4.1-mini",provider="openai",window="tokens"} 0
# HELP quotaware_reset_seconds Seconds until the window resets.
# TYPE quotaware_reset_seconds gauge
quotaware_reset_seconds{key="ab12",model="gpt-4.1-mini",provider="openai",window="project_tokens"} 48
quotaware_reset_seconds{key="ab12",model="gpt-4.1-mini",provider="openai",window="requests"} 30
quotaware_reset_seconds{key="ab12",model="gpt-4.1-mini",provider="openai",window="tokens"} 45
# HELP quotaware_responses_total Responses observed from the provider.
# TYPE quotaware_responses_total counter
quotaware_responses_total{key="ab12",model="gpt-4.1-mini",provider="openai"} 2
# HELP quotaware_retry_after_seconds Retry-After of the last 429.
# TYPE quotaware_retry_after_seconds gauge
quotaware_retry_after_seconds{key="ab12",model="gpt-4.1-mini",provider="openai"} 20
# HELP quotaware_tokens_per_call Smoothed tokens consumed per successful call.
# TYPE quotaware_tokens_per_call gauge
quotaware_tokens_per_call{key="ab12",model="gpt-4.1-mini",provider="openai"} 1200
# HELP quotaware_upstream_errors_total Calls that never produced a provider response.
# TYPE quotaware_upstream_errors_total counter
quotaware_upstream_errors_total{key="ab12",model="gpt-4.1-mini",provider="openai"} 1
# HELP quotaware_usage_missing_total Successful responses without a usable usage object.
# TYPE quotaware_usage_missing_total counter
quotaware_usage_missing_total{key="ab12",model="gpt-4.1-mini",provider="openai"} 0
`
		if err := testutil.CollectAndCompare(collector, strings.NewReader(expected),
			"quotaware_429_total", "quotaware_ceiling", "quotaware_inflight", "quotaware_limit_requests", "quotaware_limit_tokens", "quotaware_rate_limited_ratio",
			"quotaware_remaining_requests", "quotaware_remaining_tokens", "quotaware_reset_seconds",
			"quotaware_responses_total", "quotaware_retry_after_seconds", "quotaware_tokens_per_call",
			"quotaware_upstream_errors_total", "quotaware_usage_missing_total"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("passes the Prometheus metric lint", func(t *testing.T) {
		problems, err := testutil.CollectAndLint(metrics.NewCollector(populated(t), func() time.Time { return now }))
		if err != nil {
			t.Fatal(err)
		}
		if len(problems) != 0 {
			t.Fatalf("lint problems: %v", problems)
		}
	})

	t.Run("serves the registry over HTTP", func(t *testing.T) {
		handler := metrics.Handler(populated(t), func() time.Time { return now })
		recorder := httptest.NewRecorder()

		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", recorder.Code)
		}
		if !strings.Contains(recorder.Body.String(), `quotaware_latency_seconds{key="ab12",model="gpt-4.1-mini",provider="openai"} 1.5`) {
			t.Fatalf("body = %s, want the latency gauge", recorder.Body.String())
		}
	})
}
