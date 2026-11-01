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
	s.Begin(key)
	s.Begin(key)
	s.End(key, state.Sample{
		Status:        http.StatusOK,
		HasLimits:     true,
		Requests:      headroom.Quota{Limit: 1000, Remaining: 990, ResetAt: now.Add(30 * time.Second)},
		Tokens:        headroom.Quota{Limit: 600000, Remaining: 590000, ResetAt: now.Add(45 * time.Second)},
		ProjectTokens: headroom.Quota{Limit: 100000, Remaining: 20000, ResetAt: now.Add(48 * time.Second)},
		HasUsage:      true,
		Usage:         1200,
		Latency:       1500 * time.Millisecond,
		At:            now,
	})
	limited := state.Sample{Status: http.StatusTooManyRequests, HasLimits: true, RetryAfter: 20 * time.Second, At: now}
	limited.Requests = headroom.Quota{Limit: 1000, Remaining: 0, ResetAt: now.Add(30 * time.Second)}
	limited.Tokens = headroom.Quota{Limit: 600000, Remaining: 0, ResetAt: now.Add(45 * time.Second)}
	limited.ProjectTokens = headroom.Quota{Limit: 100000, Remaining: 20000, ResetAt: now.Add(48 * time.Second)}
	s.Begin(key)
	s.End(key, limited)
	return s
}

func TestCollector(t *testing.T) {
	t.Run("exposes every bucket metric with provider, model and key labels", func(t *testing.T) {
		collector := metrics.NewCollector(populated(t), func() time.Time { return now })
		expected := `
# HELP quotaware_429_total Responses with status 429.
# TYPE quotaware_429_total counter
quotaware_429_total{key="ab12",model="gpt-4.1-mini",provider="openai"} 1
# HELP quotaware_inflight Calls currently open through the proxy.
# TYPE quotaware_inflight gauge
quotaware_inflight{key="ab12",model="gpt-4.1-mini",provider="openai"} 1
# HELP quotaware_remaining_requests Requests remaining in the current window.
# TYPE quotaware_remaining_requests gauge
quotaware_remaining_requests{key="ab12",model="gpt-4.1-mini",provider="openai"} 0
# HELP quotaware_remaining_tokens Tokens remaining in the current window.
# TYPE quotaware_remaining_tokens gauge
quotaware_remaining_tokens{key="ab12",model="gpt-4.1-mini",provider="openai",scope="organization"} 0
quotaware_remaining_tokens{key="ab12",model="gpt-4.1-mini",provider="openai",scope="project"} 20000
# HELP quotaware_reset_seconds Seconds until the window resets.
# TYPE quotaware_reset_seconds gauge
quotaware_reset_seconds{key="ab12",model="gpt-4.1-mini",provider="openai",window="requests"} 30
quotaware_reset_seconds{key="ab12",model="gpt-4.1-mini",provider="openai",window="tokens_organization"} 45
quotaware_reset_seconds{key="ab12",model="gpt-4.1-mini",provider="openai",window="tokens_project"} 48
# HELP quotaware_retry_after_seconds Retry-After of the last 429.
# TYPE quotaware_retry_after_seconds gauge
quotaware_retry_after_seconds{key="ab12",model="gpt-4.1-mini",provider="openai"} 20
# HELP quotaware_tokens_per_call Smoothed tokens consumed per successful call.
# TYPE quotaware_tokens_per_call gauge
quotaware_tokens_per_call{key="ab12",model="gpt-4.1-mini",provider="openai"} 1200
`
		if err := testutil.CollectAndCompare(collector, strings.NewReader(expected),
			"quotaware_429_total", "quotaware_inflight", "quotaware_remaining_requests", "quotaware_remaining_tokens",
			"quotaware_reset_seconds", "quotaware_retry_after_seconds", "quotaware_tokens_per_call"); err != nil {
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
