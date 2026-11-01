package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dimonchik03/quotaware/internal/headroom"
	"github.com/dimonchik03/quotaware/internal/state"
)

var bucketLabels = []string{"provider", "model", "key"}

type Collector struct {
	store *state.Store
	now   func() time.Time

	limitRequests     *prometheus.Desc
	remainingRequests *prometheus.Desc
	limitTokens       *prometheus.Desc
	remainingTokens   *prometheus.Desc
	resetSeconds      *prometheus.Desc
	tokensPerCall     *prometheus.Desc
	latencySeconds    *prometheus.Desc
	inflight          *prometheus.Desc
	retryAfterSeconds *prometheus.Desc
	responses         *prometheus.Desc
	rateLimited       *prometheus.Desc
	aborted           *prometheus.Desc
	upstreamErrors    *prometheus.Desc
	headersMissing    *prometheus.Desc
	headersMalformed  *prometheus.Desc
	usageMissing      *prometheus.Desc
}

func NewCollector(store *state.Store, now func() time.Time) *Collector {
	describe := func(name, help string, extra ...string) *prometheus.Desc {
		return prometheus.NewDesc("quotaware_"+name, help, append(append([]string{}, bucketLabels...), extra...), nil)
	}
	return &Collector{
		store:             store,
		now:               now,
		limitRequests:     describe("limit_requests", "Request limit of the window as reported by the provider."),
		remainingRequests: describe("remaining_requests", "Requests remaining in the current window."),
		limitTokens:       describe("limit_tokens", "Token limit of the window as reported by the provider.", "window"),
		remainingTokens:   describe("remaining_tokens", "Tokens remaining in the current window.", "window"),
		resetSeconds:      describe("reset_seconds", "Seconds until the window resets.", "window"),
		tokensPerCall:     describe("tokens_per_call", "Smoothed tokens consumed per successful call."),
		latencySeconds:    describe("latency_seconds", "Smoothed wall-clock latency of successful calls."),
		inflight:          describe("inflight", "Calls currently open through the proxy."),
		retryAfterSeconds: describe("retry_after_seconds", "Retry-After of the last 429."),
		responses:         describe("responses_total", "Responses observed from the provider."),
		rateLimited:       describe("429_total", "Responses with status 429."),
		aborted:           describe("aborted_total", "Responses the client stopped reading before the end."),
		upstreamErrors:    describe("upstream_errors_total", "Calls that never produced a provider response."),
		headersMissing:    describe("headers_missing_total", "Responses that should carry rate-limit headers but did not."),
		headersMalformed:  describe("headers_malformed_total", "Responses whose rate-limit headers could not be parsed."),
		usageMissing:      describe("usage_missing_total", "Successful responses without a usable usage object."),
	}
}

func Handler(store *state.Store, now func() time.Time) http.Handler {
	registry := prometheus.NewRegistry()
	registry.MustRegister(NewCollector(store, now))
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs() {
		ch <- desc
	}
}

type window struct {
	name  string
	quota headroom.Quota
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	now := c.now()
	for _, b := range c.store.Buckets() {
		labels := []string{b.Key.Provider, b.Key.Model, b.Key.Key}
		gauge := func(desc *prometheus.Desc, value float64, extra ...string) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, append(append([]string{}, labels...), extra...)...)
		}
		counter := func(desc *prometheus.Desc, value int64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, float64(value), labels...)
		}
		if b.Quotas.Requests.Known() {
			gauge(c.limitRequests, b.Quotas.Requests.Limit)
			gauge(c.remainingRequests, b.Quotas.Requests.Remaining)
			gauge(c.resetSeconds, secondsUntil(b.Quotas.Requests, now), "requests")
		}
		tokenWindows := []window{
			{"tokens", b.Quotas.Tokens},
			{"project_tokens", b.Quotas.ProjectTokens},
			{"input_tokens", b.Quotas.InputTokens},
			{"output_tokens", b.Quotas.OutputTokens},
		}
		for _, tokens := range tokenWindows {
			if tokens.quota.Known() {
				gauge(c.limitTokens, tokens.quota.Limit, tokens.name)
				gauge(c.remainingTokens, tokens.quota.Remaining, tokens.name)
				gauge(c.resetSeconds, secondsUntil(tokens.quota, now), tokens.name)
			}
		}
		gauge(c.tokensPerCall, b.TokensPerCall)
		gauge(c.latencySeconds, b.Latency.Seconds())
		gauge(c.inflight, float64(b.InFlight))
		gauge(c.retryAfterSeconds, b.RetryAfter.Seconds())
		counter(c.responses, b.Responses)
		counter(c.rateLimited, b.RateLimited)
		counter(c.aborted, b.Aborted)
		counter(c.upstreamErrors, b.UpstreamErrors)
		counter(c.headersMissing, b.HeadersMissing)
		counter(c.headersMalformed, b.HeadersMalformed)
		counter(c.usageMissing, b.UsageMissing)
	}
}

func (c *Collector) descs() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.limitRequests, c.remainingRequests, c.limitTokens, c.remainingTokens, c.resetSeconds,
		c.tokensPerCall, c.latencySeconds, c.inflight, c.retryAfterSeconds,
		c.responses, c.rateLimited, c.aborted, c.upstreamErrors, c.headersMissing, c.headersMalformed, c.usageMissing,
	}
}

func secondsUntil(q headroom.Quota, now time.Time) float64 {
	if q.ResetAt.IsZero() {
		return 0
	}
	return max(q.ResetAt.Sub(now).Seconds(), 0)
}
