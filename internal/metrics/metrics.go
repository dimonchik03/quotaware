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
	responses         *prometheus.Desc
	rateLimited       *prometheus.Desc
	retryAfterSeconds *prometheus.Desc
	headersMissing    *prometheus.Desc
	usageMissing      *prometheus.Desc
}

func NewCollector(store *state.Store, now func() time.Time) *Collector {
	gauge := func(name, help string, extra ...string) *prometheus.Desc {
		return prometheus.NewDesc("quotaware_"+name, help, append(append([]string{}, bucketLabels...), extra...), nil)
	}
	return &Collector{
		store:             store,
		now:               now,
		limitRequests:     gauge("limit_requests", "Request limit of the window as reported by the provider."),
		remainingRequests: gauge("remaining_requests", "Requests remaining in the current window."),
		limitTokens:       gauge("limit_tokens", "Token limit of the window as reported by the provider.", "scope"),
		remainingTokens:   gauge("remaining_tokens", "Tokens remaining in the current window.", "scope"),
		resetSeconds:      gauge("reset_seconds", "Seconds until the window resets.", "window"),
		tokensPerCall:     gauge("tokens_per_call", "Smoothed tokens consumed per successful call."),
		latencySeconds:    gauge("latency_seconds", "Smoothed wall-clock latency of successful calls."),
		inflight:          gauge("inflight", "Calls currently open through the proxy."),
		responses:         gauge("responses_total", "Responses observed from the provider."),
		rateLimited:       gauge("429_total", "Responses with status 429."),
		retryAfterSeconds: gauge("retry_after_seconds", "Retry-After of the last 429."),
		headersMissing:    gauge("headers_missing_total", "Responses that should carry rate-limit headers but did not."),
		usageMissing:      gauge("usage_missing_total", "Successful responses without a usable usage object."),
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

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	now := c.now()
	for _, b := range c.store.Buckets() {
		labels := []string{b.Key.Provider, b.Key.Model, b.Key.Key}
		gauge := func(desc *prometheus.Desc, value float64, extra ...string) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, append(append([]string{}, labels...), extra...)...)
		}
		counter := func(desc *prometheus.Desc, value float64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, value, labels...)
		}
		if b.Requests.Known() {
			gauge(c.limitRequests, b.Requests.Limit)
			gauge(c.remainingRequests, b.Requests.Remaining)
			gauge(c.resetSeconds, secondsUntil(b.Requests, now), "requests")
		}
		for scope, tokens := range map[string]headroom.Quota{"organization": b.Tokens, "project": b.ProjectTokens} {
			if tokens.Known() {
				gauge(c.limitTokens, tokens.Limit, scope)
				gauge(c.remainingTokens, tokens.Remaining, scope)
				gauge(c.resetSeconds, secondsUntil(tokens, now), "tokens_"+scope)
			}
		}
		gauge(c.tokensPerCall, b.TokensPerCall)
		gauge(c.latencySeconds, b.Latency.Seconds())
		gauge(c.inflight, float64(b.InFlight))
		gauge(c.retryAfterSeconds, b.RetryAfter.Seconds())
		counter(c.responses, float64(b.Responses))
		counter(c.rateLimited, float64(b.RateLimited))
		counter(c.headersMissing, float64(b.HeadersMissing))
		counter(c.usageMissing, float64(b.UsageMissing))
	}
}

func (c *Collector) descs() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.limitRequests, c.remainingRequests, c.limitTokens, c.remainingTokens, c.resetSeconds,
		c.tokensPerCall, c.latencySeconds, c.inflight, c.responses, c.rateLimited,
		c.retryAfterSeconds, c.headersMissing, c.usageMissing,
	}
}

func secondsUntil(q headroom.Quota, now time.Time) float64 {
	if q.ResetAt.IsZero() {
		return 0
	}
	return max(q.ResetAt.Sub(now).Seconds(), 0)
}
