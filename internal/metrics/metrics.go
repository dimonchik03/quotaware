package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dimonchik03/quotaware/internal/aimd"
	"github.com/dimonchik03/quotaware/internal/headroom"
	"github.com/dimonchik03/quotaware/internal/state"
)

var (
	bucketLabels   = []string{"provider", "model", "key"}
	consumerLabels = []string{"namespace", "scaledobject", "trigger", "provider", "model", "key"}
)

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
	rateLimitedRatio  *prometheus.Desc
	unexplainedRatio  *prometheus.Desc
	responses         *prometheus.Desc
	rateLimited       *prometheus.Desc
	unexplained       *prometheus.Desc
	aborted           *prometheus.Desc
	upstreamErrors    *prometheus.Desc
	headersMissing    *prometheus.Desc
	headersMalformed  *prometheus.Desc
	usageMissing      *prometheus.Desc
	ceiling           *prometheus.Desc
	buckets           *prometheus.Desc
	consumers         *prometheus.Desc
	bucketsRejected   *prometheus.Desc
	consumersRejected *prometheus.Desc
	callsRejected     *prometheus.Desc
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
		rateLimitedRatio:  describe("rate_limited_ratio", "Share of 429 among the 2xx and 429 responses of the last minute."),
		unexplainedRatio:  describe("unexplained_rate_limited_ratio", "Share of unexplained 429 among the 2xx and 429 responses of the last minute; the guard on a header ceiling reacts to it."),
		responses:         describe("responses_total", "Responses observed from the provider."),
		rateLimited:       describe("429_total", "Responses with status 429."),
		unexplained:       describe("unexplained_429_total", "Responses with status 429 that their own rate-limit headers do not explain: no headers, or no spent window the ceiling reads."),
		aborted:           describe("aborted_total", "Calls the client gave up on, before the provider answered or while reading the answer."),
		upstreamErrors:    describe("upstream_errors_total", "Calls the upstream failed: no response at all, or a body that broke mid-stream."),
		headersMissing:    describe("headers_missing_total", "Responses that should carry rate-limit headers but did not."),
		headersMalformed:  describe("headers_malformed_total", "Responses whose rate-limit headers could not be parsed."),
		usageMissing:      describe("usage_missing_total", "Successful responses without a usable usage object."),
		ceiling:           prometheus.NewDesc("quotaware_ceiling", "Replica ceiling of a scaled object and trigger: one series per credential bucket, and key=\"combined\" for the quota ceiling before queue demand.", consumerLabels, nil),
		buckets:           prometheus.NewDesc("quotaware_buckets", "Buckets currently tracked.", nil, nil),
		consumers:         prometheus.NewDesc("quotaware_scaled_objects", "Ceilings currently tracked, one per scaled object, trigger and credential.", nil, nil),
		bucketsRejected:   prometheus.NewDesc("quotaware_buckets_rejected_total", "New bucket admissions refused because the bucket limit was reached.", nil, nil),
		consumersRejected: prometheus.NewDesc("quotaware_scaled_objects_rejected_total", "Ceiling evaluations refused because the ceiling limit was reached.", nil, nil),
		callsRejected:     prometheus.NewDesc("quotaware_calls_rejected_total", "Proxied calls answered 503 because the in-flight limit was reached.", nil, nil),
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
	for _, b := range c.store.Buckets(now) {
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
		gauge(c.rateLimitedRatio, ratio(b.Recent.Responses, b.Recent.RateLimited))
		gauge(c.unexplainedRatio, ratio(b.Recent.Responses, b.Recent.Unexplained))
		counter(c.responses, b.Responses)
		counter(c.rateLimited, b.RateLimited)
		counter(c.unexplained, b.Unexplained)
		counter(c.aborted, b.Aborted)
		counter(c.upstreamErrors, b.UpstreamErrors)
		counter(c.headersMissing, b.HeadersMissing)
		counter(c.headersMalformed, b.HeadersMalformed)
		counter(c.usageMissing, b.UsageMissing)
	}
	for _, ceiling := range c.store.Ceilings() {
		ch <- prometheus.MustNewConstMetric(c.ceiling, prometheus.GaugeValue, float64(ceiling.Value),
			ceiling.Consumer.Namespace, ceiling.Consumer.ScaledObject, ceiling.Consumer.Trigger,
			ceiling.Consumer.Bucket.Provider, ceiling.Consumer.Bucket.Model, ceiling.Consumer.Bucket.Key)
	}
	stats := c.store.Stats()
	ch <- prometheus.MustNewConstMetric(c.buckets, prometheus.GaugeValue, float64(stats.Buckets))
	ch <- prometheus.MustNewConstMetric(c.consumers, prometheus.GaugeValue, float64(stats.Consumers))
	ch <- prometheus.MustNewConstMetric(c.bucketsRejected, prometheus.CounterValue, float64(stats.RejectedBuckets))
	ch <- prometheus.MustNewConstMetric(c.consumersRejected, prometheus.CounterValue, float64(stats.RejectedConsumers))
	ch <- prometheus.MustNewConstMetric(c.callsRejected, prometheus.CounterValue, float64(stats.RejectedCalls))
}

func (c *Collector) descs() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.limitRequests, c.remainingRequests, c.limitTokens, c.remainingTokens, c.resetSeconds,
		c.tokensPerCall, c.latencySeconds, c.inflight, c.retryAfterSeconds, c.rateLimitedRatio, c.unexplainedRatio,
		c.responses, c.rateLimited, c.unexplained, c.aborted, c.upstreamErrors, c.headersMissing, c.headersMalformed, c.usageMissing,
		c.ceiling, c.buckets, c.consumers, c.bucketsRejected, c.consumersRejected, c.callsRejected,
	}
}

func ratio(responses, rateLimited int64) float64 {
	return aimd.Signal{Responses: responses, RateLimited: rateLimited}.Ratio()
}

func secondsUntil(q headroom.Quota, now time.Time) float64 {
	if q.ResetAt.IsZero() {
		return 0
	}
	return max(q.ResetAt.Sub(now).Seconds(), 0)
}
