package fakeprovider

import (
	"math"
	"time"
)

type quota struct {
	limit     int
	remaining int
	resetIn   time.Duration
}

type decision struct {
	allowed    bool
	retryAfter time.Duration
	requests   quota
	tokens     quota
}

type fixedWindow struct {
	start    time.Time
	requests int
	tokens   int
}

func (w *fixedWindow) take(cfg Config, epoch, now time.Time, tokens int) decision {
	elapsed := now.Sub(epoch)
	start := epoch.Add(elapsed.Truncate(time.Minute))
	if !w.start.Equal(start) {
		w.start, w.requests, w.tokens = start, 0, 0
	}
	resetIn := w.start.Add(time.Minute).Sub(now)
	allowed := w.requests < cfg.RequestsPerMinute && w.tokens+tokens <= cfg.TokensPerMinute
	if allowed {
		w.requests++
		w.tokens += tokens
	}
	return decision{
		allowed:    allowed,
		retryAfter: wholeSeconds(resetIn),
		requests:   quota{limit: cfg.RequestsPerMinute, remaining: cfg.RequestsPerMinute - w.requests, resetIn: resetIn},
		tokens:     quota{limit: cfg.TokensPerMinute, remaining: cfg.TokensPerMinute - w.tokens, resetIn: resetIn},
	}
}

type tokenBucket struct {
	requests float64
	tokens   float64
	last     time.Time
}

func (b *tokenBucket) take(cfg Config, now time.Time, tokens int) decision {
	requestRate := float64(cfg.RequestsPerMinute) / 60
	tokenRate := float64(cfg.TokensPerMinute) / 60
	if b.last.IsZero() {
		b.requests, b.tokens, b.last = float64(cfg.RequestsPerMinute), float64(cfg.TokensPerMinute), now
	}
	elapsed := now.Sub(b.last).Seconds()
	b.requests = math.Min(b.requests+elapsed*requestRate, float64(cfg.RequestsPerMinute))
	b.tokens = math.Min(b.tokens+elapsed*tokenRate, float64(cfg.TokensPerMinute))
	b.last = now

	need := float64(tokens)
	var retryAfter time.Duration
	switch {
	case b.requests < 1:
		retryAfter = wholeSeconds(secondsToDuration((1 - b.requests) / requestRate))
	case b.tokens < need:
		retryAfter = wholeSeconds(secondsToDuration((need - b.tokens) / tokenRate))
	default:
		b.requests--
		b.tokens -= need
	}
	return decision{
		allowed:    retryAfter == 0,
		retryAfter: retryAfter,
		requests: quota{
			limit:     cfg.RequestsPerMinute,
			remaining: int(b.requests),
			resetIn:   secondsToDuration((float64(cfg.RequestsPerMinute) - b.requests) / requestRate),
		},
		tokens: quota{
			limit:     cfg.TokensPerMinute,
			remaining: int(b.tokens),
			resetIn:   secondsToDuration((float64(cfg.TokensPerMinute) - b.tokens) / tokenRate),
		},
	}
}

type burstTracker struct {
	stamps []time.Time
}

func (t *burstTracker) exceeds(cfg Config, now time.Time) bool {
	cutoff := now.Add(-accelerationWindow)
	kept := t.stamps[:0]
	for _, stamp := range t.stamps {
		if stamp.After(cutoff) {
			kept = append(kept, stamp)
		}
	}
	t.stamps = kept
	share := cfg.RequestsPerMinute * int(accelerationWindow/time.Second) / 60
	return len(t.stamps) >= max(share, 1)
}

func (t *burstTracker) record(now time.Time) {
	t.stamps = append(t.stamps, now)
}

func wholeSeconds(d time.Duration) time.Duration {
	if d <= 0 {
		return time.Second
	}
	return time.Duration(math.Ceil(d.Seconds())) * time.Second
}

func secondsToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}
