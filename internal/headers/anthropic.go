package headers

import (
	"net/http"
	"time"
)

func parseAnthropic(h http.Header, now time.Time) Limits {
	limits := Limits{Provider: Anthropic}
	limits.Requests = limits.collect(anthropicWindow(h, "requests", now))
	limits.Tokens = limits.collect(anthropicWindow(h, "tokens", now))
	limits.InputTokens = limits.collect(anthropicWindow(h, "input-tokens", now))
	limits.OutputTokens = limits.collect(anthropicWindow(h, "output-tokens", now))
	return limits
}

func anthropicWindow(h http.Header, kind string, now time.Time) (Window, *MalformedError) {
	prefix := "anthropic-ratelimit-" + kind
	return window(h, prefix+"-limit", prefix+"-remaining", prefix+"-reset", now, timestampReset)
}

func timestampReset(value string, _ time.Time) (time.Time, error) {
	return time.Parse(time.RFC3339, value)
}
