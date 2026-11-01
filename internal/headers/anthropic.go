package headers

import (
	"net/http"
	"time"
)

func parseAnthropic(h http.Header, now time.Time) (Limits, error) {
	limits := Limits{Provider: Anthropic}
	var err error
	if limits.Requests, err = anthropicWindow(h, "requests", now); err != nil {
		return Limits{}, err
	}
	if limits.Tokens, err = anthropicWindow(h, "tokens", now); err != nil {
		return Limits{}, err
	}
	if limits.InputTokens, err = anthropicWindow(h, "input-tokens", now); err != nil {
		return Limits{}, err
	}
	if limits.OutputTokens, err = anthropicWindow(h, "output-tokens", now); err != nil {
		return Limits{}, err
	}
	return complete(limits, h, now)
}

func anthropicWindow(h http.Header, kind string, now time.Time) (Window, error) {
	prefix := "anthropic-ratelimit-" + kind
	return window(h, prefix+"-limit", prefix+"-remaining", prefix+"-reset", now, timestampReset)
}

func timestampReset(value string, _ time.Time) (time.Time, error) {
	return time.Parse(time.RFC3339, value)
}
