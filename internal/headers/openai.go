package headers

import (
	"net/http"
	"time"
)

func parseOpenAI(h http.Header, now time.Time) (Limits, error) {
	limits := Limits{Provider: OpenAI}
	var err error
	if limits.Requests, err = openAIWindow(h, "requests", now); err != nil {
		return Limits{}, err
	}
	if limits.Tokens, err = openAIWindow(h, "tokens", now); err != nil {
		return Limits{}, err
	}
	if limits.ProjectTokens, err = openAIWindow(h, "project-tokens", now); err != nil {
		return Limits{}, err
	}
	return complete(limits, h, now)
}

func openAIWindow(h http.Header, kind string, now time.Time) (Window, error) {
	return window(h, "x-ratelimit-limit-"+kind, "x-ratelimit-remaining-"+kind, "x-ratelimit-reset-"+kind, now, durationReset)
}

func durationReset(value string, now time.Time) (time.Time, error) {
	delay, err := time.ParseDuration(value)
	if err != nil {
		return time.Time{}, err
	}
	return now.Add(delay), nil
}
