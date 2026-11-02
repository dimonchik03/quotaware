package headers

import (
	"net/http"
	"time"
)

func parseOpenAI(h http.Header, now time.Time) Limits {
	limits := Limits{Provider: OpenAI}
	limits.Requests = limits.collect(openAIWindow(h, "requests", now))
	limits.Tokens = limits.collect(openAIWindow(h, "tokens", now))
	limits.ProjectTokens = limits.collect(openAIWindow(h, "project-tokens", now))
	return limits
}

func openAIWindow(h http.Header, kind string, now time.Time) (Window, *MalformedError) {
	return window(h, "x-ratelimit-limit-"+kind, "x-ratelimit-remaining-"+kind, "x-ratelimit-reset-"+kind, now, durationReset)
}

func durationReset(value string, now time.Time) (time.Time, error) {
	delay, err := time.ParseDuration(value)
	if err != nil {
		return time.Time{}, err
	}
	return now.Add(delay), nil
}
