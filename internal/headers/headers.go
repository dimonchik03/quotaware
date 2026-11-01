package headers

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Provider string

const (
	OpenAI    Provider = "openai"
	Anthropic Provider = "anthropic"
)

type Window struct {
	Limit     int64
	Remaining int64
	ResetAt   time.Time
}

func (w Window) Known() bool {
	return w.Limit > 0
}

type Limits struct {
	Provider     Provider
	Requests     Window
	Tokens       Window
	InputTokens  Window
	OutputTokens Window
	RetryAfter   time.Duration
}

var (
	ErrMissing         = errors.New("rate-limit headers missing")
	ErrUnknownProvider = errors.New("unknown provider")
)

type MalformedError struct {
	Header string
	Value  string
	Err    error
}

func (e *MalformedError) Error() string {
	return fmt.Sprintf("header %s has malformed value %q: %v", e.Header, e.Value, e.Err)
}

func (e *MalformedError) Unwrap() error {
	return e.Err
}

func Parse(provider Provider, h http.Header, now time.Time) (Limits, error) {
	switch provider {
	case OpenAI:
		return parseOpenAI(h, now)
	case Anthropic:
		return parseAnthropic(h, now)
	default:
		return Limits{}, fmt.Errorf("%w: %q", ErrUnknownProvider, provider)
	}
}

func parseOpenAI(h http.Header, now time.Time) (Limits, error) {
	requests, err := openAIWindow(h, "requests", now)
	if err != nil {
		return Limits{}, err
	}
	tokens, err := openAIWindow(h, "tokens", now)
	if err != nil {
		return Limits{}, err
	}
	projectTokens, err := openAIWindow(h, "project-tokens", now)
	if err != nil {
		return Limits{}, err
	}
	limits := Limits{Provider: OpenAI, Requests: requests, Tokens: tighter(tokens, projectTokens)}
	if !limits.Requests.Known() && !limits.Tokens.Known() {
		return Limits{}, ErrMissing
	}
	limits.RetryAfter, err = retryAfter(h, now)
	if err != nil {
		return Limits{}, err
	}
	return limits, nil
}

func openAIWindow(h http.Header, kind string, now time.Time) (Window, error) {
	limit, ok, err := integerHeader(h, "x-ratelimit-limit-"+kind)
	if err != nil || !ok {
		return Window{}, err
	}
	remaining, ok, err := integerHeader(h, "x-ratelimit-remaining-"+kind)
	if err != nil || !ok {
		return Window{}, err
	}
	reset, ok, err := durationHeader(h, "x-ratelimit-reset-"+kind)
	if err != nil {
		return Window{}, err
	}
	w := Window{Limit: limit, Remaining: remaining}
	if ok {
		w.ResetAt = now.Add(reset)
	}
	return w, nil
}

func parseAnthropic(h http.Header, now time.Time) (Limits, error) {
	limits := Limits{Provider: Anthropic}
	var err error
	if limits.Requests, err = anthropicWindow(h, "requests"); err != nil {
		return Limits{}, err
	}
	if limits.Tokens, err = anthropicWindow(h, "tokens"); err != nil {
		return Limits{}, err
	}
	if limits.InputTokens, err = anthropicWindow(h, "input-tokens"); err != nil {
		return Limits{}, err
	}
	if limits.OutputTokens, err = anthropicWindow(h, "output-tokens"); err != nil {
		return Limits{}, err
	}
	if !limits.Requests.Known() && !limits.Tokens.Known() {
		return Limits{}, ErrMissing
	}
	if limits.RetryAfter, err = retryAfter(h, now); err != nil {
		return Limits{}, err
	}
	return limits, nil
}

func anthropicWindow(h http.Header, kind string) (Window, error) {
	prefix := "anthropic-ratelimit-" + kind
	limit, ok, err := integerHeader(h, prefix+"-limit")
	if err != nil || !ok {
		return Window{}, err
	}
	remaining, ok, err := integerHeader(h, prefix+"-remaining")
	if err != nil || !ok {
		return Window{}, err
	}
	resetAt, _, err := timestampHeader(h, prefix+"-reset")
	if err != nil {
		return Window{}, err
	}
	return Window{Limit: limit, Remaining: remaining, ResetAt: resetAt}, nil
}

func tighter(a, b Window) Window {
	switch {
	case !b.Known():
		return a
	case !a.Known():
		return b
	case b.Remaining < a.Remaining:
		return b
	default:
		return a
	}
}

func retryAfter(h http.Header, now time.Time) (time.Duration, error) {
	value := strings.TrimSpace(h.Get("Retry-After"))
	if value == "" {
		return 0, nil
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		return time.Duration(seconds * float64(time.Second)), nil
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0, &MalformedError{Header: "Retry-After", Value: value, Err: err}
	}
	return max(at.Sub(now), 0), nil
}

func integerHeader(h http.Header, name string) (int64, bool, error) {
	value := strings.TrimSpace(h.Get(name))
	if value == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false, &MalformedError{Header: name, Value: value, Err: err}
	}
	return n, true, nil
}

func durationHeader(h http.Header, name string) (time.Duration, bool, error) {
	value := strings.TrimSpace(h.Get(name))
	if value == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, false, &MalformedError{Header: name, Value: value, Err: err}
	}
	return d, true, nil
}

func timestampHeader(h http.Header, name string) (time.Time, bool, error) {
	value := strings.TrimSpace(h.Get(name))
	if value == "" {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false, &MalformedError{Header: name, Value: value, Err: err}
	}
	return t, true, nil
}
