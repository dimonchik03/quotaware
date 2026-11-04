package headers

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dimonchik03/quotaware/internal/headroom"
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

func (w Window) Quota() headroom.Quota {
	return headroom.Quota{Limit: float64(w.Limit), Remaining: float64(w.Remaining), ResetAt: w.ResetAt}
}

type Limits struct {
	Provider      Provider
	Requests      Window
	Tokens        Window
	ProjectTokens Window
	InputTokens   Window
	OutputTokens  Window
	Malformed     []*MalformedError
}

func (l Limits) Usable() bool {
	return l.Requests.Known() || l.Tokens.Known()
}

func (l *Limits) collect(w Window, malformed *MalformedError) Window {
	if malformed != nil {
		l.Malformed = append(l.Malformed, malformed)
	}
	return w
}

var (
	ErrMissing         = errors.New("rate-limit headers missing")
	ErrUnknownProvider = errors.New("unknown provider")

	errAbsentCounterpart = errors.New("limit and remaining must be reported together")
)

type MalformedError struct {
	Header string
	Value  string
	Err    error
}

func (e *MalformedError) Error() string {
	if e.Value == "" {
		return fmt.Sprintf("header %s: %v", e.Header, e.Err)
	}
	return fmt.Sprintf("header %s has malformed value %q: %v", e.Header, e.Value, e.Err)
}

func (e *MalformedError) Unwrap() error {
	return e.Err
}

func Parse(provider Provider, h http.Header, now time.Time) (Limits, error) {
	switch provider {
	case OpenAI:
		return complete(parseOpenAI(h, now))
	case Anthropic:
		return complete(parseAnthropic(h, now))
	default:
		return Limits{}, fmt.Errorf("%w: %q", ErrUnknownProvider, provider)
	}
}

const MaxRetryAfter = 24 * time.Hour

var errRetryAfterTooLong = errors.New("delay beyond a day")

func RetryAfter(h http.Header, now time.Time) (time.Duration, error) {
	value := strings.TrimSpace(h.Get("Retry-After"))
	if value == "" {
		return 0, nil
	}
	delay, err := retryAfterDelay(value, now)
	if err != nil {
		return 0, &MalformedError{Header: "Retry-After", Value: value, Err: err}
	}
	if delay > MaxRetryAfter {
		return 0, &MalformedError{Header: "Retry-After", Value: value, Err: errRetryAfterTooLong}
	}
	return delay, nil
}

func retryAfterDelay(value string, now time.Time) (time.Duration, error) {
	if seconds, err := strconv.ParseUint(value, 10, 32); err == nil {
		return time.Duration(seconds) * time.Second, nil
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0, err
	}
	return max(at.Sub(now), 0), nil
}

func complete(limits Limits) (Limits, error) {
	if !limits.Usable() && len(limits.Malformed) == 0 {
		return Limits{}, ErrMissing
	}
	return limits, nil
}

type resetParser func(value string, now time.Time) (time.Time, error)

func window(h http.Header, limitName, remainingName, resetName string, now time.Time, parseReset resetParser) (Window, *MalformedError) {
	limit, limitPresent, err := integerHeader(h, limitName)
	if err != nil {
		return Window{}, err
	}
	remaining, remainingPresent, err := integerHeader(h, remainingName)
	if err != nil {
		return Window{}, err
	}
	switch {
	case !limitPresent && !remainingPresent:
		return Window{}, nil
	case !limitPresent:
		return Window{}, &MalformedError{Header: limitName, Err: errAbsentCounterpart}
	case !remainingPresent:
		return Window{}, &MalformedError{Header: remainingName, Err: errAbsentCounterpart}
	}
	parsed := Window{Limit: limit, Remaining: remaining}
	value := strings.TrimSpace(h.Get(resetName))
	if value == "" {
		return parsed, nil
	}
	resetAt, resetErr := parseReset(value, now)
	if resetErr != nil {
		return parsed, &MalformedError{Header: resetName, Value: value, Err: resetErr}
	}
	parsed.ResetAt = resetAt
	return parsed, nil
}

func integerHeader(h http.Header, name string) (int64, bool, *MalformedError) {
	value := strings.TrimSpace(h.Get(name))
	if value == "" {
		return 0, false, nil
	}
	count, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false, &MalformedError{Header: name, Value: value, Err: err}
	}
	return count, true, nil
}
