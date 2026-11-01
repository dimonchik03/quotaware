package headers_test

import (
	"bufio"
	"bytes"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/internal/headers"
	"github.com/dimonchik03/quotaware/internal/headroom"
)

var now = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

func fixture(t *testing.T, name string) http.Header {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), nil)
	if err != nil {
		t.Fatal(err)
	}
	return response.Header
}

func window(limit, remaining int64, resetAt time.Time) headers.Window {
	return headers.Window{Limit: limit, Remaining: remaining, ResetAt: resetAt}
}

func equalWindow(got, want headers.Window) bool {
	return got.Limit == want.Limit && got.Remaining == want.Remaining && got.ResetAt.Equal(want.ResetAt)
}

func malformedHeader(t *testing.T, err error) *headers.MalformedError {
	t.Helper()
	var malformed *headers.MalformedError
	if !errors.As(err, &malformed) {
		t.Fatalf("err = %v, want MalformedError", err)
	}
	return malformed
}

func TestParseOpenAI(t *testing.T) {
	t.Run("parses limits, remaining and duration resets relative to now", func(t *testing.T) {
		limits, err := headers.Parse(headers.OpenAI, fixture(t, "openai_200.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if limits.Provider != headers.OpenAI {
			t.Fatalf("provider = %q, want openai", limits.Provider)
		}
		if want := window(60, 59, now.Add(time.Second)); limits.Requests != want {
			t.Fatalf("requests = %+v, want %+v", limits.Requests, want)
		}
		if want := window(150000, 149984, now.Add(6*time.Minute)); limits.Tokens != want {
			t.Fatalf("tokens = %+v, want %+v", limits.Tokens, want)
		}
		if limits.ProjectTokens.Known() {
			t.Fatalf("project tokens = %+v, want unknown without project headers", limits.ProjectTokens)
		}
		if limits.RetryAfter != 0 {
			t.Fatalf("retry-after = %v, want 0 on a successful response", limits.RetryAfter)
		}
	})

	t.Run("reports retry-after and fractional resets on a 429", func(t *testing.T) {
		limits, err := headers.Parse(headers.OpenAI, fixture(t, "openai_429.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if limits.RetryAfter != 20*time.Second {
			t.Fatalf("retry-after = %v, want 20s", limits.RetryAfter)
		}
		if limits.Requests.Remaining != 0 {
			t.Fatalf("remaining requests = %d, want 0", limits.Requests.Remaining)
		}
		if want := now.Add(2700 * time.Millisecond); !limits.Tokens.ResetAt.Equal(want) {
			t.Fatalf("tokens reset = %v, want %v", limits.Tokens.ResetAt, want)
		}
	})

	t.Run("parses the project token window separately from the organization window", func(t *testing.T) {
		limits, err := headers.Parse(headers.OpenAI, fixture(t, "openai_project_200.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if want := window(1000000, 950000, now.Add(3*time.Second)); limits.Tokens != want {
			t.Fatalf("tokens = %+v, want the organization window %+v", limits.Tokens, want)
		}
		if want := window(100000, 20000, now.Add(48*time.Second)); limits.ProjectTokens != want {
			t.Fatalf("project tokens = %+v, want %+v", limits.ProjectTokens, want)
		}
	})

	t.Run("returns ErrMissing when no rate-limit header is present", func(t *testing.T) {
		_, err := headers.Parse(headers.OpenAI, fixture(t, "no_headers.http"), now)
		if !errors.Is(err, headers.ErrMissing) {
			t.Fatalf("err = %v, want ErrMissing", err)
		}
	})

	t.Run("names the header when a reset cannot be parsed", func(t *testing.T) {
		_, err := headers.Parse(headers.OpenAI, fixture(t, "openai_malformed.http"), now)
		malformed := malformedHeader(t, err)
		if malformed.Header != "x-ratelimit-reset-tokens" || malformed.Value != "soon" {
			t.Fatalf("malformed = %+v, want the reset-tokens header and its value", malformed)
		}
	})

	t.Run("names the absent counterpart of a partial window", func(t *testing.T) {
		_, err := headers.Parse(headers.OpenAI, fixture(t, "openai_partial_tokens.http"), now)
		malformed := malformedHeader(t, err)
		if malformed.Header != "x-ratelimit-remaining-tokens" {
			t.Fatalf("malformed = %+v, want the absent remaining-tokens header", malformed)
		}
	})

	t.Run("rejects an integer header with a thousands separator", func(t *testing.T) {
		h := http.Header{}
		h.Set("x-ratelimit-limit-requests", "1,000")
		h.Set("x-ratelimit-remaining-requests", "999")

		_, err := headers.Parse(headers.OpenAI, h, now)

		malformed := malformedHeader(t, err)
		if malformed.Header != "x-ratelimit-limit-requests" || malformed.Value != "1,000" {
			t.Fatalf("malformed = %+v, want the limit-requests header and its value", malformed)
		}
	})
}

func TestParseAnthropic(t *testing.T) {
	t.Run("parses RFC 3339 resets as absolute times", func(t *testing.T) {
		limits, err := headers.Parse(headers.Anthropic, fixture(t, "anthropic_200.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if limits.Provider != headers.Anthropic {
			t.Fatalf("provider = %q, want anthropic", limits.Provider)
		}
		if want := window(1000, 999, now.Add(3*time.Second)); !equalWindow(limits.Requests, want) {
			t.Fatalf("requests = %+v, want %+v", limits.Requests, want)
		}
		if want := window(80000, 79000, now.Add(45*time.Second)); !equalWindow(limits.Tokens, want) {
			t.Fatalf("tokens = %+v, want %+v", limits.Tokens, want)
		}
	})

	t.Run("keeps the thousand-rounded remaining exactly as reported", func(t *testing.T) {
		limits, err := headers.Parse(headers.Anthropic, fixture(t, "anthropic_200.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if limits.Tokens.Remaining != 79000 {
			t.Fatalf("remaining tokens = %d, want 79000", limits.Tokens.Remaining)
		}
	})

	t.Run("parses input and output token windows separately", func(t *testing.T) {
		limits, err := headers.Parse(headers.Anthropic, fixture(t, "anthropic_200.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if want := window(50000, 49000, now.Add(45*time.Second)); !equalWindow(limits.InputTokens, want) {
			t.Fatalf("input tokens = %+v, want %+v", limits.InputTokens, want)
		}
		if want := window(10000, 10000, now.Add(12*time.Second)); !equalWindow(limits.OutputTokens, want) {
			t.Fatalf("output tokens = %+v, want %+v", limits.OutputTokens, want)
		}
	})

	t.Run("reports retry-after on a 429 and leaves absent windows unknown", func(t *testing.T) {
		limits, err := headers.Parse(headers.Anthropic, fixture(t, "anthropic_429.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if limits.RetryAfter != 15*time.Second {
			t.Fatalf("retry-after = %v, want 15s", limits.RetryAfter)
		}
		if limits.Requests.Remaining != 0 {
			t.Fatalf("remaining requests = %d, want 0", limits.Requests.Remaining)
		}
		if limits.InputTokens.Known() || limits.OutputTokens.Known() {
			t.Fatalf("input/output windows = %+v/%+v, want unknown", limits.InputTokens, limits.OutputTokens)
		}
	})

	t.Run("returns ErrMissing when no rate-limit header is present", func(t *testing.T) {
		_, err := headers.Parse(headers.Anthropic, fixture(t, "no_headers.http"), now)
		if !errors.Is(err, headers.ErrMissing) {
			t.Fatalf("err = %v, want ErrMissing", err)
		}
	})

	t.Run("names the header when a reset is not RFC 3339", func(t *testing.T) {
		h := http.Header{}
		h.Set("anthropic-ratelimit-requests-limit", "1000")
		h.Set("anthropic-ratelimit-requests-remaining", "999")
		h.Set("anthropic-ratelimit-requests-reset", "tomorrow")

		_, err := headers.Parse(headers.Anthropic, h, now)

		malformed := malformedHeader(t, err)
		if malformed.Header != "anthropic-ratelimit-requests-reset" || malformed.Value != "tomorrow" {
			t.Fatalf("malformed = %+v, want the requests-reset header and its value", malformed)
		}
	})
}

func TestParseRejectsUnknownProvider(t *testing.T) {
	_, err := headers.Parse("gemini", fixture(t, "no_headers.http"), now)
	if !errors.Is(err, headers.ErrUnknownProvider) {
		t.Fatalf("err = %v, want ErrUnknownProvider", err)
	}
}

func TestRetryAfter(t *testing.T) {
	t.Run("is available on its own when the rate-limit headers are missing", func(t *testing.T) {
		h := fixture(t, "retry_after_only.http")

		_, err := headers.Parse(headers.OpenAI, h, now)
		if !errors.Is(err, headers.ErrMissing) {
			t.Fatalf("parse err = %v, want ErrMissing", err)
		}
		delay, err := headers.RetryAfter(h, now)
		if err != nil {
			t.Fatal(err)
		}
		if delay != 20*time.Second {
			t.Fatalf("retry-after = %v, want 20s", delay)
		}
	})

	t.Run("treats an HTTP date as a delay from now", func(t *testing.T) {
		h := http.Header{}
		h.Set("Retry-After", "Sun, 01 Nov 2026 18:00:30 GMT")

		delay, err := headers.RetryAfter(h, now)

		if err != nil {
			t.Fatal(err)
		}
		if delay != 30*time.Second {
			t.Fatalf("retry-after = %v, want 30s", delay)
		}
	})

	t.Run("never reports a negative delay for a date in the past", func(t *testing.T) {
		h := http.Header{}
		h.Set("Retry-After", "Sun, 01 Nov 2026 17:59:00 GMT")

		delay, err := headers.RetryAfter(h, now)

		if err != nil {
			t.Fatal(err)
		}
		if delay != 0 {
			t.Fatalf("retry-after = %v, want 0", delay)
		}
	})

	t.Run("rejects a negative number of seconds", func(t *testing.T) {
		h := http.Header{}
		h.Set("Retry-After", "-5")

		_, err := headers.RetryAfter(h, now)

		malformed := malformedHeader(t, err)
		if malformed.Header != "Retry-After" || malformed.Value != "-5" {
			t.Fatalf("malformed = %+v, want the Retry-After header and its value", malformed)
		}
	})
}

func TestWindowQuota(t *testing.T) {
	resetAt := now.Add(time.Minute)

	quota := window(60, 59, resetAt).Quota()

	want := headroom.Quota{Limit: 60, Remaining: 59, ResetAt: resetAt}
	if quota != want {
		t.Fatalf("quota = %+v, want %+v", quota, want)
	}
}
