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

	t.Run("prefers the project token window when it has less remaining", func(t *testing.T) {
		limits, err := headers.Parse(headers.OpenAI, fixture(t, "openai_project_200.http"), now)
		if err != nil {
			t.Fatal(err)
		}
		if want := window(100000, 20000, now.Add(48*time.Second)); limits.Tokens != want {
			t.Fatalf("tokens = %+v, want the project window %+v", limits.Tokens, want)
		}
		if limits.Requests.Limit != 10000 {
			t.Fatalf("requests limit = %d, want the organization window untouched", limits.Requests.Limit)
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
		var malformed *headers.MalformedError
		if !errors.As(err, &malformed) {
			t.Fatalf("err = %v, want MalformedError", err)
		}
		if malformed.Header != "x-ratelimit-reset-tokens" || malformed.Value != "soon" {
			t.Fatalf("malformed = %+v, want the reset-tokens header and its value", malformed)
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
}

func TestParseRejectsUnknownProvider(t *testing.T) {
	_, err := headers.Parse("gemini", fixture(t, "no_headers.http"), now)
	if !errors.Is(err, headers.ErrUnknownProvider) {
		t.Fatalf("err = %v, want ErrUnknownProvider", err)
	}
}

func equalWindow(got, want headers.Window) bool {
	return got.Limit == want.Limit && got.Remaining == want.Remaining && got.ResetAt.Equal(want.ResetAt)
}
