package proxy_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/internal/proxy"
	"github.com/dimonchik03/quotaware/internal/state"
	"github.com/dimonchik03/quotaware/internal/usage"
)

func observer(t *testing.T) (*proxy.Observer, *state.Store) {
	t.Helper()
	o, store, _ := observerWithToken(t, "")
	return o, store
}

func observerWithToken(t *testing.T, token string) (*proxy.Observer, *state.Store, *strings.Builder) {
	t.Helper()
	store, err := state.New(0.5)
	if err != nil {
		t.Fatal(err)
	}
	logs := &strings.Builder{}
	o, err := proxy.NewObserver(store, func() time.Time { return start }, 64*1024, token, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return o, store, logs
}

func observe(o *proxy.Observer, body string) *httptest.ResponseRecorder {
	return observeWith(o, body, "")
}

func observeWith(o *proxy.Observer, body, token string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, usage.ObservePath, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	o.ServeHTTP(recorder, request)
	return recorder
}

func TestObserver(t *testing.T) {
	t.Run("records an observation posted by a collector", func(t *testing.T) {
		o, store := observer(t)

		recorder := observe(o, `{"provider":"openai","model":"gpt-4.1-mini","credential":"ab12ab12ab12","status":200,"headers":{"x-ratelimit-limit-requests":"60","x-ratelimit-remaining-requests":"59","x-ratelimit-reset-requests":"1s"},"usage":150,"latency_ms":1200,"complete":true}`)

		if recorder.Code != http.StatusAccepted {
			t.Fatalf("status = %d body = %s, want 202", recorder.Code, recorder.Body.String())
		}
		b, ok := store.Bucket(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "ab12ab12ab12"}, start)
		if !ok || b.Quotas.Requests.Remaining != 59 || b.TokensPerCall != 150 || b.Latency != 1200*time.Millisecond || b.InFlight != 0 {
			t.Fatalf("bucket = %+v ok = %v, want the observation applied with no call left in flight", b, ok)
		}
	})

	t.Run("counts an observation without headers as missing", func(t *testing.T) {
		o, store := observer(t)

		observe(o, `{"provider":"anthropic","model":"claude-sonnet-4-5","credential":"cd34cd34cd34","status":200,"headers":{},"complete":true}`)

		b, _ := store.Bucket(state.Key{Provider: "anthropic", Model: "claude-sonnet-4-5", Key: "cd34cd34cd34"}, start)
		if b.HeadersMissing != 1 || b.UsageMissing != 1 {
			t.Fatalf("bucket = %+v, want headers and usage counted as missing", b)
		}
	})

	t.Run("records a 429 with its retry-after", func(t *testing.T) {
		o, store := observer(t)

		observe(o, `{"provider":"openai","model":"gpt-4.1-mini","credential":"ab12ab12ab12","status":429,"headers":{"retry-after":"20","x-ratelimit-limit-requests":"60","x-ratelimit-remaining-requests":"0"},"complete":true}`)

		b, _ := store.Bucket(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "ab12ab12ab12"}, start)
		if b.RateLimited != 1 || b.RetryAfter != 20*time.Second {
			t.Fatalf("bucket = %+v, want the 429 recorded", b)
		}
	})

	t.Run("counts an aborted observation without touching the averages", func(t *testing.T) {
		o, store := observer(t)

		observe(o, `{"provider":"openai","model":"gpt-4.1-mini","credential":"ab12ab12ab12","status":200,"headers":{},"usage":150,"latency_ms":1200,"complete":false}`)

		b, _ := store.Bucket(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "ab12ab12ab12"}, start)
		if b.Aborted != 1 || b.TokensPerCall != 0 || b.Latency != 0 {
			t.Fatalf("bucket = %+v, want one abort and no averages", b)
		}
	})

	t.Run("logs a malformed retry-after and keeps the observation and its quota", func(t *testing.T) {
		o, store, logs := observerWithToken(t, "")

		observe(o, `{"provider":"openai","model":"gpt-4.1-mini","credential":"ab12ab12ab12","status":429,"headers":{"retry-after":"soon","x-ratelimit-limit-requests":"60","x-ratelimit-remaining-requests":"0"},"complete":true}`)

		b, _ := store.Bucket(state.Key{Provider: "openai", Model: "gpt-4.1-mini", Key: "ab12ab12ab12"}, start)
		if b.RateLimited != 1 || !strings.Contains(logs.String(), "retry-after malformed") {
			t.Fatalf("bucket = %+v logs = %q, want the 429 kept and the header logged", b, logs.String())
		}
		if b.HeadersMalformed != 0 || !b.Quotas.Requests.Known() || b.Quotas.Requests.Remaining != 0 {
			t.Fatalf("bucket = %+v, want the rate-limit window applied although retry-after was unreadable", b)
		}
	})

	t.Run("requires the token when one is configured", func(t *testing.T) {
		o, store, _ := observerWithToken(t, "secret")
		body := `{"provider":"openai","model":"gpt-4.1-mini","credential":"ab12ab12ab12","status":200,"headers":{},"complete":true}`

		if code := observeWith(o, body, "wrong").Code; code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401 for a wrong token", code)
		}
		if code := observeWith(o, body, "secret").Code; code != http.StatusAccepted {
			t.Fatalf("status = %d, want 202 for the right token", code)
		}
		if len(store.Buckets(start)) != 1 {
			t.Fatalf("buckets = %d, want only the authorised observation", len(store.Buckets(start)))
		}
	})

	cases := []struct {
		name string
		body string
	}{
		{"rejects an unknown provider", `{"provider":"gemini","model":"m","credential":"ab12ab12ab12","status":200}`},
		{"rejects a missing model", `{"provider":"openai","credential":"ab12ab12ab12","status":200}`},
		{"rejects an overlong model", `{"provider":"openai","model":"` + strings.Repeat("m", 300) + `","credential":"ab12ab12ab12","status":200}`},
		{"rejects a credential that is not the hash", `{"provider":"openai","model":"m","credential":"sk-live-key","status":200}`},
		{"rejects an impossible status", `{"provider":"openai","model":"m","credential":"ab12ab12ab12","status":7}`},
		{"rejects a negative latency", `{"provider":"openai","model":"m","credential":"ab12ab12ab12","status":200,"latency_ms":-1}`},
		{"rejects an absurd usage", `{"provider":"openai","model":"m","credential":"ab12ab12ab12","status":200,"usage":1e12}`},
		{"rejects a body that is not JSON", `nope`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, store := observer(t)

			recorder := observe(o, tc.body)

			if recorder.Code != http.StatusBadRequest || len(store.Buckets(start)) != 0 {
				t.Fatalf("status = %d buckets = %d, want 400 and no bucket", recorder.Code, len(store.Buckets(start)))
			}
		})
	}

	t.Run("rejects an oversized observation", func(t *testing.T) {
		o, _ := observer(t)

		recorder := observe(o, `{"model":"`+strings.Repeat("x", 70*1024)+`"}`)

		if recorder.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", recorder.Code)
		}
	})
}
