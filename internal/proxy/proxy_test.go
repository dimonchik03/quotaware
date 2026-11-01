package proxy_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/bench/fakeprovider"
	"github.com/dimonchik03/quotaware/internal/proxy"
	"github.com/dimonchik03/quotaware/internal/state"
)

var start = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

type clock struct {
	now time.Time
}

func (c *clock) Now() time.Time {
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

type harness struct {
	front *httptest.Server
	store *state.Store
	clock *clock
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	c := &clock{now: start}
	cfg := fakeprovider.DefaultConfig()
	cfg.RequestsPerMinute = 3
	cfg.TokensPerMinute = 10000
	cfg.Latency = 1500 * time.Millisecond
	cfg.Jitter = 0
	cfg.OutputTokens = 5
	upstream := httptest.NewServer(fakeprovider.New(cfg, c.Now, c.Advance))
	t.Cleanup(upstream.Close)
	return newHarnessWithUpstream(t, c, upstream.URL)
}

func newHarnessWithUpstream(t *testing.T, c *clock, upstream string) *harness {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.New(0.5)
	if err != nil {
		t.Fatal(err)
	}
	cfg := proxy.DefaultConfig()
	cfg.OpenAI, cfg.Anthropic = target, target
	cfg.MaxRequestBytes = 1024
	p, err := proxy.New(cfg, store, c.Now)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(p)
	t.Cleanup(front.Close)
	return &harness{front: front, store: store, clock: c}
}

func (h *harness) post(t *testing.T, path, credential, body string) (*http.Response, string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, h.front.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(path, "/v1/messages") {
		request.Header.Set("x-api-key", credential)
	} else {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, string(raw)
}

func (h *harness) bucket(t *testing.T, provider, model string) state.Bucket {
	t.Helper()
	for _, b := range h.store.Buckets() {
		if b.Key.Provider == provider && b.Key.Model == model {
			return b
		}
	}
	t.Fatalf("no bucket for %s/%s in %+v", provider, model, h.store.Buckets())
	return state.Bucket{}
}

const (
	chat    = `{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hello"}]}`
	message = `{"model":"claude-sonnet-4-5","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`
)

func TestProxy(t *testing.T) {
	t.Run("passes a chat completion through and records limits, usage and latency", func(t *testing.T) {
		h := newHarness(t)

		response, body := h.post(t, "/v1/chat/completions", "sk-test", chat)

		if response.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.StatusCode, body)
		}
		if response.Header.Get("x-ratelimit-remaining-requests") != "2" {
			t.Fatalf("rate-limit headers not passed through: %v", response.Header)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(body), &payload); err != nil || payload["usage"] == nil {
			t.Fatalf("body = %s, want the provider JSON with usage", body)
		}
		b := h.bucket(t, "openai", "gpt-4.1-mini")
		if b.Requests.Limit != 3 || b.Requests.Remaining != 2 || b.Tokens.Limit != 10000 {
			t.Fatalf("quotas = %+v / %+v, want the fake provider limits", b.Requests, b.Tokens)
		}
		if b.TokensPerCall <= 5 || b.Latency != 1500*time.Millisecond {
			t.Fatalf("tokens per call = %v, latency = %v, want usage above the output size and 1.5s", b.TokensPerCall, b.Latency)
		}
		if b.InFlight != 0 || b.Responses != 1 || b.UsageMissing != 0 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want one clean response and no calls in flight", b)
		}
	})

	t.Run("records a 429 with its retry-after and the limits it carries", func(t *testing.T) {
		h := newHarness(t)
		for range 3 {
			h.post(t, "/v1/chat/completions", "sk-test", chat)
		}

		response, _ := h.post(t, "/v1/chat/completions", "sk-test", chat)

		if response.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429", response.StatusCode)
		}
		b := h.bucket(t, "openai", "gpt-4.1-mini")
		if b.RateLimited != 1 || b.RetryAfter <= 0 || b.Requests.Remaining != 0 {
			t.Fatalf("bucket = %+v, want the 429 recorded with retry-after and zero remaining", b)
		}
		if b.InFlight != 0 {
			t.Fatalf("in flight = %d, want 0", b.InFlight)
		}
	})

	t.Run("captures usage from an OpenAI stream that includes it and passes the stream through", func(t *testing.T) {
		h := newHarness(t)

		response, body := h.post(t, "/v1/chat/completions", "sk-test", `{"model":"gpt-4.1-mini","stream":true,"stream_options":{"include_usage":true},"messages":[]}`)

		if ct := response.Header.Get("Content-Type"); ct != "text/event-stream" {
			t.Fatalf("content type = %q, want text/event-stream", ct)
		}
		if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
			t.Fatalf("stream = %q, want it to end with [DONE]", body)
		}
		if b := h.bucket(t, "openai", "gpt-4.1-mini"); b.TokensPerCall <= 5 || b.UsageMissing != 0 {
			t.Fatalf("bucket = %+v, want usage captured from the final chunk", b)
		}
	})

	t.Run("counts usage as missing for an OpenAI stream without include_usage", func(t *testing.T) {
		h := newHarness(t)

		h.post(t, "/v1/chat/completions", "sk-test", `{"model":"gpt-4.1-mini","stream":true,"messages":[]}`)

		if b := h.bucket(t, "openai", "gpt-4.1-mini"); b.UsageMissing != 1 || b.TokensPerCall != 0 {
			t.Fatalf("bucket = %+v, want usage missing and no tokens per call", b)
		}
	})

	t.Run("captures usage from an Anthropic stream", func(t *testing.T) {
		h := newHarness(t)

		response, body := h.post(t, "/v1/messages", "sk-ant-test", `{"model":"claude-sonnet-4-5","max_tokens":64,"stream":true,"messages":[]}`)

		if response.StatusCode != http.StatusOK || !strings.Contains(body, "event: message_stop") {
			t.Fatalf("status = %d, stream = %q, want a complete message stream", response.StatusCode, body)
		}
		b := h.bucket(t, "anthropic", "claude-sonnet-4-5")
		if b.TokensPerCall <= 5 || b.Tokens.Limit != 10000 {
			t.Fatalf("bucket = %+v, want usage from message_delta and Anthropic limits", b)
		}
	})

	t.Run("keys buckets by provider, model and a hash of the credential", func(t *testing.T) {
		h := newHarness(t)

		h.post(t, "/v1/chat/completions", "sk-first", chat)
		h.post(t, "/v1/chat/completions", "sk-second", chat)
		h.post(t, "/v1/messages", "sk-first", message)

		buckets := h.store.Buckets()
		if len(buckets) != 3 {
			t.Fatalf("buckets = %d, want 3", len(buckets))
		}
		for _, b := range buckets {
			if len(b.Key.Key) != 12 || strings.Contains(b.Key.Key, "sk-") {
				t.Fatalf("key label = %q, want a 12-character hash that never leaks the credential", b.Key.Key)
			}
		}
	})

	t.Run("rejects a request without a model and touches no bucket", func(t *testing.T) {
		h := newHarness(t)

		response, _ := h.post(t, "/v1/chat/completions", "sk-test", `{"messages":[]}`)

		if response.StatusCode != http.StatusBadRequest || len(h.store.Buckets()) != 0 {
			t.Fatalf("status = %d, buckets = %d, want 400 and none", response.StatusCode, len(h.store.Buckets()))
		}
	})

	t.Run("rejects an oversized request body", func(t *testing.T) {
		h := newHarness(t)

		response, _ := h.post(t, "/v1/chat/completions", "sk-test", `{"model":"gpt-4.1-mini","messages":[{"content":"`+strings.Repeat("x", 2048)+`"}]}`)

		if response.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", response.StatusCode)
		}
	})

	t.Run("answers 404 for paths it does not proxy", func(t *testing.T) {
		h := newHarness(t)

		response, _ := h.post(t, "/v1/embeddings", "sk-test", chat)

		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", response.StatusCode)
		}
	})

	t.Run("answers 502 and releases the in-flight slot when the upstream is unreachable", func(t *testing.T) {
		closed := httptest.NewServer(http.NotFoundHandler())
		closed.Close()
		h := newHarnessWithUpstream(t, &clock{now: start}, closed.URL)

		response, _ := h.post(t, "/v1/chat/completions", "sk-test", chat)

		if response.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", response.StatusCode)
		}
		if b := h.bucket(t, "openai", "gpt-4.1-mini"); b.InFlight != 0 || b.Responses != 0 {
			t.Fatalf("bucket = %+v, want the slot released and nothing observed", b)
		}
	})

	t.Run("requires both upstreams", func(t *testing.T) {
		store, _ := state.New(0.5)

		if _, err := proxy.New(proxy.DefaultConfig(), store, time.Now); err == nil {
			t.Fatal("expected an error without upstreams")
		}
	})
}
