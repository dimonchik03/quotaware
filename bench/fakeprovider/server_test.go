package fakeprovider_test

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/bench/fakeprovider"
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

func config() fakeprovider.Config {
	cfg := fakeprovider.DefaultConfig()
	cfg.RequestsPerMinute = 3
	cfg.TokensPerMinute = 10000
	cfg.Latency = 0
	cfg.Jitter = 0
	cfg.OutputTokens = 5
	return cfg
}

func newServer(t *testing.T, cfg fakeprovider.Config) (*fakeprovider.Server, *clock) {
	t.Helper()
	c := &clock{now: start}
	return fakeprovider.New(cfg, c.Now, func(time.Duration) {}), c
}

func post(t *testing.T, s *fakeprovider.Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	s.ServeHTTP(recorder, request)
	return recorder
}

func decode(t *testing.T, body string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("invalid JSON %q: %v", body, err)
	}
	return payload
}

func eventOfType(t *testing.T, events []map[string]any, kind string) map[string]any {
	t.Helper()
	for _, event := range events {
		if event["type"] == kind {
			return event
		}
	}
	t.Fatalf("no %s event in %v", kind, events)
	return nil
}

func sseData(t *testing.T, body string) []map[string]any {
	t.Helper()
	var events []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		events = append(events, decode(t, strings.TrimPrefix(line, "data: ")))
	}
	return events
}

const chat = `{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hello"}]}`
const msg = `{"model":"claude-sonnet-4-5","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`

func TestChatCompletions(t *testing.T) {
	t.Run("answers with usage and OpenAI rate-limit headers", func(t *testing.T) {
		s, _ := newServer(t, config())

		response := post(t, s, "/v1/chat/completions", chat)

		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		h := response.Header()
		if h.Get("x-ratelimit-limit-requests") != "3" || h.Get("x-ratelimit-remaining-requests") != "2" {
			t.Fatalf("request headers = %v, want limit 3 remaining 2", h)
		}
		if h.Get("x-ratelimit-reset-requests") != "1m0s" {
			t.Fatalf("reset-requests = %q, want 1m0s at the start of a window", h.Get("x-ratelimit-reset-requests"))
		}
		usage := decode(t, response.Body.String())["usage"].(map[string]any)
		if usage["completion_tokens"].(float64) != 5 || usage["prompt_tokens"].(float64) < 1 {
			t.Fatalf("usage = %v, want 5 completion tokens and some prompt tokens", usage)
		}
	})

	t.Run("returns 429 with retry-after once the request window is exhausted", func(t *testing.T) {
		s, c := newServer(t, config())
		for range 3 {
			post(t, s, "/v1/chat/completions", chat)
		}
		c.Advance(20 * time.Second)

		response := post(t, s, "/v1/chat/completions", chat)

		if response.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429", response.Code)
		}
		if response.Header().Get("Retry-After") != "40" {
			t.Fatalf("retry-after = %q, want 40 seconds until the window ends", response.Header().Get("Retry-After"))
		}
		if response.Header().Get("x-ratelimit-remaining-requests") != "0" {
			t.Fatalf("remaining = %q, want 0", response.Header().Get("x-ratelimit-remaining-requests"))
		}
		if decode(t, response.Body.String())["error"].(map[string]any)["code"] != "rate_limit_exceeded" {
			t.Fatalf("body = %s, want an OpenAI rate limit error", response.Body.String())
		}
	})

	t.Run("returns 429 on tokens when a call would exceed the window's token limit", func(t *testing.T) {
		cfg := config()
		cfg.TokensPerMinute = 30
		s, _ := newServer(t, cfg)
		post(t, s, "/v1/chat/completions", chat)

		response := post(t, s, "/v1/chat/completions", chat)

		if response.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429 once the token window is spent", response.Code)
		}
		if response.Header().Get("x-ratelimit-remaining-requests") != "2" {
			t.Fatalf("remaining requests = %q, want 2 because the request window still has room", response.Header().Get("x-ratelimit-remaining-requests"))
		}
	})

	t.Run("opens a fresh window after a minute", func(t *testing.T) {
		s, c := newServer(t, config())
		for range 3 {
			post(t, s, "/v1/chat/completions", chat)
		}
		c.Advance(61 * time.Second)

		response := post(t, s, "/v1/chat/completions", chat)

		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 in the new window", response.Code)
		}
		if response.Header().Get("x-ratelimit-remaining-requests") != "2" {
			t.Fatalf("remaining = %q, want 2", response.Header().Get("x-ratelimit-remaining-requests"))
		}
	})

	t.Run("streams chunks and a final usage chunk when asked", func(t *testing.T) {
		s, _ := newServer(t, config())

		response := post(t, s, "/v1/chat/completions", `{"model":"gpt-4.1-mini","stream":true,"stream_options":{"include_usage":true},"messages":[]}`)

		if ct := response.Header().Get("Content-Type"); ct != "text/event-stream" {
			t.Fatalf("content type = %q, want text/event-stream", ct)
		}
		events := sseData(t, response.Body.String())
		last := events[len(events)-1]
		if last["usage"] == nil {
			t.Fatalf("last event = %v, want usage", last)
		}
		if !strings.HasSuffix(strings.TrimSpace(response.Body.String()), "data: [DONE]") {
			t.Fatalf("stream does not end with [DONE]: %q", response.Body.String())
		}
	})
}

func TestMessages(t *testing.T) {
	t.Run("answers with Anthropic headers and thousand-rounded remaining tokens", func(t *testing.T) {
		s, _ := newServer(t, config())

		response := post(t, s, "/v1/messages", msg)

		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
		}
		h := response.Header()
		if h.Get("anthropic-ratelimit-requests-remaining") != "2" {
			t.Fatalf("remaining requests = %q, want 2", h.Get("anthropic-ratelimit-requests-remaining"))
		}
		if h.Get("anthropic-ratelimit-tokens-remaining") != "10000" {
			t.Fatalf("remaining tokens = %q, want 10000 after rounding a small spend to the nearest thousand", h.Get("anthropic-ratelimit-tokens-remaining"))
		}
		if _, err := time.Parse(time.RFC3339, h.Get("anthropic-ratelimit-requests-reset")); err != nil {
			t.Fatalf("reset = %q, want RFC 3339", h.Get("anthropic-ratelimit-requests-reset"))
		}
		usage := decode(t, response.Body.String())["usage"].(map[string]any)
		if usage["output_tokens"].(float64) != 5 {
			t.Fatalf("usage = %v, want 5 output tokens", usage)
		}
	})

	t.Run("refills the bucket continuously instead of per window", func(t *testing.T) {
		s, c := newServer(t, config())
		for range 3 {
			post(t, s, "/v1/messages", msg)
		}
		if response := post(t, s, "/v1/messages", msg); response.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429 with an empty bucket", response.Code)
		}
		c.Advance(21 * time.Second)

		response := post(t, s, "/v1/messages", msg)

		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 after one request refilled", response.Code)
		}
	})

	t.Run("returns 429 with the token refill time when the bucket lacks tokens", func(t *testing.T) {
		cfg := config()
		cfg.TokensPerMinute = 40
		s, _ := newServer(t, cfg)
		post(t, s, "/v1/messages", msg)

		response := post(t, s, "/v1/messages", msg)

		if response.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429 once the token bucket is drained", response.Code)
		}
		if response.Header().Get("retry-after") == "" || response.Header().Get("retry-after") == "0" {
			t.Fatalf("retry-after = %q, want the seconds until enough tokens refill", response.Header().Get("retry-after"))
		}
	})

	t.Run("reports retry-after as the seconds until one request refills", func(t *testing.T) {
		s, _ := newServer(t, config())
		for range 3 {
			post(t, s, "/v1/messages", msg)
		}

		response := post(t, s, "/v1/messages", msg)

		if response.Header().Get("retry-after") != "20" {
			t.Fatalf("retry-after = %q, want 20", response.Header().Get("retry-after"))
		}
		if decode(t, response.Body.String())["type"] != "error" {
			t.Fatalf("body = %s, want an Anthropic error envelope", response.Body.String())
		}
	})

	t.Run("streams message events ending with message_delta usage", func(t *testing.T) {
		s, _ := newServer(t, config())

		response := post(t, s, "/v1/messages", `{"model":"claude-sonnet-4-5","max_tokens":64,"stream":true,"messages":[]}`)

		body := response.Body.String()
		if !strings.Contains(body, "event: message_start") || !strings.Contains(body, "event: message_stop") {
			t.Fatalf("stream = %q, want message_start and message_stop", body)
		}
		delta := eventOfType(t, sseData(t, body), "message_delta")
		if delta["usage"].(map[string]any)["output_tokens"].(float64) != 5 {
			t.Fatalf("message_delta = %v, want 5 output tokens", delta)
		}
	})
}

func TestServer(t *testing.T) {
	t.Run("produces identical responses for the same seed", func(t *testing.T) {
		first, _ := newServer(t, config())
		second, _ := newServer(t, config())

		a := post(t, first, "/v1/chat/completions", chat).Body.String()
		b := post(t, second, "/v1/chat/completions", chat).Body.String()

		if a != b {
			t.Fatalf("responses differ for the same seed:\n%s\n%s", a, b)
		}
	})

	t.Run("rejects a burst above the acceleration share when enabled", func(t *testing.T) {
		cfg := config()
		cfg.RequestsPerMinute = 60
		cfg.Acceleration = true
		s, _ := newServer(t, cfg)
		for range 10 {
			if response := post(t, s, "/v1/chat/completions", chat); response.Code != http.StatusOK {
				t.Fatalf("status = %d within the ten-second share", response.Code)
			}
		}

		response := post(t, s, "/v1/chat/completions", chat)

		if response.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429 for the eleventh request in ten seconds", response.Code)
		}
		if response.Header().Get("x-ratelimit-remaining-requests") != "50" {
			t.Fatalf("remaining = %q, want 50: an acceleration rejection must not consume the window", response.Header().Get("x-ratelimit-remaining-requests"))
		}
	})

	t.Run("rejects an oversized body with 413", func(t *testing.T) {
		s, _ := newServer(t, config())

		response := post(t, s, "/v1/chat/completions", `{"model":"gpt-4.1-mini","messages":[{"content":"`+strings.Repeat("x", 9<<20)+`"}]}`)

		if response.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want 413", response.Code)
		}
	})

	t.Run("rejects a request without a model", func(t *testing.T) {
		s, _ := newServer(t, config())

		response := post(t, s, "/v1/chat/completions", `{"messages":[]}`)

		if response.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", response.Code)
		}
	})

	t.Run("answers unknown routes with 404", func(t *testing.T) {
		s, _ := newServer(t, config())

		response := post(t, s, "/v1/embeddings", chat)

		if response.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", response.Code)
		}
	})

	t.Run("rejects an invalid configuration", func(t *testing.T) {
		cfg := config()
		cfg.TokensPerMinute = 0

		if err := cfg.Validate(); err == nil {
			t.Fatal("expected a validation error")
		}
	})
}
