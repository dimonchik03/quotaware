package proxy_test

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/bench/fakeprovider"
	"github.com/dimonchik03/quotaware/internal/proxy"
	"github.com/dimonchik03/quotaware/internal/state"
)

var start = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type harness struct {
	front *httptest.Server
	store *state.Store
	clock *clock
	logs  *syncBuffer
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
	return newHarnessWithHandler(t, c, fakeprovider.New(cfg, c.Now, c.Advance), 0)
}

func newHarnessWithHandler(t *testing.T, c *clock, upstream http.Handler, captureLimit int64) *harness {
	t.Helper()
	server := httptest.NewServer(upstream)
	t.Cleanup(server.Close)
	return newHarnessWithUpstream(t, c, server.URL, captureLimit)
}

func newHarnessWithUpstream(t *testing.T, c *clock, upstream string, captureLimit int64) *harness {
	t.Helper()
	return newHarnessWithOptions(t, c, upstream, func(cfg *proxy.Config) {
		if captureLimit > 0 {
			cfg.MaxCaptureBytes = captureLimit
		}
	})
}

func newHarnessWithOptions(t *testing.T, c *clock, upstream string, configure func(*proxy.Config)) *harness {
	t.Helper()
	target, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.New(0.5, state.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	cfg := proxy.DefaultConfig()
	cfg.OpenAI, cfg.Anthropic = target, target
	cfg.MaxRequestBytes = 1024
	cfg.Logger = slog.New(slog.NewTextHandler(logs, nil))
	configure(&cfg)
	p, err := proxy.New(cfg, store, c.Now)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(p)
	t.Cleanup(front.Close)
	return &harness{front: front, store: store, clock: c, logs: logs}
}

func (h *harness) request(t *testing.T, ctx context.Context, path, credential, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.front.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	switch {
	case credential == "":
	case strings.HasPrefix(path, "/v1/messages"):
		request.Header.Set("x-api-key", credential)
	default:
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func (h *harness) post(t *testing.T, path, credential, body string) (*http.Response, string) {
	t.Helper()
	response := h.request(t, context.Background(), path, credential, body)
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, string(raw)
}

func (h *harness) bucket(t *testing.T, provider, model string) state.Bucket {
	t.Helper()
	for _, b := range h.store.Buckets(h.clock.Now()) {
		if b.Key.Provider == provider && b.Key.Model == model {
			return b
		}
	}
	t.Fatalf("no bucket for %s/%s in %+v", provider, model, h.store.Buckets(h.clock.Now()))
	return state.Bucket{}
}

func (h *harness) waitForIdle(t *testing.T, provider, model string) state.Bucket {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b := h.bucket(t, provider, model); b.InFlight == 0 {
			return b
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("bucket %s/%s never went idle: %+v", provider, model, h.bucket(t, provider, model))
	return state.Bucket{}
}

func (h *harness) waitForLog(t *testing.T, text string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(h.logs.String(), text) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("logs = %q, want %q", h.logs.String(), text)
}

func rateLimitHeaders(h http.Header) {
	h.Set("x-ratelimit-limit-requests", "60")
	h.Set("x-ratelimit-remaining-requests", "59")
	h.Set("x-ratelimit-reset-requests", "1s")
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
		if b.Quotas.Requests.Limit != 3 || b.Quotas.Requests.Remaining != 2 || b.Quotas.Tokens.Limit != 10000 {
			t.Fatalf("quotas = %+v, want the fake provider limits", b.Quotas)
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
		if b.RateLimited != 1 || b.RetryAfter <= 0 || b.Quotas.Requests.Remaining != 0 || b.InFlight != 0 {
			t.Fatalf("bucket = %+v, want the 429 recorded with retry-after and zero remaining", b)
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
		if b.TokensPerCall <= 5 || b.Quotas.Tokens.Limit != 10000 {
			t.Fatalf("bucket = %+v, want usage from message_delta and Anthropic limits", b)
		}
	})

	t.Run("inflates a gzip response to read its usage", func(t *testing.T) {
		h := newHarnessWithHandler(t, &clock{now: start}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			rateLimitHeaders(w.Header())
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Encoding", "gzip")
			writer := gzip.NewWriter(w)
			fmt.Fprint(writer, `{"usage":{"prompt_tokens":100,"completion_tokens":50}}`)
			_ = writer.Close()
		}), 0)

		response, body := h.post(t, "/v1/chat/completions", "sk-test", chat)

		if response.StatusCode != http.StatusOK || !strings.Contains(body, `"usage"`) {
			t.Fatalf("status = %d, body = %q, want the inflated JSON at the client", response.StatusCode, body)
		}
		if b := h.bucket(t, "openai", "gpt-4.1-mini"); b.TokensPerCall != 150 {
			t.Fatalf("tokens per call = %v, want 150 from the gzip body", b.TokensPerCall)
		}
	})

	t.Run("forwards stream events as they arrive instead of buffering the stream", func(t *testing.T) {
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }) })
		h := newHarnessWithHandler(t, &clock{now: start}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			rateLimitHeaders(w.Header())
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-release
			fmt.Fprint(w, "data: second\n\n")
		}), 0)

		response := h.request(t, context.Background(), "/v1/chat/completions", "sk-test", chat)
		defer func() { _ = response.Body.Close() }()
		reader := bufio.NewReader(response.Body)
		firstLine := make(chan string, 1)
		go func() {
			line, _ := reader.ReadString('\n')
			firstLine <- line
		}()

		select {
		case line := <-firstLine:
			if line != "data: first\n" {
				t.Fatalf("first line = %q", line)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("the first event did not reach the client while the upstream was still streaming")
		}
		once.Do(func() { close(release) })
		rest, _ := io.ReadAll(reader)
		if !strings.Contains(string(rest), "data: second") {
			t.Fatalf("rest = %q, want the second event", rest)
		}
	})

	t.Run("dates the quotas by when the headers arrived, not when the body ended", func(t *testing.T) {
		release := make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }) })
		c := &clock{now: start}
		h := newHarnessWithHandler(t, c, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			rateLimitHeaders(w.Header())
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-release
			fmt.Fprint(w, "data: [DONE]\n\n")
		}), 0)
		response := h.request(t, context.Background(), "/v1/chat/completions", "sk-test", chat)
		defer func() { _ = response.Body.Close() }()
		reader := bufio.NewReader(response.Body)
		if _, err := reader.ReadString('\n'); err != nil {
			t.Fatal(err)
		}

		c.Advance(time.Minute)
		once.Do(func() { close(release) })
		_, _ = io.ReadAll(reader)

		b := h.waitForIdle(t, "openai", "gpt-4.1-mini")
		if !b.ObservedAt.Equal(start) {
			t.Fatalf("observed at = %v, want %v when the headers arrived", b.ObservedAt, start)
		}
	})

	t.Run("releases the slot and counts an abort when the client stops reading a stream", func(t *testing.T) {
		h := newHarnessWithHandler(t, &clock{now: start}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rateLimitHeaders(w.Header())
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "data: first\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}), 0)
		ctx, cancel := context.WithCancel(context.Background())
		response := h.request(t, ctx, "/v1/chat/completions", "sk-test", chat)
		if _, err := bufio.NewReader(response.Body).ReadString('\n'); err != nil {
			t.Fatal(err)
		}

		cancel()
		_ = response.Body.Close()

		b := h.waitForIdle(t, "openai", "gpt-4.1-mini")
		if b.Aborted != 1 || b.Responses != 1 || b.Latency != 0 || b.UsageMissing != 0 {
			t.Fatalf("bucket = %+v, want one abort with no latency or usage update", b)
		}
	})

	t.Run("still forwards a response that is too large to inspect and counts its usage as missing", func(t *testing.T) {
		long := `{"filler":"` + strings.Repeat("x", 2000) + `","usage":{"prompt_tokens":1,"completion_tokens":1}}`
		h := newHarnessWithHandler(t, &clock{now: start}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			rateLimitHeaders(w.Header())
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, long)
		}), 256)

		_, body := h.post(t, "/v1/chat/completions", "sk-test", chat)

		if body != long {
			t.Fatalf("client received %d bytes, want the full %d", len(body), len(long))
		}
		if b := h.bucket(t, "openai", "gpt-4.1-mini"); b.UsageMissing != 1 || b.TokensPerCall != 0 {
			t.Fatalf("bucket = %+v, want usage missing because the body exceeded the capture limit", b)
		}
	})

	t.Run("counts and logs a malformed reset and keeps the window without it", func(t *testing.T) {
		h := newHarnessWithHandler(t, &clock{now: start}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("x-ratelimit-limit-requests", "60")
			w.Header().Set("x-ratelimit-remaining-requests", "59")
			w.Header().Set("x-ratelimit-reset-requests", "soon")
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
		}), 0)

		h.post(t, "/v1/chat/completions", "sk-test", chat)

		b := h.bucket(t, "openai", "gpt-4.1-mini")
		if b.HeadersMalformed != 1 || b.HeadersMissing != 0 {
			t.Fatalf("bucket = %+v, want one malformed header count", b)
		}
		if b.Quotas.Requests.Remaining != 59 || !b.Quotas.Requests.ResetAt.IsZero() {
			t.Fatalf("quotas = %+v, want the requests window kept without its reset", b.Quotas)
		}
		if !strings.Contains(h.logs.String(), "x-ratelimit-reset-requests") {
			t.Fatalf("logs = %q, want the malformed header named", h.logs.String())
		}
	})

	t.Run("keys buckets by provider, model and a hash of the credential", func(t *testing.T) {
		h := newHarness(t)

		h.post(t, "/v1/chat/completions", "sk-first", chat)
		h.post(t, "/v1/chat/completions", "sk-second", chat)
		h.post(t, "/v1/messages", "sk-first", message)
		h.post(t, "/v1/chat/completions", "", chat)

		buckets := h.store.Buckets(h.clock.Now())
		if len(buckets) != 4 {
			t.Fatalf("buckets = %d, want 4", len(buckets))
		}
		anonymous := 0
		for _, b := range buckets {
			if b.Key.Key == "anonymous" {
				anonymous++
				continue
			}
			if len(b.Key.Key) != 12 || strings.Contains(b.Key.Key, "sk-") {
				t.Fatalf("key label = %q, want a 12-character hash that never leaks the credential", b.Key.Key)
			}
		}
		if anonymous != 1 {
			t.Fatalf("anonymous buckets = %d, want exactly the request without a credential", anonymous)
		}
	})

	t.Run("rejects a request without a model and touches no bucket", func(t *testing.T) {
		h := newHarness(t)

		response, _ := h.post(t, "/v1/chat/completions", "sk-test", `{"messages":[]}`)

		if response.StatusCode != http.StatusBadRequest || len(h.store.Buckets(h.clock.Now())) != 0 {
			t.Fatalf("status = %d, buckets = %d, want 400 and none", response.StatusCode, len(h.store.Buckets(h.clock.Now())))
		}
	})

	t.Run("rejects a model name longer than the label limit", func(t *testing.T) {
		h := newHarness(t)

		response, body := h.post(t, "/v1/chat/completions", "sk-test", `{"model":"`+strings.Repeat("m", 300)+`","messages":[]}`)

		if response.StatusCode != http.StatusBadRequest || !strings.Contains(body, "too long") || len(h.store.Buckets(h.clock.Now())) != 0 {
			t.Fatalf("status = %d, body = %s, want 400 naming the length and no bucket", response.StatusCode, body)
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

	t.Run("lets the transport replay the request body", func(t *testing.T) {
		var replayed string
		h := newHarnessWithOptions(t, &clock{now: start}, "http://upstream.invalid", func(cfg *proxy.Config) {
			cfg.Transport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				if r.GetBody == nil {
					return nil, fmt.Errorf("request has no replayable body")
				}
				body, err := r.GetBody()
				if err != nil {
					return nil, err
				}
				raw, err := io.ReadAll(body)
				if err != nil {
					return nil, err
				}
				replayed = string(raw)
				header := http.Header{"Content-Type": []string{"application/json"}}
				rateLimitHeaders(header)
				return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`)), Request: r}, nil
			})
		})

		response, _ := h.post(t, "/v1/chat/completions", "sk-test", chat)

		if response.StatusCode != http.StatusOK || replayed != chat {
			t.Fatalf("status = %d replayed = %q, want 200 and the original body", response.StatusCode, replayed)
		}
	})

	t.Run("answers 408 when the client stalls while sending the body", func(t *testing.T) {
		h := newHarnessWithOptions(t, &clock{now: start}, "http://upstream.invalid", func(cfg *proxy.Config) {
			cfg.ReadBodyTimeout = 200 * time.Millisecond
		})
		conn, err := net.Dial("tcp", strings.TrimPrefix(h.front.URL, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()

		fmt.Fprint(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: quotaware\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"model\":")
		status, err := bufio.NewReader(conn).ReadString('\n')

		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(status, "HTTP/1.1 408") {
			t.Fatalf("status line = %q, want 408", status)
		}
		if buckets := h.store.Buckets(h.clock.Now()); len(buckets) != 0 {
			t.Fatalf("buckets = %d, want none for a request that never arrived", len(buckets))
		}
	})

	t.Run("counts a client that gives up before the upstream answers as an abort, not an upstream error", func(t *testing.T) {
		release := make(chan struct{})
		h := newHarnessWithHandler(t, &clock{now: start}, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		}), 0)
		t.Cleanup(func() { close(release) })
		ctx, cancel := context.WithCancel(context.Background())
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.front.URL+"/v1/chat/completions", strings.NewReader(chat))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer sk-test")
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		if _, err := http.DefaultClient.Do(request); err == nil {
			t.Fatal("expected the cancelled request to fail")
		}

		b := h.waitForIdle(t, "openai", "gpt-4.1-mini")
		if b.Aborted != 1 || b.UpstreamErrors != 0 || b.Responses != 0 {
			t.Fatalf("bucket = %+v, want one abort and no upstream error", b)
		}
		h.waitForLog(t, "client gave up before the upstream answered")
		if strings.Contains(h.logs.String(), "upstream request failed") {
			t.Fatalf("logs = %q, want no upstream failure for a client that left", h.logs.String())
		}
	})

	t.Run("counts an upstream that breaks mid-body as an upstream error, not an abort", func(t *testing.T) {
		h := newHarnessWithHandler(t, &clock{now: start}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			rateLimitHeaders(w.Header())
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, `{"usage":`)
			w.(http.Flusher).Flush()
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			_ = conn.Close()
		}), 0)

		request, err := http.NewRequest(http.MethodPost, h.front.URL+"/v1/chat/completions", strings.NewReader(chat))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer sk-test")
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			_, err = io.ReadAll(response.Body)
			_ = response.Body.Close()
		}

		if err == nil {
			t.Fatal("expected the truncated response to fail")
		}
		b := h.waitForIdle(t, "openai", "gpt-4.1-mini")
		if b.UpstreamErrors != 1 || b.Aborted != 0 || b.Responses != 1 {
			t.Fatalf("bucket = %+v, want one upstream error on a recorded response and no abort", b)
		}
	})

	t.Run("answers 502, releases the slot and logs when the upstream is unreachable", func(t *testing.T) {
		closed := httptest.NewServer(http.NotFoundHandler())
		closed.Close()
		h := newHarnessWithUpstream(t, &clock{now: start}, closed.URL, 0)

		response, _ := h.post(t, "/v1/chat/completions", "sk-test", chat)

		if response.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", response.StatusCode)
		}
		if b := h.bucket(t, "openai", "gpt-4.1-mini"); b.InFlight != 0 || b.Responses != 0 || b.UpstreamErrors != 1 {
			t.Fatalf("bucket = %+v, want the slot released and one upstream error", b)
		}
		if !strings.Contains(h.logs.String(), "upstream request failed") {
			t.Fatalf("logs = %q, want the failure logged", h.logs.String())
		}
	})

	t.Run("answers 504, releases the slot and counts an upstream error when the upstream never sends headers", func(t *testing.T) {
		release := make(chan struct{})
		silent := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			select {
			case <-r.Context().Done():
			case <-release:
			}
		}))
		t.Cleanup(silent.Close)
		t.Cleanup(func() { close(release) })
		h := newHarnessWithOptions(t, &clock{now: start}, silent.URL, func(cfg *proxy.Config) {
			cfg.UpstreamHeaderTimeout = 100 * time.Millisecond
		})

		started := time.Now()
		response, _ := h.post(t, "/v1/chat/completions", "sk-test", chat)
		elapsed := time.Since(started)

		if response.StatusCode != http.StatusGatewayTimeout {
			t.Fatalf("status = %d, want 504", response.StatusCode)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("answered after %v, want the 504 soon after the 100 ms header timeout", elapsed)
		}
		if b := h.bucket(t, "openai", "gpt-4.1-mini"); b.InFlight != 0 || b.UpstreamErrors != 1 {
			t.Fatalf("bucket = %+v, want the slot released and one upstream error", b)
		}
		if !strings.Contains(h.logs.String(), "upstream request failed") {
			t.Fatalf("logs = %q, want the failure logged", h.logs.String())
		}
	})

	t.Run("answers 400 to a malformed workload label before opening a call", func(t *testing.T) {
		h := newHarness(t)
		request, err := http.NewRequest(http.MethodPost, h.front.URL+"/v1/chat/completions", strings.NewReader(chat))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer sk-test")
		request.Header.Set("X-Quotaware-Workload", "Not a DNS label")

		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = response.Body.Close() }()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}

		if response.StatusCode != http.StatusBadRequest || !strings.Contains(string(raw), "invalid workload label") {
			t.Fatalf("status = %d body = %s, want 400 naming the workload label", response.StatusCode, raw)
		}
		if buckets := h.store.Buckets(h.clock.Now()); len(buckets) != 0 {
			t.Fatalf("buckets = %+v, want no call opened for a request the proxy refused", buckets)
		}
	})

	t.Run("answers 503 and counts the call when the in-flight limit is reached", func(t *testing.T) {
		entered := make(chan struct{})
		released := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(released) }) }
		busy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(entered)
			<-released
			rateLimitHeaders(w.Header())
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(busy.Close)
		t.Cleanup(release)
		h := newHarnessWithOptions(t, &clock{now: start}, busy.URL, func(cfg *proxy.Config) {
			cfg.MaxInFlight = 1
		})
		first := make(chan int, 1)
		go func() {
			request, err := http.NewRequest(http.MethodPost, h.front.URL+"/v1/chat/completions", strings.NewReader(chat))
			if err != nil {
				first <- 0
				return
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				first <- 0
				return
			}
			_ = response.Body.Close()
			first <- response.StatusCode
		}()
		<-entered

		rejected, body := h.post(t, "/v1/chat/completions", "sk-test", chat)
		release()

		if rejected.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "in flight") {
			t.Fatalf("status = %d body = %s, want 503 naming the in-flight limit", rejected.StatusCode, body)
		}
		if got := <-first; got != http.StatusOK {
			t.Fatalf("first call = %d, want it to finish once the upstream releases it", got)
		}
		if h.store.Stats().RejectedCalls != 1 {
			t.Fatalf("stats = %+v, want one rejected call", h.store.Stats())
		}
	})

	t.Run("requires both upstreams", func(t *testing.T) {
		store, _ := state.New(0.5, state.DefaultLimits())

		if _, err := proxy.New(proxy.DefaultConfig(), store, time.Now); err == nil {
			t.Fatal("expected an error without upstreams")
		}
	})

	t.Run("rejects bounds that would not bound", func(t *testing.T) {
		cases := []struct {
			name      string
			configure func(*proxy.Config)
		}{
			{"a zero in-flight limit", func(cfg *proxy.Config) { cfg.MaxInFlight = 0 }},
			{"a zero upstream header timeout", func(cfg *proxy.Config) { cfg.UpstreamHeaderTimeout = 0 }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				store, _ := state.New(0.5, state.DefaultLimits())
				cfg := proxy.DefaultConfig()
				cfg.OpenAI, _ = url.Parse("http://provider")
				cfg.Anthropic = cfg.OpenAI
				tc.configure(&cfg)

				if _, err := proxy.New(cfg, store, time.Now); err == nil {
					t.Fatalf("expected an error for %s", tc.name)
				}
			})
		}
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRejectedCallFinishingAfterEvictionLeavesNewCallInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Quotaware-Workload") != "" {
			t.Error("workload metadata reached provider")
		}
		close(entered)
		<-release
	}))
	defer upstream.Close()
	store, err := state.New(.5, state.Limits{Buckets: 1, Consumers: 10})
	if err != nil {
		t.Fatal(err)
	}
	c := &clock{now: start}
	cfg := proxy.DefaultConfig()
	cfg.OpenAI, _ = url.Parse(upstream.URL)
	cfg.Anthropic = cfg.OpenAI
	handler, err := proxy.New(cfg, store, c.Now)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(handler)
	defer front.Close()
	store.Observe(state.Key{Provider: "openai", Model: "occupied", Key: "anonymous"}, state.Sample{At: start})
	c.Advance(11 * time.Minute)
	finished := make(chan error, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, front.URL+"/v1/chat/completions", strings.NewReader(`{"model":"next"}`))
		if err != nil {
			finished <- err
			return
		}
		req.Header.Set("X-Quotaware-Workload", "indexer")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		finished <- err
	}()
	<-entered
	store.Evict(c.Now(), 10*time.Minute)
	key := state.Key{Provider: "openai", Model: "next", Key: "anonymous"}
	store.Begin(key, c.Now(), "")
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	b := store.Buckets(c.Now())[0]
	if b.InFlight != 1 || b.Responses != 0 || store.Stats().RejectedBuckets != 1 {
		t.Fatalf("bucket = %+v, stats = %+v", b, store.Stats())
	}
}
