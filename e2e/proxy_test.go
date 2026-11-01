//go:build e2e

package e2e

import (
	"bufio"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var client = &http.Client{Timeout: 30 * time.Second}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func waitUntilReady(t *testing.T, proxyURL, adminURL string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		health, err := client.Get(adminURL + "/healthz")
		if err == nil {
			_ = health.Body.Close()
			response, body := post(t, proxyURL+"/v1/chat/completions", "sk-ready", `{"model":"readiness-probe","messages":[]}`)
			if response.StatusCode == http.StatusOK && strings.Contains(body, "usage") {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatal("proxy and fake provider did not become ready in time")
}

func post(t *testing.T, target, credential, body string) (*http.Response, string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("x-api-key", credential)
	response, err := client.Do(request)
	if err != nil {
		return &http.Response{StatusCode: 0}, ""
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, string(raw)
}

func metrics(t *testing.T, adminURL string) string {
	t.Helper()
	response, err := client.Get(adminURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func metricValue(t *testing.T, exposition, name string, labels ...string) float64 {
	t.Helper()
	scanner := bufio.NewScanner(strings.NewReader(exposition))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		matches := true
		for _, label := range labels {
			if !strings.Contains(line, label) {
				matches = false
			}
		}
		if !matches {
			continue
		}
		value, err := strconv.ParseFloat(line[strings.LastIndex(line, " ")+1:], 64)
		if err != nil {
			t.Fatalf("cannot parse %q: %v", line, err)
		}
		return value
	}
	t.Fatalf("metric %s with labels %v not found in:\n%s", name, labels, exposition)
	return 0
}

func TestProxyAgainstTheFakeProvider(t *testing.T) {
	proxyURL := env("E2E_PROXY_URL", "http://localhost:18080")
	adminURL := env("E2E_ADMIN_URL", "http://localhost:19100")
	waitUntilReady(t, proxyURL, adminURL)

	t.Run("a chat completion is answered with provider headers and observed in the metrics", func(t *testing.T) {
		response, body := post(t, proxyURL+"/v1/chat/completions", "sk-e2e", `{"model":"gpt-4.1-mini","messages":[{"role":"user","content":"hello"}]}`)

		if response.StatusCode != http.StatusOK || !strings.Contains(body, `"usage"`) {
			t.Fatalf("status = %d, body = %s", response.StatusCode, body)
		}
		if response.Header.Get("x-ratelimit-limit-requests") != "30" {
			t.Fatalf("headers = %v, want the fake provider limits passed through", response.Header)
		}
		exposition := metrics(t, adminURL)
		if remaining := metricValue(t, exposition, "quotaware_remaining_requests", `provider="openai"`, `model="gpt-4.1-mini"`); remaining >= 30 {
			t.Fatalf("remaining requests = %v, want it below the limit after a call", remaining)
		}
		if tokens := metricValue(t, exposition, "quotaware_tokens_per_call", `provider="openai"`, `model="gpt-4.1-mini"`); tokens <= 0 {
			t.Fatalf("tokens per call = %v, want usage captured", tokens)
		}
		if latency := metricValue(t, exposition, "quotaware_latency_seconds", `provider="openai"`, `model="gpt-4.1-mini"`); latency < 0.05 {
			t.Fatalf("latency = %v, want the fake provider latency observed", latency)
		}
	})

	t.Run("an Anthropic stream passes through and its usage is captured", func(t *testing.T) {
		response, body := post(t, proxyURL+"/v1/messages", "sk-ant-e2e", `{"model":"claude-sonnet-4-5","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hello"}]}`)

		if response.StatusCode != http.StatusOK || !strings.Contains(body, "event: message_stop") {
			t.Fatalf("status = %d, body = %s", response.StatusCode, body)
		}
		if _, err := time.Parse(time.RFC3339, response.Header.Get("anthropic-ratelimit-tokens-reset")); err != nil {
			t.Fatalf("anthropic reset header = %q, want RFC 3339", response.Header.Get("anthropic-ratelimit-tokens-reset"))
		}
		exposition := metrics(t, adminURL)
		if tokens := metricValue(t, exposition, "quotaware_tokens_per_call", `provider="anthropic"`, `model="claude-sonnet-4-5"`); tokens <= 0 {
			t.Fatalf("tokens per call = %v, want usage captured from the stream", tokens)
		}
	})

	t.Run("429s are counted once the request limit is exhausted and no call stays in flight", func(t *testing.T) {
		limited := 0
		for range 35 {
			response, _ := post(t, proxyURL+"/v1/chat/completions", "sk-burst", `{"model":"gpt-4.1","messages":[{"role":"user","content":"burst"}]}`)
			if response.StatusCode == http.StatusTooManyRequests {
				limited++
			}
		}
		if limited == 0 {
			t.Fatal("expected at least one 429 from 35 calls against a limit of 30 per minute")
		}

		exposition := metrics(t, adminURL)
		if counted := metricValue(t, exposition, "quotaware_429_total", `model="gpt-4.1"`); counted != float64(limited) {
			t.Fatalf("quotaware_429_total = %v, want %d", counted, limited)
		}
		if retryAfter := metricValue(t, exposition, "quotaware_retry_after_seconds", `model="gpt-4.1"`); retryAfter <= 0 {
			t.Fatalf("retry-after = %v, want the provider's value", retryAfter)
		}
		if inflight := metricValue(t, exposition, "quotaware_inflight", `model="gpt-4.1"`); inflight != 0 {
			t.Fatalf("inflight = %v, want 0 after every call returned", inflight)
		}
	})
}
