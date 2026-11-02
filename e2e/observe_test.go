//go:build e2e

package e2e

import (
	"net/http"
	"strings"
	"testing"
)

func TestObserveEndpointAgainstTheBuiltImage(t *testing.T) {
	proxyURL := env("E2E_PROXY_URL", "http://localhost:18080")
	adminURL := env("E2E_ADMIN_URL", "http://localhost:19100")
	waitUntilReady(t, proxyURL, adminURL)

	request, err := http.NewRequest(http.MethodPost, adminURL+"/v1/observe", strings.NewReader(`{"provider":"anthropic","model":"claude-observed","credential":"e2e0e2e0e2e0","status":200,"headers":{"anthropic-ratelimit-requests-limit":"50","anthropic-ratelimit-requests-remaining":"49","anthropic-ratelimit-requests-reset":"2030-01-01T00:00:00Z"},"usage":120,"latency_ms":900,"complete":true}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without a token = %d, want 401", response.StatusCode)
	}

	request.Header.Set("Authorization", "Bearer "+env("E2E_OBSERVE_TOKEN", "e2e-observe-token"))
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()

	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", response.StatusCode)
	}
	exposition := metricsText(t, adminURL)
	if remaining := metricValue(t, exposition, "quotaware_remaining_requests", `provider="anthropic"`, `model="claude-observed"`); remaining != 49 {
		t.Fatalf("remaining = %v, want the observation applied", remaining)
	}
	if tokens := metricValue(t, exposition, "quotaware_tokens_per_call", `model="claude-observed"`); tokens != 120 {
		t.Fatalf("tokens per call = %v, want 120", tokens)
	}
}
