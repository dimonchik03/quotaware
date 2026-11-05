//go:build e2e

package e2e

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dimonchik03/quotaware/internal/scaler/externalscaler"
	"github.com/dimonchik03/quotaware/internal/usage"
)

func TestScalerAgainstTheFakeProvider(t *testing.T) {
	proxyURL := env("E2E_PROXY_URL", "http://localhost:18080")
	adminURL := env("E2E_ADMIN_URL", "http://localhost:19100")
	grpcAddress := env("E2E_GRPC_ADDRESS", "localhost:19090")
	waitUntilReady(t, proxyURL, adminURL)

	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := externalscaler.NewExternalScalerClient(conn)
	ref := &externalscaler.ScaledObjectRef{
		Name:      "llm-workers",
		Namespace: "e2e",
		ScalerMetadata: map[string]string{
			"bucket":         "openai/gpt-4.1-scaler",
			"concurrency":    "1",
			"minReplicas":    "0",
			"defaultCeiling": "7",
			"maxReplicas":    "10",
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("announces the ceiling metric with a target of one", func(t *testing.T) {
		response, err := client.GetMetricSpec(ctx, ref)

		if err != nil {
			t.Fatal(err)
		}
		spec := response.GetMetricSpecs()[0]
		if spec.GetMetricName() != "quotaware-ceiling" || spec.GetTargetSize() != 1 {
			t.Fatalf("spec = %+v, want quotaware-ceiling with target size one", spec)
		}
	})

	t.Run("reports a ceiling from what the proxy observed and exposes it as a series", func(t *testing.T) {
		response, _ := mustPost(t, proxyURL+"/v1/chat/completions", "sk-scaler-e2e", `{"model":"gpt-4.1-scaler","messages":[{"role":"user","content":"hello"}]}`)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", response.StatusCode)
		}

		metrics, err := client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: ref, MetricName: "s1-quotaware-ceiling"})

		if err != nil {
			t.Fatal(err)
		}
		value := metrics.GetMetricValues()[0]
		if value.GetMetricName() != "s1-quotaware-ceiling" || value.GetMetricValue() != 0 {
			t.Fatalf("value = %+v, want the requested name and 0 from 3 requests per minute, not the default of 7", value)
		}
		exposition := metricsText(t, adminURL)
		credential := `key="` + usage.CredentialHash(http.Header{"Authorization": {"Bearer sk-scaler-e2e"}}) + `"`
		if ceiling := metricValue(t, exposition, "quotaware_ceiling", `namespace="e2e"`, `scaledobject="llm-workers"`, `model="gpt-4.1-scaler"`, credential); ceiling != 0 {
			t.Fatalf("quotaware_ceiling = %v, want 0 on the bucket of the credential the proxy saw", ceiling)
		}
	})

	t.Run("a collector reporting only exhausted project tokens stops new workers", func(t *testing.T) {
		body := `{"provider":"openai","model":"project-only","credential":"0123456789ab","status":200,"complete":true,"usage":100,"latency_ms":1000,"headers":{"x-ratelimit-limit-project-tokens":"6000","x-ratelimit-remaining-project-tokens":"0","x-ratelimit-reset-project-tokens":"60s"}}`
		post, err := http.NewRequestWithContext(ctx, http.MethodPost, adminURL+usage.ObservePath, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		post.Header.Set("Authorization", "Bearer "+env("E2E_OBSERVE_TOKEN", "e2e-observe-token"))
		response, err := http.DefaultClient.Do(post)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			t.Fatal(response.Status)
		}
		project := &externalscaler.ScaledObjectRef{Name: "project-only", Namespace: "e2e", ScalerMetadata: map[string]string{"bucket": "openai/project-only", "concurrency": "1", "minReplicas": "0", "defaultCeiling": "7"}}
		result, err := client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: project})
		if err != nil {
			t.Fatal(err)
		}
		if got := result.GetMetricValues()[0].GetMetricValue(); got != 0 {
			t.Fatalf("exhausted project window returned %d, want 0", got)
		}
	})

	t.Run("never activates a scaled object from the cap trigger", func(t *testing.T) {
		active, err := client.IsActive(ctx, ref)

		if err != nil || active.GetResult() {
			t.Fatalf("active = %v err = %v, want inactive", active.GetResult(), err)
		}
	})
}
