//go:build e2e

package e2e

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/dimonchik03/quotaware/internal/queue"
	"github.com/dimonchik03/quotaware/internal/scaler/externalscaler"
)

func TestSingleTriggerModeAgainstRedis(t *testing.T) {
	proxyURL := env("E2E_PROXY_URL", "http://localhost:18080")
	adminURL := env("E2E_ADMIN_URL", "http://localhost:19100")
	grpcAddress := env("E2E_GRPC_ADDRESS", "localhost:19090")
	redisAddress := env("E2E_REDIS_ADDRESS", "localhost:16379")
	waitUntilReady(t, proxyURL, adminURL)

	conn, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := externalscaler.NewExternalScalerClient(conn)
	jobs := queue.NewRedis(redisAddress)
	defer func() { _ = jobs.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := jobs.Clear(ctx, "e2e-jobs"); err != nil {
		t.Fatal(err)
	}
	ref := func(address string) *externalscaler.ScaledObjectRef {
		return &externalscaler.ScaledObjectRef{
			Name:      "queued-workers",
			Namespace: "e2e",
			ScalerMetadata: map[string]string{
				"bucket":         "openai/gpt-4.1-queue",
				"concurrency":    "1",
				"minReplicas":    "0",
				"defaultCeiling": "7",
				"maxReplicas":    "10",
				"redisAddress":   address,
				"listName":       "e2e-jobs",
				"listLength":     "10",
			},
		}
	}
	allowed := ref("redis:6379")
	waitForTheDefaultCeiling(t, ctx, client, allowed)

	t.Run("reports the queue demand below the ceiling and activates while the list has items", func(t *testing.T) {
		if err := jobs.Push(ctx, "e2e-jobs", make([]string, 25)...); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = jobs.Clear(context.Background(), "e2e-jobs") })

		metrics, err := client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: allowed, MetricName: "s0-quotaware-ceiling"})
		if err != nil {
			t.Fatal(err)
		}
		active, err := client.IsActive(ctx, allowed)
		if err != nil {
			t.Fatal(err)
		}

		if value := metrics.GetMetricValues()[0].GetMetricValue(); value != 3 || !active.GetResult() {
			t.Fatalf("value = %d active = %v, want ceil(25/10) = 3 below the default ceiling of 7 and active", value, active.GetResult())
		}
	})

	t.Run("is inactive and reports nothing once the list is empty", func(t *testing.T) {
		if err := jobs.Clear(ctx, "e2e-jobs"); err != nil {
			t.Fatal(err)
		}

		metrics, err := client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: allowed, MetricName: "s0-quotaware-ceiling"})
		if err != nil {
			t.Fatal(err)
		}
		active, err := client.IsActive(ctx, allowed)
		if err != nil {
			t.Fatal(err)
		}

		if cap := metricValue(t, metricsText(t, adminURL), "quotaware_ceiling", `scaledobject="queued-workers"`, `key="combined"`); cap != 7 {
			t.Fatalf("pre-demand quota ceiling = %v, want 7", cap)
		}
		if value := metrics.GetMetricValues()[0].GetMetricValue(); value != 0 || active.GetResult() {
			t.Fatalf("value = %d active = %v, want 0 and inactive", value, active.GetResult())
		}
	})

	t.Run("keeps only the deployment with an open provider call active after its queue drains", func(t *testing.T) {
		if os.Getenv("QUOTAWARE_E2E_OWNS_COMPOSE") != "1" {
			t.Skip("requires the disposable provider started by make e2e")
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyURL+"/v1/chat/completions", strings.NewReader(`{"model":"gpt-4.1-workload"}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Quotaware-Workload", "indexer")
		callDone := make(chan error, 1)
		composeProvider(t, "pause")
		t.Cleanup(func() {
			select {
			case err := <-callDone:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("provider call did not finish")
			}
		})
		go func() {
			response, err := http.DefaultClient.Do(request)
			if err == nil {
				_, err = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
			callDone <- err
		}()
		t.Cleanup(func() { composeProvider(t, "unpause") })

		deadline := time.Now().Add(5 * time.Second)
		for metricValueOr(metricsText(t, adminURL), "quotaware_inflight", 0, `model="gpt-4.1-workload"`) != 1 {
			if time.Now().After(deadline) {
				t.Fatal("call never entered the proxy")
			}
			time.Sleep(10 * time.Millisecond)
		}
		for _, tc := range []struct {
			label string
			value int64
		}{{"indexer", 1}, {"summarizer", 0}} {
			worker := ref("redis:6379")
			worker.Name = tc.label
			worker.ScalerMetadata["bucket"] = "openai/gpt-4.1-workload"
			worker.ScalerMetadata["workload"] = tc.label
			worker.ScalerMetadata["listName"] = "e2e-" + tc.label
			active, err := client.IsActive(ctx, worker)
			if err != nil {
				t.Fatal(err)
			}
			metric, err := client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: worker})
			if err != nil {
				t.Fatal(err)
			}
			if got := metric.GetMetricValues()[0].GetMetricValue(); got != tc.value || active.Result != (tc.value > 0) {
				t.Fatalf("%s active=%v, replicas=%d; want %d", tc.label, active.Result, got, tc.value)
			}
		}
	})

	t.Run("pushes a change of activity over StreamIsActive", func(t *testing.T) {
		stream, err := client.StreamIsActive(ctx, allowed)
		if err != nil {
			t.Fatal(err)
		}
		first, err := stream.Recv()
		if err != nil || first.GetResult() {
			t.Fatalf("first = %v err = %v, want inactive on an empty list", first.GetResult(), err)
		}
		if err := jobs.Push(ctx, "e2e-jobs", ""); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = jobs.Clear(context.Background(), "e2e-jobs") })

		pushed, err := stream.Recv()

		if err != nil || !pushed.GetResult() {
			t.Fatalf("pushed = %v err = %v, want active once a job arrives", pushed.GetResult(), err)
		}
	})

	t.Run("refuses a redis address that is not on the allow list", func(t *testing.T) {
		_, err := client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: ref("elsewhere:6379"), MetricName: "s0-quotaware-ceiling"})

		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("code = %v (%v), want PermissionDenied", status.Code(err), err)
		}
	})
}

func composeProvider(t *testing.T, action string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", action, "fakeprovider")
	cmd.Dir = ".."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compose %s fakeprovider: %v: %s", action, err, out)
	}
}

func TestAJobHandedBackIsPoppedNext(t *testing.T) {
	waitUntilReady(t, env("E2E_PROXY_URL", "http://localhost:18080"), env("E2E_ADMIN_URL", "http://localhost:19100"))
	jobs := queue.NewRedis(env("E2E_REDIS_ADDRESS", "localhost:16379"))
	defer func() { _ = jobs.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := jobs.Clear(ctx, "e2e-requeue"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jobs.Clear(context.Background(), "e2e-requeue") })
	if err := jobs.Push(ctx, "e2e-requeue", "first", "second"); err != nil {
		t.Fatal(err)
	}

	if err := jobs.Requeue(ctx, "e2e-requeue", "handed back"); err != nil {
		t.Fatal(err)
	}

	popped, err := jobs.Pop(ctx, "e2e-requeue", time.Second)
	if err != nil || popped != "handed back" {
		t.Fatalf("popped %q, err = %v, want the handed-back job before the waiting ones", popped, err)
	}
}

func waitForTheDefaultCeiling(t *testing.T, ctx context.Context, client externalscaler.ExternalScalerClient, ref *externalscaler.ScaledObjectRef) {
	t.Helper()
	for {
		_, err := client.GetMetrics(ctx, &externalscaler.GetMetricsRequest{ScaledObjectRef: ref})
		if status.Code(err) != codes.Unavailable {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
