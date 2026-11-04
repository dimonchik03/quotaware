//go:build e2e

package e2e

import (
	"context"
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

		if value := metrics.GetMetricValues()[0].GetMetricValue(); value != 0 || active.GetResult() {
			t.Fatalf("value = %d active = %v, want 0 and inactive", value, active.GetResult())
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
