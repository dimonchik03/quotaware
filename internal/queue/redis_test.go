package queue_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/internal/queue"
)

func silentServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted = append(accepted, conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		for _, conn := range accepted {
			_ = conn.Close()
		}
	})
	return listener.Addr().String()
}

func timedOut(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout())
}

func TestRedisLenAgainstAServerThatNeverAnswers(t *testing.T) {
	t.Run("returns when the context deadline passes", func(t *testing.T) {
		t.Parallel()
		reader := queue.NewRedis(silentServer(t))
		t.Cleanup(func() { _ = reader.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		started := time.Now()
		_, err := reader.Len(ctx, "jobs")

		if !timedOut(err) {
			t.Fatalf("err = %v, want a timeout", err)
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("Len took %v, want it to return at the context deadline", elapsed)
		}
	})

	t.Run("gives up within the read timeout when the context has no deadline", func(t *testing.T) {
		t.Parallel()
		reader := queue.NewRedis(silentServer(t))
		t.Cleanup(func() { _ = reader.Close() })

		started := time.Now()
		_, err := reader.Len(context.Background(), "jobs")

		if !timedOut(err) {
			t.Fatalf("err = %v, want a timeout", err)
		}
		if elapsed := time.Since(started); elapsed > 4*time.Second {
			t.Fatalf("Len took %v, want it bounded by the two-second read timeout", elapsed)
		}
	})
}
