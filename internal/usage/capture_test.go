package usage_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/dimonchik03/quotaware/internal/usage"
)

type failingReader struct {
	data string
	err  error
	read bool
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, r.err
	}
	r.read = true
	return copy(p, r.data), nil
}

func (r *failingReader) Close() error { return nil }

func TestCapture(t *testing.T) {
	t.Run("hands over the whole body as completed at end of file", func(t *testing.T) {
		var got []byte
		var outcome usage.Outcome
		c := usage.NewCapture(io.NopCloser(strings.NewReader("hello")), 64, func(body []byte, o usage.Outcome) { got, outcome = body, o })

		if _, err := io.ReadAll(c); err != nil {
			t.Fatal(err)
		}
		_ = c.Close()

		if string(got) != "hello" || outcome != usage.Completed {
			t.Fatalf("body = %q outcome = %v, want hello completed", got, outcome)
		}
	})

	t.Run("reports an abandoned body when it is closed before the end", func(t *testing.T) {
		var outcome usage.Outcome
		c := usage.NewCapture(io.NopCloser(strings.NewReader("hello")), 64, func(_ []byte, o usage.Outcome) { outcome = o })

		if _, err := c.Read(make([]byte, 2)); err != nil {
			t.Fatal(err)
		}
		_ = c.Close()

		if outcome != usage.Abandoned {
			t.Fatalf("outcome = %v, want abandoned", outcome)
		}
	})

	t.Run("reports a failed body when the read breaks before the end", func(t *testing.T) {
		var outcome usage.Outcome
		calls := 0
		c := usage.NewCapture(&failingReader{data: "hel", err: errors.New("connection reset")}, 64, func(_ []byte, o usage.Outcome) { outcome, calls = o, calls+1 })

		_, err := io.ReadAll(c)
		_ = c.Close()

		if err == nil || outcome != usage.Failed || calls != 1 {
			t.Fatalf("err = %v outcome = %v calls = %d, want the read error, failed, reported once", err, outcome, calls)
		}
	})

	t.Run("reports an abandoned body when the read was cancelled on our side", func(t *testing.T) {
		var outcome usage.Outcome
		c := usage.NewCapture(&failingReader{data: "hel", err: context.Canceled}, 64, func(_ []byte, o usage.Outcome) { outcome = o })

		_, _ = io.ReadAll(c)
		_ = c.Close()

		if outcome != usage.Abandoned {
			t.Fatalf("outcome = %v, want abandoned", outcome)
		}
	})

	t.Run("drops the buffer once the limit is exceeded", func(t *testing.T) {
		var got []byte
		c := usage.NewCapture(io.NopCloser(strings.NewReader(strings.Repeat("x", 100))), 10, func(body []byte, _ usage.Outcome) { got = body })

		if _, err := io.ReadAll(c); err != nil {
			t.Fatal(err)
		}

		if got != nil {
			t.Fatalf("body = %d bytes, want none after overflow", len(got))
		}
	})
}
