package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestSimulate(t *testing.T) {
	t.Run("prints one ceiling per observation and carries the previous ceiling forward", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"simulate", "-concurrency", "4", "-max", "40", "-input", "testdata/observations.jsonl"}, strings.NewReader(""), &stdout, &stderr)

		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		want := []string{
			"at\tceiling\treason\tallowed_rate\tpod_rate",
			"2026-11-01T18:00:00Z\t4\theadroom\t8.36\t2.00",
			"2026-11-01T18:00:20Z\t3\theadroom\t7.44\t2.00",
			"2026-11-01T18:00:55Z\t3\theadroom\t6.80\t2.00",
			"2026-11-01T18:00:59Z\t3\tdrain\t0.00\t2.00",
			"2026-11-01T18:01:05Z\t4\theadroom\t8.50\t2.00",
			"2026-11-01T18:04:00Z\t4\tfrozen\t0.00\t0.00",
		}
		if len(lines) != len(want) {
			t.Fatalf("got %d lines, want %d:\n%s", len(lines), len(want), stdout.String())
		}
		for i := range want {
			if lines[i] != want[i] {
				t.Fatalf("line %d = %q, want %q", i+1, lines[i], want[i])
			}
		}
	})

	t.Run("reads observations from stdin by default", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := `{"at":"2026-11-01T18:00:00Z","requests":{"limit":600,"remaining":600,"reset_at":"2026-11-01T18:01:00Z"},"latency_ms":1000,"in_flight":0}` + "\n"

		code := run([]string{"simulate"}, strings.NewReader(input), &stdout, &stderr)

		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "2026-11-01T18:00:00Z\t8\theadroom\t8.50\t1.00") {
			t.Fatalf("stdout = %q, want a ceiling of 8 from 600 requests per minute", stdout.String())
		}
	})

	t.Run("names the line that cannot be decoded and exits with 1", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := `{"at":"2026-11-01T18:00:00Z"}` + "\n" + `not json` + "\n"

		code := run([]string{"simulate"}, strings.NewReader(input), &stdout, &stderr)

		if code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), "line 2") {
			t.Fatalf("stderr = %q, want it to name line 2", stderr.String())
		}
	})

	t.Run("rejects an observation without a timestamp", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"simulate"}, strings.NewReader(`{"in_flight":1}`+"\n"), &stdout, &stderr)

		if code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), "missing at") {
			t.Fatalf("stderr = %q, want it to report the missing timestamp", stderr.String())
		}
	})

	t.Run("rejects an invalid configuration and exits with 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"simulate", "-safety", "2"}, strings.NewReader(""), &stdout, &stderr)

		if code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if !strings.Contains(stderr.String(), "safety") {
			t.Fatalf("stderr = %q, want it to name the safety flag", stderr.String())
		}
	})

	t.Run("fails when the input file does not exist", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"simulate", "-input", "testdata/missing.jsonl"}, strings.NewReader(""), &stdout, &stderr)

		if code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
	})
}
