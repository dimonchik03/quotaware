package main

import (
	"bytes"
	"errors"
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
			"2026-11-01T18:00:55Z\t4\theadroom\t8.22\t2.00",
			"2026-11-01T18:00:59Z\t4\theadroom\t8.22\t2.00",
			"2026-11-01T18:01:05Z\t4\theadroom\t8.50\t2.00",
			"2026-11-01T18:01:25Z\t4\theadroom\t8.50\t2.00",
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

	t.Run("without a look-ahead a spent window drops the bound to the drain floor until the reset", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"simulate", "-concurrency", "4", "-max", "40", "-look-ahead", "0s", "-input", "testdata/observations.jsonl"}, strings.NewReader(""), &stdout, &stderr)

		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		if code != 0 || len(lines) != 8 || lines[4] != "2026-11-01T18:00:59Z\t3\tdrain\t0.00\t2.00" || lines[5] != "2026-11-01T18:01:05Z\t2\tdrain\t8.50\t2.00" {
			t.Fatalf("exit = %d, stdout = %q, want the spent window held at the drain floor and the raise delayed past the reset", code, stdout.String())
		}
	})

	t.Run("fits the default look-ahead into a window shorter than it", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"simulate", "-concurrency", "4", "-window", "20s", "-input", "testdata/observations.jsonl"}, strings.NewReader(""), &stdout, &stderr)

		if code != 0 || stderr.Len() != 0 {
			t.Fatalf("exit = %d, stderr = %q, want the simulation to run with a 20s look-ahead", code, stderr.String())
		}
	})

	t.Run("refuses a look-ahead set beyond the window", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"simulate", "-window", "20s", "-look-ahead", "30s", "-input", "testdata/observations.jsonl"}, strings.NewReader(""), &stdout, &stderr)

		if code != 2 || !strings.Contains(stderr.String(), "look ahead") {
			t.Fatalf("exit = %d, stderr = %q, want a refusal naming the look-ahead", code, stderr.String())
		}
	})

	t.Run("steps up from the bound before the drain guard, as the scaler does", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := `{"at":"2026-11-01T18:00:00Z","requests":{"limit":600,"remaining":600,"reset_at":"2026-11-01T18:01:00Z"},"latency_ms":2000,"in_flight":40}` + "\n" +
			`{"at":"2026-11-01T18:00:30Z","requests":{"limit":1200,"remaining":1200,"reset_at":"2026-11-01T18:01:00Z"},"latency_ms":2000,"in_flight":0}` + "\n"

		code := run([]string{"simulate", "-concurrency", "4", "-max", "40"}, strings.NewReader(input), &stdout, &stderr)

		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		if code != 0 || len(lines) != 3 || !strings.HasPrefix(lines[1], "2026-11-01T18:00:00Z\t10\tdrain") || !strings.HasPrefix(lines[2], "2026-11-01T18:00:30Z\t6\tstep_up") {
			t.Fatalf("exit = %d, stdout = %q, want the drain floor of 10 and then a raise from the bound of 4 by two to 6", code, stdout.String())
		}
	})

	t.Run("fails with 1 when the output cannot be written", func(t *testing.T) {
		var stderr bytes.Buffer

		code := run([]string{"simulate", "-input", "testdata/observations.jsonl"}, strings.NewReader(""), failingWriter{}, &stderr)

		if code != 1 || !strings.Contains(stderr.String(), "write") {
			t.Fatalf("exit = %d, stderr = %q, want 1 naming the write", code, stderr.String())
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

	t.Run("applies an optional project token window as a second constraint", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := `{"at":"2026-11-01T18:00:00Z","requests":{"limit":1000,"remaining":900,"reset_at":"2026-11-01T18:00:30Z"},"tokens":{"limit":600000,"remaining":500000,"reset_at":"2026-11-01T18:00:30Z"},"project_tokens":{"limit":600000,"remaining":240000,"reset_at":"2026-11-01T18:00:30Z"},"tokens_per_call":1000,"latency_ms":2000}` + "\n"

		code := run([]string{"simulate", "-concurrency", "4", "-max", "40"}, strings.NewReader(input), &stdout, &stderr)

		if code != 0 {
			t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "2026-11-01T18:00:00Z\t3\theadroom\t6.80\t2.00") {
			t.Fatalf("stdout = %q, want the project window to bound the ceiling at 3", stdout.String())
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

	t.Run("rejects an observation with an unknown field", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := `{"at":"2026-11-01T18:00:00Z","token_per_call":1000}` + "\n"

		code := run([]string{"simulate"}, strings.NewReader(input), &stdout, &stderr)

		if code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), `"token_per_call"`) {
			t.Fatalf("stderr = %q, want it to name the unknown field", stderr.String())
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

	t.Run("names the line that exceeds the size limit", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		input := `{"at":"2026-11-01T18:00:00Z"}` + "\n" + strings.Repeat("x", 2*1024*1024) + "\n"

		code := run([]string{"simulate"}, strings.NewReader(input), &stdout, &stderr)

		if code != 1 {
			t.Fatalf("exit code = %d, want 1", code)
		}
		if !strings.Contains(stderr.String(), "line 2") {
			t.Fatalf("stderr = %q, want it to name line 2", stderr.String())
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

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("no space left on device")
}
