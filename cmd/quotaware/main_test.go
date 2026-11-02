package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	t.Run("prints usage and exits with 2 when no command is given", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run(nil, strings.NewReader(""), &stdout, &stderr)

		if code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if stderr.String() != usageText {
			t.Fatalf("stderr = %q, want %q", stderr.String(), usageText)
		}
	})

	t.Run("prints the build version", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"version"}, strings.NewReader(""), &stdout, &stderr)

		if code != 0 || strings.TrimSpace(stdout.String()) != "dev" {
			t.Fatalf("exit = %d stdout = %q, want 0 and the dev version", code, stdout.String())
		}
	})

	t.Run("names the unknown command and exits with 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer

		code := run([]string{"launch"}, strings.NewReader(""), &stdout, &stderr)

		if code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if !strings.Contains(stderr.String(), `unknown command "launch"`) {
			t.Fatalf("stderr = %q, want it to name the unknown command", stderr.String())
		}
		if !strings.HasSuffix(stderr.String(), usageText) {
			t.Fatalf("stderr = %q, want it to end with usage", stderr.String())
		}
	})
}
