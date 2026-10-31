package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	t.Run("prints usage and exits with 2 when no command is given", func(t *testing.T) {
		var stderr bytes.Buffer

		code := run(nil, &stderr)

		if code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if stderr.String() != usage {
			t.Fatalf("stderr = %q, want %q", stderr.String(), usage)
		}
	})

	t.Run("names the unknown command and exits with 2", func(t *testing.T) {
		var stderr bytes.Buffer

		code := run([]string{"launch"}, &stderr)

		if code != 2 {
			t.Fatalf("exit code = %d, want 2", code)
		}
		if !strings.Contains(stderr.String(), `unknown command "launch"`) {
			t.Fatalf("stderr = %q, want it to name the unknown command", stderr.String())
		}
		if !strings.HasSuffix(stderr.String(), usage) {
			t.Fatalf("stderr = %q, want it to end with usage", stderr.String())
		}
	})
}
