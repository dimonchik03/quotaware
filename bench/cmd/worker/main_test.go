package main

import (
	"bytes"
	"os"
	"os/exec"
	"testing"
)

func TestHelpDoesNotPrintTheEnvironmentCredential(t *testing.T) {
	if os.Getenv("QUOTAWARE_WORKER_HELP_TEST") == "1" {
		os.Exit(run([]string{"-h"}))
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelpDoesNotPrintTheEnvironmentCredential$")
	cmd.Env = append(os.Environ(), "QUOTAWARE_WORKER_HELP_TEST=1", "API_KEY=private-worker-test-key")
	out, err := cmd.CombinedOutput()
	if err != nil || !bytes.Contains(out, []byte("-api-key")) || bytes.Contains(out, []byte("private-worker-test-key")) {
		t.Fatalf("help output incorrect, exit=%v", err)
	}
}
