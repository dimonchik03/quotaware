package main

import (
	"fmt"
	"io"
	"os"
)

const usage = `usage: quotaware <command> [flags]

commands:
  all        run the proxy and the KEDA external scaler in one process
  proxy      observe provider rate-limit headers in front of OpenAI and Anthropic
  simulate   replay a JSONL file of observations and print the replica ceiling per line
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "all":
		return runAll(args[1:], stderr)
	case "proxy":
		return runProxy(args[1:], stderr)
	case "simulate":
		return runSimulate(args[1:], stdin, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "quotaware: unknown command %q\n%s", args[0], usage)
		return 2
	}
}
