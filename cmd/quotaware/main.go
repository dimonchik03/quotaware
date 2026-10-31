package main

import (
	"fmt"
	"io"
	"os"
)

const usage = "usage: quotaware <command>\n"

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	fmt.Fprintf(stderr, "quotaware: unknown command %q\n%s", args[0], usage)
	return 2
}
