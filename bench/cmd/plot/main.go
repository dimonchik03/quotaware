package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dimonchik03/quotaware/bench/plot"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("plot", flag.ContinueOnError)
	results := flags.String("results", "", "directory with the A and B run exports")
	concurrency := flags.Int("concurrency", 2, "concurrent calls per worker pod, for idle pod-seconds")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *results == "" {
		fmt.Fprintln(os.Stderr, "plot: -results is required")
		return 2
	}
	baseline, err := plot.LoadRun(filepath.Join(*results, "A"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "plot: %v\n", err)
		return 1
	}
	capped, err := plot.LoadRun(filepath.Join(*results, "B"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "plot: %v\n", err)
		return 1
	}
	report := plot.Build(baseline, capped, *concurrency)
	if err := report.Write(*results); err != nil {
		fmt.Fprintf(os.Stderr, "plot: %v\n", err)
		return 1
	}
	fmt.Print(report.Markdown())
	return 0
}
