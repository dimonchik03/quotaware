package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dimonchik03/quotaware/internal/headroom"
)

type quotaLine struct {
	Limit     float64   `json:"limit"`
	Remaining float64   `json:"remaining"`
	ResetAt   time.Time `json:"reset_at"`
}

type observationLine struct {
	At            time.Time  `json:"at"`
	ObservedAt    *time.Time `json:"observed_at"`
	Requests      quotaLine  `json:"requests"`
	Tokens        quotaLine  `json:"tokens"`
	ProjectTokens quotaLine  `json:"project_tokens"`
	TokensPerCall float64    `json:"tokens_per_call"`
	LatencyMS     float64    `json:"latency_ms"`
	InFlight      int        `json:"in_flight"`
}

func (line observationLine) observation() headroom.Observation {
	observedAt := line.At
	if line.ObservedAt != nil {
		observedAt = *line.ObservedAt
	}
	return headroom.Observation{
		Requests:      headroom.Quota(line.Requests),
		Tokens:        headroom.Quota(line.Tokens),
		ProjectTokens: headroom.Quota(line.ProjectTokens),
		TokensPerCall: line.TokensPerCall,
		Latency:       time.Duration(line.LatencyMS * float64(time.Millisecond)),
		InFlight:      line.InFlight,
		ObservedAt:    observedAt,
	}
}

func runSimulate(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("quotaware simulate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	cfg := headroom.DefaultConfig()
	input := flags.String("input", "-", "JSONL file of observations, - for stdin")
	flags.IntVar(&cfg.Concurrency, "concurrency", cfg.Concurrency, "concurrent provider calls per worker pod")
	flags.Float64Var(&cfg.Safety, "safety", cfg.Safety, "fraction of the allowed rate to use")
	flags.Float64Var(&cfg.Share, "share", cfg.Share, "fraction of the organization quota this workload may use")
	flags.DurationVar(&cfg.Window, "window", cfg.Window, "rate-limit window of the provider")
	flags.IntVar(&cfg.MinReplicas, "min", cfg.MinReplicas, "minimum replica count")
	flags.IntVar(&cfg.MaxReplicas, "max", cfg.MaxReplicas, "maximum replica count")
	flags.IntVar(&cfg.MaxStepUp, "max-step-up", cfg.MaxStepUp, "largest raise of the ceiling per step interval")
	flags.DurationVar(&cfg.StepInterval, "step-interval", cfg.StepInterval, "shortest time between two raises of the ceiling")
	flags.DurationVar(&cfg.StaleAfter, "stale-after", cfg.StaleAfter, "age after which the last ceiling is frozen")
	flags.DurationVar(&cfg.StaleHard, "stale-hard", cfg.StaleHard, "age after which the default ceiling is reported")
	flags.IntVar(&cfg.DefaultCeiling, "default-ceiling", cfg.DefaultCeiling, "ceiling reported without usable observations")

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(stderr, "quotaware simulate: %v\n", err)
		return 2
	}
	source, err := openInput(*input, stdin)
	if err != nil {
		fmt.Fprintf(stderr, "quotaware simulate: %v\n", err)
		return 1
	}
	if err := simulate(cfg, source, stdout); err != nil {
		fmt.Fprintf(stderr, "quotaware simulate: %v\n", err)
		return 1
	}
	return 0
}

func openInput(path string, stdin io.Reader) (io.Reader, error) {
	if path == "-" {
		return stdin, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(raw), nil
}

func simulate(cfg headroom.Config, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	fmt.Fprintln(out, "at\tceiling\treason\tallowed_rate\tpod_rate")
	var previous headroom.Previous
	number := 0
	for scanner.Scan() {
		number++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		line, err := decodeObservation(raw)
		if err != nil {
			return fmt.Errorf("line %d: %w", number, err)
		}
		result := headroom.Compute(cfg, line.observation(), previous, line.At)
		fmt.Fprintf(out, "%s\t%d\t%s\t%.2f\t%.2f\n",
			line.At.Format(time.RFC3339), result.Ceiling, result.Reason, result.AllowedRate, result.PodRate)
		if !previous.Known || previous.Ceiling != result.Ceiling {
			previous.At = line.At
		}
		previous.Ceiling, previous.Known = result.Ceiling, true
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("line %d: %w", number+1, err)
	}
	return nil
}

func decodeObservation(raw []byte) (observationLine, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var line observationLine
	if err := decoder.Decode(&line); err != nil {
		return observationLine{}, err
	}
	if line.At.IsZero() {
		return observationLine{}, errors.New("missing at")
	}
	return line, nil
}
