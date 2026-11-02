package plot

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Report struct {
	Baseline Run
	Capped   Run
	Phases   []Band
	Rows     []Row
}

type Row struct {
	Metric   string
	Baseline string
	Capped   string
}

func Build(baseline, capped Run, concurrency int) Report {
	report := Report{Baseline: baseline, Capped: capped, Phases: phases(baseline)}
	measure := func(metric string, f func(Run) string) {
		report.Rows = append(report.Rows, Row{Metric: metric, Baseline: f(baseline), Capped: f(capped)})
	}
	measure("Jobs produced", func(r Run) string { return count(r.Totals["jobs_total"]) })
	measure("Jobs completed", func(r Run) string { return count(r.Totals["completed_total"]) })
	measure("Jobs failed", func(r Run) string { return count(r.Totals["failed_total"]) })
	measure("429 responses", func(r Run) string { return count(r.Totals["rate_limited_total"]) })
	measure("Retries", func(r Run) string { return count(r.Totals["retries_total"]) })
	measure("Peak 429 per minute", func(r Run) string { return fmt.Sprintf("%.0f", r.Series["rate_limited_per_minute"].Max()) })
	measure("Job latency p50", func(r Run) string { return seconds(r.Totals["latency_p50"]) })
	measure("Job latency p95", func(r Run) string { return seconds(r.Totals["latency_p95"]) })
	measure("Peak replicas", func(r Run) string { return fmt.Sprintf("%.0f", r.Series["replicas"].Max()) })
	measure("Pod-seconds", func(r Run) string { return fmt.Sprintf("%.0f", r.Series["replicas"].Integral()) })
	measure("Idle pod-seconds (in backoff)", func(r Run) string {
		return fmt.Sprintf("%.0f", r.Totals["backoff_seconds_total"]/float64(max(concurrency, 1)))
	})
	measure("Time to drain the burst", func(r Run) string { return drainTime(r, phases(r)) })
	measure("Time to react after the first 429", func(r Run) string { return reactionTime(r) })
	return report
}

func phases(run Run) []Band {
	multiplier := run.Series["multiplier"]
	var bands []Band
	for i, p := range multiplier {
		label := phaseLabel(p.Value)
		if len(bands) > 0 && bands[len(bands)-1].Label == label {
			bands[len(bands)-1].To = p.At
			continue
		}
		if i > 0 {
			bands[len(bands)-1].To = p.At
		}
		bands = append(bands, Band{Label: label, From: p.At, To: p.At})
	}
	return bands
}

func phaseLabel(multiplier float64) string {
	switch {
	case multiplier == 0:
		return "idle"
	case multiplier < 1:
		return fmt.Sprintf("%.1fx quota", multiplier)
	default:
		return fmt.Sprintf("%.0fx quota", multiplier)
	}
}

func drainTime(run Run, bands []Band) string {
	var burstEnd time.Time
	for _, band := range bands {
		if strings.HasPrefix(band.Label, "3x") || (burstEnd.IsZero() && band.Label != "idle" && band.To.After(burstEnd)) {
			burstEnd = band.To
		}
	}
	if burstEnd.IsZero() {
		return "n/a"
	}
	drained, ok := run.Series["queue_length"].FirstAtOrBelow(0, burstEnd)
	if !ok {
		return "not drained"
	}
	return duration(drained.Sub(burstEnd))
}

func reactionTime(run Run) string {
	if len(run.Series["ceiling"]) == 0 {
		return "n/a (no ceiling)"
	}
	first, ok := run.Series["rate_limited_per_minute"].FirstAbove(0, run.Start)
	if !ok {
		return "no 429 observed"
	}
	drop, ok := run.Series["replicas"].FirstDropAfter(first)
	if !ok {
		return "never scaled down"
	}
	return duration(drop.Sub(first))
}

func count(v float64) string {
	return fmt.Sprintf("%.0f", v)
}

func seconds(v float64) string {
	if math.IsNaN(v) || v == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1f s", v)
}

func duration(d time.Duration) string {
	return fmt.Sprintf("%.0f s", d.Seconds())
}

func (r Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "| Metric | %s (queue trigger) | %s (queue trigger + quotaware) |\n", r.Baseline.Name, r.Capped.Name)
	b.WriteString("|---|---|---|\n")
	for _, row := range r.Rows {
		fmt.Fprintf(&b, "| %s | %s | %s |\n", row.Metric, row.Baseline, row.Capped)
	}
	return b.String()
}

func (r Report) Charts() map[string]string {
	origin := r.Baseline.Start
	shifted := func(s Series, from time.Time) Series {
		out := make(Series, len(s))
		for i, p := range s {
			out[i] = Point{At: origin.Add(p.At.Sub(from)), Value: p.Value}
		}
		return out
	}
	bands := make([]Band, len(r.Phases))
	for i, band := range r.Phases {
		bands[i] = Band{Label: band.Label, From: origin.Add(band.From.Sub(r.Baseline.Start)), To: origin.Add(band.To.Sub(r.Baseline.Start))}
	}
	replicas := Chart{Title: "Worker replicas", YLabel: "pods", Origin: origin, Bands: bands, Lines: []Line{
		{Label: r.Baseline.Name + " replicas", Color: "#c0392b", Series: shifted(r.Baseline.Series["replicas"], r.Baseline.Start)},
		{Label: r.Capped.Name + " replicas", Color: "#2471a3", Series: shifted(r.Capped.Series["replicas"], r.Capped.Start)},
		{Label: r.Capped.Name + " ceiling", Color: "#2471a3", Dashed: true, Series: shifted(r.Capped.Series["ceiling"], r.Capped.Start)},
	}}
	limits := Chart{Title: "429 responses per minute", YLabel: "429 / min", Origin: origin, Bands: bands, Lines: []Line{
		{Label: r.Baseline.Name, Color: "#c0392b", Series: shifted(r.Baseline.Series["rate_limited_per_minute"], r.Baseline.Start)},
		{Label: r.Capped.Name, Color: "#2471a3", Series: shifted(r.Capped.Series["rate_limited_per_minute"], r.Capped.Start)},
	}}
	queue := Chart{Title: "Queue length", YLabel: "jobs", Origin: origin, Bands: bands, Lines: []Line{
		{Label: r.Baseline.Name, Color: "#c0392b", Series: shifted(r.Baseline.Series["queue_length"], r.Baseline.Start)},
		{Label: r.Capped.Name, Color: "#2471a3", Series: shifted(r.Capped.Series["queue_length"], r.Capped.Start)},
	}}
	return map[string]string{"replicas.svg": replicas.SVG(), "rate_limits.svg": limits.SVG(), "queue.svg": queue.SVG()}
}

func (r Report) Write(dir string) error {
	for name, svg := range r.Charts() {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(svg), 0o644); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(r.Markdown()), 0o644)
}
