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
	Capped   []Run
	Phases   []Band
	Rows     []Row
}

type Row struct {
	Metric string
	Values []string
}

var palette = []string{"#c0392b", "#2471a3", "#1e8449", "#7d3c98", "#b9770e"}

func Build(baseline Run, capped []Run, concurrency int) Report {
	report := Report{Baseline: baseline, Capped: capped, Phases: phases(baseline)}
	runs := report.runs()
	measure := func(metric string, f func(Run) string) {
		row := Row{Metric: metric}
		for _, run := range runs {
			row.Values = append(row.Values, f(run))
		}
		report.Rows = append(report.Rows, row)
	}
	measure("Jobs produced", func(r Run) string { return count(r.Totals["jobs_total"]) })
	measure("Jobs completed", func(r Run) string { return count(r.Totals["completed_total"]) })
	measure("Jobs failed", func(r Run) string { return count(r.Totals["failed_total"]) })
	measure("Jobs abandoned on shutdown", func(r Run) string { return count(r.Totals["abandoned_total"]) })
	measure("Jobs unaccounted for", func(r Run) string {
		return count(r.Totals["jobs_total"] - r.Totals["completed_total"] - r.Totals["failed_total"] - r.Totals["abandoned_total"])
	})
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
	return report
}

func (r Report) runs() []Run {
	return append([]Run{r.Baseline}, r.Capped...)
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
	b.WriteString("| Metric |")
	for _, run := range r.runs() {
		fmt.Fprintf(&b, " %s |", run.Title())
	}
	b.WriteString("\n|---|")
	for range r.runs() {
		b.WriteString("---|")
	}
	b.WriteString("\n")
	for _, row := range r.Rows {
		fmt.Fprintf(&b, "| %s |", row.Metric)
		for _, value := range row.Values {
			fmt.Fprintf(&b, " %s |", value)
		}
		b.WriteString("\n")
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
	var replicaLines, limitLines, queueLines []Line
	for i, run := range r.runs() {
		color := palette[i%len(palette)]
		replicaLines = append(replicaLines, Line{Label: run.Name + " replicas", Color: color, Series: shifted(run.Series["replicas"], run.Start)})
		if i > 0 {
			replicaLines = append(replicaLines, Line{Label: run.Name + " ceiling", Color: color, Dashed: true, Series: shifted(run.Series["ceiling"], run.Start)})
		}
		limitLines = append(limitLines, Line{Label: run.Name, Color: color, Series: shifted(run.Series["rate_limited_per_minute"], run.Start)})
		queueLines = append(queueLines, Line{Label: run.Name, Color: color, Series: shifted(run.Series["queue_length"], run.Start)})
	}
	replicas := Chart{Title: "Worker replicas", YLabel: "pods", Origin: origin, Bands: bands, Lines: replicaLines}
	limits := Chart{Title: "429 responses per minute", YLabel: "429 / min", Origin: origin, Bands: bands, Lines: limitLines}
	queue := Chart{Title: "Queue length", YLabel: "jobs", Origin: origin, Bands: bands, Lines: queueLines}
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
