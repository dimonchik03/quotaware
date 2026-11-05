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
	retryLabel, peakLabel, backoffLabel, drainLabel := "Retries", "Peak retries per minute", "Idle pod-seconds (in backoff)", "Time to drain the burst"
	if len(baseline.Series["provider_rpm"]) > 0 {
		retryLabel = "Retries scheduled"
		peakLabel = "Peak scheduled retries per minute"
		backoffLabel = "Scheduled backoff (pod-seconds)"
		drainLabel = "Time from last load phase to empty queue"
	}
	measure(retryLabel, func(r Run) string { return count(r.Totals["retries_total"]) })
	measure(peakLabel, func(r Run) string { return fmt.Sprintf("%.0f", r.Series["retries_per_minute"].Max()) })
	measure("Job latency p50", func(r Run) string { return seconds(r.Totals["latency_p50"]) })
	measure("Job latency p95", func(r Run) string { return seconds(r.Totals["latency_p95"]) })
	measure("Peak replicas", func(r Run) string { return fmt.Sprintf("%.0f", r.Series["replicas"].Max()) })
	measure("Pod-seconds", func(r Run) string { return fmt.Sprintf("%.0f", r.Series["replicas"].Integral()) })
	measure(backoffLabel, func(r Run) string {
		return fmt.Sprintf("%.0f", r.Totals["backoff_seconds_total"]/float64(max(concurrency, 1)))
	})
	measure(drainLabel, func(r Run) string { return drainTime(r, phases(r)) })
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
		return fmt.Sprintf("%gx quota", multiplier)
	}
}

func drainTime(run Run, bands []Band) string {
	var burstEnd time.Time
	for _, band := range bands {
		if band.Label != "idle" && band.To.After(burstEnd) {
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
	changing := len(r.Baseline.Series["provider_rpm"]) > 0
	if changing {
		bands = nil
		for _, point := range r.Baseline.Series["provider_rpm"] {
			label := fmt.Sprintf("%.0f RPM", point.Value)
			if len(bands) > 0 {
				bands[len(bands)-1].To = point.At
				if bands[len(bands)-1].Label == label {
					continue
				}
			}
			bands = append(bands, Band{Label: label, From: point.At, To: point.At})
		}
	}
	var replicaLines, retryLines, queueLines []Line
	for i, run := range r.runs() {
		color := palette[i%len(palette)]
		name := run.Name
		if changing {
			name = run.Label
		}
		replicaLines = append(replicaLines, Line{Label: name, Color: color, Series: shifted(run.Series["replicas"], run.Start)})
		if i > 0 && len(run.Series["ceiling"]) > 0 {
			replicaLines = append(replicaLines, Line{Label: name + " ceiling", Color: color, Dashed: true, Series: shifted(run.Series["ceiling"], run.Start)})
		}
		retryLines = append(retryLines, Line{Label: name, Color: color, Series: shifted(run.Series["retries_per_minute"], run.Start)})
		queueLines = append(queueLines, Line{Label: name, Color: color, Series: shifted(run.Series["queue_length"], run.Start)})
	}
	replicas := Chart{Title: "Worker replicas", YLabel: "pods", Origin: origin, Bands: bands, Lines: replicaLines}
	retries := Chart{Title: "Retries scheduled per minute", YLabel: "retries / min", Origin: origin, Bands: bands, Lines: retryLines}
	queue := Chart{Title: "Queue length", YLabel: "jobs", Origin: origin, Bands: bands, Lines: queueLines}
	charts := map[string]string{"replicas.svg": replicas.SVG(), "retries.svg": retries.SVG(), "queue.svg": queue.SVG()}
	if changing && len(r.Capped) == 2 {
		static, adaptive := r.Capped[0], r.Capped[1]
		var constrained []Band
		nominal := fmt.Sprintf("%.0f RPM", r.Baseline.Series["provider_rpm"].Max())
		for _, band := range bands {
			if band.Label != nominal {
				constrained = append(constrained, band)
			}
		}
		chart := Chart{Title: "Static and adaptive worker counts as provider quota changes", YLabel: "worker pods", Origin: origin, Bands: constrained,
			Lines: []Line{{Label: "Static cap of 10", Color: palette[1], Series: shifted(static.Series["replicas"], static.Start)}, {Label: "Quotaware", Color: palette[2], Series: shifted(adaptive.Series["replicas"], adaptive.Start)}}}
		charts["static-vs-adaptive.svg"] = chart.SVG()
	}
	return charts
}

func (r Report) Write(dir string) error {
	for name, svg := range r.Charts() {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(svg), 0o644); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(r.Markdown()), 0o644)
}
