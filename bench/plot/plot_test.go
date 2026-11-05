package plot_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/bench/plot"
)

var start = time.Date(2026, 11, 1, 18, 0, 0, 0, time.UTC)

func matrixJSON(t *testing.T, values [][2]float64) []byte {
	t.Helper()
	samples := make([][]any, len(values))
	for i, v := range values {
		samples[i] = []any{v[0], strconvFloat(v[1])}
	}
	raw, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []map[string]any{{"metric": map[string]string{}, "values": samples}}}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func strconvFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func vectorJSON(t *testing.T, value float64) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": []map[string]any{{"metric": map[string]string{}, "value": []any{float64(start.Unix()), strconvFloat(value)}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writeRun(t *testing.T, root, name string, replicas, retried, queue, multiplier, ceiling [][2]float64, totals map[string]float64) {
	t.Helper()
	writeRunAt(t, root, name, start, replicas, retried, queue, multiplier, ceiling, totals)
}

func shift(values [][2]float64, seconds float64) [][2]float64 {
	shifted := make([][2]float64, len(values))
	for i, v := range values {
		shifted[i] = [2]float64{v[0] + seconds, v[1]}
	}
	return shifted
}

func writeRunAt(t *testing.T, root, name string, runStart time.Time, replicas, retried, queue, multiplier, ceiling [][2]float64, totals map[string]float64) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(file string, raw []byte) {
		if err := os.WriteFile(filepath.Join(dir, file), raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("run.json", []byte(`{"start":`+strconvFloat(float64(runStart.Unix()))+`,"end":`+strconvFloat(float64(runStart.Add(4*time.Minute).Unix()))+`}`))
	write("replicas.json", matrixJSON(t, replicas))
	write("retries_per_minute.json", matrixJSON(t, retried))
	write("queue_length.json", matrixJSON(t, queue))
	write("multiplier.json", matrixJSON(t, multiplier))
	write("ceiling.json", matrixJSON(t, ceiling))
	for _, metric := range []string{"jobs_total", "completed_total", "failed_total", "abandoned_total", "retries_total", "rate_limited_total", "latency_p50", "latency_p95", "backoff_seconds_total"} {
		write(metric+".json", vectorJSON(t, totals[metric]))
	}
}

func at(seconds float64) float64 {
	return float64(start.Unix()) + seconds
}

func TestReport(t *testing.T) {
	root := t.TempDir()
	multiplier := [][2]float64{{at(0), 1}, {at(60), 1}, {at(120), 3}, {at(180), 0}, {at(240), 0}}
	writeRun(t, root, "A",
		[][2]float64{{at(0), 1}, {at(60), 4}, {at(120), 12}, {at(180), 12}, {at(240), 12}},
		[][2]float64{{at(0), 0}, {at(60), 0}, {at(120), 300}, {at(180), 200}, {at(240), 0}},
		[][2]float64{{at(0), 0}, {at(60), 10}, {at(120), 400}, {at(180), 300}, {at(240), 0}},
		multiplier,
		nil,
		map[string]float64{"jobs_total": 900, "completed_total": 900, "failed_total": 0, "rate_limited_total": 500, "retries_total": 500, "latency_p50": 12, "latency_p95": 60, "backoff_seconds_total": 840})
	later := 1800.0
	writeRunAt(t, root, "B", start.Add(30*time.Minute),
		shift([][2]float64{{at(0), 1}, {at(60), 4}, {at(120), 8}, {at(180), 8}, {at(240), 8}}, later),
		shift([][2]float64{{at(0), 0}, {at(60), 0}, {at(120), 0}, {at(180), 0}, {at(240), 0}}, later),
		shift([][2]float64{{at(0), 0}, {at(60), 10}, {at(120), 500}, {at(180), 200}, {at(240), 0}}, later),
		shift(multiplier, later),
		shift([][2]float64{{at(0), 1}, {at(60), 8}, {at(120), 8}, {at(180), 8}, {at(240), 8}}, later),
		map[string]float64{"jobs_total": 900, "completed_total": 900, "failed_total": 0, "rate_limited_total": 0, "retries_total": 0, "latency_p50": 15, "latency_p95": 40, "backoff_seconds_total": 0})
	baseline, err := plot.LoadRun(filepath.Join(root, "A"))
	if err != nil {
		t.Fatal(err)
	}
	capped, err := plot.LoadRun(filepath.Join(root, "B"))
	if err != nil {
		t.Fatal(err)
	}

	report := plot.Build(baseline, []plot.Run{capped}, 2)

	t.Run("loads series and totals from Prometheus responses", func(t *testing.T) {
		if len(baseline.Series["replicas"]) != 5 || baseline.Totals["rate_limited_total"] != 500 {
			t.Fatalf("run = %+v", baseline)
		}
	})

	t.Run("derives the phases from the load multiplier", func(t *testing.T) {
		if len(report.Phases) != 3 || report.Phases[1].Label != "3x quota" || report.Phases[2].Label != "idle" {
			t.Fatalf("phases = %+v", report.Phases)
		}
	})

	t.Run("summarises both runs in a table", func(t *testing.T) {
		table := report.Markdown()
		for _, want := range []string{"| Metric | A | B |", "| 429 responses | 500 | 0 |", "| Peak replicas | 12 | 8 |", "| Peak retries per minute | 300 | 0 |", "| Idle pod-seconds (in backoff) | 420 | 0 |", "| Job latency p95 | 60.0 s | 40.0 s |", "| Time to drain the burst | 60 s | 60 s |", "| Jobs unaccounted for | 0 | 0 |"} {
			if !strings.Contains(table, want) {
				t.Fatalf("table lacks %q:\n%s", want, table)
			}
		}
	})

	t.Run("integrates pod-seconds under the replica curve", func(t *testing.T) {
		if !strings.Contains(report.Markdown(), "| Pod-seconds | 2070 | 1470 |") {
			t.Fatalf("table:\n%s", report.Markdown())
		}
	})

	t.Run("lays out one column per capped run and names runs by their label", func(t *testing.T) {
		writeRunAt(t, root, "C", start.Add(60*time.Minute),
			shift([][2]float64{{at(0), 1}, {at(60), 5}, {at(120), 10}, {at(180), 10}, {at(240), 10}}, 3600),
			shift([][2]float64{{at(0), 0}, {at(60), 0}, {at(120), 20}, {at(180), 0}, {at(240), 0}}, 3600),
			shift([][2]float64{{at(0), 0}, {at(60), 10}, {at(120), 450}, {at(180), 100}, {at(240), 0}}, 3600),
			shift(multiplier, 3600),
			shift([][2]float64{{at(0), 1}, {at(60), 10}, {at(120), 10}, {at(180), 10}, {at(240), 10}}, 3600),
			map[string]float64{"jobs_total": 900, "completed_total": 880, "failed_total": 0, "abandoned_total": 5, "rate_limited_total": 40, "retries_total": 40, "latency_p50": 13, "latency_p95": 45, "backoff_seconds_total": 10})
		if err := os.WriteFile(filepath.Join(root, "C", "run.json"), []byte(`{"start":`+strconvFloat(float64(start.Add(60*time.Minute).Unix()))+`,"end":`+strconvFloat(float64(start.Add(64*time.Minute).Unix()))+`,"label":"safety 1","commit":"abc1234"}`), 0o644); err != nil {
			t.Fatal(err)
		}
		third, err := plot.LoadRun(filepath.Join(root, "C"))
		if err != nil {
			t.Fatal(err)
		}

		wide := plot.Build(baseline, []plot.Run{capped, third}, 2)

		if third.Commit != "abc1234" || third.Title() != "C (safety 1)" {
			t.Fatalf("run = %+v, want the commit and the label loaded", third)
		}
		table := wide.Markdown()
		for _, want := range []string{"| Metric | A | B | C (safety 1) |", "| 429 responses | 500 | 0 | 40 |", "| Jobs abandoned on shutdown | 0 | 0 | 5 |", "| Jobs unaccounted for | 0 | 0 | 15 |"} {
			if !strings.Contains(table, want) {
				t.Fatalf("table lacks %q:\n%s", want, table)
			}
		}
		if svg := wide.Charts()["replicas.svg"]; strings.Count(svg, "<path") != 5 {
			t.Fatalf("replica chart has %d lines, want replicas for three runs and ceilings for two", strings.Count(svg, "<path"))
		}
	})

	t.Run("writes three charts and the summary", func(t *testing.T) {
		out := t.TempDir()

		if err := report.Write(out); err != nil {
			t.Fatal(err)
		}

		for _, name := range []string{"replicas.svg", "retries.svg", "queue.svg", "summary.md"} {
			raw, err := os.ReadFile(filepath.Join(out, name))
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(name, ".svg") && (!strings.HasPrefix(string(raw), "<svg") || !strings.Contains(string(raw), "<path")) {
				t.Fatalf("%s is not a chart with lines", name)
			}
		}
	})
}

func TestDrainTimeRunsFromTheEndOfTheLastLoadedPhase(t *testing.T) {
	root := t.TempDir()
	writeRun(t, root, "A",
		[][2]float64{{at(0), 1}, {at(60), 4}, {at(120), 8}, {at(180), 8}, {at(240), 2}},
		[][2]float64{{at(0), 0}, {at(60), 0}, {at(120), 0}, {at(180), 0}, {at(240), 0}},
		[][2]float64{{at(0), 0}, {at(60), 10}, {at(120), 400}, {at(180), 300}, {at(240), 0}},
		[][2]float64{{at(0), 1}, {at(60), 1}, {at(120), 2}, {at(180), 0}, {at(240), 0}},
		nil,
		map[string]float64{"jobs_total": 10, "completed_total": 10})
	baseline, err := plot.LoadRun(filepath.Join(root, "A"))
	if err != nil {
		t.Fatal(err)
	}

	table := plot.Build(baseline, nil, 2).Markdown()

	if !strings.Contains(table, "| Time to drain the burst | 60 s |") {
		t.Fatalf("table:\n%s\nwant the drain measured from the end of the 2x phase", table)
	}
}

func TestChangingQuotaChartShowsMeasuredWindowAndOnlyLiveCeilings(t *testing.T) {
	series := func(v ...float64) plot.Series {
		var s plot.Series
		for i, n := range v {
			s = append(s, plot.Point{At: start.Add(time.Duration(i) * time.Minute), Value: n})
		}
		return s
	}
	baseline := plot.Run{Name: "A", Label: "queue only", Start: start, Series: map[string]plot.Series{"replicas": series(2, 20, 30), "provider_rpm": series(600, 180, 600), "multiplier": series(1.5, 1.5, 0)}}
	fixed := plot.Run{Name: "B", Label: "static maximum of 10", Start: start, Series: map[string]plot.Series{"replicas": series(2, 10, 10)}}
	adaptive := plot.Run{Name: "C", Label: "quotaware, safety 1", Start: start, Series: map[string]plot.Series{"replicas": series(2, 3, 8), "ceiling": series(3, 3, 8)}}
	report := plot.Build(baseline, []plot.Run{fixed, adaptive}, 2)
	svg := report.Charts()["replicas.svg"]
	for _, want := range []string{"180 RPM", "static maximum of 10", "quotaware, safety 1"} {
		if !strings.Contains(svg, want) {
			t.Fatalf("chart omits %q", want)
		}
	}
	if comparison := report.Charts()["static-vs-adaptive.svg"]; !strings.Contains(comparison, "Static cap of 10") || !strings.Contains(comparison, "180 RPM") {
		t.Fatal("missing direct comparison during the restricted quota window")
	}
	if strings.Contains(svg, "B ceiling") || strings.Contains(svg, "2x quota") {
		t.Fatal("static arm has a phantom ceiling or incorrect load label")
	}
}
