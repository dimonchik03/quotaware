package plot_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dimonchik03/quotaware/bench/plot"
)

func runDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	files["run.json"] = `{"start":` + strconvFloat(float64(start.Unix())) + `,"end":` + strconvFloat(float64(start.Add(time.Minute).Unix())) + `}`
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestLoadRun(t *testing.T) {
	t.Run("leaves a gap where Prometheus has no value instead of a zero", func(t *testing.T) {
		dir := runDir(t, map[string]string{
			"latency_p95_over_time.json": `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1793556000,"NaN"],[1793556015,"12.5"]]}]}}`,
		})

		run, err := plot.LoadRun(dir)

		if err != nil || len(run.Series["latency_p95_over_time"]) != 1 || run.Series["latency_p95_over_time"][0].Value != 12.5 {
			t.Fatalf("series = %+v err = %v, want only the sample that has a value", run.Series["latency_p95_over_time"], err)
		}
	})

	t.Run("refuses a total that is not a number", func(t *testing.T) {
		dir := runDir(t, map[string]string{
			"latency_p95.json": `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1793556000,"NaN"]}]}}`,
		})

		_, err := plot.LoadRun(dir)

		if err == nil || !strings.Contains(err.Error(), "latency_p95") {
			t.Fatalf("err = %v, want a refusal naming the file", err)
		}
	})

	t.Run("refuses a total made of several series", func(t *testing.T) {
		dir := runDir(t, map[string]string{
			"jobs_total.json": `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"pod":"a"},"value":[1793556000,"3"]},{"metric":{"pod":"b"},"value":[1793556000,"4"]}]}}`,
		})

		_, err := plot.LoadRun(dir)

		if err == nil || !strings.Contains(err.Error(), "jobs_total") {
			t.Fatalf("err = %v, want a refusal naming the file", err)
		}
	})
}
