package plot

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type Point struct {
	At    time.Time
	Value float64
}

type Series []Point

type Run struct {
	Name   string
	Label  string
	Commit string
	Dirty  bool
	Start  time.Time
	End    time.Time
	Series map[string]Series
	Totals map[string]float64
}

func (r Run) Title() string {
	if r.Label == "" {
		return r.Name
	}
	return r.Name + " (" + r.Label + ")"
}

type response struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Value  []any   `json:"value"`
			Values [][]any `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func LoadRun(dir string) (Run, error) {
	run := Run{Name: filepath.Base(dir), Series: map[string]Series{}, Totals: map[string]float64{}}
	window, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		return Run{}, err
	}
	var bounds struct {
		Start  int64  `json:"start"`
		End    int64  `json:"end"`
		Label  string `json:"label"`
		Commit string `json:"commit"`
		Dirty  bool   `json:"dirty"`
	}
	if err := json.Unmarshal(window, &bounds); err != nil {
		return Run{}, fmt.Errorf("run.json: %w", err)
	}
	run.Start, run.End = time.Unix(bounds.Start, 0).UTC(), time.Unix(bounds.End, 0).UTC()
	run.Label, run.Commit, run.Dirty = bounds.Label, bounds.Commit, bounds.Dirty
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return Run{}, err
	}
	for _, file := range files {
		name := filepath.Base(file[:len(file)-len(".json")])
		if name == "run" {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return Run{}, err
		}
		var parsed response
		if err := json.Unmarshal(raw, &parsed); err != nil {
			return Run{}, fmt.Errorf("%s: %w", file, err)
		}
		if parsed.Status != "success" {
			return Run{}, fmt.Errorf("%s: status %q", file, parsed.Status)
		}
		switch parsed.Data.ResultType {
		case "matrix":
			series, err := matrix(parsed)
			if err != nil {
				return Run{}, fmt.Errorf("%s: %w", file, err)
			}
			run.Series[name] = series
		case "vector":
			total, err := vector(parsed)
			if err != nil {
				return Run{}, fmt.Errorf("%s: %w", file, err)
			}
			run.Totals[name] = total
		default:
			return Run{}, fmt.Errorf("%s: unsupported result type %q", file, parsed.Data.ResultType)
		}
	}
	return run, nil
}

func matrix(parsed response) (Series, error) {
	var series Series
	for _, result := range parsed.Data.Result {
		for _, pair := range result.Values {
			point, err := point(pair)
			if err != nil {
				return nil, err
			}
			if !math.IsNaN(point.Value) {
				series = append(series, point)
			}
		}
	}
	sort.Slice(series, func(i, j int) bool { return series[i].At.Before(series[j].At) })
	return series, nil
}

func vector(parsed response) (float64, error) {
	switch len(parsed.Data.Result) {
	case 0:
		return 0, nil
	case 1:
	default:
		return 0, fmt.Errorf("want one series, got %d", len(parsed.Data.Result))
	}
	point, err := point(parsed.Data.Result[0].Value)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(point.Value) {
		return 0, errors.New("value is not a number")
	}
	return point.Value, nil
}

func point(pair []any) (Point, error) {
	if len(pair) != 2 {
		return Point{}, errors.New("sample must be a timestamp and a value")
	}
	stamp, ok := pair[0].(float64)
	if !ok {
		return Point{}, errors.New("timestamp must be a number")
	}
	text, ok := pair[1].(string)
	if !ok {
		return Point{}, errors.New("value must be a string")
	}
	value, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return Point{}, err
	}
	return Point{At: time.Unix(0, int64(stamp*float64(time.Second))).UTC(), Value: value}, nil
}

func (s Series) Integral() float64 {
	total := 0.0
	for i := 1; i < len(s); i++ {
		dt := s[i].At.Sub(s[i-1].At).Seconds()
		total += (s[i].Value + s[i-1].Value) / 2 * dt
	}
	return total
}

func (s Series) Max() float64 {
	peak := 0.0
	for _, p := range s {
		peak = math.Max(peak, p.Value)
	}
	return peak
}

func (s Series) FirstAtOrBelow(threshold float64, after time.Time) (time.Time, bool) {
	for _, p := range s {
		if !p.At.Before(after) && p.Value <= threshold {
			return p.At, true
		}
	}
	return time.Time{}, false
}
