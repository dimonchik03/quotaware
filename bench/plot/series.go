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
	Start  time.Time
	End    time.Time
	Series map[string]Series
	Totals map[string]float64
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
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	}
	if err := json.Unmarshal(window, &bounds); err != nil {
		return Run{}, fmt.Errorf("run.json: %w", err)
	}
	run.Start, run.End = time.Unix(bounds.Start, 0).UTC(), time.Unix(bounds.End, 0).UTC()
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
			series = append(series, point)
		}
	}
	sort.Slice(series, func(i, j int) bool { return series[i].At.Before(series[j].At) })
	return series, nil
}

func vector(parsed response) (float64, error) {
	if len(parsed.Data.Result) == 0 {
		return 0, nil
	}
	point, err := point(parsed.Data.Result[0].Value)
	if err != nil {
		return 0, err
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
	if math.IsNaN(value) {
		value = 0
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

func (s Series) FirstAbove(threshold float64, after time.Time) (time.Time, bool) {
	for _, p := range s {
		if !p.At.Before(after) && p.Value > threshold {
			return p.At, true
		}
	}
	return time.Time{}, false
}

func (s Series) FirstAtOrBelow(threshold float64, after time.Time) (time.Time, bool) {
	for _, p := range s {
		if !p.At.Before(after) && p.Value <= threshold {
			return p.At, true
		}
	}
	return time.Time{}, false
}

func (s Series) FirstDropAfter(after time.Time) (time.Time, bool) {
	for i := 1; i < len(s); i++ {
		if !s[i].At.Before(after) && s[i].Value < s[i-1].Value {
			return s[i].At, true
		}
	}
	return time.Time{}, false
}

func (s Series) LastAbove(threshold float64) (time.Time, bool) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i].Value > threshold {
			return s[i].At, true
		}
	}
	return time.Time{}, false
}
