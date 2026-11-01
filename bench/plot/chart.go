package plot

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type Line struct {
	Label  string
	Color  string
	Dashed bool
	Series Series
}

type Band struct {
	Label string
	From  time.Time
	To    time.Time
}

type Chart struct {
	Title  string
	YLabel string
	Origin time.Time
	Lines  []Line
	Bands  []Band
}

const (
	chartWidth  = 900
	chartHeight = 360
	marginLeft  = 64
	marginRight = 24
	marginTop   = 40
	marginBot   = 48
)

func (c Chart) SVG() string {
	plotWidth := float64(chartWidth - marginLeft - marginRight)
	plotHeight := float64(chartHeight - marginTop - marginBot)
	maxX, maxY := 1.0, 1.0
	for _, line := range c.Lines {
		for _, p := range line.Series {
			maxX = math.Max(maxX, p.At.Sub(c.Origin).Seconds())
			maxY = math.Max(maxY, p.Value)
		}
	}
	maxY = niceCeiling(maxY)
	x := func(at time.Time) float64 { return marginLeft + at.Sub(c.Origin).Seconds()/maxX*plotWidth }
	y := func(v float64) float64 { return marginTop + plotHeight - v/maxY*plotHeight }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" font-family="Helvetica, Arial, sans-serif" font-size="12">`+"\n", chartWidth, chartHeight, chartWidth, chartHeight)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="white"/>`+"\n", chartWidth, chartHeight)
	fmt.Fprintf(&b, `<text x="%d" y="22" font-size="16" font-weight="bold">%s</text>`+"\n", marginLeft, escape(c.Title))
	for _, band := range c.Bands {
		from, to := x(band.From), x(band.To)
		fmt.Fprintf(&b, `<rect x="%.1f" y="%d" width="%.1f" height="%.1f" fill="#f2f2f2"/>`+"\n", from, marginTop, math.Max(to-from, 0), plotHeight)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" fill="#777" font-size="11">%s</text>`+"\n", from+4, marginTop+14, escape(band.Label))
	}
	for _, tick := range ticks(maxY) {
		fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#ddd"/>`+"\n", marginLeft, y(tick), chartWidth-marginRight, y(tick))
		fmt.Fprintf(&b, `<text x="%d" y="%.1f" text-anchor="end" fill="#555">%s</text>`+"\n", marginLeft-8, y(tick)+4, formatTick(tick))
	}
	for minute := 0.0; minute <= maxX/60; minute += minuteStep(maxX / 60) {
		at := c.Origin.Add(time.Duration(minute * float64(time.Minute)))
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%d" x2="%.1f" y2="%.1f" stroke="#eee"/>`+"\n", x(at), marginTop, x(at), marginTop+plotHeight)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" text-anchor="middle" fill="#555">%.0f</text>`+"\n", x(at), chartHeight-marginBot+18, minute)
	}
	fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%.1f" stroke="#333"/>`+"\n", marginLeft, marginTop, marginLeft, marginTop+plotHeight)
	fmt.Fprintf(&b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#333"/>`+"\n", marginLeft, marginTop+plotHeight, chartWidth-marginRight, marginTop+plotHeight)
	fmt.Fprintf(&b, `<text x="%d" y="%d" text-anchor="middle" fill="#333">minutes</text>`+"\n", marginLeft+int(plotWidth/2), chartHeight-marginBot+36)
	fmt.Fprintf(&b, `<text transform="translate(16 %d) rotate(-90)" text-anchor="middle" fill="#333">%s</text>`+"\n", marginTop+int(plotHeight/2), escape(c.YLabel))
	for _, line := range c.Lines {
		if len(line.Series) == 0 {
			continue
		}
		var path strings.Builder
		for i, p := range line.Series {
			verb := "L"
			if i == 0 {
				verb = "M"
			}
			fmt.Fprintf(&path, "%s%.1f %.1f ", verb, x(p.At), y(p.Value))
		}
		dash := ""
		if line.Dashed {
			dash = ` stroke-dasharray="6 4"`
		}
		fmt.Fprintf(&b, `<path d="%s" fill="none" stroke="%s" stroke-width="2"%s/>`+"\n", strings.TrimSpace(path.String()), line.Color, dash)
	}
	legendX := chartWidth - marginRight - 180
	for i, line := range c.Lines {
		yPos := marginTop + 12 + i*16
		dash := ""
		if line.Dashed {
			dash = ` stroke-dasharray="6 4"`
		}
		fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="%s" stroke-width="2"%s/>`+"\n", legendX, yPos, legendX+24, yPos, line.Color, dash)
		fmt.Fprintf(&b, `<text x="%d" y="%d" fill="#333">%s</text>`+"\n", legendX+30, yPos+4, escape(line.Label))
	}
	b.WriteString("</svg>\n")
	return b.String()
}

func niceCeiling(v float64) float64 {
	if v <= 0 {
		return 1
	}
	magnitude := math.Pow(10, math.Floor(math.Log10(v)))
	for _, factor := range []float64{1, 2, 2.5, 5, 10} {
		if v <= factor*magnitude {
			return factor * magnitude
		}
	}
	return 10 * magnitude
}

func ticks(maxY float64) []float64 {
	step := maxY / 5
	var result []float64
	for v := 0.0; v <= maxY+step/2; v += step {
		result = append(result, v)
	}
	return result
}

func formatTick(v float64) string {
	if v == math.Trunc(v) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func minuteStep(totalMinutes float64) float64 {
	switch {
	case totalMinutes <= 6:
		return 1
	case totalMinutes <= 15:
		return 2
	case totalMinutes <= 40:
		return 5
	default:
		return 10
	}
}

func escape(s string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return replacer.Replace(s)
}
