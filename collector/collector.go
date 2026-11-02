package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dimonchik03/quotaware/internal/headers"
	"github.com/dimonchik03/quotaware/internal/usage"
)

type Config struct {
	Endpoint        string
	Provider        string
	Token           string
	Client          *http.Client
	QueueSize       int
	PostTimeout     time.Duration
	CloseTimeout    time.Duration
	MaxCaptureBytes int64
	MaxRequestBytes int64
	Logger          *slog.Logger
}

func DefaultConfig(quotaware, provider string) Config {
	return Config{
		Endpoint:        strings.TrimSuffix(quotaware, "/") + usage.ObservePath,
		Provider:        provider,
		Client:          http.DefaultClient,
		QueueSize:       256,
		PostTimeout:     5 * time.Second,
		CloseTimeout:    10 * time.Second,
		MaxCaptureBytes: 4 << 20,
		MaxRequestBytes: 8 << 20,
		Logger:          slog.Default(),
	}
}

type Observation = usage.Observation

type Collector struct {
	cfg        Config
	path       string
	queue      chan Observation
	dropped    atomic.Int64
	failed     atomic.Int64
	unobserved atomic.Int64
	failing    atomic.Bool
	stop       chan struct{}
	wg         sync.WaitGroup
	closed     sync.Once
}

func New(cfg Config) (*Collector, error) {
	provider := headers.Provider(cfg.Provider)
	path := map[headers.Provider]string{headers.OpenAI: "/v1/chat/completions", headers.Anthropic: "/v1/messages"}[provider]
	switch {
	case cfg.Endpoint == "":
		return nil, errors.New("endpoint is required")
	case path == "":
		return nil, errors.New("provider must be openai or anthropic")
	case cfg.Client == nil || cfg.Logger == nil:
		return nil, errors.New("client and logger are required")
	case cfg.QueueSize <= 0 || cfg.MaxCaptureBytes <= 0 || cfg.MaxRequestBytes <= 0:
		return nil, errors.New("queue size and byte limits must be positive")
	case cfg.PostTimeout <= 0 || cfg.CloseTimeout <= 0:
		return nil, errors.New("post and close timeouts must be positive")
	}
	c := &Collector{cfg: cfg, path: path, queue: make(chan Observation, cfg.QueueSize), stop: make(chan struct{})}
	c.wg.Add(1)
	go c.deliver()
	return c, nil
}

func (c *Collector) Wrap(next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return roundTripper{collector: c, next: next}
}

func (c *Collector) Dropped() int64 {
	return c.dropped.Load()
}

func (c *Collector) Failed() int64 {
	return c.failed.Load()
}

func (c *Collector) Unobserved() int64 {
	return c.unobserved.Load()
}

func (c *Collector) Close() error {
	c.closed.Do(func() {
		close(c.stop)
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(c.cfg.CloseTimeout):
			c.cfg.Logger.Warn("quotaware collector closed with observations still queued", "queued", len(c.queue))
		}
	})
	return nil
}

func (c *Collector) enqueue(observation Observation) {
	select {
	case c.queue <- observation:
	default:
		c.dropped.Add(1)
	}
}

func (c *Collector) deliver() {
	defer c.wg.Done()
	deadline := time.Time{}
	for {
		select {
		case observation := <-c.queue:
			c.post(observation)
		case <-c.stop:
			deadline = time.Now().Add(c.cfg.CloseTimeout)
		}
		if deadline.IsZero() {
			continue
		}
		for {
			if time.Now().After(deadline) {
				return
			}
			select {
			case observation := <-c.queue:
				c.post(observation)
			default:
				return
			}
		}
	}
}

func (c *Collector) post(observation Observation) {
	body, err := json.Marshal(observation)
	if err != nil {
		c.failure(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.PostTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		c.failure(err)
		return
	}
	request.Header.Set("Content-Type", "application/json")
	if c.cfg.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	}
	response, err := c.cfg.Client.Do(request)
	if err != nil {
		c.failure(err)
		return
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode >= 300 {
		c.failure(fmt.Errorf("quotaware answered %d", response.StatusCode))
		return
	}
	if c.failing.CompareAndSwap(true, false) {
		c.cfg.Logger.Info("quotaware observations delivered again")
	}
}

func (c *Collector) failure(err error) {
	c.failed.Add(1)
	if c.failing.CompareAndSwap(false, true) {
		c.cfg.Logger.Warn("quotaware observations not delivered", "error", err.Error())
	}
}

type roundTripper struct {
	collector *Collector
	next      http.RoundTripper
}

func (r roundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	c := r.collector
	if request.Method != http.MethodPost || !strings.HasSuffix(request.URL.Path, c.path) || request.Body == nil {
		return r.next.RoundTrip(request)
	}
	prefix, err := io.ReadAll(io.LimitReader(request.Body, c.cfg.MaxRequestBytes+1))
	if err != nil {
		return nil, fmt.Errorf("quotaware collector: read request body: %w", err)
	}
	clone := request.Clone(request.Context())
	if int64(len(prefix)) > c.cfg.MaxRequestBytes {
		c.unobserved.Add(1)
		clone.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(prefix), request.Body), request.Body}
		return r.next.RoundTrip(clone)
	}
	_ = request.Body.Close()
	clone.Body = io.NopCloser(bytes.NewReader(prefix))
	clone.ContentLength = int64(len(prefix))
	var payload struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(prefix, &payload) != nil || payload.Model == "" {
		c.unobserved.Add(1)
		return r.next.RoundTrip(clone)
	}
	started := time.Now()
	response, err := r.next.RoundTrip(clone)
	if err != nil {
		c.unobserved.Add(1)
		return nil, err
	}
	observation := Observation{
		Provider:   c.cfg.Provider,
		Model:      payload.Model,
		Credential: usage.CredentialHash(request.Header),
		Status:     response.StatusCode,
		Headers:    rateLimitHeaders(response.Header),
	}
	contentType := response.Header.Get("Content-Type")
	encoding := response.Header.Get("Content-Encoding")
	succeeded := response.StatusCode >= 200 && response.StatusCode < 300
	response.Body = usage.NewCapture(response.Body, c.cfg.MaxCaptureBytes, func(captured []byte, complete bool) {
		observation.LatencyMS = float64(time.Since(started)) / float64(time.Millisecond)
		observation.Complete = complete
		if complete && succeeded {
			if decoded, ok := usage.Decompress(encoding, captured, c.cfg.MaxCaptureBytes); ok {
				if tokens, ok := usage.Extract(headers.Provider(c.cfg.Provider), contentType, decoded); ok {
					observation.Usage = &tokens
				}
			}
		}
		c.enqueue(observation)
	})
	return response, nil
}

func rateLimitHeaders(h http.Header) map[string]string {
	picked := map[string]string{}
	for name, values := range h {
		lower := strings.ToLower(name)
		if (strings.HasPrefix(lower, "x-ratelimit-") || strings.HasPrefix(lower, "anthropic-ratelimit-") || lower == "retry-after") && len(values) > 0 {
			picked[lower] = values[0]
		}
	}
	return picked
}
