package fakeprovider

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	accelerationWindow = 10 * time.Second
	maxRequestBytes    = 8 << 20
	charsPerToken      = 4
)

type Config struct {
	RequestsPerMinute int
	OpenAIWindowRPM   []int
	TokensPerMinute   int
	Latency           time.Duration
	Jitter            time.Duration
	OutputTokens      int
	Seed              int64
	Acceleration      bool
}

func DefaultConfig() Config {
	return Config{
		RequestsPerMinute: 60,
		TokensPerMinute:   100000,
		Latency:           1500 * time.Millisecond,
		Jitter:            300 * time.Millisecond,
		OutputTokens:      150,
		Seed:              1,
	}
}

func (c Config) Validate() error {
	for _, rpm := range c.OpenAIWindowRPM {
		if rpm <= 0 {
			return fmt.Errorf("scheduled requests per minute must be positive")
		}
	}
	switch {
	case c.RequestsPerMinute <= 0:
		return fmt.Errorf("requests per minute must be positive")
	case c.TokensPerMinute <= 0:
		return fmt.Errorf("tokens per minute must be positive")
	case c.Latency < 0 || c.Jitter < 0:
		return fmt.Errorf("latency and jitter must not be negative")
	case c.OutputTokens <= 0:
		return fmt.Errorf("output tokens must be positive")
	}
	return nil
}

type Server struct {
	cfg   Config
	now   func() time.Time
	sleep func(time.Duration)
	epoch time.Time
	mux   *http.ServeMux

	mu       sync.Mutex
	random   *rand.Rand
	windows  map[string]*fixedWindow
	buckets  map[string]*tokenBucket
	bursts   map[string]*burstTracker
	sequence int64
}

func New(cfg Config, now func() time.Time, sleep func(time.Duration)) *Server {
	s := &Server{
		cfg:     cfg,
		now:     now,
		sleep:   sleep,
		epoch:   now(),
		mux:     http.NewServeMux(),
		random:  rand.New(rand.NewSource(cfg.Seed)),
		windows: map[string]*fixedWindow{},
		buckets: map[string]*tokenBucket{},
		bursts:  map[string]*burstTracker{},
	}
	s.mux.HandleFunc("POST /v1/chat/completions", s.chatCompletions)
	s.mux.HandleFunc("POST /v1/messages", s.messages)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("no route for %s %s", r.Method, r.URL.Path)})
	})
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

type request struct {
	Model         string `json:"model"`
	Stream        bool   `json:"stream"`
	MaxTokens     int    `json:"max_tokens"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

type call struct {
	request  request
	input    int
	output   int
	decision decision
	id       int64
	words    []string
	now      time.Time
}

func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	c, ok := s.admit(w, r, func(model string) quotaSource {
		window, found := s.windows[model]
		if !found {
			window = &fixedWindow{epoch: s.epoch}
			if len(s.cfg.OpenAIWindowRPM) > 0 {
				window.epoch = s.now()
			}
			s.windows[model] = window
		}
		return window
	})
	if !ok {
		return
	}
	writeOpenAIHeaders(w.Header(), c.decision)
	if !c.decision.allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(c.decision.retryAfter/time.Second)))
		writeJSON(w, http.StatusTooManyRequests, openAIRateLimitError(c.request.Model))
		return
	}
	s.pause()
	if c.request.Stream {
		s.streamChatCompletion(w, c)
		return
	}
	writeJSON(w, http.StatusOK, chatCompletion(c))
}

func (s *Server) messages(w http.ResponseWriter, r *http.Request) {
	c, ok := s.admit(w, r, func(model string) quotaSource {
		bucket, found := s.buckets[model]
		if !found {
			bucket = &tokenBucket{}
			s.buckets[model] = bucket
		}
		return bucket
	})
	if !ok {
		return
	}
	writeAnthropicHeaders(w.Header(), c.decision, c.now)
	if !c.decision.allowed {
		w.Header().Set("retry-after", strconv.Itoa(int(c.decision.retryAfter/time.Second)))
		writeJSON(w, http.StatusTooManyRequests, anthropicRateLimitError())
		return
	}
	s.pause()
	if c.request.Stream {
		s.streamMessage(w, c)
		return
	}
	writeJSON(w, http.StatusOK, message(c))
}

type sourceLookup func(model string) quotaSource

func (s *Server) admit(w http.ResponseWriter, r *http.Request, lookup sourceLookup) (call, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unreadable body"})
		return call{}, false
	}
	if len(body) > maxRequestBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "request body too large"})
		return call{}, false
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil || req.Model == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "model is required"})
		return call{}, false
	}
	input := max(len(body)/charsPerToken, 1)
	output := s.cfg.OutputTokens
	if req.MaxTokens > 0 {
		output = min(output, req.MaxTokens)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	c := call{request: req, input: input, output: output, now: now}
	source := lookup(req.Model)
	if s.cfg.Acceleration {
		tracker, found := s.bursts[req.Model]
		if !found {
			tracker = &burstTracker{}
			s.bursts[req.Model] = tracker
		}
		if tracker.exceeds(s.cfg, now) {
			c.decision = source.peek(s.cfg, now)
			c.decision.retryAfter = time.Second
			return c, true
		}
		tracker.record(now)
	}
	c.decision = source.take(s.cfg, now, input+output)
	if c.decision.allowed {
		s.sequence++
		c.id = s.sequence
		c.words = s.generateWords(output)
	}
	return c, true
}

func (s *Server) pause() {
	s.sleep(s.latency())
}

func (s *Server) latency() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cfg.Jitter == 0 {
		return s.cfg.Latency
	}
	offset := time.Duration((s.random.Float64()*2 - 1) * float64(s.cfg.Jitter))
	return max(s.cfg.Latency+offset, 0)
}

var vocabulary = []string{"quota", "window", "token", "bucket", "reset", "ceiling", "replica", "latency", "header", "limit"}

func (s *Server) generateWords(count int) []string {
	words := make([]string, count)
	for i := range words {
		words[i] = vocabulary[s.random.Intn(len(vocabulary))]
	}
	return words
}

func writeOpenAIHeaders(h http.Header, d decision) {
	h.Set("x-ratelimit-limit-requests", strconv.Itoa(d.requests.limit))
	h.Set("x-ratelimit-remaining-requests", strconv.Itoa(max(d.requests.remaining, 0)))
	h.Set("x-ratelimit-reset-requests", d.requests.resetIn.Round(time.Millisecond).String())
	h.Set("x-ratelimit-limit-tokens", strconv.Itoa(d.tokens.limit))
	h.Set("x-ratelimit-remaining-tokens", strconv.Itoa(max(d.tokens.remaining, 0)))
	h.Set("x-ratelimit-reset-tokens", d.tokens.resetIn.Round(time.Millisecond).String())
}

func writeAnthropicHeaders(h http.Header, d decision, now time.Time) {
	h.Set("anthropic-ratelimit-requests-limit", strconv.Itoa(d.requests.limit))
	h.Set("anthropic-ratelimit-requests-remaining", strconv.Itoa(max(d.requests.remaining, 0)))
	h.Set("anthropic-ratelimit-requests-reset", now.Add(d.requests.resetIn).UTC().Format(time.RFC3339))
	h.Set("anthropic-ratelimit-tokens-limit", strconv.Itoa(d.tokens.limit))
	h.Set("anthropic-ratelimit-tokens-remaining", strconv.Itoa(roundToThousand(max(d.tokens.remaining, 0))))
	h.Set("anthropic-ratelimit-tokens-reset", now.Add(d.tokens.resetIn).UTC().Format(time.RFC3339))
}

func roundToThousand(n int) int {
	return int(math.Round(float64(n)/1000) * 1000)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(payload)
}
