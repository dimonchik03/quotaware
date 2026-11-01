package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"github.com/dimonchik03/quotaware/internal/headers"
	"github.com/dimonchik03/quotaware/internal/state"
)

type Config struct {
	OpenAI          *url.URL
	Anthropic       *url.URL
	MaxRequestBytes int64
	MaxCaptureBytes int64
}

func DefaultConfig() Config {
	return Config{MaxRequestBytes: 8 << 20, MaxCaptureBytes: 4 << 20}
}

type Proxy struct {
	cfg   Config
	store *state.Store
	now   func() time.Time
	mux   *http.ServeMux
}

var errTooLarge = errors.New("request body too large")

func New(cfg Config, store *state.Store, now func() time.Time) (*Proxy, error) {
	switch {
	case cfg.OpenAI == nil || cfg.Anthropic == nil:
		return nil, errors.New("both upstreams are required")
	case cfg.MaxRequestBytes <= 0 || cfg.MaxCaptureBytes <= 0:
		return nil, errors.New("request and capture limits must be positive")
	}
	p := &Proxy{cfg: cfg, store: store, now: now, mux: http.NewServeMux()}
	p.mux.Handle("POST /v1/chat/completions", p.route(headers.OpenAI, cfg.OpenAI))
	p.mux.Handle("POST /v1/messages", p.route(headers.Anthropic, cfg.Anthropic))
	p.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "quotaware proxies POST /v1/chat/completions and POST /v1/messages only")
	})
	return p, nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mux.ServeHTTP(w, r)
}

type callContextKey struct{}

type call struct {
	key      state.Key
	provider headers.Provider
	started  time.Time
	once     sync.Once
}

func (p *Proxy) route(provider headers.Provider, upstream *url.URL) http.Handler {
	reverse := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(upstream)
		},
		ModifyResponse: p.observe,
		ErrorHandler:   p.failed,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r, p.cfg.MaxRequestBytes)
		switch {
		case errors.Is(err, errTooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		case err != nil:
			writeError(w, http.StatusBadRequest, "unreadable request body")
			return
		}
		model, ok := modelOf(body)
		if !ok {
			writeError(w, http.StatusBadRequest, "model is required")
			return
		}
		c := &call{
			key:      state.Key{Provider: string(provider), Model: model, Key: credentialHash(r.Header)},
			provider: provider,
			started:  p.now(),
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		p.store.Begin(c.key)
		reverse.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), callContextKey{}, c)))
	})
}

func (p *Proxy) observe(response *http.Response) error {
	c, ok := response.Request.Context().Value(callContextKey{}).(*call)
	if !ok {
		return nil
	}
	now := p.now()
	limits, limitsErr := headers.Parse(c.provider, response.Header, now)
	retryAfter, retryErr := headers.RetryAfter(response.Header, now)
	if retryErr != nil {
		retryAfter = 0
	}
	contentType := response.Header.Get("Content-Type")
	status := response.StatusCode
	response.Body = &capture{
		ReadCloser: response.Body,
		limit:      p.cfg.MaxCaptureBytes,
		done: func(body []byte, complete bool) {
			c.once.Do(func() {
				p.store.End(c.key, p.sample(c, status, limits, limitsErr == nil, retryAfter, contentType, body, complete))
			})
		},
	}
	return nil
}

func (p *Proxy) sample(c *call, status int, limits headers.Limits, hasLimits bool, retryAfter time.Duration, contentType string, body []byte, complete bool) state.Sample {
	now := p.now()
	s := state.Sample{
		Status:     status,
		HasLimits:  hasLimits,
		Latency:    now.Sub(c.started),
		RetryAfter: retryAfter,
		At:         now,
	}
	if hasLimits {
		s.Requests = limits.Requests.Quota()
		s.Tokens = limits.Tokens.Quota()
		s.ProjectTokens = limits.ProjectTokens.Quota()
	}
	if complete && status >= 200 && status < 300 {
		s.Usage, s.HasUsage = extractUsage(c.provider, contentType, body)
	}
	return s
}

func (p *Proxy) failed(w http.ResponseWriter, r *http.Request, _ error) {
	if c, ok := r.Context().Value(callContextKey{}).(*call); ok {
		c.once.Do(func() {
			p.store.End(c.key, state.Sample{Status: 0})
		})
	}
	writeError(w, http.StatusBadGateway, "upstream unavailable")
}

func readBody(r *http.Request, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errTooLarge
	}
	return body, nil
}

func modelOf(body []byte) (string, bool) {
	var request struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &request); err != nil || request.Model == "" {
		return "", false
	}
	return request.Model, true
}

func credentialHash(h http.Header) string {
	credential := h.Get("Authorization")
	if credential == "" {
		credential = h.Get("x-api-key")
	}
	if credential == "" {
		return "anonymous"
	}
	sum := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(sum[:])[:12]
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": message, "type": "quotaware_proxy_error"},
	})
}

type capture struct {
	io.ReadCloser
	limit    int64
	buffer   bytes.Buffer
	overflow bool
	done     func(body []byte, complete bool)
	once     sync.Once
}

func (c *capture) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if n > 0 && !c.overflow {
		if int64(c.buffer.Len()+n) > c.limit {
			c.overflow = true
			c.buffer.Reset()
		} else {
			c.buffer.Write(p[:n])
		}
	}
	if errors.Is(err, io.EOF) {
		c.finish(true)
	}
	return n, err
}

func (c *capture) Close() error {
	err := c.ReadCloser.Close()
	c.finish(false)
	return err
}

func (c *capture) finish(complete bool) {
	c.once.Do(func() {
		if c.overflow {
			c.done(nil, false)
			return
		}
		c.done(c.buffer.Bytes(), complete)
	})
}
