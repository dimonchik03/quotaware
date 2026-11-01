package proxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
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
	MaxModelBytes   int
	Logger          *slog.Logger
}

func DefaultConfig() Config {
	return Config{MaxRequestBytes: 8 << 20, MaxCaptureBytes: 4 << 20, MaxModelBytes: 256, Logger: slog.Default()}
}

type Proxy struct {
	cfg       Config
	store     *state.Store
	now       func() time.Time
	transport http.RoundTripper
	mux       *http.ServeMux
}

var errTooLarge = errors.New("request body too large")

func New(cfg Config, store *state.Store, now func() time.Time) (*Proxy, error) {
	switch {
	case cfg.OpenAI == nil || cfg.Anthropic == nil:
		return nil, errors.New("both upstreams are required")
	case cfg.MaxRequestBytes <= 0 || cfg.MaxCaptureBytes <= 0 || cfg.MaxModelBytes <= 0:
		return nil, errors.New("request, capture and model limits must be positive")
	case cfg.Logger == nil:
		return nil, errors.New("logger is required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 100
	p := &Proxy{cfg: cfg, store: store, now: now, transport: transport, mux: http.NewServeMux()}
	p.mux.Handle("POST /v1/chat/completions", p.route(headers.OpenAI, cfg.OpenAI))
	p.mux.Handle("POST /v1/messages", p.route(headers.Anthropic, cfg.Anthropic))
	p.mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
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
		Transport:      p.transport,
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
		model, err := modelOf(body, p.cfg.MaxModelBytes)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		c := &call{
			key:      state.Key{Provider: string(provider), Model: model, Key: credentialHash(r.Header)},
			provider: provider,
			started:  p.now(),
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		p.store.Begin(c.key, c.started)
		reverse.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), callContextKey{}, c)))
	})
}

func (p *Proxy) observe(response *http.Response) error {
	c, ok := response.Request.Context().Value(callContextKey{}).(*call)
	if !ok {
		return nil
	}
	now := p.now()
	quotas, headersStatus := p.parseLimits(c, response.Header, now)
	retryAfter, err := headers.RetryAfter(response.Header, now)
	if err != nil {
		p.cfg.Logger.Warn("retry-after malformed", "provider", c.provider, "model", c.key.Model, "error", err.Error())
		retryAfter = 0
	}
	status := response.StatusCode
	contentType := response.Header.Get("Content-Type")
	encoding := response.Header.Get("Content-Encoding")
	response.Body = &capture{
		ReadCloser: response.Body,
		limit:      p.cfg.MaxCaptureBytes,
		done: func(body []byte, complete bool) {
			c.once.Do(func() {
				p.store.End(c.key, p.sample(c, status, headersStatus, quotas, retryAfter, contentType, encoding, body, complete))
			})
		},
	}
	return nil
}

func (p *Proxy) parseLimits(c *call, h http.Header, now time.Time) (state.Quotas, state.HeadersStatus) {
	limits, err := headers.Parse(c.provider, h, now)
	switch {
	case err == nil:
		return state.Quotas{
			Requests:      limits.Requests.Quota(),
			Tokens:        limits.Tokens.Quota(),
			ProjectTokens: limits.ProjectTokens.Quota(),
			InputTokens:   limits.InputTokens.Quota(),
			OutputTokens:  limits.OutputTokens.Quota(),
		}, state.HeadersPresent
	case errors.Is(err, headers.ErrMissing):
		return state.Quotas{}, state.HeadersMissing
	default:
		p.cfg.Logger.Warn("rate-limit headers malformed", "provider", c.provider, "model", c.key.Model, "error", err.Error())
		return state.Quotas{}, state.HeadersMalformed
	}
}

func (p *Proxy) sample(c *call, status int, headersStatus state.HeadersStatus, quotas state.Quotas, retryAfter time.Duration, contentType, encoding string, body []byte, complete bool) state.Sample {
	now := p.now()
	s := state.Sample{
		Status:     status,
		Complete:   complete,
		Headers:    headersStatus,
		Quotas:     quotas,
		Latency:    now.Sub(c.started),
		RetryAfter: retryAfter,
		At:         now,
	}
	if complete && status >= 200 && status < 300 {
		if decoded, ok := decompress(encoding, body, p.cfg.MaxCaptureBytes); ok {
			s.Usage, s.HasUsage = extractUsage(c.provider, contentType, decoded)
		}
	}
	return s
}

func (p *Proxy) failed(w http.ResponseWriter, r *http.Request, err error) {
	if c, ok := r.Context().Value(callContextKey{}).(*call); ok {
		c.once.Do(func() {
			p.store.Fail(c.key, p.now())
		})
		p.cfg.Logger.Error("upstream request failed", "provider", c.provider, "model", c.key.Model, "error", err.Error())
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

func modelOf(body []byte, limit int) (string, error) {
	var request struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &request); err != nil || request.Model == "" {
		return "", errors.New("model is required")
	}
	if len(request.Model) > limit {
		return "", errors.New("model is too long")
	}
	return request.Model, nil
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
		body := c.buffer.Bytes()
		if c.overflow {
			body = nil
		}
		c.done(body, complete)
	})
}
