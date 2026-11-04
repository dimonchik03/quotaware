package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/dimonchik03/quotaware/internal/headers"
	"github.com/dimonchik03/quotaware/internal/state"
	"github.com/dimonchik03/quotaware/internal/usage"
)

type Config struct {
	OpenAI          *url.URL
	Anthropic       *url.URL
	MaxRequestBytes int64
	MaxCaptureBytes int64
	MaxModelBytes   int
	ReadBodyTimeout time.Duration
	Transport       http.RoundTripper
	Logger          *slog.Logger
}

func DefaultConfig() Config {
	return Config{MaxRequestBytes: 8 << 20, MaxCaptureBytes: 4 << 20, MaxModelBytes: usage.MaxModelBytes, ReadBodyTimeout: 30 * time.Second, Logger: slog.Default()}
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
	case cfg.ReadBodyTimeout <= 0:
		return nil, errors.New("read body timeout must be positive")
	case cfg.Logger == nil:
		return nil, errors.New("logger is required")
	}
	transport := cfg.Transport
	if transport == nil {
		pooled := http.DefaultTransport.(*http.Transport).Clone()
		pooled.MaxIdleConnsPerHost = 100
		transport = pooled
	}
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
		body, err := readBodyWithin(w, r, p.cfg.MaxRequestBytes, p.cfg.ReadBodyTimeout)
		switch {
		case errors.Is(err, errTooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		case errors.Is(err, os.ErrDeadlineExceeded):
			writeError(w, http.StatusRequestTimeout, "request body not received in time")
			return
		case errors.Is(err, errUnboundedRead):
			writeError(w, http.StatusInternalServerError, err.Error())
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
			key:      state.Key{Provider: string(provider), Model: model, Key: usage.CredentialHash(r.Header)},
			provider: provider,
			started:  p.now(),
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
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
	quotas, headersStatus := parseLimits(p.cfg.Logger, c.provider, c.key.Model, response.Header, now)
	retryAfter, err := headers.RetryAfter(response.Header, now)
	if err != nil {
		p.cfg.Logger.Warn("retry-after malformed", "provider", c.provider, "model", c.key.Model, "error", err.Error())
		retryAfter, headersStatus = 0, state.HeadersMalformed
	}
	status := response.StatusCode
	contentType := response.Header.Get("Content-Type")
	encoding := response.Header.Get("Content-Encoding")
	response.Body = usage.NewCapture(response.Body, p.cfg.MaxCaptureBytes, func(body []byte, outcome usage.Outcome) {
		c.once.Do(func() {
			p.store.End(c.key, p.sample(c, status, headersStatus, quotas, retryAfter, now, contentType, encoding, body, outcome))
		})
	})
	return nil
}

func (p *Proxy) sample(c *call, status int, headersStatus state.HeadersStatus, quotas state.Quotas, retryAfter time.Duration, headersAt time.Time, contentType, encoding string, body []byte, outcome usage.Outcome) state.Sample {
	now := p.now()
	complete := outcome == usage.Completed
	s := state.Sample{
		Status:     status,
		Complete:   complete,
		Failed:     outcome == usage.Failed,
		Headers:    headersStatus,
		Quotas:     quotas,
		Latency:    now.Sub(c.started),
		RetryAfter: retryAfter,
		HeadersAt:  headersAt,
		At:         now,
	}
	if complete && status >= 200 && status < 300 {
		if decoded, ok := usage.Decompress(encoding, body, p.cfg.MaxCaptureBytes); ok {
			s.Usage, s.HasUsage = usage.Extract(c.provider, contentType, decoded)
		}
	}
	return s
}

func (p *Proxy) failed(w http.ResponseWriter, r *http.Request, err error) {
	c, ok := r.Context().Value(callContextKey{}).(*call)
	if !ok {
		writeError(w, http.StatusBadGateway, "upstream unavailable")
		return
	}
	if errors.Is(err, context.Canceled) {
		c.once.Do(func() {
			p.store.Abort(c.key, p.now())
		})
		p.cfg.Logger.Info("client gave up before the upstream answered", "provider", c.provider, "model", c.key.Model)
		return
	}
	c.once.Do(func() {
		p.store.Fail(c.key, p.now())
	})
	p.cfg.Logger.Error("upstream request failed", "provider", c.provider, "model", c.key.Model, "error", err.Error())
	writeError(w, http.StatusBadGateway, "upstream unavailable")
}

var errUnboundedRead = errors.New("request body read cannot be bounded on this connection")

func readBodyWithin(w http.ResponseWriter, r *http.Request, limit int64, timeout time.Duration) ([]byte, error) {
	controller := http.NewResponseController(w)
	if err := controller.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, errUnboundedRead
	}
	body, err := readBody(r, limit)
	if err != nil {
		return nil, err
	}
	if err := controller.SetReadDeadline(time.Time{}); err != nil {
		return nil, errUnboundedRead
	}
	return body, nil
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

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"message": message, "type": "quotaware_proxy_error"},
	})
}
