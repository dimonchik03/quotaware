package proxy

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dimonchik03/quotaware/internal/headers"
	"github.com/dimonchik03/quotaware/internal/state"
	"github.com/dimonchik03/quotaware/internal/usage"
)

type Observer struct {
	store    *state.Store
	now      func() time.Time
	maxBytes int64
	token    string
	logger   *slog.Logger
}

func NewObserver(store *state.Store, now func() time.Time, maxBytes int64, token string, logger *slog.Logger) (*Observer, error) {
	if store == nil || now == nil || logger == nil || maxBytes <= 0 {
		return nil, errors.New("store, clock, logger and a positive size limit are required")
	}
	return &Observer{store: store, now: now, maxBytes: maxBytes, token: token, logger: logger}, nil
}

func (o *Observer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !o.authorized(r) {
		writeError(w, http.StatusUnauthorized, "observe token missing or wrong")
		return
	}
	body, err := readBody(r, o.maxBytes)
	switch {
	case errors.Is(err, errTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "observation too large")
		return
	case err != nil:
		writeError(w, http.StatusBadRequest, "observation body unreadable")
		return
	}
	var observation usage.Observation
	if err := json.Unmarshal(body, &observation); err != nil {
		writeError(w, http.StatusBadRequest, "observation is not valid JSON")
		return
	}
	if err := observation.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	provider := headers.Provider(observation.Provider)
	now := o.now()
	headersAt := now.Add(-time.Duration(observation.HeadersAgeMS * float64(time.Millisecond)))
	h := http.Header{}
	for name, value := range observation.Headers {
		h.Set(name, value)
	}
	quotas, status := parseLimits(o.logger, provider, observation.Model, h, headersAt)
	retryAfter, err := headers.RetryAfter(h, headersAt)
	if err != nil {
		o.logger.Warn("retry-after malformed", "provider", provider, "model", observation.Model, "error", err.Error())
		retryAfter, status = 0, state.HeadersMalformed
	}
	sample := state.Sample{
		Status:     observation.Status,
		Complete:   observation.Complete,
		Headers:    status,
		Quotas:     quotas,
		Latency:    time.Duration(observation.LatencyMS * float64(time.Millisecond)),
		RetryAfter: retryAfter,
		HeadersAt:  headersAt,
		At:         now,
	}
	if observation.Usage != nil && *observation.Usage > 0 {
		sample.HasUsage, sample.Usage = true, *observation.Usage
	}
	key := state.Key{Provider: string(provider), Model: observation.Model, Key: observation.Credential}
	o.store.Observe(key, sample)
	w.WriteHeader(http.StatusAccepted)
}

func (o *Observer) authorized(r *http.Request) bool {
	if o.token == "" {
		return true
	}
	presented, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return bearer && subtle.ConstantTimeCompare([]byte(presented), []byte(o.token)) == 1
}

func parseLimits(logger *slog.Logger, provider headers.Provider, model string, h http.Header, now time.Time) (state.Quotas, state.HeadersStatus) {
	limits, err := headers.Parse(provider, h, now)
	switch {
	case errors.Is(err, headers.ErrMissing):
		return state.Quotas{}, state.HeadersMissing
	case err != nil:
		logger.Warn("rate-limit headers unreadable", "provider", provider, "model", model, "error", err.Error())
		return state.Quotas{}, state.HeadersMalformed
	}
	for _, malformed := range limits.Malformed {
		logger.Warn("rate-limit header malformed", "provider", provider, "model", model, "error", malformed.Error())
	}
	if len(limits.Malformed) > 0 {
		return quotasOf(limits), state.HeadersMalformed
	}
	return quotasOf(limits), state.HeadersPresent
}

func quotasOf(limits headers.Limits) state.Quotas {
	return state.Quotas{
		Requests:      limits.Requests.Quota(),
		Tokens:        limits.Tokens.Quota(),
		ProjectTokens: limits.ProjectTokens.Quota(),
		InputTokens:   limits.InputTokens.Quota(),
		OutputTokens:  limits.OutputTokens.Quota(),
	}
}
