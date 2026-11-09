package state

import (
	"errors"
	"maps"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/dimonchik03/quotaware/internal/headroom"
)

type Key struct {
	Provider string
	Model    string
	Key      string
}

type HeadersStatus int

const (
	HeadersPresent HeadersStatus = iota
	HeadersMissing
	HeadersMalformed
)

type Quotas struct {
	Requests      headroom.Quota
	Tokens        headroom.Quota
	ProjectTokens headroom.Quota
	InputTokens   headroom.Quota
	OutputTokens  headroom.Quota
}

func (q Quotas) Usable() bool {
	return q.Requests.Known() || q.Tokens.Known() || q.ProjectTokens.Known()
}

func (q Quotas) merge(update Quotas) Quotas {
	return Quotas{
		Requests:      keep(q.Requests, update.Requests),
		Tokens:        keep(q.Tokens, update.Tokens),
		ProjectTokens: keep(q.ProjectTokens, update.ProjectTokens),
		InputTokens:   keep(q.InputTokens, update.InputTokens),
		OutputTokens:  keep(q.OutputTokens, update.OutputTokens),
	}
}

func (q Quotas) reportedAt(at time.Time) Quotas {
	for _, window := range []*headroom.Quota{&q.Requests, &q.Tokens, &q.ProjectTokens, &q.InputTokens, &q.OutputTokens} {
		if window.Known() {
			window.ReportedAt = at
		}
	}
	return q
}

func (q Quotas) exhausted(tokensPerCall float64) bool {
	if spent(q.Requests, 1) {
		return true
	}
	return tokensPerCall > 0 && (spent(q.Tokens, tokensPerCall) || spent(q.ProjectTokens, tokensPerCall))
}

func spent(window headroom.Quota, perCall float64) bool {
	return window.Known() && window.Remaining < perCall
}

func keep(current, update headroom.Quota) headroom.Quota {
	if update.Known() {
		return update
	}
	return current
}

type Sample struct {
	Status     int
	Complete   bool
	Failed     bool
	Headers    HeadersStatus
	Quotas     Quotas
	HasUsage   bool
	Usage      float64
	Latency    time.Duration
	RetryAfter time.Duration
	HeadersAt  time.Time
	At         time.Time
}

type Recent struct {
	Responses             int64
	RateLimited           int64
	Unexplained           int64
	RetryAfter            time.Duration
	UnexplainedRetryAfter time.Duration
}

type Bucket struct {
	Key              Key
	Quotas           Quotas
	TokensPerCall    float64
	Latency          time.Duration
	InFlight         int
	workloads        map[string]int
	ObservedAt       time.Time
	LastSeen         time.Time
	Responses        int64
	Succeeded        int64
	RateLimited      int64
	Unexplained      int64
	Refused          int64
	Aborted          int64
	UpstreamErrors   int64
	HeadersMissing   int64
	HeadersMalformed int64
	UsageMissing     int64
	RetryAfter       time.Duration
	Recent           Recent
}

const (
	binSeconds   = 5
	binCount     = 12
	WindowLength = binSeconds * binCount * time.Second
)

type bin struct {
	slot                  int64
	responses             int64
	rateLimited           int64
	unexplained           int64
	retryAfter            time.Duration
	unexplainedRetryAfter time.Duration
}

type window struct {
	bins [binCount]bin
}

func (w *window) record(at time.Time, rateLimited, unexplained bool, retryAfter time.Duration) {
	slot := at.Unix() / binSeconds
	b := &w.bins[(slot%binCount+binCount)%binCount]
	switch {
	case b.slot > slot:
		return
	case b.slot < slot:
		*b = bin{slot: slot}
	}
	b.responses++
	if rateLimited {
		b.rateLimited++
		b.retryAfter = max(b.retryAfter, retryAfter)
	}
	if unexplained {
		b.unexplained++
		b.unexplainedRetryAfter = max(b.unexplainedRetryAfter, retryAfter)
	}
}

func (w *window) recent(at time.Time) Recent {
	latest := at.Unix() / binSeconds
	var r Recent
	for _, b := range w.bins {
		if b.slot > latest-binCount && b.slot <= latest {
			r.Responses += b.responses
			r.RateLimited += b.rateLimited
			r.Unexplained += b.unexplained
			r.RetryAfter = max(r.RetryAfter, b.retryAfter)
			r.UnexplainedRetryAfter = max(r.UnexplainedRetryAfter, b.unexplainedRetryAfter)
		}
	}
	return r
}

func (b Bucket) OpenCalls(workload string) int {
	if workload == "" {
		return b.InFlight
	}
	return b.workloads[workload]
}

func (b *Bucket) finish(workload string) {
	b.InFlight = max(b.InFlight-1, 0)
	if workload == "" {
		return
	}
	if b.workloads[workload] <= 1 {
		delete(b.workloads, workload)
		return
	}
	b.workloads[workload]--
}

func (b Bucket) Accepted() bool {
	return b.Succeeded > 0 || b.RateLimited > 0
}

func (b Bucket) RefusedOnly() bool {
	return b.Refused > 0 && !b.Accepted()
}

func (b Bucket) Observation() headroom.Observation {
	return headroom.Observation{
		Requests:      b.Quotas.Requests,
		Tokens:        b.Quotas.Tokens,
		ProjectTokens: b.Quotas.ProjectTokens,
		TokensPerCall: b.TokensPerCall,
		Latency:       b.Latency,
		InFlight:      b.InFlight,
		ObservedAt:    b.ObservedAt,
	}
}

type Consumer struct {
	Namespace    string
	ScaledObject string
	Trigger      string
	Bucket       Key
}

type Mode string

const (
	ModeHeadroom Mode = "headroom"
	ModeAIMD     Mode = "aimd"
	ModeCombined Mode = "combined"
)

type Ceiling struct {
	Consumer       Consumer
	Value          int
	Bound          int
	Held           int
	Mode           Mode
	BoundChangedAt time.Time
	Seen           time.Time
	HoldUntil      time.Time
}

const (
	DefaultMaxBuckets   = 10000
	DefaultMaxConsumers = 1000
)

var ErrTooManyConsumers = errors.New("too many scaled objects track a ceiling")

type Limits struct {
	Buckets   int
	Consumers int
}

func DefaultLimits() Limits {
	return Limits{Buckets: DefaultMaxBuckets, Consumers: DefaultMaxConsumers}
}

type Stats struct {
	Buckets           int
	Consumers         int
	RejectedBuckets   int64
	RejectedConsumers int64
	RejectedCalls     int64
}

type Store struct {
	alpha    float64
	limits   Limits
	mu       sync.Mutex
	buckets  map[Key]*Bucket
	windows  map[Key]*window
	ceilings map[Consumer]Ceiling
	stats    Stats
}

func New(alpha float64, limits Limits) (*Store, error) {
	switch {
	case math.IsNaN(alpha) || alpha <= 0 || alpha > 1:
		return nil, errors.New("alpha must be within (0, 1]")
	case limits.Buckets <= 0 || limits.Consumers < 2:
		return nil, errors.New("max-buckets must be positive and max-scaled-objects at least 2")
	}
	return &Store{alpha: alpha, limits: limits, buckets: map[Key]*Bucket{}, windows: map[Key]*window{}, ceilings: map[Consumer]Ceiling{}}, nil
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	stats := s.stats
	stats.Buckets, stats.Consumers = len(s.buckets), len(s.ceilings)
	return stats
}

func (s *Store) RejectCall() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.RejectedCalls++
}

func (s *Store) UpdateCeiling(consumer Consumer, update func(previous Ceiling, known bool) Ceiling) (Ceiling, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, known := s.ceilings[consumer]
	if !known && len(s.ceilings) >= s.limits.Consumers {
		s.stats.RejectedConsumers++
		return Ceiling{}, ErrTooManyConsumers
	}
	next := update(previous, known)
	next.Consumer = consumer
	s.ceilings[consumer] = next
	return next, nil
}

func (s *Store) Ceilings() []Ceiling {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]Ceiling, 0, len(s.ceilings))
	for _, c := range s.ceilings {
		all = append(all, c)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].Consumer.less(all[j].Consumer)
	})
	return all
}

func (c Consumer) less(other Consumer) bool {
	switch {
	case c.Namespace != other.Namespace:
		return c.Namespace < other.Namespace
	case c.ScaledObject != other.ScaledObject:
		return c.ScaledObject < other.ScaledObject
	case c.Trigger != other.Trigger:
		return c.Trigger < other.Trigger
	default:
		return c.Bucket.less(other.Bucket)
	}
}

func (s *Store) Begin(key Key, now time.Time, workload string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bucket(key)
	if !ok {
		return false
	}
	b.InFlight++
	if workload != "" {
		if b.workloads == nil {
			b.workloads = make(map[string]int)
		}
		b.workloads[workload]++
	}
	b.LastSeen = latest(b.LastSeen, now)
	return true
}

func (s *Store) Fail(key Key, now time.Time, workload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bucket(key)
	if !ok {
		return
	}
	b.finish(workload)
	b.UpstreamErrors++
	b.LastSeen = latest(b.LastSeen, now)
}

func (s *Store) Abort(key Key, now time.Time, workload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bucket(key)
	if !ok {
		return
	}
	b.finish(workload)
	b.Aborted++
	b.LastSeen = latest(b.LastSeen, now)
}

func (s *Store) End(key Key, sample Sample, workload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bucket(key)
	if !ok {
		return
	}
	b.finish(workload)
	s.record(key, b, sample)
}

func (s *Store) Observe(key Key, sample Sample) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bucket(key)
	if !ok {
		return false
	}
	s.record(key, b, sample)
	return true
}

func (s *Store) record(key Key, b *Bucket, sample Sample) {
	b.LastSeen = latest(b.LastSeen, sample.At)
	b.Responses++
	if succeeded(sample.Status) {
		b.Succeeded++
	}
	rateLimited := sample.Status == http.StatusTooManyRequests
	unexplained := rateLimited && !sample.Quotas.exhausted(b.TokensPerCall)
	if rateLimited {
		b.RateLimited++
		b.RetryAfter = sample.RetryAfter
	}
	if unexplained {
		b.Unexplained++
	}
	if refused(sample.Status) {
		b.Refused++
	}
	if carriesLimits(sample.Status) {
		s.window(key).record(sample.At, rateLimited, unexplained, sample.RetryAfter)
	}
	switch sample.Headers {
	case HeadersPresent:
		b.apply(sample)
	case HeadersMissing:
		if carriesLimits(sample.Status) {
			b.HeadersMissing++
		}
	case HeadersMalformed:
		b.HeadersMalformed++
		if sample.Quotas.Usable() {
			b.apply(sample)
		}
	}
	if sample.Failed {
		b.UpstreamErrors++
		return
	}
	if !sample.Complete {
		b.Aborted++
		return
	}
	if !succeeded(sample.Status) {
		return
	}
	b.Latency = time.Duration(s.smooth(b.Latency.Seconds(), sample.Latency.Seconds()) * float64(time.Second))
	if sample.HasUsage {
		b.TokensPerCall = s.smooth(b.TokensPerCall, sample.Usage)
	} else {
		b.UsageMissing++
	}
}

func (b *Bucket) apply(sample Sample) {
	if sample.HeadersAt.Before(b.ObservedAt) {
		return
	}
	b.Quotas = b.Quotas.merge(sample.Quotas.reportedAt(sample.HeadersAt))
	b.ObservedAt = sample.HeadersAt
}

func (s *Store) Evict(now time.Time, idleFor time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted := 0
	for key, b := range s.buckets {
		if b.InFlight == 0 && now.Sub(b.LastSeen) > idleFor {
			delete(s.buckets, key)
			delete(s.windows, key)
			evicted++
		}
	}
	for consumer, c := range s.ceilings {
		if now.Sub(c.Seen) > idleFor {
			delete(s.ceilings, consumer)
		}
	}
	return evicted
}

func (s *Store) Buckets(now time.Time) []Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]Bucket, 0, len(s.buckets))
	for _, b := range s.buckets {
		all = append(all, s.snapshot(b, now))
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].Key.less(all[j].Key)
	})
	return all
}

func (k Key) less(other Key) bool {
	switch {
	case k.Provider != other.Provider:
		return k.Provider < other.Provider
	case k.Model != other.Model:
		return k.Model < other.Model
	default:
		return k.Key < other.Key
	}
}

func (s *Store) snapshot(b *Bucket, now time.Time) Bucket {
	copied := *b
	copied.workloads = maps.Clone(b.workloads)
	if w, ok := s.windows[b.Key]; ok {
		copied.Recent = w.recent(now)
	}
	return copied
}

func (s *Store) window(key Key) *window {
	w, ok := s.windows[key]
	if !ok {
		w = &window{}
		s.windows[key] = w
	}
	return w
}

func (s *Store) bucket(key Key) (*Bucket, bool) {
	b, ok := s.buckets[key]
	if ok {
		return b, true
	}
	if len(s.buckets) >= s.limits.Buckets {
		s.stats.RejectedBuckets++
		return nil, false
	}
	b = &Bucket{Key: key}
	s.buckets[key] = b
	return b, true
}

func (s *Store) smooth(previous, sample float64) float64 {
	if previous == 0 {
		return sample
	}
	return s.alpha*sample + (1-s.alpha)*previous
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func carriesLimits(status int) bool {
	return succeeded(status) || status == http.StatusTooManyRequests
}

func refused(status int) bool {
	return status == http.StatusUnauthorized || status == http.StatusForbidden
}

func succeeded(status int) bool {
	return status >= 200 && status < 300
}
