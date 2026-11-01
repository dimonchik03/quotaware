package state

import (
	"errors"
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

func (q Quotas) merge(update Quotas) Quotas {
	return Quotas{
		Requests:      keep(q.Requests, update.Requests),
		Tokens:        keep(q.Tokens, update.Tokens),
		ProjectTokens: keep(q.ProjectTokens, update.ProjectTokens),
		InputTokens:   keep(q.InputTokens, update.InputTokens),
		OutputTokens:  keep(q.OutputTokens, update.OutputTokens),
	}
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
	Headers    HeadersStatus
	Quotas     Quotas
	HasUsage   bool
	Usage      float64
	Latency    time.Duration
	RetryAfter time.Duration
	At         time.Time
}

type Bucket struct {
	Key              Key
	Quotas           Quotas
	TokensPerCall    float64
	Latency          time.Duration
	InFlight         int
	ObservedAt       time.Time
	LastSeen         time.Time
	Responses        int64
	RateLimited      int64
	Aborted          int64
	UpstreamErrors   int64
	HeadersMissing   int64
	HeadersMalformed int64
	UsageMissing     int64
	RetryAfter       time.Duration
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
	Bucket       Key
}

type Ceiling struct {
	Consumer Consumer
	Value    int
	Reason   headroom.Reason
	At       time.Time
}

type Store struct {
	alpha    float64
	mu       sync.Mutex
	buckets  map[Key]*Bucket
	ceilings map[Consumer]Ceiling
}

func New(alpha float64) (*Store, error) {
	if alpha <= 0 || alpha > 1 {
		return nil, errors.New("alpha must be within (0, 1]")
	}
	return &Store{alpha: alpha, buckets: map[Key]*Bucket{}, ceilings: map[Consumer]Ceiling{}}, nil
}

func (s *Store) RecordCeiling(c Ceiling) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ceilings[c.Consumer] = c
}

func (s *Store) Ceiling(consumer Consumer) (Ceiling, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.ceilings[consumer]
	return c, ok
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
	default:
		return c.Bucket.less(other.Bucket)
	}
}

func (s *Store) Begin(key Key, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(key)
	b.InFlight++
	b.LastSeen = latest(b.LastSeen, now)
}

func (s *Store) Fail(key Key, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(key)
	b.InFlight = max(b.InFlight-1, 0)
	b.UpstreamErrors++
	b.LastSeen = latest(b.LastSeen, now)
}

func (s *Store) End(key Key, sample Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(key)
	b.InFlight = max(b.InFlight-1, 0)
	b.LastSeen = latest(b.LastSeen, sample.At)
	b.Responses++
	if sample.Status == http.StatusTooManyRequests {
		b.RateLimited++
		b.RetryAfter = sample.RetryAfter
	}
	switch sample.Headers {
	case HeadersPresent:
		b.Quotas = b.Quotas.merge(sample.Quotas)
		b.ObservedAt = sample.At
	case HeadersMissing:
		if carriesLimits(sample.Status) {
			b.HeadersMissing++
		}
	case HeadersMalformed:
		b.HeadersMalformed++
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

func (s *Store) Evict(now time.Time, idleFor time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	evicted := 0
	for key, b := range s.buckets {
		if b.InFlight == 0 && now.Sub(b.LastSeen) > idleFor {
			delete(s.buckets, key)
			evicted++
		}
	}
	for consumer := range s.ceilings {
		if _, ok := s.buckets[consumer.Bucket]; !ok {
			delete(s.ceilings, consumer)
		}
	}
	return evicted
}

func (s *Store) Bucket(key Key) (Bucket, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[key]
	if !ok {
		return Bucket{}, false
	}
	return *b, true
}

func (s *Store) Buckets() []Bucket {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]Bucket, 0, len(s.buckets))
	for _, b := range s.buckets {
		all = append(all, *b)
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

func (s *Store) bucket(key Key) *Bucket {
	b, ok := s.buckets[key]
	if !ok {
		b = &Bucket{Key: key}
		s.buckets[key] = b
	}
	return b
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

func succeeded(status int) bool {
	return status >= 200 && status < 300
}
