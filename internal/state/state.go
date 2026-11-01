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

type Sample struct {
	Status        int
	HasLimits     bool
	Requests      headroom.Quota
	Tokens        headroom.Quota
	ProjectTokens headroom.Quota
	HasUsage      bool
	Usage         float64
	Latency       time.Duration
	RetryAfter    time.Duration
	At            time.Time
}

type Bucket struct {
	Key            Key
	Requests       headroom.Quota
	Tokens         headroom.Quota
	ProjectTokens  headroom.Quota
	TokensPerCall  float64
	Latency        time.Duration
	InFlight       int
	ObservedAt     time.Time
	Responses      int64
	RateLimited    int64
	HeadersMissing int64
	UsageMissing   int64
	RetryAfter     time.Duration
}

func (b Bucket) Observation() headroom.Observation {
	return headroom.Observation{
		Requests:      b.Requests,
		Tokens:        b.Tokens,
		ProjectTokens: b.ProjectTokens,
		TokensPerCall: b.TokensPerCall,
		Latency:       b.Latency,
		InFlight:      b.InFlight,
		ObservedAt:    b.ObservedAt,
	}
}

type Store struct {
	alpha   float64
	mu      sync.Mutex
	buckets map[Key]*Bucket
}

func New(alpha float64) (*Store, error) {
	if alpha <= 0 || alpha > 1 {
		return nil, errors.New("alpha must be within (0, 1]")
	}
	return &Store{alpha: alpha, buckets: map[Key]*Bucket{}}, nil
}

func (s *Store) Begin(key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bucket(key).InFlight++
}

func (s *Store) End(key Key, sample Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.bucket(key)
	b.InFlight = max(b.InFlight-1, 0)
	if sample.Status == 0 {
		return
	}
	b.Responses++
	if sample.Status == http.StatusTooManyRequests {
		b.RateLimited++
		b.RetryAfter = sample.RetryAfter
	}
	switch {
	case sample.HasLimits:
		b.Requests, b.Tokens, b.ProjectTokens = sample.Requests, sample.Tokens, sample.ProjectTokens
		b.ObservedAt = sample.At
	case carriesLimits(sample.Status):
		b.HeadersMissing++
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

func carriesLimits(status int) bool {
	return succeeded(status) || status == http.StatusTooManyRequests
}

func succeeded(status int) bool {
	return status >= 200 && status < 300
}
