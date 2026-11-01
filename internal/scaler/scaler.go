package scaler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dimonchik03/quotaware/internal/headroom"
	"github.com/dimonchik03/quotaware/internal/scaler/externalscaler"
	"github.com/dimonchik03/quotaware/internal/state"
)

const MetricName = "quotaware-ceiling"

type QueueDepth interface {
	Len(ctx context.Context, name string) (int64, error)
	Close() error
}

type QueueFactory func(address string) QueueDepth

type Config struct {
	Defaults     headroom.Config
	PushInterval time.Duration
}

func DefaultConfig() Config {
	return Config{Defaults: headroom.DefaultConfig(), PushInterval: 5 * time.Second}
}

type Service struct {
	externalscaler.UnimplementedExternalScalerServer
	cfg    Config
	store  *state.Store
	queues QueueFactory
	now    func() time.Time
	logger *slog.Logger

	mu          sync.Mutex
	queueByAddr map[string]QueueDepth
}

func New(cfg Config, store *state.Store, queues QueueFactory, now func() time.Time, logger *slog.Logger) (*Service, error) {
	switch {
	case store == nil:
		return nil, errors.New("store is required")
	case now == nil:
		return nil, errors.New("clock is required")
	case logger == nil:
		return nil, errors.New("logger is required")
	case cfg.PushInterval <= 0:
		return nil, errors.New("push interval must be positive")
	}
	if err := cfg.Defaults.Validate(); err != nil {
		return nil, fmt.Errorf("defaults: %w", err)
	}
	return &Service{cfg: cfg, store: store, queues: queues, now: now, logger: logger, queueByAddr: map[string]QueueDepth{}}, nil
}

func (s *Service) Register(server *grpc.Server) {
	externalscaler.RegisterExternalScalerServer(server, s)
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var failure error
	for address, reader := range s.queueByAddr {
		failure = errors.Join(failure, reader.Close())
		delete(s.queueByAddr, address)
	}
	return failure
}

type trigger struct {
	provider   string
	model      string
	credential string
	cfg        headroom.Config
	queue      *queueTrigger
}

type queueTrigger struct {
	address    string
	list       string
	listLength int64
}

func (s *Service) parse(ref *externalscaler.ScaledObjectRef) (trigger, error) {
	metadata := ref.GetScalerMetadata()
	bucket := metadata["bucket"]
	provider, model, ok := strings.Cut(bucket, "/")
	if !ok || provider == "" || model == "" {
		return trigger{}, status.Error(codes.InvalidArgument, `metadata "bucket" must be "provider/model"`)
	}
	cfg, err := s.parseConfig(metadata)
	if err != nil {
		return trigger{}, err
	}
	queue, err := s.parseQueue(metadata)
	if err != nil {
		return trigger{}, err
	}
	return trigger{provider: provider, model: model, credential: metadata["credential"], cfg: cfg, queue: queue}, nil
}

type intField struct {
	name   string
	target *int
}

type floatField struct {
	name   string
	target *float64
}

type durationField struct {
	name   string
	target *time.Duration
}

func (s *Service) parseConfig(metadata map[string]string) (headroom.Config, error) {
	cfg := s.cfg.Defaults
	if _, present := metadata["concurrency"]; !present {
		return headroom.Config{}, status.Error(codes.InvalidArgument, `metadata "concurrency" is required`)
	}
	ints := []intField{
		{"concurrency", &cfg.Concurrency},
		{"maxStepUp", &cfg.MaxStepUp},
		{"defaultCeiling", &cfg.DefaultCeiling},
		{"minReplicas", &cfg.MinReplicas},
		{"maxReplicas", &cfg.MaxReplicas},
	}
	for _, field := range ints {
		if err := parseInt(metadata, field.name, field.target); err != nil {
			return headroom.Config{}, err
		}
	}
	floats := []floatField{{"safety", &cfg.Safety}, {"share", &cfg.Share}}
	for _, field := range floats {
		if err := parseFloat(metadata, field.name, field.target); err != nil {
			return headroom.Config{}, err
		}
	}
	durations := []durationField{
		{"window", &cfg.Window},
		{"stepInterval", &cfg.StepInterval},
		{"staleAfter", &cfg.StaleAfter},
		{"staleHard", &cfg.StaleHard},
	}
	for _, field := range durations {
		if err := parseDuration(metadata, field.name, field.target); err != nil {
			return headroom.Config{}, err
		}
	}
	if err := cfg.Validate(); err != nil {
		return headroom.Config{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return cfg, nil
}

func (s *Service) parseQueue(metadata map[string]string) (*queueTrigger, error) {
	address, present := metadata["redisAddress"]
	if !present {
		return nil, nil
	}
	if s.queues == nil {
		return nil, status.Error(codes.FailedPrecondition, "queue reading is not enabled on this scaler")
	}
	q := &queueTrigger{address: address, list: metadata["listName"], listLength: 1}
	if q.address == "" || q.list == "" {
		return nil, status.Error(codes.InvalidArgument, `metadata "redisAddress" and "listName" are both required for queue mode`)
	}
	if raw, present := metadata["listLength"]; present {
		length, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || length <= 0 {
			return nil, status.Error(codes.InvalidArgument, `metadata "listLength" must be a positive integer`)
		}
		q.listLength = length
	}
	return q, nil
}

func parseInt(metadata map[string]string, name string, target *int) error {
	raw, present := metadata[name]
	if !present {
		return nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "metadata %q must be an integer", name)
	}
	*target = value
	return nil
}

func parseFloat(metadata map[string]string, name string, target *float64) error {
	raw, present := metadata[name]
	if !present {
		return nil
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "metadata %q must be a number", name)
	}
	*target = value
	return nil
}

func parseDuration(metadata map[string]string, name string, target *time.Duration) error {
	raw, present := metadata[name]
	if !present {
		return nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "metadata %q must be a duration such as 2m", name)
	}
	*target = value
	return nil
}

func (s *Service) GetMetricSpec(_ context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.GetMetricSpecResponse, error) {
	if _, err := s.parse(ref); err != nil {
		return nil, err
	}
	return &externalscaler.GetMetricSpecResponse{
		MetricSpecs: []*externalscaler.MetricSpec{{MetricName: MetricName, TargetSize: 1, TargetSizeFloat: 1}},
	}, nil
}

func (s *Service) GetMetrics(ctx context.Context, request *externalscaler.GetMetricsRequest) (*externalscaler.GetMetricsResponse, error) {
	ref := request.GetScaledObjectRef()
	t, err := s.parse(ref)
	if err != nil {
		return nil, err
	}
	value := s.ceiling(ref, t)
	if t.queue != nil {
		demand, err := s.demand(ctx, t.queue)
		if err != nil {
			return nil, err
		}
		value = min(value, demand)
	}
	name := request.GetMetricName()
	if name == "" {
		name = MetricName
	}
	return &externalscaler.GetMetricsResponse{
		MetricValues: []*externalscaler.MetricValue{{MetricName: name, MetricValue: int64(value), MetricValueFloat: float64(value)}},
	}, nil
}

func (s *Service) IsActive(ctx context.Context, ref *externalscaler.ScaledObjectRef) (*externalscaler.IsActiveResponse, error) {
	t, err := s.parse(ref)
	if err != nil {
		return nil, err
	}
	active, err := s.active(ctx, t)
	if err != nil {
		return nil, err
	}
	return &externalscaler.IsActiveResponse{Result: active}, nil
}

func (s *Service) StreamIsActive(ref *externalscaler.ScaledObjectRef, stream externalscaler.ExternalScaler_StreamIsActiveServer) error {
	t, err := s.parse(ref)
	if err != nil {
		return err
	}
	ctx := stream.Context()
	last, err := s.active(ctx, t)
	if err != nil {
		return err
	}
	if err := stream.Send(&externalscaler.IsActiveResponse{Result: last}); err != nil {
		return err
	}
	if t.queue == nil {
		<-ctx.Done()
		return nil
	}
	ticker := time.NewTicker(s.cfg.PushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			active, err := s.active(ctx, t)
			if err != nil {
				return err
			}
			if active == last {
				continue
			}
			last = active
			if err := stream.Send(&externalscaler.IsActiveResponse{Result: active}); err != nil {
				return err
			}
		}
	}
}

func (s *Service) active(ctx context.Context, t trigger) (bool, error) {
	if t.queue == nil {
		return false, nil
	}
	depth, err := s.queueDepth(ctx, t.queue)
	if err != nil {
		return false, err
	}
	return depth > 0, nil
}

func (s *Service) demand(ctx context.Context, q *queueTrigger) (int, error) {
	depth, err := s.queueDepth(ctx, q)
	if err != nil {
		return 0, err
	}
	return int(math.Ceil(float64(depth) / float64(q.listLength))), nil
}

func (s *Service) queueDepth(ctx context.Context, q *queueTrigger) (int64, error) {
	s.mu.Lock()
	reader, ok := s.queueByAddr[q.address]
	if !ok {
		reader = s.queues(q.address)
		s.queueByAddr[q.address] = reader
	}
	s.mu.Unlock()
	depth, err := reader.Len(ctx, q.list)
	if err != nil {
		return 0, status.Errorf(codes.Unavailable, "queue %s at %s: %v", q.list, q.address, err)
	}
	return depth, nil
}

func (s *Service) ceiling(ref *externalscaler.ScaledObjectRef, t trigger) int {
	now := s.now()
	total := 0
	matched := 0
	for _, b := range s.store.Buckets() {
		if b.Key.Provider != t.provider || b.Key.Model != t.model || (t.credential != "" && b.Key.Key != t.credential) {
			continue
		}
		matched++
		consumer := state.Consumer{Namespace: ref.GetNamespace(), ScaledObject: ref.GetName(), Bucket: b.Key}
		total += s.evaluate(consumer, t.cfg, b.Observation(), now)
	}
	if matched == 0 {
		consumer := state.Consumer{Namespace: ref.GetNamespace(), ScaledObject: ref.GetName(), Bucket: state.Key{Provider: t.provider, Model: t.model, Key: t.credential}}
		return s.evaluate(consumer, t.cfg, headroom.Observation{}, now)
	}
	return min(total, t.cfg.MaxReplicas)
}

func (s *Service) evaluate(consumer state.Consumer, cfg headroom.Config, obs headroom.Observation, now time.Time) int {
	var change *headroom.Result
	var from int
	ceiling := s.store.UpdateCeiling(consumer, func(previous state.Ceiling, known bool) state.Ceiling {
		result := headroom.Compute(cfg, obs, headroom.Previous{Ceiling: previous.Value, Known: known, At: previous.ChangedAt}, now)
		next := state.Ceiling{Value: result.Ceiling, ChangedAt: previous.ChangedAt, Seen: now}
		if !known || result.Ceiling != previous.Value {
			next.ChangedAt = now
			change, from = &result, previous.Value
		}
		return next
	})
	if change != nil {
		s.logger.Info("ceiling changed",
			"namespace", consumer.Namespace, "scaledobject", consumer.ScaledObject,
			"bucket", consumer.Bucket.Provider+"/"+consumer.Bucket.Model, "credential", consumer.Bucket.Key,
			"from", from, "to", change.Ceiling, "reason", string(change.Reason),
			"allowed_rate", change.AllowedRate, "pod_rate", change.PodRate)
	}
	return ceiling.Value
}
