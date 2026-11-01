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
}

type QueueFactory func(address string) QueueDepth

type Config struct {
	Defaults     headroom.Config
	PushInterval time.Duration
}

func DefaultConfig() Config {
	defaults := headroom.DefaultConfig()
	defaults.MinReplicas = 0
	defaults.MaxReplicas = 100
	return Config{Defaults: defaults, PushInterval: 5 * time.Second}
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
	case cfg.PushInterval <= 0:
		return nil, errors.New("push interval must be positive")
	case logger == nil:
		return nil, errors.New("logger is required")
	}
	if err := cfg.Defaults.Validate(); err != nil {
		return nil, fmt.Errorf("defaults: %w", err)
	}
	return &Service{cfg: cfg, store: store, queues: queues, now: now, logger: logger, queueByAddr: map[string]QueueDepth{}}, nil
}

func (s *Service) Register(server *grpc.Server) {
	externalscaler.RegisterExternalScalerServer(server, s)
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
	t := trigger{provider: provider, model: model, credential: metadata["credential"], cfg: s.cfg.Defaults}
	if _, present := metadata["concurrency"]; !present {
		return trigger{}, status.Error(codes.InvalidArgument, `metadata "concurrency" is required`)
	}
	ints := map[string]*int{
		"concurrency":    &t.cfg.Concurrency,
		"maxStepUp":      &t.cfg.MaxStepUp,
		"defaultCeiling": &t.cfg.DefaultCeiling,
		"minReplicas":    &t.cfg.MinReplicas,
		"maxReplicas":    &t.cfg.MaxReplicas,
	}
	for name, target := range ints {
		if err := parseInt(metadata, name, target); err != nil {
			return trigger{}, err
		}
	}
	floats := map[string]*float64{"safety": &t.cfg.Safety, "share": &t.cfg.Share}
	for name, target := range floats {
		if err := parseFloat(metadata, name, target); err != nil {
			return trigger{}, err
		}
	}
	durations := map[string]*time.Duration{"window": &t.cfg.Window, "staleAfter": &t.cfg.StaleAfter, "staleHard": &t.cfg.StaleHard}
	for name, target := range durations {
		if err := parseDuration(metadata, name, target); err != nil {
			return trigger{}, err
		}
	}
	if err := t.cfg.Validate(); err != nil {
		return trigger{}, status.Error(codes.InvalidArgument, err.Error())
	}
	if address, present := metadata["redisAddress"]; present {
		if s.queues == nil {
			return trigger{}, status.Error(codes.FailedPrecondition, "queue reading is not enabled on this scaler")
		}
		q := &queueTrigger{address: address, list: metadata["listName"], listLength: 1}
		if q.address == "" || q.list == "" {
			return trigger{}, status.Error(codes.InvalidArgument, `metadata "redisAddress" and "listName" are both required for queue mode`)
		}
		if raw, present := metadata["listLength"]; present {
			length, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || length <= 0 {
				return trigger{}, status.Error(codes.InvalidArgument, `metadata "listLength" must be a positive integer`)
			}
			q.listLength = length
		}
		t.queue = q
	}
	return t, nil
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
		MetricSpecs: []*externalscaler.MetricSpec{{MetricName: MetricName, TargetSize: 1}},
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
	return &externalscaler.GetMetricsResponse{
		MetricValues: []*externalscaler.MetricValue{{MetricName: MetricName, MetricValue: int64(value)}},
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
		var previous headroom.Previous
		if last, ok := s.store.Ceiling(consumer); ok {
			previous = headroom.Previous{Ceiling: last.Value, Known: true}
		}
		result := headroom.Compute(t.cfg, b.Observation(), previous, now)
		s.record(consumer, result, previous, now)
		total += result.Ceiling
	}
	if matched == 0 {
		return t.cfg.DefaultCeiling
	}
	return total
}

func (s *Service) record(consumer state.Consumer, result headroom.Result, previous headroom.Previous, now time.Time) {
	s.store.RecordCeiling(state.Ceiling{Consumer: consumer, Value: result.Ceiling, Reason: result.Reason, At: now})
	if !previous.Known || previous.Ceiling != result.Ceiling {
		s.logger.Info("ceiling changed",
			"namespace", consumer.Namespace, "scaledobject", consumer.ScaledObject,
			"bucket", consumer.Bucket.Provider+"/"+consumer.Bucket.Model, "credential", consumer.Bucket.Key,
			"from", previous.Ceiling, "to", result.Ceiling, "reason", string(result.Reason),
			"allowed_rate", result.AllowedRate, "pod_rate", result.PodRate)
	}
}
