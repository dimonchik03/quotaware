package scaler

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dimonchik03/quotaware/internal/aimd"
	"github.com/dimonchik03/quotaware/internal/headroom"
	"github.com/dimonchik03/quotaware/internal/scaler/externalscaler"
	"github.com/dimonchik03/quotaware/internal/state"
)

const (
	MetricName             = "quotaware-ceiling"
	streamsPerScaledObject = 2
)

func NewServer(maxScaledObjects int) *grpc.Server {
	return grpc.NewServer(grpc.MaxConcurrentStreams(uint32(streamsPerScaledObject * maxScaledObjects)))
}

type QueueDepth interface {
	Len(ctx context.Context, name string) (int64, error)
	Close() error
}

type QueueFactory func(address string) QueueDepth

const maxRefLength = 253

type Config struct {
	Defaults      headroom.Config
	AIMD          aimd.Config
	PushInterval  time.Duration
	AllowedQueues []string
}

func DefaultConfig() Config {
	fallback := aimd.DefaultConfig()
	fallback.Window = state.WindowLength
	return Config{Defaults: headroom.DefaultConfig(), AIMD: fallback, PushInterval: 5 * time.Second}
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
	if err := cfg.AIMD.Validate(); err != nil {
		return nil, fmt.Errorf("aimd defaults: %w", err)
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
	fingerprint string
	provider    string
	model       string
	credential  string
	cfg         headroom.Config
	aimd        aimd.Config
	queue       *queueTrigger
}

type queueTrigger struct {
	address    string
	list       string
	listLength int64
}

func (s *Service) parse(ref *externalscaler.ScaledObjectRef) (trigger, error) {
	if ref.GetName() == "" || len(ref.GetName()) > maxRefLength || len(ref.GetNamespace()) > maxRefLength {
		return trigger{}, status.Errorf(codes.InvalidArgument, "scaled object name is required and name and namespace must be at most %d characters", maxRefLength)
	}
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
	fallback, err := s.parseAIMD(metadata, cfg)
	if err != nil {
		return trigger{}, err
	}
	queue, err := s.parseQueue(metadata)
	if err != nil {
		return trigger{}, err
	}
	return trigger{fingerprint: fingerprint(metadata), provider: provider, model: model, credential: metadata["credential"], cfg: cfg, aimd: fallback, queue: queue}, nil
}

const fingerprintBytes = 16

func fingerprint(metadata map[string]string) string {
	var encoded []byte
	for _, name := range slices.Sorted(maps.Keys(metadata)) {
		encoded = appendField(encoded, name)
		encoded = appendField(encoded, metadata[name])
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:fingerprintBytes])
}

func appendField(encoded []byte, field string) []byte {
	return append(binary.AppendUvarint(encoded, uint64(len(field))), field...)
}

func (s *Service) parseAIMD(metadata map[string]string, cfg headroom.Config) (aimd.Config, error) {
	aimdConfig := s.cfg.AIMD
	aimdConfig.Interval, aimdConfig.Window, aimdConfig.Min, aimdConfig.Max = cfg.StepInterval, state.WindowLength, cfg.MinReplicas, cfg.MaxReplicas
	aimdConfig.Start = cfg.DefaultCeiling
	floats := []floatField{{"aimdThreshold", &aimdConfig.Threshold}, {"aimdDecrease", &aimdConfig.Decrease}}
	for _, field := range floats {
		if err := parseFloat(metadata, field.name, field.target); err != nil {
			return aimd.Config{}, err
		}
	}
	if err := parseInt(metadata, "aimdIncrease", &aimdConfig.Increase); err != nil {
		return aimd.Config{}, err
	}
	if err := aimdConfig.Validate(); err != nil {
		return aimd.Config{}, status.Error(codes.InvalidArgument, "aimd: "+err.Error())
	}
	return aimdConfig, nil
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
	if !slices.Contains(s.cfg.AllowedQueues, q.address) {
		return nil, status.Errorf(codes.PermissionDenied, "redisAddress %q is not on this scaler's allow list", q.address)
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
	value, err := s.ceiling(ref, t)
	if err != nil {
		return nil, err
	}
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

func (s *Service) ceiling(ref *externalscaler.ScaledObjectRef, t trigger) (int, error) {
	now := s.now()
	total, matched, waiting := 0, 0, 0
	for _, b := range s.store.Buckets(now) {
		if b.Key.Provider != t.provider || b.Key.Model != t.model || (t.credential != "" && b.Key.Key != t.credential) {
			continue
		}
		if !b.Accepted() {
			if !b.Answered() {
				waiting += b.InFlight
			}
			continue
		}
		matched++
		consumer := state.Consumer{Namespace: ref.GetNamespace(), ScaledObject: ref.GetName(), Trigger: t.fingerprint, Bucket: b.Key}
		value, err := s.evaluate(consumer, t, b.Observation(), b.Recent, now)
		if err != nil {
			return 0, err
		}
		total += value
	}
	unanswered := headroom.DrainFloor(t.cfg, headroom.Observation{InFlight: waiting})
	if matched == 0 {
		consumer := state.Consumer{Namespace: ref.GetNamespace(), ScaledObject: ref.GetName(), Trigger: t.fingerprint, Bucket: state.Key{Provider: t.provider, Model: t.model, Key: t.credential}}
		fallback, err := s.evaluate(consumer, t, headroom.Observation{}, state.Recent{}, now)
		if err != nil {
			return 0, err
		}
		return min(max(fallback, unanswered), t.cfg.MaxReplicas), nil
	}
	return min(total+unanswered, t.cfg.MaxReplicas), nil
}

type decision struct {
	value       int
	bound       int
	mode        state.Mode
	reason      string
	holdUntil   time.Time
	allowedRate float64
	podRate     float64
}

func (s *Service) evaluate(consumer state.Consumer, t trigger, obs headroom.Observation, recent state.Recent, now time.Time) (int, error) {
	var change *decision
	var from int
	ceiling, err := s.store.UpdateCeiling(consumer, func(previous state.Ceiling, known bool) state.Ceiling {
		d := decide(t, obs, recent, previous, known, now)
		next := state.Ceiling{Value: d.value, Bound: d.bound, Mode: d.mode, ChangedAt: previous.ChangedAt, Seen: now, HoldUntil: d.holdUntil}
		if !known || d.value != previous.Value {
			next.ChangedAt = now
			change, from = &d, previous.Value
		}
		return next
	})
	switch {
	case errors.Is(err, state.ErrTooManyConsumers):
		return 0, status.Error(codes.ResourceExhausted, err.Error())
	case err != nil:
		return 0, status.Error(codes.Internal, err.Error())
	}
	if change != nil {
		s.logger.Info("ceiling changed",
			"namespace", consumer.Namespace, "scaledobject", consumer.ScaledObject, "trigger", consumer.Trigger,
			"bucket", consumer.Bucket.Provider+"/"+consumer.Bucket.Model, "credential", consumer.Bucket.Key,
			"from", from, "to", change.value, "mode", string(change.mode), "reason", change.reason,
			"allowed_rate", change.allowedRate, "pod_rate", change.podRate)
	}
	return ceiling.Value, nil
}

func decide(t trigger, obs headroom.Observation, recent state.Recent, previous state.Ceiling, known bool, now time.Time) decision {
	signal := aimd.Signal{Responses: recent.Responses, RateLimited: recent.RateLimited, RetryAfter: recent.RetryAfter}
	aimdState := aimd.State{Ceiling: previous.Bound, Known: known && previous.Mode == state.ModeAIMD, ChangedAt: previous.ChangedAt, HoldUntil: previous.HoldUntil}
	drainFloor := headroom.DrainFloor(t.cfg, obs)
	usable := (obs.Requests.Known() || obs.Tokens.Known()) && !obs.ObservedAt.IsZero() && now.Sub(obs.ObservedAt) <= t.cfg.StaleHard
	if !usable {
		step := aimd.Step(t.aimd, aimdState, signal, now)
		return decision{value: max(step.Ceiling, drainFloor), bound: step.Ceiling, mode: state.ModeAIMD, reason: "aimd_" + string(step.Action), holdUntil: step.HoldUntil}
	}
	result := headroom.Compute(t.cfg, obs, headroom.Previous{Ceiling: previous.Bound, Known: known, At: previous.ChangedAt}, now)
	d := decision{value: result.Ceiling, bound: result.Bound, mode: state.ModeHeadroom, reason: string(result.Reason), allowedRate: result.AllowedRate, podRate: result.PodRate}
	guardState := aimd.State{Ceiling: previous.Bound, Known: known, HoldUntil: previous.HoldUntil}
	if guarded, held, until := aimd.Guard(t.aimd, result.Bound, guardState, signal, now); held {
		d.value, d.bound = max(guarded, drainFloor), guarded
		d.reason = "aimd_guard"
		d.holdUntil = until
	}
	return d
}
