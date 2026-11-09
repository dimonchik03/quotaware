package queue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	dialTimeout = 2 * time.Second
	readTimeout = 2 * time.Second
	maxRetries  = 1
)

type Redis struct {
	client *redis.Client
}

func NewRedis(address string) *Redis {
	return &Redis{client: redis.NewClient(&redis.Options{
		Addr:                  address,
		DialTimeout:           dialTimeout,
		ReadTimeout:           readTimeout,
		MaxRetries:            maxRetries,
		ContextTimeoutEnabled: true,
	})}
}

func (r *Redis) Len(ctx context.Context, name string) (int64, error) {
	return r.client.LLen(ctx, name).Result()
}

func (r *Redis) Push(ctx context.Context, name string, payloads ...string) error {
	values := make([]any, len(payloads))
	for i, payload := range payloads {
		values[i] = payload
	}
	return r.client.LPush(ctx, name, values...).Err()
}

func (r *Redis) Requeue(ctx context.Context, name, payload string) error {
	return r.client.RPush(ctx, name, payload).Err()
}

func (r *Redis) Pop(ctx context.Context, name string, timeout time.Duration) (string, error) {
	result, err := r.client.BRPop(ctx, timeout, name).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return result[1], nil
}

func (r *Redis) Clear(ctx context.Context, name string) error {
	return r.client.Del(ctx, name).Err()
}

func (r *Redis) Close() error {
	return r.client.Close()
}
