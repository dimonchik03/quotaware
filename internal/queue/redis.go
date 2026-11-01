package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(address string) *Redis {
	return &Redis{client: redis.NewClient(&redis.Options{Addr: address})}
}

func (r *Redis) Len(ctx context.Context, name string) (int64, error) {
	return r.client.LLen(ctx, name).Result()
}

func (r *Redis) Close() error {
	return r.client.Close()
}
