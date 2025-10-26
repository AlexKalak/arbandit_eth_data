package redisdb

import (
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisDatabaseConfig struct {
	RedisServer string
}

type RedisDatabase struct {
	rdb *redis.Client
}

func (d *RedisDatabase) GetDB() (*redis.Client, error) {
	if d.rdb == nil {
		return nil, errors.New("redis database uninitialized")
	}

	return d.rdb, nil
}

func New(config RedisDatabaseConfig) (*RedisDatabase, error) {
	fmt.Println("redis server: ", config.RedisServer)
	rdb := redis.NewClient(&redis.Options{
		Addr: config.RedisServer,
	})

	return &RedisDatabase{
		rdb: rdb,
	}, nil
}
