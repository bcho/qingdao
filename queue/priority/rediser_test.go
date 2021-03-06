package priority

import (
	"os"
	"time"

	"gopkg.in/redis.v4"
)

// A basic mock rediser
type mockRediser struct {
	watch         func(func(*redis.Tx) error, ...string) error
	del           func(...string) *redis.IntCmd
	blpop         func(time.Duration, ...string) *redis.StringSliceCmd
	zadd          func(string, ...redis.Z) *redis.IntCmd
	zrangeByScore func(string, redis.ZRangeBy) *redis.StringSliceCmd
}

func (m mockRediser) Watch(a func(*redis.Tx) error, b ...string) error {
	return m.watch(a, b...)
}

func (m mockRediser) Del(a ...string) *redis.IntCmd {
	return m.del(a...)
}

func (m mockRediser) BLPop(a time.Duration, b ...string) *redis.StringSliceCmd {
	return m.blpop(a, b...)
}

func (m mockRediser) ZAdd(a string, b ...redis.Z) *redis.IntCmd {
	return m.zadd(a, b...)
}

func (m mockRediser) ZRangeByScore(a string, b redis.ZRangeBy) *redis.StringSliceCmd {
	return m.zrangeByScore(a, b)
}

func makeMockRediser() mockRediser {
	return mockRediser{
		watch:         func(func(*redis.Tx) error, ...string) error { return nil },
		del:           func(...string) *redis.IntCmd { return nil },
		blpop:         func(time.Duration, ...string) *redis.StringSliceCmd { return nil },
		zadd:          func(string, ...redis.Z) *redis.IntCmd { return nil },
		zrangeByScore: func(string, redis.ZRangeBy) *redis.StringSliceCmd { return nil },
	}
}

// A real redis (for ci)
func makeRealRediser() *redis.Client {
	addr := os.Getenv("QINGDAO_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	return redis.NewClient(&redis.Options{Addr: addr})
}
