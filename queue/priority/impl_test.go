package priority

import (
	"strings"
	"testing"
	"time"

	"github.com/bcho/qingdao/job"
	"github.com/bcho/qingdao/queue"

	"gopkg.in/redis.v4"
)

var (
	noopWatch         = func(func(*redis.Tx) error, string) error { return nil }
	noopBLPop         = func(time.Duration, string) *redis.StringSliceCmd { return nil }
	noopZAdd          = func(string, redis.Z) *redis.IntCmd { return nil }
	noopZRangeByScore = func(string, redis.ZRangeBy) *redis.StringSliceCmd { return nil }
)

type mockRediser struct {
	watch         func(func(*redis.Tx) error, string) error
	blpop         func(time.Duration, string) *redis.StringSliceCmd
	zadd          func(string, redis.Z) *redis.IntCmd
	zrangeByScore func(string, redis.ZRangeBy) *redis.StringSliceCmd
}

func (m mockRediser) Watch(a func(*redis.Tx) error, b string) error {
	return m.watch(a, b)
}

func (m mockRediser) BLPop(a time.Duration, b string) *redis.StringSliceCmd {
	return m.blpop(a, b)
}

func (m mockRediser) ZAdd(a string, b redis.Z) *redis.IntCmd {
	return m.zadd(a, b)
}

func (m mockRediser) ZRangeByScore(a string, b redis.ZRangeBy) *redis.StringSliceCmd {
	return m.zrangeByScore(a, b)
}

func makeMockRediser() mockRediser {
	return mockRediser{
		watch:         noopWatch,
		blpop:         noopBLPop,
		zadd:          noopZAdd,
		zrangeByScore: noopZRangeByScore,
	}
}

func TestNew_WithRedis(t *testing.T) {
	_, err := New()
	if err != ErrRedisRequired {
		t.Errorf("redis required")
	}

	qi, err := New(WithRedis(makeMockRediser()))
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	q := qi.(*impl)
	if q.redis == nil {
		t.Errorf("unexpected redis: %+v", q.redis)
	}
}

func TestNew_WithName(t *testing.T) {
	name := "test.queue"
	qi, err := New(WithName(name), WithRedis(makeMockRediser()))
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	q := qi.(*impl)
	if q.name != name {
		t.Errorf("exepect: %s, got: %s", name, q.name)
	}

	if !strings.HasPrefix(q.scheduleSetName, name) {
		t.Errorf("unexpected: %s", q.scheduleSetName)
	}
}

func TestNew_WithJobScorer(t *testing.T) {
	qi, err := New(
		WithJobScorer(func(job.Job) float64 { return 42.0 }),
		WithRedis(makeMockRediser()),
	)
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	q := qi.(*impl)
	j := job.New()
	if q.getJobScore(*j) != 42.0 {
		t.Errorf("unexpected getJobScore")
	}
}

func TestNew_WithMaxScoreGetter(t *testing.T) {
	qi, err := New(
		WithMaxScoreGetter(func(queue.PriorityQueue) string { return "test.score" }),
		WithRedis(makeMockRediser()),
	)
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	q := qi.(*impl)
	if q.getMaxScore(q) != "test.score" {
		t.Errorf("unexpected getMaxScore")
	}
}

func TestQueue_EnqueueDequeue(t *testing.T)       {}
func TestPriorityQueue_ScheduleLock(t *testing.T) {}
