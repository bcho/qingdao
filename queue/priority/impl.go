// Priority queue implementation
package priority

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bcho/qingdao/job"
	"github.com/bcho/qingdao/queue"

	"golang.org/x/net/context"
	redis "gopkg.in/redis.v4"
)

const defaultName = "qd.queue.priority"

var (
	ErrRedisRequired = errors.New("redis client requird. set with `WithRedis`")
	ErrScheduling    = errors.New("queue is already scheduling")
	ErrNotScheduling = errors.New("queue is not scheduling")
)

type optSetter func(*impl) error

type rediser interface {
	Watch(func(*redis.Tx) error, ...string) error
	Del(...string) *redis.IntCmd
	BLPop(time.Duration, ...string) *redis.StringSliceCmd
	ZAdd(string, ...redis.Z) *redis.IntCmd
	ZRangeByScore(string, redis.ZRangeBy) *redis.StringSliceCmd
}

// Set redis client for queue.
func WithRedis(c rediser) optSetter {
	return func(q *impl) error {
		q.redis = c
		return nil
	}
}

// Set queue name.
func WithName(name string) optSetter {
	return func(q *impl) error {
		q.name = name
		return nil
	}
}

// Set job score function.
//
// If not score function set, every job has the same priority 1.0
func WithJobScorer(scorer JobScorer) optSetter {
	return func(q *impl) error {
		q.getJobScore = scorer
		return nil
	}
}

// Set job max score getter function.
//
// Max score will be used as `ZRANGEBYSCORE` max value.
// If no getter set, `+inf` will be used.
func WithMaxScoreGetter(getter MaxScoreGetter) optSetter {
	return func(q *impl) error {
		q.getMaxScore = getter
		return nil
	}
}

// JobScorer calculate job score.
type JobScorer func(job.Job) float64

// MaxScoreGetter calculate max job score in this batch.
type MaxScoreGetter func(queue.PriorityQueue) string

// Sort jobs with `AvailableAt`
func WithPriorityByAvailableAt(q *impl) error {
	q.getJobScore = func(j job.Job) float64 {
		return float64(j.AvailableAt.Unix())
	}
	q.getMaxScore = func(queue.PriorityQueue) string {
		return fmt.Sprintf("%d", time.Now().Unix())
	}

	return nil
}

type impl struct {
	name string

	getJobScore JobScorer
	getMaxScore MaxScoreGetter

	scheduleLock    *sync.RWMutex
	scheduleSetName string
	scheduling      bool

	redis rediser
}

// New creates a priority queue with options.
func New(setters ...optSetter) (queue.PriorityQueue, error) {
	q := &impl{
		name: defaultName,

		scheduleLock: &sync.RWMutex{},
		scheduling:   false,
	}

	for _, setter := range setters {
		if err := setter(q); err != nil {
			return nil, err
		}
	}

	if q.redis == nil {
		return nil, ErrRedisRequired
	}

	q.scheduleSetName = fmt.Sprintf("%s.zset", q.name)

	return q, nil
}

func (q impl) Truncate(ctx context.Context) error {
	return q.redis.Del(q.name, q.scheduleSetName).Err()
}

func (q impl) Enqueue(ctx context.Context, job *job.Job) error {
	jobData, err := job.Marshal()
	if err != nil {
		return err
	}

	var jobScore float64
	if q.getJobScore == nil {
		jobScore = 1.0
	} else {
		jobScore = q.getJobScore(*job)
	}

	return q.redis.ZAdd(
		q.scheduleSetName,
		redis.Z{Score: jobScore, Member: jobData},
	).Err()
}

func (q impl) Dequeue(ctx context.Context, timeout time.Duration) (*job.Job, error) {
	if !q.isScheduling() {
		return nil, ErrNotScheduling
	}

	d, err := q.redis.BLPop(timeout, q.name).Result()
	if err == redis.Nil {
		return nil, queue.ErrTimeout
	}
	if err != nil {
		return nil, err
	}

	return job.NewFromBytes([]byte(d[1]))
}

func (q impl) isScheduling() bool {
	q.scheduleLock.RLock()
	defer q.scheduleLock.RUnlock()
	return q.scheduling
}

func (q *impl) StartSchedule(context.Context) error {
	q.scheduleLock.Lock()
	if q.scheduling {
		q.scheduleLock.Unlock()
		return ErrScheduling
	} else {
		q.scheduling = true
		q.scheduleLock.Unlock()
	}

	for {
		var maxScore string
		if q.getMaxScore == nil {
			maxScore = "+inf"
		} else {
			maxScore = q.getMaxScore(q)
		}

		q.redis.Watch(func(tx *redis.Tx) error {
			rv := q.redis.ZRangeByScore(
				q.scheduleSetName,
				redis.ZRangeBy{
					Min:    "-inf",
					Max:    maxScore,
					Offset: 0,
					Count:  1,
				},
			)
			values, err := rv.Result()
			if err != nil || len(values) != 1 {
				tx.Unwatch(q.scheduleSetName)
				time.Sleep(1 * time.Second)
				return err
			}

			_, err = tx.MultiExec(func() error {
				jobData := values[0]
				tx.RPush(q.name, jobData)
				tx.ZRem(q.scheduleSetName, jobData)
				return nil
			})

			return err
		}, q.scheduleSetName)

		if !q.isScheduling() {
			break
		}
	}

	return nil
}

func (q *impl) StopSchedule(context.Context) error {
	q.scheduleLock.Lock()
	q.scheduling = false
	q.scheduleLock.Unlock()

	return nil
}
