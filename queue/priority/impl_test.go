package priority

import (
	"strings"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"github.com/bcho/qingdao/job"
	"github.com/bcho/qingdao/queue"
)

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

func makeTestQueue(t *testing.T) *impl {
	qi, err := New(
		WithName("test-queue"),
		WithRedis(makeRealRediser()),
	)
	if err != nil {
		t.Fatalf("makeTestQueue: %+v", err)
	}

	q, ok := qi.(*impl)
	if !ok {
		t.Fatalf("makeTestQueue: unable to new `*impl`")
	}

	return q
}

func TestQueue_EnqueueDequeue(t *testing.T) {}

func TestPriorityQueue_ScheduleLock(t *testing.T) {
	q := makeTestQueue(t)

	var (
		startWg = sync.WaitGroup{}
		stopWg  = sync.WaitGroup{}
		ctx     = context.Background()
	)

	if q.isScheduling() {
		t.Fatalf("queue should not be scheduling")
	}

	startWg.Add(1)
	stopWg.Add(1)
	go func() {
		startWg.Done()
		if err := q.StartSchedule(ctx); err != nil {
			t.Fatalf("StartSchedule: %+v", err)
		}
		stopWg.Done()
	}()

	startWg.Wait()

	if !q.isScheduling() {
		t.Fatalf("queue should be scheduling")
	}

	if q.StartSchedule(ctx) != ErrScheduling {
		t.Fatalf("StartSchedule starts twice")
	}

	if err := q.StopSchedule(ctx); err != nil {
		t.Fatalf("StopSchedule: %+v", err)
	}

	stopWg.Wait()

	if q.isScheduling() {
		t.Fatalf("queue should not be scheduling")
	}
}
