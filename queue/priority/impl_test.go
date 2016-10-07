package priority

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

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

func makeTestQueue(t *testing.T, optSetters ...optSetter) *impl {
	optSetters = append(
		optSetters,
		WithName("test-queue"),
		WithRedis(makeRealRediser()),
	)
	qi, err := New(optSetters...)
	if err != nil {
		t.Fatalf("makeTestQueue: %+v", err)
	}

	if err := qi.Truncate(context.Background()); err != nil {
		t.Fatalf("makeTestQueue: unable to truncate: %+v", err)
	}

	q, ok := qi.(*impl)
	if !ok {
		t.Fatalf("makeTestQueue: unable to new `*impl`")
	}

	return q
}
func isJob(t *testing.T, j *job.Job, expected ...*job.Job) {
	for _, expectedJob := range expected {
		if bytes.Compare(j.Id, expectedJob.Id) == 0 {
			return
		}
	}

	t.Fatalf("isJob: %s != %+v", j, expected)
}

func TestQueue_EnqueueDequeue(t *testing.T) {
	q := makeTestQueue(t)
	ctx := context.Background()

	defer func() {
		if err := q.StopSchedule(ctx); err != nil {
			t.Fatalf("StopSchedule: %+v", err)
		}
	}()

	j1 := job.New()
	j2 := job.New()

	if err := q.Enqueue(ctx, j1); err != nil {
		t.Fatalf("Enqueue: %+v", err)
	}

	if err := q.Enqueue(ctx, j2); err != nil {
		t.Fatalf("Enqueue: %+v", err)
	}

	_, err := q.Dequeue(ctx, 1*time.Millisecond)
	if err != ErrNotScheduling {
		t.Fatalf("Dequeue requires scheduling")
	}

	var startWg sync.WaitGroup
	startWg.Add(1)
	go func() {
		startWg.Done()
		if err := q.StartSchedule(ctx); err != nil {
			t.Fatalf("StartSchedule: %+v", err)
		}
	}()

	startWg.Wait()

	dj1, err := q.Dequeue(ctx, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("Dequeue: %+v", err)
	}
	isJob(t, dj1, j1, j2)

	dj2, err := q.Dequeue(ctx, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("Dequeue: %+v", err)
	}
	isJob(t, dj2, j1, j2)

	dj3, err := q.Dequeue(ctx, 3*time.Second)
	if err != queue.ErrTimeout {
		t.Fatalf("Dequeue should be timeout: %+v %+v", err, dj3)
	}
}

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

func TestPriorityQueue_WithPriorityByAvailable(t *testing.T) {
	q := makeTestQueue(t, WithPriorityByAvailableAt())
	ctx := context.Background()

	defer func() {
		if err := q.StopSchedule(ctx); err != nil {
			t.Fatalf("StopSchedule: %+v", err)
		}
	}()

	var startWg sync.WaitGroup
	startWg.Add(1)
	go func() {
		startWg.Done()
		if err := q.StartSchedule(ctx); err != nil {
			t.Fatalf("StartSchedule: %+v", err)
		}
	}()

	startWg.Wait()

	j1 := job.New()
	j1.AvailableAt = time.Now().Add(150 * time.Millisecond)
	if err := q.Enqueue(ctx, j1); err != nil {
		t.Fatalf("Enqueue: %+v", err)
	}

	j2 := job.New()
	j2.AvailableAt = time.Now().Add(100 * time.Millisecond)
	if err := q.Enqueue(ctx, j2); err != nil {
		t.Fatalf("Enqueue: %+v", err)
	}

	dj, err := q.Dequeue(ctx, 1*time.Second)
	if err != nil {
		t.Fatalf("Dequeue: %+v", err)
	}
	isJob(t, dj, j2)

	dj, err = q.Dequeue(ctx, 1*time.Millisecond)
	if err != nil {
		t.Fatalf("Dequeue: %+v", err)
	}
	isJob(t, dj, j1)

	dj, err = q.Dequeue(ctx, 3*time.Second)
	if err != queue.ErrTimeout {
		t.Fatalf("Dequeue should be timeout: %+v %+v", err, dj)
	}
}
