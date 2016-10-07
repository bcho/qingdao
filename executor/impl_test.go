package executor

import (
	"os"
	"sync"
	"testing"

	"golang.org/x/net/context"

	"github.com/bcho/qingdao/queue"
	"github.com/bcho/qingdao/queue/priority"
	"gopkg.in/redis.v4"
)

func makeRedis() *redis.Client {
	addr := os.Getenv("QINGDAO_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	return redis.NewClient(&redis.Options{Addr: addr})
}

func makeTestQueue(t *testing.T) queue.Queue {
	q, err := priority.New(
		priority.WithName("test-queue-executor"),
		priority.WithRedis(makeRedis()),
	)
	if err != nil {
		t.Fatalf("makeTestQueue: %+v", err)
	}

	if err := q.Truncate(context.Background()); err != nil {
		t.Fatalf("makeTestQueue: unable to truncate: %+v", err)
	}

	return q
}

func TestNew_WithQueue(t *testing.T) {
	_, err := New()
	if err != ErrQueueRequired {
		t.Errorf("queue required")
	}

	ei, err := New(WithQueue(makeTestQueue(t)))
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	e := ei.(*impl)
	if e.q == nil {
		t.Errorf("unexpected queue")
	}
}

func TestNew_WithExecutors(t *testing.T) {
	ei, err := New(WithQueue(makeTestQueue(t)))
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	e := ei.(*impl)
	if e.executorsCount <= 0 {
		t.Errorf("expect executors has default value, got: %d", e.executorsCount)
	}

	count := 10
	ei, err = New(
		WithQueue(makeTestQueue(t)),
		WithExecutors(count),
	)
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	e = ei.(*impl)
	if e.executorsCount != count {
		t.Errorf("expect executors count: %d, got: %d", count, e.executorsCount)
	}
}

func TestNew_BeforeStart(t *testing.T) {
	ei, err := New(
		WithQueue(makeTestQueue(t)),
		BeforeStart(func(context.Context, Executor, ExecutorState) error { return nil }),
	)
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	e := ei.(*impl)
	if e.hookBeforeStart == nil {
		t.Errorf("hook BeforeStart should not be nil")
	}
}

func TestNew_AfterStop(t *testing.T) {
	ei, err := New(
		WithQueue(makeTestQueue(t)),
		AfterStop(func(context.Context, Executor, ExecutorState) error { return nil }),
	)
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	e := ei.(*impl)
	if e.hookAfterStop == nil {
		t.Errorf("hook AfterStop should not be nil")
	}
}

func TestExecutor_Basic(t *testing.T) {
	ctx := context.Background()

	e, err := New(WithQueue(makeTestQueue(t)))
	if err != nil {
		t.Errorf("unexpected %+v", err)
	}

	if e.State() != ExecutorStateStopped {
		t.Fatalf("unexpected state: %s", e.State())
	}

	var startWg, stopWg sync.WaitGroup

	startWg.Add(1)
	stopWg.Add(1)
	go func() {
		startWg.Done()
		if err := e.Start(ctx); err != nil {
			t.Fatalf("Start: %+v", err)
		}
		stopWg.Done()
	}()

	startWg.Wait()

	if e.ErrChan() == nil {
		t.Fatalf("ErrChan should not be nil")
	}

	if e.NewJobChan() == nil {
		t.Fatalf("NewJobChan should not be nil")
	}

	if e.State() != ExecutorStateRunning {
		t.Fatalf("unexpected state: %s", e.State())
	}

	if err := e.Start(ctx); err != ErrRunning {
		t.Fatalf("start twice")
	}

	if err := e.Stop(ctx); err != nil {
		t.Fatalf("Stop: %+v")
	}

	stopWg.Wait()

	if e.State() != ExecutorStateStopped {
		t.Fatalf("unexpected state: %s", e.State())
	}
}

func TestExecutor_ExecuteWithoutExecutor(t *testing.T) {}
func TestExecutor_ExecuteWithError(t *testing.T)       {}
func TestExecutor_ExecuteWithNewJobs(t *testing.T)     {}
