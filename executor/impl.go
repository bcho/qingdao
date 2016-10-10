package executor

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/bcho/qingdao/job"
	"github.com/bcho/qingdao/queue"
)

var (
	ErrQueueRequired = errors.New("job queue required, use `WithQueue` to set")
	ErrRunning       = errors.New("executor is running")
	ErrStopped       = errors.New("executor has stopped")
)

type optSetter func(*impl) error

// WithQueue sets executor's job queue.
func WithQueue(q queue.Queue) optSetter {
	return func(e *impl) error {
		e.q = q
		return nil
	}
}

// WithExecutors sets child executors count.
func WithExecutors(count int) optSetter {
	return func(e *impl) error {
		e.executorsCount = count
		return nil
	}
}

// BeforeStart sets hook func runs before start.
func BeforeStart(h ExecutorStateHookFn) optSetter {
	return func(e *impl) error {
		e.hookBeforeStart = h
		return nil
	}
}

// AfterStop sets hook func runs after stop.
func AfterStop(h ExecutorStateHookFn) optSetter {
	return func(e *impl) error {
		e.hookAfterStop = h
		return nil
	}
}

func WithDequeueTimeout(timeout time.Duration) optSetter {
	return func(e *impl) error {
		e.jobDequeueTimeout = timeout
		return nil
	}
}

type reqStop struct {
	resp chan struct{}
}

func makeReqStop() reqStop {
	return reqStop{resp: make(chan struct{})}
}

func (r reqStop) ack() {
	r.resp <- struct{}{}
}

func (r reqStop) do(c chan reqStop) {
	c <- r
	<-r.resp
}

func (r reqStop) doAndClose(c chan reqStop) {
	r.do(c)
	close(c)
}

type impl struct {
	state   ExecutorState
	errChan chan error
	l       *sync.RWMutex

	q                 queue.Queue
	jobDequeueTimeout time.Duration

	hookBeforeStart ExecutorStateHookFn
	hookAfterStop   ExecutorStateHookFn

	// child executors
	executorsCount    int
	executorsJobChan  chan *job.Job
	executorsStopChan chan reqStop

	// accept new job
	newJobChan     chan *job.Job
	stopNewJobChan chan reqStop
}

// New creates an executor with options.
func New(setters ...optSetter) (Executor, error) {
	e := &impl{
		state:   ExecutorStateStopped,
		errChan: make(chan error, 1024),
		l:       &sync.RWMutex{},

		jobDequeueTimeout: 30 * time.Second,

		executorsCount:   4,
		executorsJobChan: make(chan *job.Job, 1024),

		newJobChan:     make(chan *job.Job, 1024),
		stopNewJobChan: make(chan reqStop),
	}

	for _, setter := range setters {
		if err := setter(e); err != nil {
			return nil, err
		}
	}

	e.executorsStopChan = make(chan reqStop, e.executorsCount)

	if e.q == nil {
		return nil, ErrQueueRequired
	}

	return e, nil
}

func (e *impl) State() ExecutorState {
	e.l.RLock()
	defer e.l.RUnlock()
	return e.state
}

func (e impl) ErrChan() chan error       { return e.errChan }
func (e impl) NewJobChan() chan *job.Job { return e.newJobChan }

func (e *impl) Start(c context.Context) error {
	e.l.Lock()

	if e.state == ExecutorStateRunning {
		e.l.Unlock()
		return ErrRunning
	}

	if e.hookBeforeStart != nil {
		if err := e.hookBeforeStart(c, e, ExecutorStateRunning); err != nil {
			e.l.Unlock()
			return err
		}
	}
	e.state = ExecutorStateRunning

	e.spawnExecutors(c, e.executorsCount)

	go e.acceptNewJob(c)
	e.l.Unlock()

	return e.readFromQueue(c)
}

func (e *impl) spawnExecutors(c context.Context, count int) {
	wg := &sync.WaitGroup{}
	for id := 0; id < count; id++ {
		wg.Add(1)
		go e.spawnExecutor(c, id, wg)
	}
	wg.Wait()
}

func (e *impl) spawnExecutor(c context.Context, executorId int, wg *sync.WaitGroup) {
	wg.Done()
	for {
		select {
		case req := <-e.executorsStopChan:
			req.ack()
			return
		case job := <-e.executorsJobChan:
			newJobs, exeErr := ExecuteJob(c, job)
			e.handleExecutionError(c, job, exeErr)
			e.acceptNewJobs(c, newJobs)
		}
	}
}

func (e *impl) handleExecutionError(c context.Context, job *job.Job, err error) {
	if err == nil {
		return
	}

	switch err := err.(type) {
	case ErrJobControl:
		err.Handle(c, job, e)
	default:
		e.errChan <- err
	}
}

func (e *impl) acceptNewJobs(c context.Context, jobs []*job.Job) {
	for _, newJob := range jobs {
		e.newJobChan <- newJob
	}
}

func (e *impl) acceptNewJob(c context.Context) {
	for {
		select {
		case req := <-e.stopNewJobChan:
			close(e.newJobChan)
			req.ack()
			return
		case job := <-e.newJobChan:
			// TODO check duplicate
			if err := e.q.Enqueue(c, job); err != nil {
				e.errChan <- err
			}
		}
	}
}

func (e *impl) readFromQueue(c context.Context) error {
	for {
		job, err := e.q.Dequeue(c, e.jobDequeueTimeout)
		if e.State() != ExecutorStateRunning {
			break
		}
		if err == queue.ErrTimeout {
			continue
		}
		if err != nil {
			e.errChan <- err
			continue
		}

		e.executorsJobChan <- job
	}

	close(e.executorsJobChan)
	return nil
}

func (e *impl) Stop(c context.Context) error {
	e.l.Lock()
	defer e.l.Unlock()

	if e.state != ExecutorStateRunning {
		return ErrStopped
	}

	if err := e.stopExecutors(); err != nil {
		return err
	}
	if err := e.stopAcceptNewJob(); err != nil {
		return err
	}

	e.state = ExecutorStateStopped

	if e.hookAfterStop != nil {
		e.hookAfterStop(c, e, ExecutorStateStopped)
	}

	return nil
}

func (e *impl) stopExecutors() error {
	for i := 0; i < e.executorsCount; i++ {
		req := makeReqStop()
		req.do(e.executorsStopChan)
	}
	close(e.executorsStopChan)

	return nil
}

func (e *impl) stopAcceptNewJob() error {
	makeReqStop().doAndClose(e.stopNewJobChan)

	return nil
}
