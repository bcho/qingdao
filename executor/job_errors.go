package executor

import (
	"fmt"
	"time"

	"github.com/bcho/qingdao/job"
	"golang.org/x/net/context"
)

func IsFatalError(err error) bool {
	if err == nil {
		return false
	}

	switch err := err.(type) {
	case ErrJobControl:
		return err.IsFatal()
	default:
		return true
	}
}

// Job execution control error.
type ErrJobControl interface {
	Handle(context.Context, *job.Job, Executor)
	Error() string
	IsFatal() bool
}

type errJobNotExecuted struct{}

func (e errJobNotExecuted) Handle(c context.Context, job *job.Job, _ Executor) {}

func (e errJobNotExecuted) Error() string { return "job has not been executed" }
func (e errJobNotExecuted) IsFatal() bool { return false }

// This job has not been executed.
var ErrJobNotExecuted = errJobNotExecuted{}

// This job has to wait for some time.
type ErrJobWaitFor struct {
	WaitFor time.Duration
}

func (e ErrJobWaitFor) Handle(_ context.Context, job *job.Job, executor Executor) {
	job.AvailableAt = job.AvailableAt.Add(e.WaitFor)
	executor.NewJobChan() <- job
}

func (e ErrJobWaitFor) Error() string {
	return fmt.Sprintf("job should wait for %s", e.WaitFor)
}

func (e ErrJobWaitFor) IsFatal() bool { return false }
