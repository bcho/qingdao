package executor

import (
	"github.com/bcho/qingdao/job"
	"golang.org/x/net/context"
)

// JobExecutor defines a job execution.
//
// The first return value represents new jobs generated from this execution.
type JobExecutor func(context.Context, *job.Job) ([]*job.Job, error)

// Registered job executors.
var jobExecutors = map[job.JobType]JobExecutor{}

// Register a job executor for given job type.
func RegisterJobExecutor(t job.JobType, e JobExecutor) {
	jobExecutors[t] = e
}

// Execute a job.
func ExecuteJob(c context.Context, job *job.Job) ([]*job.Job, error) {
	if executor, exists := jobExecutors[job.Type]; exists {
		return executor(c, job)
	} else {
		return nil, ErrJobNotExecuted
	}
}
