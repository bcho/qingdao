package executor

import (
	"errors"
	"testing"

	"github.com/bcho/qingdao/job"
	"golang.org/x/net/context"
)

func TestRegisterJobExecutor(t *testing.T) {
	resetJobExecutors()
	mockExecutor := func(context.Context, *job.Job) ([]*job.Job, error) {
		return nil, nil
	}
	mockJobType := job.JobType("mock-job-type")

	if _, exist := jobExecutors[mockJobType]; exist {
		t.Errorf("should not contain executor")
	}

	RegisterJobExecutor(mockJobType, mockExecutor)

	if _, exist := jobExecutors[mockJobType]; !exist {
		t.Errorf("should contain executor")
	}
}

func TestExecuteJob(t *testing.T) {
	resetJobExecutors()
	ctx := context.Background()
	j := job.New()

	var (
		newJobs []*job.Job
		err     error
	)

	newJobs, err = ExecuteJob(ctx, j)
	if err != ErrJobNotExecuted {
		t.Errorf("job should not be executed")
	}
	if newJobs != nil {
		t.Errorf("should not produce new jobs")
	}

	mockErr := errors.New("mock error")

	RegisterJobExecutor(
		j.Type,
		func(context.Context, *job.Job) ([]*job.Job, error) {
			return []*job.Job{job.New(), job.New()}, mockErr
		},
	)

	newJobs, err = ExecuteJob(ctx, j)
	if err != mockErr {
		t.Errorf("expect mockErr, got: %+v", err)
	}
	if len(newJobs) != 2 {
		t.Errorf("unexpecte new jobs: %+v", newJobs)
	}
}
