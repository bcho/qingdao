// Queue implements job queue.
package queue

import (
	"time"

	"golang.org/x/net/context"

	"github.com/bcho/qingdao/job"
)

// Basic queue.
type Queue interface {
	// Enqueue a job.
	Enqueue(context.Context, *job.Job) error

	// Dequeue a job with timeout.
	// Returns `ErrTimeout` when timeout.
	Dequeue(context.Context, time.Duration) (*job.Job, error)

	// Truncate job queue.
	Truncate(context.Context) error
}

// Queue with priority.
type PriorityQueue interface {
	Queue

	// Start scheduling. (BLOCK)
	StartSchedule(context.Context) error

	// Stop scheduling.
	StopSchedule(context.Context) error
}
