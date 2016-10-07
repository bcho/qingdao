// Executor implments jobs executor.
package executor

import (
	"github.com/bcho/qingdao/job"
	"golang.org/x/net/context"
)

type Executor interface {
	// Start executor. (BLOCK)
	Start(context.Context) error

	// Stop executor.
	Stop(context.Context) error

	// Get executor's state
	State() ExecutorState

	// ErrChan returns executor's error channel.
	ErrChan() chan error

	// NewJobChan returns executor's new job channel.
	//
	// This channel can be used to sent new job to queue.
	NewJobChan() chan *job.Job
}

type ExecutorState string

const (
	ExecutorStateStopped ExecutorState = "stopped"
	ExecutorStateRunning               = "running"
)

type ExecutorStateHookFn func(context.Context, Executor, ExecutorState) error
