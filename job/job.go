// Job provides execute context.
package job

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// JobType categorizes jobs.
type JobType string

const JobTypeUnknown JobType = "unknown"

const jobIdSize = 16

type Job struct {
	// unique identity for this job, used in bloomfilter
	Identity []byte `json:"identity"`
	// job marshaled payload
	Payload []byte `json:"payload"`

	// job meta (internal use)
	Id          []byte    `json:"id"`
	Type        JobType   `json:"type"`
	CreatedAt   time.Time `json:"created_at"`
	FinishedAt  time.Time `json:"finished_at"`
	AvailableAt time.Time `json:"available_at"` // when can we run the job
}

func genJobId() (id []byte) {
	id = make([]byte, jobIdSize)
	rand.Read(id)
	return
}

func New() *Job {
	return &Job{
		Id:          genJobId(),
		Type:        JobTypeUnknown,
		CreatedAt:   time.Now(),
		AvailableAt: time.Now(),
	}
}

// NewFromBytes decode job from bytes
func NewFromBytes(raw []byte) (j *Job, err error) {
	j = &Job{}
	err = json.Unmarshal(raw, j)

	return
}

// Marshal a job to bytes
func (j Job) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func (j Job) String() string {
	return fmt.Sprintf("<job:%s>", j.Type)
}
