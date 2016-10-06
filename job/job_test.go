package job

import (
	"bytes"
	"testing"
)

func TestJob_New(t *testing.T) {
	job := New()

	if len(job.Id) != jobIdSize {
		t.Errorf("unexpected job id: %+v", job.Id)
	}

	if job.Type != JobTypeUnknown {
		t.Errorf("unexpected job type: %s", job.Type)
	}

	if job.CreatedAt.IsZero() {
		t.Errorf("CreatedAt should not be zero")
	}

	if !job.FinishedAt.IsZero() {
		t.Errorf("FinishedAt should be zero")
	}

	if job.AvailableAt.IsZero() {
		t.Errorf("AvailableAt should not be zero")
	}

	if job.Identity != nil {
		t.Errorf("Identity should be empty")
	}

	if job.Payload != nil {
		t.Errorf("Payload should be empty")
	}
}

func Job_NewFromBytes_Marshal(t *testing.T) {
	job := New()
	job.Payload = []byte("test payload")
	job.Identity = []byte("test identity")

	marshaled, err := job.Marshal()
	if err != nil {
		t.Errorf("marshal failed: %+v", err)
	}

	unmarshaledJob, err := NewFromBytes(marshaled)
	if err != nil {
		t.Errorf("NewFromBytes failed: %+v", err)
	}

	if bytes.Compare(unmarshaledJob.Id, job.Id) != 0 {
		t.Errorf("job id mismatch")
	}

	if unmarshaledJob.Type != job.Type {
		t.Errorf("job type mismatch")
	}

	if bytes.Compare(unmarshaledJob.Identity, job.Identity) != 0 {
		t.Errorf("job identity mismatch")
	}

	if bytes.Compare(unmarshaledJob.Payload, job.Payload) != 0 {
		t.Errorf("job payload mismatch")
	}
}
