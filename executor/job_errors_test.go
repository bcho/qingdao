package executor

import (
	"errors"
	"testing"
	"time"
)

func TestIsFatalError(t *testing.T) {
	cases := []struct {
		is  bool
		err error
	}{
		{false, nil},
		{true, errors.New("normal error")},
		{false, ErrJobNotExecuted},
		{false, ErrJobWaitFor{time.Second}},
	}

	for _, c := range cases {
		if IsFatalError(c.err) != c.is {
			t.Errorf("expect: %t, got: %t", c.is, !c.is)
		}
	}
}
