package brine

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecutionErrorMessage(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("root cause")

	tests := []struct {
		name string
		err  *ExecutionError
		want string
	}{
		{
			name: "nil receiver returns sentinel text",
			err:  nil,
			want: ErrExecution.Error(),
		},
		{
			name: "nil result nil cause returns sentinel text",
			err:  &ExecutionError{},
			want: ErrExecution.Error(),
		},
		{
			name: "nil result with cause includes cause",
			err:  &ExecutionError{cause: sentinel},
			want: fmt.Sprintf("%s: %v", ErrExecution, sentinel),
		},
		{
			name: "result with expected minions reports count",
			err: &ExecutionError{
				JID: "20240101000000000001",
				Result: &Result{
					Request:  &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
					Expected: []string{"minion-1", "minion-2"},
					ByMinion: map[string]MinionResult{
						"minion-1": {Minion: "minion-1", RetCode: 1, Failure: &Failure{Kind: FailureRetCode}},
					},
					Missing: []string{"minion-2"},
				},
			},
			want: "salt execution failed: 2 of 2 minions failed (jid: 20240101000000000001)",
		},
		{
			name: "result-level failure uses message",
			err: NewExecutionError(&Result{
				Request: &Request{Kind: KindLocal, Target: Glob("*"), Function: "test.ping"},
				Failure: &Failure{Kind: FailureNoReturn, Message: "Salt target matched no minions"},
			}, nil),
			want: "salt execution failed: Salt target matched no minions (jid: )",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.err.Error())
		})
	}
}

func TestExecutionErrorIs(t *testing.T) {
	t.Parallel()

	err := NewExecutionError(nil, nil)
	assert.ErrorIs(t, err, ErrExecution)
}
