package taskrunner

import (
	"context"
	"errors"
	"time"
)

type TaskStatus int

const (
	Success TaskStatus = iota
	Aborted
	ExternallyInterrupted
	Retry
	Failed
)

type Task struct {
	Id        int64
	Queue     string
	CreatedAt time.Time
	Command   string
	Dir       string
	EnvVars   []string
	InData    []byte
}

type TaskResult struct {
	ExitCode int
	Status   TaskStatus
	OutData  []byte
	Logs     []byte
}

type TaskRunner interface {
	RunTask(task Task, ctx context.Context) (*TaskResult, error)
}

func NewTaskRunnerFunc(runner_type string) (TaskRunner, error) {
	switch runner_type {
	case "process":
		return &ProcessTaskRunner{}, nil
	default:
		return nil, errors.New("unknown task runner type")
	}
}
