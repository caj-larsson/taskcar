package taskrunner

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

const (
	ExitCodeSuccess     = 0
	ExitCodeRetry       = 75
	ExitCodeInterrupted = -1
	ExitCodeFailedToRun = -2
)

// ProcessTaskRunner is a task runner that executes tasks as a subprocess
type ProcessTaskRunner struct {
}

// RunTask executes a task using the process runner.
func (p *ProcessTaskRunner) RunTask(task Task, ctx context.Context) (*TaskResult, error) {
	result := &TaskResult{
		Status:   Failed,
		ExitCode: ExitCodeFailedToRun,
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", task.Command)
	cmd.Env = append(os.Environ(), task.EnvVars...)

	// Do not log env as it may contain secrets
	slog.Debug("Running command", "command", task.Command)

	cmdResult, err := RunCommand(cmd, task.InData)
	if err != nil {
		return nil, err
	}

	result.OutData = cmdResult.OutData
	result.Logs = cmdResult.ErrData
	result.ExitCode = cmdResult.ExitCode

	switch cmdResult.ExitCode {
	case ExitCodeSuccess:
		result.Status = Success
	case ExitCodeRetry:
		result.Status = Retry
	case ExitCodeInterrupted:
		if cmdResult.Interrupted {
			if ctx.Err() != nil {
				result.Status = Aborted
			} else {
				result.Status = ExternallyInterrupted
			}
		} else {
			// Failed to start
			result.Status = Failed
		}
	}
	return result, nil
}

type cmdResult struct {
	ExitCode    int
	Interrupted bool
	OutData     []byte
	ErrData     []byte
}

// RunCommand executes a command, pipes in the input data, reads the output and
// error data, returns exitCode and interrupted status.
func RunCommand(cmd *exec.Cmd, stdinData []byte) (*cmdResult, error) {
	outPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	errPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	inPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	var outData, errData []byte
	var outErr, errErr error
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		errData, errErr = io.ReadAll(errPipe)
	}()
	go func() {
		defer wg.Done()
		outData, outErr = io.ReadAll(outPipe)
	}()

	if _, err = inPipe.Write(stdinData); err != nil {
		return nil, fmt.Errorf("failed to write to stdin: %w", err)
	}
	if err := inPipe.Close(); err != nil {
		return nil, fmt.Errorf("failed to close stdin: %w", err)
	}
	// Wait for pipes to be read
	wg.Wait()

	if errErr != nil {
		// Not a critical issue, cmd.Wait will capture the error
		slog.Error("ProcessTaskRunner: Error reading stderr", "error", errErr)
	}
	if outErr != nil {
		// Not a critical issue, cmd.Wait will capture the error
		slog.Error("ProcessTaskRunner: Error reading stdout", "error", outErr)
	}
	exitCode := ExitCodeSuccess
	signalled := false
	slog.Debug("waiting for command to finish")
	if err := cmd.Wait(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
			if exitCode == ExitCodeInterrupted {
				slog.Debug("Exit code-1, checking for signal")
				if status, ok := exitError.Sys().(syscall.WaitStatus); ok {
					if status.Signaled() {
						slog.Debug("Process was signalled")
						signalled = true
					}
				}
			}
		} else {
			return nil, err
		}
	}
	return &cmdResult{
		ExitCode:    exitCode,
		Interrupted: signalled,
		OutData:     outData,
		ErrData:     errData,
	}, nil
}
