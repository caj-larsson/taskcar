package taskrunner_test

import (
	"context"

	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"taskcar/taskrunner"
	"testing"
	"time"
)

func TestCmdRunner(t *testing.T) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "sh", "-c", "echo '123' > /dev/stdout && echo 'logs' > /dev/stderr")

	cmdRes, err := taskrunner.RunCommand(cmd, []byte{})

	if err != nil {
		t.Fatalf("Failed to run command: %v", err)
	}

	assert.Equal(t, 0, cmdRes.ExitCode, "Expected exit code 0")
	assert.Equal(t, []byte("123\n"), cmdRes.OutData, "Expected output data to match input data")
	assert.Equal(t, []byte("logs\n"), cmdRes.ErrData, "Expected logs to be logs")
	assert.Equal(t, false, cmdRes.Interrupted, "Expected task not be interrupted")
}

func TestProcessRunner(t *testing.T) {
	ctx := context.Background()
	runner, err := taskrunner.NewTaskRunnerFunc("process")
	if err != nil {
		t.Fatalf("Failed to create task runner: %v", err)
	}

	task := taskrunner.Task{
		Command: "echo $TEST_VAR && echo logs > /dev/stderr",
		EnvVars: []string{"TEST_VAR=123"},
		InData:  []byte{},
	}

	result, err := runner.RunTask(task, ctx)
	if err != nil {
		t.Fatalf("Failed to run task: %v", err)
	}

	assert.Equal(t, 0, result.ExitCode, "Expected exit code 0")
	assert.Equal(t, taskrunner.Success, result.Status, "Expected task status to be Success")
	assert.Equal(t, []byte("123\n"), result.OutData, "Expected output data to match input data")
	assert.Equal(t, []byte("logs\n"), result.Logs, "Expected logs to be logs")
}

func TestProcessRunnerWithError(t *testing.T) {
	ctx := context.Background()
	runner, err := taskrunner.NewTaskRunnerFunc("process")
	if err != nil {
		t.Fatalf("Failed to create task runner: %v", err)
	}

	task := taskrunner.Task{
		Command: "exit 1",
		EnvVars: []string{},
		InData:  []byte("test input"),
	}

	result, err := runner.RunTask(task, ctx)
	if err != nil {
		t.Fatalf("Failed to run task: %v", err)
	}

	assert.Equal(t, 1, result.ExitCode, "Expected exit code 1")
	assert.Equal(t, taskrunner.Failed, result.Status, "Expected task status to be Failed")
	assert.Equal(t, []byte{}, result.OutData, "Expected output data to be empty")
	assert.Equal(t, []byte{}, result.Logs, "Expected logs to be empty")
}

func TestProcessRunnerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	runner, err := taskrunner.NewTaskRunnerFunc("process")
	if err != nil {
		t.Fatalf("Failed to create task runner: %v", err)
	}

	task := taskrunner.Task{
		Command: "echo outdata && echo logdata > /dev/stderr && sleep 5",
		EnvVars: []string{},
		InData:  []byte("test input"),
	}

	// Simulate cancellation after 1 second
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	result, err := runner.RunTask(task, ctx)
	if err != nil {
		t.Fatalf("Failed to run task: %v", err)
	}

	assert.Equal(t, taskrunner.Aborted, result.Status, "Expected task status to be Aborted")
	assert.Equal(t, []byte("outdata\n"), result.OutData, "Expected output data to be empty")
	assert.Equal(t, []byte("logdata\n"), result.Logs, "Expected logs to be empty")
}

func TestProcessRunnerRetry(t *testing.T) {
	ctx := context.Background()
	runner, err := taskrunner.NewTaskRunnerFunc("process")
	if err != nil {
		t.Fatalf("Failed to create task runner: %v", err)
	}

	task := taskrunner.Task{
		Command: "exit 75",
		EnvVars: []string{},
		InData:  []byte("test input"),
	}

	result, err := runner.RunTask(task, ctx)
	if err != nil {
		t.Fatalf("Failed to run task: %v", err)
	}

	assert.Equal(t, taskrunner.Retry, result.Status, "Expected task status to be Retry")
	assert.Equal(t, []byte{}, result.OutData, "Expected output data to be empty")
	assert.Equal(t, []byte{}, result.Logs, "Expected logs to be empty")
}

func findChildSleep() (int, error) {
	// Get the current process PID
	currentPID := os.Getpid()

	// Use ps to find children
	cmd := exec.Command("ps", "-o", "pid,comm", "--ppid", fmt.Sprintf("%d", currentPID), "--noheaders")
	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("failed to find child processes: %v", err)
	}

	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		if line == "" {
			continue
		}
		var pid int
		var comm string
		_, err := fmt.Sscanf(strings.TrimSpace(line), "%d %s", &pid, &comm)
		if err == nil {
			if comm == "sleep" {
				return pid, nil
			}
		} else {
			return 0, fmt.Errorf("failed to parse output: %v", err)
		}
	}
	return 0, fmt.Errorf("no child sleep process found")
}

func TestProcessRunnerExternallyInterrupted(t *testing.T) {
	ctx := context.Background()
	runner, err := taskrunner.NewTaskRunnerFunc("process")
	if err != nil {
		t.Fatalf("Failed to create task runner: %v", err)
	}

	task := taskrunner.Task{
		Command: "sleep 5",
		EnvVars: []string{},
		InData:  []byte("test input"),
	}

	// Find the process ID of the task and send a SIGINT signal
	go func() {
		time.Sleep(50 * time.Millisecond)
		sleepPID, err := findChildSleep()
		if err != nil {
			panic(err)
		}
		if err := syscall.Kill(sleepPID, syscall.SIGINT); err != nil {
			panic(err)
		}
	}()

	result, err := runner.RunTask(task, ctx)
	if err != nil {
		t.Fatalf("Failed to run task: %v", err)
	}

	assert.Equal(t, taskrunner.ExternallyInterrupted, result.Status, "Expected task status to be ExternallyInterrupted")
	assert.Equal(t, []byte{}, result.OutData, "Expected output data to be empty")
	assert.Equal(t, []byte{}, result.Logs, "Expected logs to be empty")
}
