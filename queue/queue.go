package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"taskcar/config"
	"taskcar/db"
	"taskcar/listener"
	"taskcar/taskrunner"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Queue represents a task queue that processes tasks from a PostgreSQL database.
type Queue struct {
	NodeId int64
	Name   string
	config config.QueueConfig
	pool   *pgxpool.Pool
	// checkChan signals that we must check if there are tasks available
	checkChan  listener.MessageChan
	taskRunner taskrunner.TaskRunner
	// envVars are the environment variables to be used for the task
	envVars []string
	// ctx is used to cancel operations of the queue
	ctx context.Context
	// dbCtx is separate context for database operations, so during cleanup there
	// is no race condition between task clean up and database connection shutdown.
	dbCtx    context.Context
	dbCancel context.CancelFunc
	// Done is a channel that is closed when the queue is shut down
	Done chan struct{}
}

// New creates a new Queue instance
func New(pool *pgxpool.Pool, nodeId int64, checkChan listener.MessageChan, cfg config.QueueConfig, ctx context.Context) (*Queue, error) {
	if cfg.QueueName == "" {
		return nil, errors.New("queue name is required")
	}
	runner, err := taskrunner.NewTaskRunnerFunc("process")
	if err != nil {
		return nil, err
	}
	dbCtx, dbCancel := context.WithCancel(context.Background())

	q := &Queue{
		NodeId:     nodeId,
		Name:       cfg.QueueName,
		config:     cfg,
		pool:       pool,
		checkChan:  checkChan,
		taskRunner: runner,
		ctx:        ctx,
		dbCtx:      dbCtx,
		dbCancel:   dbCancel,
		Done:       make(chan struct{}, 1),
	}

	err = q.setSecrets()
	if err != nil {
		return nil, fmt.Errorf("failed to set secrets: %w", err)
	}

	return q, nil
}

// -- Queue Setup Methods --

// setSecrets fetches the secret values from the database then generates the Env
// format for the queues configured variable names and stores them in the queue.
func (q *Queue) setSecrets() error {
	secretNames := make([]string, len(q.config.Secrets))
	for i, secret := range q.config.Secrets {
		secretNames[i] = secret.Name
	}

	secrets := make(map[string]string)

	conn, err := q.pool.Acquire(q.dbCtx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()
	queries := db.New(conn)
	secretRows, err := queries.GetSecrets(q.dbCtx, secretNames)
	if err != nil {
		return fmt.Errorf("failed to get secrets: %w", err)
	}

	for _, secret := range secretRows {
		secrets[secret.Name] = secret.Value
	}

	q.envVars = q.config.Secrets.GenerateEnvSlice(secrets)

	return nil
}

// -- Task Queue Processing --

// ProcessTasksForever starts processing tasks in the queue indefinitely or
// until the context is done.
func (q *Queue) ProcessTasksForever() {
	for {
		if !q.processTaskAndWait() {
			break
		}
	}
}

// processTaskAndWait processesall available tasks and then wait for
// notification of new tasks or shutdown. Returns true if the queue should
// continue processing tasks, false if it should stop.
func (q *Queue) processTaskAndWait() bool {
	for {
		if !q.executeTasks() {
			break
		}
	}
	slog.Debug("Waiting for notification or termination")
	select {
	case msg := <-q.checkChan:
		slog.Debug("Received check message, reloading task", "reason", msg.Data)
		return true
	case <-q.ctx.Done():
		slog.Debug("Queue Ctx is done, canceling dbCtx")
		q.dbCancel()
		slog.Debug("Waiting for tasks to finish")
		<-q.dbCtx.Done()
		slog.Debug("Tasks finished, shutting down")
		close(q.Done)
		slog.Info("Queue Done", "queue", q.Name)
		return false
	}
}

// executeTasks processes all currently available tasks. Returns true unless
// processing failed irrecoverably or the context is done.
func (q *Queue) executeTasks() bool {
	if q.ctx.Err() != nil {
		slog.Debug("Queue worker: Context is done, exiting")
		return false
	}
	conn, err := q.pool.Acquire(q.dbCtx)
	if err != nil {
		slog.Error("Failed to acquire connection", "error", err)
		time.Sleep(1 * time.Second)
		return true
	}
	defer conn.Release()

	queries := db.New(conn)
	task, err := q.takeTask(queries)
	if err != nil {
		slog.Error("Failed to take task", "error", err)
		return true
	}
	if task == nil {
		slog.Debug("No task available, waiting for notification")
		return false
	}
	slog.Debug("Task available, processing", "taskId", task.Id)

	result, err := q.taskRunner.RunTask(*task, q.ctx)
	if err != nil {
		slog.Error("Failed to run task", "error", err)
		q.failTask(queries, task, &taskrunner.TaskResult{}, true)
		return true
	}

	if result.Status == taskrunner.Success {
		slog.Debug("Task completed successfully", "taskId", task.Id)
		err = q.completeTask(queries, task, result)
		if err != nil {
			slog.Error("Failed to complete task", "error", err)
			result.Status = taskrunner.Failed
		}
	}
	// Handle error after switch
	switch result.Status {
	case taskrunner.Failed:
		slog.Debug("Task failed permanently", "taskId", task.Id)
		err = q.failTask(queries, task, result, true)
	case taskrunner.Aborted:
		slog.Debug("Task aborted", "taskId", task.Id)
		err = q.failTask(queries, task, result, false)
	case taskrunner.Retry:
		slog.Debug("Task failed, retrying", "taskId", task.Id)
		err = q.failTask(queries, task, result, false)
		if err == nil {
			err = q.backoffTask(queries, task, result)
		}
	}

	if err != nil {
		slog.Error("Failed to process task", "error", err)
		return false
	}
	return true
}

// -- Task Operations --

// takeTask from the database making them unavailable to other workers
func (q *Queue) takeTask(queries *db.Queries) (*taskrunner.Task, error) {
	query := db.DequeTasksParams{
		NodeID: &q.NodeId,
		Queue:  q.Name,
		Limit:  1,
	}
	task, err := queries.DequeTasks(q.ctx, query)
	if err != nil {
		if pgx.ErrNoRows == err {
			return nil, nil
		}
		return nil, err
	}

	if task == nil {
		return nil, nil
	}

	if len(task) > 1 {
		return nil, errors.New("multiple tasks returned, expected only one")
	}

	return &taskrunner.Task{
		Id:        task[0].TaskID,
		Queue:     task[0].Queue,
		CreatedAt: task[0].CreatedAt.Time,
		Command:   q.config.Command,
		InData:    task[0].InData,
		EnvVars:   q.envVars,
	}, nil
}

type NewTask struct {
	Queue string          `json:"queue"`
	Data  json.RawMessage `json:"data"`
}

type TaskOutput struct {
	Data     json.RawMessage `json:"data"`
	NewTasks []NewTask       `json:"new_tasks"`
}

// completeTask creates  a completed task and cleans up the original task.
func (q *Queue) completeTask(
	queries *db.Queries,
	task *taskrunner.Task,
	result *taskrunner.TaskResult,
) error {
	slog.Info(
		"Completing task",
		"taskId", task.Id,
		"status", result.Status,
		"outData", string(result.OutData),
	)
	// TODO: remove the queries param and create one here for tx
	var outData TaskOutput
	err := json.Unmarshal(result.OutData, &outData)
	if err != nil {
		return fmt.Errorf("failed to unmarshal task output: %w", err)
	}

	err = queries.CreateCompletedTask(q.dbCtx, db.CreateCompletedTaskParams{
		TaskID:  task.Id,
		OutData: result.OutData,
		Log:     string(result.Logs),
	})
	if err != nil {
		slog.Error("Failed to complete task", "error", err)
		return err
	}

	err = queries.DeleteTask(q.dbCtx, task.Id)
	if err != nil {
		slog.Error("Failed to delete task", "error", err)
		return err
	}

	for _, newTask := range outData.NewTasks {
		_, err = queries.CreateTask(q.dbCtx, db.CreateTaskParams{
			Queue:  newTask.Queue,
			InData: newTask.Data,
		})
		if err != nil {
			return fmt.Errorf("failed to create new task: %w", err)
		}
	}
	return nil
}

// failTask creates a failed task, if permanent failure also remove the task
// from the queue
func (q *Queue) failTask(queries *db.Queries, task *taskrunner.Task, result *taskrunner.TaskResult, permanently bool) error {
	slog.Info("Failing task", "taskId", task.Id)
	err := queries.CreateTaskFailure(q.dbCtx, db.CreateTaskFailureParams{
		TaskID: task.Id,
		Log:    string(result.Logs),
	})
	if err != nil {
		slog.Error("Failed to fail task", "error", err)
		return err
	}
	if permanently {
		err = queries.DeleteTask(q.dbCtx, task.Id)
		if err != nil {
			slog.Error("Failed to delete task", "error", err)

		}
		return err
	}
	err = queries.ReleaseTask(q.dbCtx, db.ReleaseTaskParams{
		TaskID: task.Id,
		NodeID: &q.NodeId,
	})
	if err != nil {
		slog.Error("Failed to release task", "error", err)
	}
	return err
}

// backoffTask makes the task unavailable for a period of time according to
// queue config. This may mean blocking the entire queue, just this task or some
// property filter.
func (q *Queue) backoffTask(queries *db.Queries, task *taskrunner.Task, result *taskrunner.TaskResult) error {
	slog.Info("Backing off task", "taskId", task.Id)
	err := queries.TaskSetBackoff(q.dbCtx, task.Id)
	if err != nil {
		slog.Error("Failed to backoff task", "error", err)
	}
	return err
}
