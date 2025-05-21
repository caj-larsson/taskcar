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
	dbCtx           context.Context
	dbCancel        context.CancelFunc
	backoffDuration time.Duration
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
		NodeId:          nodeId,
		Name:            cfg.QueueName,
		config:          cfg,
		pool:            pool,
		checkChan:       checkChan,
		taskRunner:      runner,
		ctx:             ctx,
		dbCtx:           dbCtx,
		dbCancel:        dbCancel,
		backoffDuration: 100 * time.Millisecond,
		Done:            make(chan struct{}, 1),
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

	task, err := q.takeTask()
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
		q.failTask(task, &taskrunner.TaskResult{}, true)
		return true
	}

	var processErr error
	// Handle error after switch
	switch result.Status {
	case taskrunner.Success:
		slog.Debug("Task completed successfully", "taskId", task.Id)
		processErr = q.completeTask(task, result)
		if processErr != nil {
			slog.Error("Failed to complete task", "error", processErr)
			failErr := q.failTask(task, result, true)
			if failErr != nil {
				slog.Error("Failed to fail task", "error", failErr)
				processErr = failErr
			}
		}
	case taskrunner.Failed:
		slog.Debug("Task failed permanently", "taskId", task.Id)
		processErr = q.failTask(task, result, true)
	case taskrunner.Aborted:
		slog.Debug("Task aborted", "taskId", task.Id)
		processErr = q.failTask(task, result, false)
	case taskrunner.Retry:
		slog.Debug("Task failed, retrying", "taskId", task.Id)
		processErr = q.failWithBackoff(task, result)
	case taskrunner.ExternallyInterrupted:
		slog.Debug("Task interrupted externally", "taskId", task.Id)
		processErr = q.failTask(task, result, false)
	}

	if processErr != nil {
		slog.Error("Failed to process task", "error", processErr)
		return false
	}
	return true
}

// -- Task Operations --

// takeTask from the database making them unavailable to other workers
func (q *Queue) takeTask() (*taskrunner.Task, error) {
	slog.Debug("taking task from queue", "queue", q.Name)

	var task *taskrunner.Task

	err := q.queriesTx(func(queries *db.Queries) error {

		query := db.DequeTasksParams{
			NodeID: &q.NodeId,
			Queue:  q.Name,
			Limit:  1,
		}
		taskRows, err := queries.DequeTasks(q.ctx, query)
		if err != nil {
			if pgx.ErrNoRows == err {
				return nil
			}
			return err
		}

		if taskRows == nil {
			return nil
		}

		if len(taskRows) > 1 {
			return errors.New("multiple tasks returned, expected only one")
		}
		task = &taskrunner.Task{
			Id:        taskRows[0].TaskID,
			Queue:     taskRows[0].Queue,
			CreatedAt: taskRows[0].CreatedAt.Time,
			Command:   q.config.Command,
			InData:    taskRows[0].InData,
			EnvVars:   q.envVars,
			Dir:       q.config.Dir,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to take task: %w", err)
	}
	return task, nil
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
	task *taskrunner.Task,
	result *taskrunner.TaskResult,
) error {
	slog.Info(
		"Completing task",
		"taskId", task.Id,
		"status", result.Status,
		"outData", string(result.OutData),
	)
	var outData TaskOutput
	err := json.Unmarshal(result.OutData, &outData)
	if err != nil {
		return fmt.Errorf("failed to unmarshal task output: %w", err)
	}

	err = q.queriesTx(func(queries *db.Queries) error {
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

		notifiedQueus := make(map[string]bool)

		for _, newTask := range outData.NewTasks {
			slog.Debug("Creating new task", "queue", newTask.Queue)
			_, err = queries.CreateTask(q.dbCtx, db.CreateTaskParams{
				Queue:  newTask.Queue,
				InData: newTask.Data,
			})
			if err != nil {
				return fmt.Errorf("failed to create new task: %w", err)
			}
			if _, ok := notifiedQueus[newTask.Queue]; !ok {
				slog.Debug("Notifying channel", "queue", newTask.Queue)
				err = queries.NotifyChannel(q.dbCtx, newTask.Queue)
				if err != nil {
					return fmt.Errorf("failed to notify channel: %w", err)
				}
				notifiedQueus[newTask.Queue] = true
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to complete task: %w", err)
	}
	return nil
}

// failTask creates a failed task, if permanent failure also remove the task
// from the queue
func (q *Queue) failTask(
	task *taskrunner.Task,
	result *taskrunner.TaskResult,
	permanently bool,
) error {
	slog.Info("Failing task", "taskId", task.Id)

	err := q.queriesTx(func(queries *db.Queries) error {
		err := queries.CreateTaskFailure(q.dbCtx, db.CreateTaskFailureParams{
			TaskID: task.Id,
			Log:    string(result.Logs),
		})
		if err != nil {
			return fmt.Errorf("failed to create task failure: %w", err)
		}
		if permanently {
			err = queries.DeleteTask(q.dbCtx, task.Id)
			if err != nil {
				return fmt.Errorf("failed to delete task: %w", err)
			}
			// When deleting a we don't need to relase it.
			return nil
		}
		err = queries.ReleaseTask(q.dbCtx, db.ReleaseTaskParams{
			TaskID: task.Id,
			NodeID: &q.NodeId,
		})
		if err != nil {
			return fmt.Errorf("failed to release task: %w", err)
		}
		return nil
	})
	if err != nil {
		slog.Error("Failed to release task", "error", err)
	}
	return err
}

// failWithBackoff makes the task unavailable for a period of time according to
// queue config. This may mean blocking the entire queue, just this task or some
// property filter.
func (q *Queue) failWithBackoff(
	task *taskrunner.Task,
	result *taskrunner.TaskResult,
) error {
	slog.Info("Backing off task", "taskId", task.Id)

	err := q.queriesTx(func(queries *db.Queries) error {
		err := queries.TaskSetBackoff(q.dbCtx, task.Id)
		if err != nil {
			return fmt.Errorf("failed to set backoff: %w", err)
		}

		err = queries.CreateTaskFailure(q.dbCtx, db.CreateTaskFailureParams{
			TaskID: task.Id,
			Log:    string(result.Logs),
		})
		if err != nil {
			return fmt.Errorf("failed to create task failure: %w", err)
		}

		err = queries.ReleaseTask(q.dbCtx, db.ReleaseTaskParams{
			TaskID: task.Id,
			NodeID: &q.NodeId,
		})
		if err != nil {
			return fmt.Errorf("failed to release task: %w", err)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to backoff task: %w", err)
	}
	return nil
}

// queriesTx executes a function within a transaction. It handles pool
// connection and transcation life cycles. It will attempt to reconnect forever
// or until the context is cancelled.
func (q *Queue) queriesTx(f func(*db.Queries) error) error {
	var conn *pgxpool.Conn
	backoffDuration := q.backoffDuration
	for {
		if q.dbCtx.Err() == nil {
			var err error
			conn, err = q.pool.Acquire(q.dbCtx)
			if err != nil {
				slog.Warn("Failed to acquire connection", "error", err)
				time.Sleep(backoffDuration)
				backoffDuration = min(backoffDuration*2, 5*time.Second)
				continue
			}
			break
		}
		return fmt.Errorf("failed to acquire connection: %w", q.dbCtx.Err())
	}
	defer conn.Release()
	tx, err := conn.Begin(q.dbCtx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use a closure to scope the transaction
	txErr := func() error {
		queries := db.New(tx)

		if err := f(queries); err != nil {
			return err
		}

		if err := tx.Commit(q.dbCtx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}()

	// Handle rollback if there was an error
	if txErr != nil {
		if rbErr := tx.Rollback(q.dbCtx); rbErr != nil {
			slog.Error(
				"Failed to rollback transaction",
				"rberror", rbErr,
				"txerror", txErr,
			)
		}
		return txErr
	}
	return nil
}
