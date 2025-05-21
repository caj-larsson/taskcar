package integrationtest_test

import (
	"context"
	"taskcar/config"
	"taskcar/listener"
	"taskcar/queue"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"taskcar/db"
	"taskcar/testing/testdb"
)

func (s *IntTestSuite) TestQueueCancellation() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	pool := s.NewPool(ctx)
	defer pool.Close()

	q, _ := s.createTestQueue(ctx, "test", pool, "echo '{}'")
	go q.ProcessTasksForever()

	time.Sleep(10 * time.Millisecond) // Don't cancel before it's started
	cancel()
	err := timelyCleanup(q)
	if err != nil {
		t.Fatalf("Failed to stop queue: %v", err)
	}
}

func (s *IntTestSuite) TestQueueStart() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := s.NewPool(ctx)
	defer pool.Close()

	q, notifyChan := s.createTestQueue(ctx, "test", pool, "echo -n '{}'")

	go q.ProcessTasksForever()

	testQueries := testdb.New(pool)

	counts, _ := testQueries.CurrentTasks(ctx)

	assert.Equal(t, int64(0), counts.TaskCount, "There should be not tasks")
	createTask(ctx, pool, "test", []byte("{}"))

	notifyChan <- listener.Message{Queue: "test", Data: "{}"}
	time.Sleep(100 * time.Millisecond) // Wait for the task to be processed

	counts, _ = testQueries.CurrentTasks(ctx)

	assert.Equal(t, int64(0), counts.TaskCount, "Expected task to be processed and deleted from the queue")
	assert.Equal(t, int64(1), counts.CompletedTaskCount, "Expected task to be processed and deleted from the queue")

	// Explicitly cancel and wait for the queue to clean up, if this fails the test should hang
	cancel()
	err := timelyCleanup(q)
	if err != nil {
		t.Fatalf("Failed to stop queue: %v", err)
	}
}

func (s *IntTestSuite) TestQueuePermanentlyFailingTask() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := s.NewPool(ctx)
	defer pool.Close()

	q, notifyChan := s.createTestQueue(ctx, "test", pool, "exit 1")
	go q.ProcessTasksForever()

	createTask(ctx, pool, "test", []byte("{}"))
	notifyChan <- listener.Message{Queue: "test", Data: "{}"}
	time.Sleep(100 * time.Millisecond) // Wait for the task to be processed

	testQueries := testdb.New(pool)
	counts, _ := testQueries.CurrentTasks(ctx)
	assert.Equal(t, int64(0), counts.TaskCount, "Expected no available tasks")
	assert.Equal(t, int64(1), counts.FailedTaskCount, "Expected one failed")

	cancel()
	err := timelyCleanup(q)
	if err != nil {
		t.Fatalf("Failed to stop queue: %v", err)
	}
}

func (s *IntTestSuite) TestQueueRetryableTask() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := s.NewPool(ctx)
	defer pool.Close()

	q, notifyChan := s.createTestQueue(ctx, "test", pool, "exit 75")
	go q.ProcessTasksForever()

	createTask(ctx, pool, "test", []byte("{\"id\":1	}"))
	notifyChan <- listener.Message{Queue: "test", Data: "{}"}
	time.Sleep(100 * time.Millisecond) // Wait for the task to be processed

	testQueries := testdb.New(pool)
	counts, _ := testQueries.CurrentTasks(ctx)
	assert.Equal(t, int64(1), counts.TaskCount, "Expected no available tasks")
	assert.Equal(t, int64(1), counts.FailedTaskCount, "Expected one failed")

	cancel()
	err := timelyCleanup(q)
	if err != nil {
		t.Fatalf("Failed to stop queue: %v", err)
	}
}

func (s *IntTestSuite) TestTaskReleasedAfterQueueShutdown() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := s.NewPool(ctx)

	q1ctx, q1cancel := context.WithCancel(ctx)
	q, notifyChan := s.createTestQueue(q1ctx, "test", pool, "sleep 10")
	go q.ProcessTasksForever()
	createTask(q1ctx, pool, "test", []byte("{}"))
	notifyChan <- listener.Message{Queue: "test", Data: "{}"}
	time.Sleep(100 * time.Millisecond) // Wait for the task to be started
	q1cancel()
	<-q.Done

	node2 := upsertTestNode(ctx, pool, nil)
	queries := db.New(pool)
	rows, err := queries.DequeTasks(ctx, db.DequeTasksParams{
		NodeID: &node2,
		Queue:  "test",
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("Failed to dequeue task: %v", err)
	}

	assert.Equal(t, 1, len(rows), "Expected one task to be dequeued")
}

func (s *IntTestSuite) TestSecretUsageAndDir() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := s.NewPool(ctx)
	defer pool.Close()

	notifyChan := make(chan listener.Message, 1024)
	cfg := config.QueueConfig{
		QueueName: "test",
		// Extra quotes to escape the shell for escaped json string
		Command: "echo -n {\\\"data\\\":{\\\"secret\\\":\\\"$SECRET\\\", \\\"PWD\\\": \\\"$PWD\\\"}}",
		Secrets: []config.SecretConfig{
			{
				Name:   "test",
				EnvKey: "SECRET",
			},
		},
		Dir: "/tmp",
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	defer conn.Release()
	queries := db.New(conn)

	queries.CreateSecret(ctx, db.CreateSecretParams{
		Name:  "test",
		Value: "testsecretvalue",
	})

	queries.UpsertQueue(ctx, db.UpsertQueueParams{
		Queue:       cfg.QueueName,
		MaxAttempts: 3,
		BackoffPath: []string{"id"},
		BackoffInt: pgtype.Interval{
			Valid:        true,
			Microseconds: 1000 * 1000 * 10,
		},
	})
	nodeId := upsertTestNode(ctx, pool, nil)

	queueCtx, queueCancel := context.WithCancel(ctx)
	q, err := queue.New(pool, nodeId, notifyChan, cfg, queueCtx)
	if err != nil {
		panic(err)
	}

	taskId := createTask(ctx, pool, "test", []byte("{}"))

	go q.ProcessTasksForever()

	time.Sleep(100 * time.Millisecond) // Don't cancel before it's started
	queueCancel()

	<-q.Done
	testQueries := testdb.New(pool)
	cc, err := testQueries.GetCompletedTask(ctx, taskId)
	if err != nil {
		t.Fatalf("Failed to get completed task: %v", err)
	}
	assert.Equal(
		t,
		"{\"data\": {\"PWD\": \"/tmp\", \"secret\": \"testsecretvalue\"}}",
		string(cc.OutData),
		"Expected task to be processed and deleted from the queue",
	)
}

func (s *IntTestSuite) TestCreatesNewTasks() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := s.NewPool(ctx)
	defer pool.Close()

	q, notifyChan := s.createTestQueue(
		ctx,
		"test",
		pool,
		"echo -n '{\"new_tasks\":[{\"queue\": \"test2\", \"data\": {}}]}'",
	)

	go q.ProcessTasksForever()

	testQueries := testdb.New(pool)

	counts, _ := testQueries.CurrentTasks(ctx)

	assert.Equal(t, int64(0), counts.TaskCount, "There should be not tasks")
	createTask(ctx, pool, "test", []byte("{}"))

	notifyChan <- listener.Message{Queue: "test", Data: "{}"}
	time.Sleep(200 * time.Millisecond) // Wait for the task to be processed

	counts, _ = testQueries.CurrentTasks(ctx)

	assert.Equal(t, int64(1), counts.TaskCount, "Expected one unprocessed task")
	assert.Equal(t, int64(1), counts.CompletedTaskCount, "Expected task to be processed and deleted from the queue")

	// Explicitly cancel and wait for the queue to clean up, if this fails the test should hang
	cancel()
	err := timelyCleanup(q)
	if err != nil {
		t.Fatalf("Failed to stop queue: %v", err)
	}
}
