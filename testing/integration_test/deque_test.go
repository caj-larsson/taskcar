package integrationtest_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"taskcar/db"
	"taskcar/testing/testdb"
)

func (s *IntTestSuite) TestBackoffPreventsDeque() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := s.NewPool(ctx)
	defer pool.Close()
	_, _ = s.createTestQueue(ctx, pool, "exit 75")
	queries := db.New(pool)
	task_id, err := queries.CreateTask(ctx, db.CreateTaskParams{
		Queue:  "test",
		InData: []byte("{\"id\":1}"),
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	err = queries.TaskSetBackoff(ctx, task_id)
	if err != nil {
		t.Fatalf("Failed to set backoff: %v", err)
	}
	testQueries := testdb.New(pool)
	ct, err := testQueries.CurrentTasks(ctx)
	if err != nil {
		t.Fatalf("Failed to get current tasks: %v", err)
	}
	assert.Equal(t, int64(1), ct.TaskCount, "Expected task to be in the queue")
	assert.Equal(t, int64(1), ct.BackoffedTaskCount, "Expected task to be backed off")

	nodeID := int64(1)
	rows, err := queries.DequeTasks(ctx, db.DequeTasksParams{
		NodeID: &nodeID,
		Queue:  "test",
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("Failed to dequeue tasks: %v", err)
	}
	assert.Equal(t, 0, len(rows), "Expected not dequeue tasks")
}

func (s *IntTestSuite) TestNodeLockPreventsDeque() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := s.NewPool(ctx)
	defer pool.Close()
	q1, _ := s.createTestQueue(ctx, pool, "exit 75")
	queries := db.New(pool)
	_, err := queries.CreateTask(ctx, db.CreateTaskParams{
		Queue:  "test",
		InData: []byte("{\"id\":1}"),
	})
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}

	testQueries := testdb.New(pool)
	ct, err := testQueries.CurrentTasks(ctx)
	if err != nil {
		t.Fatalf("Failed to get current tasks: %v", err)
	}
	assert.Equal(t, int64(1), ct.TaskCount, "Expected task to be in the queue")

	rows, err := queries.DequeTasks(ctx, db.DequeTasksParams{
		NodeID: &q1.NodeId,
		Queue:  "test",
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("Failed to dequeue tasks: %v", err)
	}

	assert.Equal(t, 1, len(rows), "Expected to deque task")
	node2hostname := "test2"

	node2ID := upsertTestNode(ctx, pool, &node2hostname)

	assert.NotEqual(t, q1.NodeId, node2ID, "Nodes must be different")
	rows, err = queries.DequeTasks(ctx, db.DequeTasksParams{
		NodeID: &node2ID,
		Queue:  "test",
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("Failed to dequeue tasks: %v", err)
	}

	assert.Equal(t, 0, len(rows), "There should be no available tasks")
}
