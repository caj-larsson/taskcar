package integrationtest_test

import (
	"context"
	"taskcar/config"
	"taskcar/taskcar"
	"taskcar/testing/testdb"
	"time"
)

func (s *IntTestSuite) TestNodeRunsNewTasks() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := s.NewPool(ctx)
	defer pool.Close()
	testQueries := testdb.New(pool)
	tc, err := taskcar.NewTaskcar(
		config.Config{
			PGConnStr: s.ConnString,
			Queues: []config.QueueConfig{
				{
					QueueName: "test",
					Command:   "echo -n '{\"new_tasks\":[{\"queue\": \"test2\", \"data\": {}}]}'",
				},
				{
					QueueName: "test2",
					Command:   "echo -n '{}'",
				},
			},
		},
		pool,
		ctx,
	)
	if err != nil {
		t.Fatalf("Failed to create taskcar: %v", err)
	}
	createTask(ctx, pool, "test", []byte("{}"))

	go func() {
		err := tc.Serve()
		if err != nil {
			t.Fatalf("Failed to serve taskcar: %v", err)
		}
	}()

	go func() {
		time.Sleep(time.Second)
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
			counts, err := testQueries.CurrentTasks(ctx)
			if err != nil {
				t.Fatalf("Failed to get current tasks: %v", err)
			}
			if counts.TaskCount == 0 && counts.CompletedTaskCount == 2 {
				t.Logf("Taskcar processed tasks successfully")
				return
			}
		}
	}

}
