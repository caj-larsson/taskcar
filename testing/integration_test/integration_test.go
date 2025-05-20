package integrationtest_test

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"os"
	"taskcar/config"
	"taskcar/db"
	"taskcar/listener"
	"taskcar/queue"
	"taskcar/testing/dbsuite"
	"testing"
	"time"
)

type IntTestSuite struct {
	dbsuite.DBTestSuite
}

func TestIntegration(t *testing.T) {
	// Skip integration tests if environment flag is not set
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration tests. Set INTEGRATION_TEST=true to run.")
	}

	suite.Run(t, new(IntTestSuite))
}

func (s *IntTestSuite) createTestQueue(ctx context.Context, pool *pgxpool.Pool, command string) (*queue.Queue, chan listener.Message) {
	notifyChan := make(chan listener.Message, 1024)
	cfg := config.QueueConfig{
		QueueName: "test",
		Command:   command,
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Release()
	queries := db.New(conn)
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

	q, err := queue.New(pool, nodeId, notifyChan, cfg, ctx)
	if err != nil {
		panic(err)
	}

	return q, notifyChan
}

func createTask(ctx context.Context, pool *pgxpool.Pool, queue string, inData []byte) int64 {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Release()
	queries := db.New(conn)

	task_id, err := queries.CreateTask(ctx, db.CreateTaskParams{
		Queue:  queue,
		InData: inData,
	})
	if err != nil {
		panic(err)
	}
	return task_id
}

func upsertTestNode(ctx context.Context, pool *pgxpool.Pool, nodeHost *string) int64 {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		panic(err)
	}
	defer conn.Release()
	queries := db.New(conn)

	var hostname string

	if nodeHost == nil {
		hostname = "test"
	} else {
		hostname = *nodeHost
	}

	node, err := queries.UpsertNode(ctx, db.UpsertNodeParams{
		Hostname: hostname,
		Ip:       "localhost",
	})
	if err != nil {
		panic(err)
	}
	return node.NodeID
}

type taskcounts struct {
	TaskCount          int64
	CompletedTaskCount int64
	FailedTaskCount    int64
}

func (s *IntTestSuite) NewPool(ctx context.Context) *pgxpool.Pool {
	pool, err := pgxpool.New(ctx, s.ConnString)
	if err != nil {
		s.T().Fatalf("Failed to create connection pool: %v", err)
	}
	return pool
}
func timelyCleanup(queue *queue.Queue) error {

	select {
	case <-queue.Done:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("queue did not clean up in time")
	}
}
