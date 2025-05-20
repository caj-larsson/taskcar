package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"taskcar/config"
	"taskcar/listener"
	"taskcar/node"
	"taskcar/queue"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Taskcar struct {
	cfg     config.Config
	pool    *pgxpool.Pool
	ctx     context.Context
	node    *node.Node
	queueWg sync.WaitGroup
	done    chan struct{}
}

func NewTaskcar(cfg config.Config, pool *pgxpool.Pool, ctx context.Context) (*Taskcar, error) {
	taskNode, err := node.New(pool, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	return &Taskcar{
		cfg:     cfg,
		pool:    pool,
		ctx:     ctx,
		node:    taskNode,
		queueWg: sync.WaitGroup{},
		done:    make(chan struct{}, 1),
	}, nil
}

func (tc *Taskcar) Serve() error {
	tc.node.Start()

	listeners := tc.setupListeners()
	if err := tc.startQueues(listeners); err != nil {
		return fmt.Errorf("failed to start queues: %w", err)
	}
	slog.Info("TaskCar started, waiting for tasks")
	tc.waitForShutdown()
	return nil
}

func (tc *Taskcar) setupListeners() map[string]listener.MessageChan {
	queueNames := make([]string, len(tc.cfg.Queues))
	for i, qCfg := range tc.cfg.Queues {
		queueNames[i] = qCfg.QueueName
	}

	return listener.ListenQueues(queueNames, tc.cfg.PGConnStr, tc.ctx)
}

func (tc *Taskcar) startQueues(listeners map[string]listener.MessageChan) error {
	for _, qCfg := range tc.cfg.Queues {
		slog.Info("Queue config", "name", qCfg.QueueName)
		listenChan := listeners[qCfg.QueueName]
		q, err := queue.New(tc.pool, tc.node.NodeID, listenChan, qCfg, tc.ctx)
		if err != nil {
			slog.Error("Failed to create queue", "error", err)
			return fmt.Errorf("failed to create queue %s: %w", qCfg.QueueName, err)
		}

		tc.queueWg.Add(1)
		go func(q *queue.Queue) {
			q.ProcessTasksForever()
			tc.queueWg.Done()
		}(q)
	}
	return nil
}

func (tc *Taskcar) waitForShutdown() {
	slog.Debug("Waiting for all queues to finish")
	done := make(chan struct{})

	go func() {
		tc.queueWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Debug("All queues finished gracefully")
	case <-time.After(30 * time.Second):
		slog.Warn("Timeout waiting for queues to finish, shutting down")
	}

	slog.Debug("All queues finished, waiting for node shutdown")
	<-tc.node.Done
	slog.Debug("TaskCar finished")
	close(tc.done)
}
