package listener

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Message contains the information from the postgres notification and
// represents that a queue should attempt to process tasks.
type Message struct {
	Queue string
	Data  string
}

type MessageChan chan Message

// ListenQueues creates a map of channels for the specified queues and starts a
// goroutine that will message the channels when a notification is received on
// the corresponding PostgreSQL channel or when a reconnection has occurred,
// because the node may have missed notifications.
func ListenQueues(
	queues []string,
	connString string,
	ctx context.Context,
) map[string]MessageChan {

	res := make(map[string]MessageChan, len(queues))

	for _, queue := range queues {
		msgChan := make(chan Message, 1024)
		res[queue] = msgChan
	}

	go listen(res, connString, ctx)

	return res
}

// listen listens and forwards notifications, handling connection errors and and
// cleanup
func listen(
	chans map[string]MessageChan,
	connString string,
	ctx context.Context,
) {
	// Run as goroutine, no one else will clean up the channels
	defer func() {
		for _, ch := range chans {
			close(ch)
		}
	}()

	pool, err := pgxpool.New(ctx, connString)

	if err != nil {
		slog.Error("Listener: failed to create connection pool", "error", err)
		return
	}
	defer pool.Close()

	for {
		if ctx.Err() != nil {
			slog.Debug("Listener: context is done, exiting")
			return
		}
		err := listenOnPool(chans, pool, ctx)
		if err != nil {
			slog.Error("Listener: failed to listen on pool", "error", err)
		}
	}
}

// listenOnPool listens to PostgreSQL notifications on the specified pool and
// will return if the context is done or if a pool becomes unavailable. If it
// returns an error the node should not continue to operate. If it does not the
// node shall continue to attempt to listen for connections on a new pool.
func listenOnPool(
	chans map[string]MessageChan,
	pool *pgxpool.Pool,
	ctx context.Context,
) error {
	slog.Debug("Listener: Attempting to acquire connection")

	conn, err := pool.Acquire(ctx)
	if err != nil {
		slog.Error("Listener: failed to acquire connection", "error", err)
		time.Sleep(1 * time.Second)
		return nil // Not considered an error, just run again
	}

	defer conn.Release()

	for queue := range chans {
		slog.Debug("Listener: listening to queue", "queue", queue)
		_, err := conn.Conn().Exec(ctx, "LISTEN "+queue)
		if err != nil {
			slog.Error(
				"Listener: failed to listen to queue",
				"queue", queue,
				"error", err,
			)
		}
		select {
		case chans[queue] <- Message{Queue: queue, Data: "Connected"}:
			slog.Debug("Listener: sent connected message to channel", "queue", queue)
		case <-ctx.Done():
			return ctx.Err()
		default:
			slog.Warn(
				"Listener: channel is full, skipping connected message",
				"queue",
				queue,
			)
		}
	}
	for {
		slog.Debug("Listener: waiting for notifications")
		notification, err := conn.Conn().WaitForNotification(ctx)

		if err != nil {
			slog.Error("Listener: failed to receive notification", "error", err)
			break
		}
		slog.Info(
			"Listener: received notification on channel",
			"channel",
			notification.Channel,
		)
		message := Message{
			Queue: notification.Channel,
			Data:  notification.Payload,
		}
		slog.Debug(
			"Listener: sending message to channel",
			"queue",
			message.Queue,
			"data", message.Data,
		)
		msgCh, ok := chans[message.Queue]

		if !ok {
			slog.Error("Listener: channel not found", "queue", message.Queue)
			return fmt.Errorf("Listener: channel not found: %s", message.Queue)
		}
		select {
		case <-ctx.Done():
			slog.Debug("Listener: context is done, exiting")
			return ctx.Err()
		case msgCh <- message:
			slog.Debug(
				"Listener: message sent to channel",
				"queue",
				message.Queue,
				"data",
				message.Data,
			)
		default:
			slog.Warn(
				"Listener: channel is full, skipping message",
				"queue",
				message.Queue,
				"data",
				message.Data,
			)
		}
	}
	return nil
}
