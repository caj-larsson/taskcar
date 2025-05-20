package node

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"net"
	"os"
	"taskcar/db"
	"time"
)

// Node represents a taskcar node that processes tasks from a PostgreSQL database.
type Node struct {
	NodeID int64
	pool   *pgxpool.Pool
	// cbCtx is separate context for database operations, so during cleanup there
	// are no race conditions between task clean up and database connection shutdown.
	dbCtx    context.Context
	dbCancel context.CancelFunc
	// ctx is used to cancel operations of the node
	ctx context.Context
	// Done is a channel that is closed when the node is shut down
	Done chan struct{}
}

// New creates a new Node instance and upserts the node information into the
// database. It returns the Node instance and an error if any occurs.
func New(pool *pgxpool.Pool, ctx context.Context) (*Node, error) {
	hostname, ip, err := resolveNodeIdentity()
	if err != nil {
		return nil, fmt.Errorf("failed to resolve node identity: %w", err)
	}

	dbCtx, dbCancel := context.WithCancel(context.Background())

	nodeID, err := registerNodeInDB(pool, dbCtx, hostname, ip)

	if err != nil {
		dbCancel()
		return nil, fmt.Errorf("failed to register node in database: %w", err)
	}

	node := &Node{
		NodeID:   nodeID,
		pool:     pool,
		dbCtx:    dbCtx,
		dbCancel: dbCancel,
		ctx:      ctx,
		Done:     make(chan struct{}, 1),
	}

	return node, nil
}

// resolveNodeIdentity gets the hostname and IP address of the node.
func resolveNodeIdentity() (hostname, ip string, err error) {
	hostname, err = os.Hostname()
	if err != nil {
		return "", "", err
	}

	addr, err := net.ResolveIPAddr("ip", hostname)
	if err != nil {
		return "", "", err
	}

	return hostname, addr.IP.String(), nil
}

// registerNodeInDB registers the node in the database by upserting, and returns
// the nodeID.
func registerNodeInDB(pool *pgxpool.Pool, dbCtx context.Context, hostname, ip string) (int64, error) {
	conn, err := pool.Acquire(dbCtx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	queries := db.New(conn)
	nodeRow, err := queries.UpsertNode(dbCtx, db.UpsertNodeParams{
		Hostname: hostname,
		Ip:       ip,
	})

	if err != nil {
		return 0, err
	}

	return nodeRow.NodeID, nil
}

// Close shuts down the Queue instances cleanly, then shuts down the node before
// closing database context.
func (n *Node) Close() error {
	slog.Debug("Node: Closing node")
	defer n.dbCancel()

	conn, err := n.pool.Acquire(n.dbCtx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	queries := db.New(conn)
	err = queries.NodeShutdown(n.dbCtx, n.NodeID)
	if err != nil {
		return fmt.Errorf("failed to shut down node: %w", err)
	}
	return nil
}

// Start runs node background processes such as heartbeat waiting for context to
// cancel
func (n *Node) Start() {
	// Start the heartbeat in a separate goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		backoff := time.Second
		maxBackoff := time.Minute
		consecutiveFailures := 0
		maxFailures := 5

		for {
			select {
			case <-ticker.C:
				if err := n.heartbeat(); err != nil {
					consecutiveFailures++
					slog.Error("Failed to send heartbeat",
						"error", err,
						"consecutiveFailures", consecutiveFailures)

					if consecutiveFailures >= maxFailures {
						slog.Error("Too many consecutive heartbeat failures, exiting")
						// Still try to close cleanly
						if closeErr := n.Close(); closeErr != nil {
							slog.Error("Failed to close node", "error", closeErr)
						}
						close(n.Done)
						return
					}

					// Backoff before next attempt
					time.Sleep(backoff)
					backoff = min(backoff*2, maxBackoff)
				} else {
					// Reset on success
					consecutiveFailures = 0
					backoff = time.Second
				}
			case <-n.ctx.Done():
				err := n.Close()
				if err != nil {
					slog.Error("Failed to close node", "error", err)
				} else {
					slog.Debug("Node: Closed")
				}
				close(n.Done)
				slog.Debug("Node: Done")
				return
			}
		}
	}()
}

// heartbeat updates the nodes heartbeat timestamp in the database.
func (n *Node) heartbeat() error {
	conn, err := n.pool.Acquire(n.dbCtx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	queries := db.New(conn)

	return queries.NodeHeartbeat(n.dbCtx, n.NodeID)
}
