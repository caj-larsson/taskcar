-- name: DequeTasks :many
UPDATE taskcar.task
SET locked_by = sqlc.arg(node_id), attempts = attempts + 1
WHERE task.task_id = (
    SELECT
        t.task_id
    FROM taskcar.task t

    WHERE t.queue = sqlc.arg(queue)
       AND t.task_id NOT IN (SELECT task_id from taskcar.task_backedoff)
       AND t.locked_by IS NULL
    ORDER BY created_at asc
    LIMIT sqlc.arg('limit')::int
    FOR UPDATE SKIP LOCKED
)
RETURNING task_id, queue, created_at, in_data;

-- name: ReleaseTask :exec
UPDATE taskcar.task
SET locked_by = null
WHERE task_id = sqlc.arg(task_id)
  AND locked_by = sqlc.arg(node_id);

-- name: TaskSetBackoff :exec
INSERT INTO taskcar.task_backoff(
queue, backoff_path, backoff_key, until
) SELECT
    q.queue, q.backoff_path, t.in_data #> q.backoff_path, now() + q.backoff_int
FROM taskcar.task t
JOIN taskcar.queue q ON t.queue = q.queue
WHERE t.task_id = sqlc.arg(task_id);

-- name: CreateCompletedTask :exec
INSERT INTO taskcar.completed_task(
    task_id, queue, task_created_at, in_data, out_data, log, attempts, node_id
)
SELECT
  t.task_id, t.queue, t.created_at, t.in_data, sqlc.arg(out_data)::jsonb, sqlc.arg(log), t.attempts, t.locked_by
FROM taskcar.task t
WHERE t.task_id = sqlc.arg(task_id);

-- name: CreateTaskFailure :exec
INSERT INTO taskcar.failed_task(
    task_id, queue, task_created_at, in_data, log, attempts, node_id
)
SELECT
    t.task_id, t.queue, t.created_at, t.in_data, sqlc.arg(log), t.attempts, t.locked_by
FROM taskcar.task t
WHERE t.task_id = sqlc.arg(task_id);

-- name: DeleteTask :exec
DELETE FROM taskcar.task
WHERE task_id = sqlc.arg(task_id);

-- name: CreateTask :one
INSERT INTO taskcar.task(
    queue, in_data
)
VALUES(
    sqlc.arg(queue), sqlc.arg(in_data)::jsonb
)
RETURNING task_id;

-- name: UpsertNode :one
INSERT INTO taskcar.node(
    hostname, ip_address, up
) VALUES(
    sqlc.arg(hostname), sqlc.arg(ip), true
)
ON CONFLICT (hostname) DO UPDATE
SET
    heartbeat_at = now(),
    ip_address = sqlc.arg(ip)
RETURNING node_id, created_at, hostname, ip_address, heartbeat_at;

-- name: NodeHeartbeat :exec
UPDATE taskcar.node
SET heartbeat_at = now()
WHERE node_id = sqlc.arg(node_id);

-- name: NodeShutdown :exec
UPDATE taskcar.node
SET up = false
WHERE node_id = sqlc.arg(node_id);

-- name: UpsertQueue :exec
INSERT INTO taskcar.queue(
    queue, max_attempts, backoff_path, backoff_int
) VALUES(
    sqlc.arg(queue),
    sqlc.arg(max_attempts),
    sqlc.arg(backoff_path),
    sqlc.arg(backoff_int)
) on CONFLICT (queue) DO UPDATE
SET
    max_attempts = sqlc.arg(max_attempts),
    backoff_path = sqlc.arg(backoff_path),
    backoff_int = sqlc.arg(backoff_int);

-- name: GetSecrets :many
SELECT
    name, value
FROM taskcar.secret
WHERE
    name = ANY(sqlc.arg('names')::text[]);


-- name: CreateSecret :one
INSERT INTO taskcar.secret(
    name, value
) VALUES(
    sqlc.arg(name), sqlc.arg(value)
) RETURNING secret_id;


-- name: NotifyChannel :exec
SELECT pg_notify(
    sqlc.arg(channel),
    ''
);
