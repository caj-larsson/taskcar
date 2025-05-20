
-- name: CurrentTasks :one
SELECT
    (SELECT count(*) FROM taskcar.task) as task_count,
    (SELECT count(*) FROM taskcar.completed_task) as completed_task_count,
    (SELECT count(*) FROM taskcar.failed_task) as failed_task_count,
    (SELECT count(*) FROM taskcar.task_backedoff) as backoffed_task_count;


-- name: CleanAfterTest :exec
TRUNCATE
    taskcar.task,
    taskcar.completed_task,
    taskcar.failed_task,
    taskcar.task_backoff
    CASCADE;

-- name: GetCompletedTask :one
SELECT
    *
FROM taskcar.completed_task
WHERE
    task_id = sqlc.arg('task_id');
