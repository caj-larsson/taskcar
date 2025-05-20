DO $$
BEGIN
CREATE ROLE taskcar WITH LOGIN PASSWORD 'localdev';
EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
END
$$;

CREATE SCHEMA IF NOT EXISTS taskcar;
GRANT ALL PRIVILEGES ON SCHEMA taskcar TO taskcar;


CREATE TABLE taskcar.systemconfig(
    config_id bigserial PRIMARY KEY,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    name text NOT NULL,
    value text NOT NULL
);

CREATE TABLE taskcar.node(
    node_id bigserial PRIMARY KEY,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    hostname text NOT NULL UNIQUE,
    ip_address text NOT NULL,
    heartbeat_at timestamp with time zone DEFAULT now() NOT NULL,
    up boolean NOT NULL
);

CREATE TABLE taskcar.secret(
    secret_id bigserial PRIMARY KEY,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    name text NOT NULL,
    value text NOT NULL
);

CREATE TABLE taskcar.queue(
    queue TEXT PRIMARY KEY,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    max_attempts int NOT NULL DEFAULT 10,
    backoff_path TEXT[] NOT NULL,
    backoff_int interval NOT NULL
);

CREATE TABLE taskcar.task (
    task_id bigserial PRIMARY KEY,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    queue text NOT NULL,
    in_data jsonb NOT NULL,
    attempts int NOT NULL DEFAULT 0,
    locked_by bigint references taskcar.node(node_id)
);
CREATE INDEX ON taskcar.task(queue, created_at) WHERE (locked_by is null);

CREATE TABLE taskcar.completed_task (
    task_id bigint PRIMARY KEY,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    queue text NOT NULL,
    task_created_at timestamp with time zone,
    in_data jsonb NOT NULL,
    out_data jsonb NOT NULL,
    log text NOT NULL,
    attempts int NOT NULL,
    node_id bigint references taskcar.node(node_id) NOT NULL
);
CREATE INDEX ON taskcar.completed_task(queue, created_at);

CREATE TABLE taskcar.failed_task (
    id bigserial PRIMARY KEY,
    task_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    queue text NOT NULL,
    task_created_at timestamp with time zone NOT NULL,
    node_id bigint references taskcar.node(node_id) NOT NULL,
    in_data jsonb NOT NULL,
    log text NOT NULL,
    attempts int NOT NULL
);

CREATE TABLE taskcar.task_backoff (
    id bigserial PRIMARY KEY,
    queue text NOT NULL,
    backoff_key jsonb NOT NULL,
    until timestamp with time zone NOT NULL,
    backoff_path text[] NOT NULL
);

CREATE OR REPLACE VIEW taskcar.task_backedoff AS
SELECT t.task_id
FROM  taskcar.task t
JOIN taskcar.task_backoff tb ON t.in_data#>tb.backoff_path = tb.backoff_key
    AND tb.until > now();


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA taskcar TO taskcar;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA taskcar TO taskcar;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA taskcar TO taskcar;
