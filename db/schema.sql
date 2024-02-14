CREATE SCHEMA fila;

CREATE TYPE fila.job_state AS ENUM (
    'available',
    'processing',
    'successful',
    -- TODO: Add `failed` in the future.
    'cancelled'
);

CREATE TABLE fila.jobs (
    "id" UUID PRIMARY KEY,
    "queue" TEXT NOT NULL,
    "state" fila.job_state NOT NULL,
    "name" TEXT NOT NULL,
    "payload" JSONB,
    "attempts" SMALLINT,
    "scheduled_at" TIMESTAMPTZ NOT NULL,
    "finished_at" TIMESTAMPTZ
);

COMMENT ON COLUMN fila.jobs.attempts IS
    'The number of finished executions, including the successful one, if any';

CREATE INDEX jobs_queue_state_idx
    ON fila.jobs ("queue", "state");

CREATE TABLE fila.failures (
    "id" UUID PRIMARY KEY,
    "job_id" UUID REFERENCES fila.jobs,
    "data" JSONB,
    "attempt" SMALLINT NOT NULL,
    "created_at" TIMESTAMPTZ NOT NULL
);
