CREATE TABLE jobs (
  id               BIGSERIAL PRIMARY KEY,
  idempotency_key  TEXT NOT NULL UNIQUE
                   CHECK (length(idempotency_key) BETWEEN 1 AND 255),
  payload          JSONB NOT NULL,
  state            TEXT NOT NULL DEFAULT 'queued'
                   CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
  attempt          INT NOT NULL DEFAULT 0,      
  worker_id        TEXT,
  lease_expires_at TIMESTAMPTZ,
  last_error       TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at      TIMESTAMPTZ,

  CONSTRAINT lease_iff_running
    CHECK ((state = 'running') = (lease_expires_at IS NOT NULL)),
  CONSTRAINT running_has_worker
    CHECK (state <> 'running' OR worker_id IS NOT NULL),
  CONSTRAINT terminal_has_finished_at
    CHECK ((state IN ('succeeded','failed','cancelled')) = (finished_at IS NOT NULL))
);

CREATE INDEX jobs_state_id_idx ON jobs (state, id);

CREATE INDEX jobs_running_lease_idx ON jobs (lease_expires_at) WHERE state = 'running';

CREATE TABLE job_runs (
  id          BIGSERIAL PRIMARY KEY,
  job_id      BIGINT NOT NULL REFERENCES jobs(id),
  attempt     INT NOT NULL,
  worker_id   TEXT NOT NULL,
  claimed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX job_runs_job_id_idx ON job_runs (job_id);

CREATE TABLE executions (
  id          BIGSERIAL PRIMARY KEY,
  job_id      BIGINT NOT NULL REFERENCES jobs(id),
  attempt     INT NOT NULL,
  worker_id   TEXT NOT NULL,
  executed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX executions_job_id_idx ON executions (job_id);
