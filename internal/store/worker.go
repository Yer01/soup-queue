package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrNoJob means there is no queued job to claim right now.
var ErrNoJob = errors.New("no queued job")

// Claim takes the oldest queued job, marks it running for workerID with a lease
// of the given length, bumps attempt (the fencing token) and records the claim
// in job_runs. All of it happens in one statement, so a claim and its run-log
// row can never be separated by a crash.
//
// SKIP LOCKED lets concurrent workers pick different rows without waiting on
// each other. The lease end is computed from the database clock.
func (s *Store) Claim(ctx context.Context, workerID string, lease time.Duration) (Job, error) {
	row := s.pool.QueryRow(ctx, `
		WITH picked AS (
			SELECT id FROM jobs
			WHERE state = 'queued'
			ORDER BY id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE jobs j
			SET state = 'running',
			    attempt = j.attempt + 1,
			    worker_id = $1,
			    lease_expires_at = now() + make_interval(secs => $2),
			    updated_at = now()
			FROM picked
			WHERE j.id = picked.id AND j.state = 'queued'
			RETURNING j.*
		), run AS (
			INSERT INTO job_runs (job_id, attempt, worker_id)
			SELECT id, attempt, worker_id FROM claimed
		)
		SELECT `+jobColumns+` FROM claimed`,
		workerID, lease.Seconds())
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoJob
	}
	if err != nil {
		return Job{}, fmt.Errorf("claim: %w", err)
	}
	return job, nil
}

// Heartbeat extends the lease of a job this worker still owns. It returns false
// when the worker no longer owns the job: it was cancelled, or its lease
// expired and the job was requeued or re-claimed. The worker must then stop.
func (s *Store) Heartbeat(ctx context.Context, id int64, attempt int, lease time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET lease_expires_at = now() + make_interval(secs => $3),
		    updated_at = now()
		WHERE id = $1 AND attempt = $2 AND state = 'running'`,
		id, attempt, lease.Seconds())
	if err != nil {
		return false, fmt.Errorf("heartbeat job %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Complete marks the job succeeded and writes its side effect (one executions
// row) in the same statement, but only if this worker still owns the job at
// this attempt. It returns false when the result was rejected: the worker is a
// zombie whose lease was taken over, or the job was cancelled first.
func (s *Store) Complete(ctx context.Context, id int64, attempt int, workerID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH done AS (
			UPDATE jobs
			SET state = 'succeeded',
			    lease_expires_at = NULL,
			    finished_at = now(),
			    updated_at = now()
			WHERE id = $1 AND attempt = $2 AND state = 'running'
			RETURNING id, attempt
		)
		INSERT INTO executions (job_id, attempt, worker_id)
		SELECT id, attempt, $3 FROM done`,
		id, attempt, workerID)
	if err != nil {
		return false, fmt.Errorf("complete job %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Fail marks the job failed with a reason, under the same fence as Complete.
func (s *Store) Fail(ctx context.Context, id int64, attempt int, reason string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET state = 'failed',
		    last_error = $3,
		    lease_expires_at = NULL,
		    finished_at = now(),
		    updated_at = now()
		WHERE id = $1 AND attempt = $2 AND state = 'running'`,
		id, attempt, reason)
	if err != nil {
		return false, fmt.Errorf("fail job %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Release puts a job this worker owns back in the queue, for a graceful
// shutdown. Without it the job would wait for its lease to expire.
func (s *Store) Release(ctx context.Context, id int64, attempt int) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET state = 'queued',
		    worker_id = NULL,
		    lease_expires_at = NULL,
		    updated_at = now()
		WHERE id = $1 AND attempt = $2 AND state = 'running'`,
		id, attempt)
	if err != nil {
		return false, fmt.Errorf("release job %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

// Reaped describes one job whose lease had expired.
type Reaped struct {
	ID       int64
	Attempt  int
	WorkerID *string // the worker that lost the lease
	State    State   // queued (retry) or failed (attempts used up)
}

// ReapExpired handles up to batch running jobs whose lease has expired: their
// worker died, froze or lost the database. Each goes back to queued, or to
// failed once attempt >= maxAttempts so a job that keeps killing workers cannot
// loop forever. attempt is left as is; the next Claim bumps it, which fences
// out the old worker if it ever comes back.
//
// Safe to run from every worker at once: SKIP LOCKED splits the rows, and a
// heartbeat that commits first moves lease_expires_at forward, so the row no
// longer matches when it is re-checked.
func (s *Store) ReapExpired(ctx context.Context, maxAttempts, batch int) ([]Reaped, error) {
	rows, err := s.pool.Query(ctx, `
		WITH expired AS (
			SELECT id, worker_id FROM jobs
			WHERE state = 'running' AND lease_expires_at < now()
			ORDER BY id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET state = CASE WHEN j.attempt >= $1 THEN 'failed' ELSE 'queued' END,
		    worker_id = CASE WHEN j.attempt >= $1 THEN j.worker_id END,
		    lease_expires_at = NULL,
		    finished_at = CASE WHEN j.attempt >= $1 THEN now() END,
		    last_error = 'lease expired on attempt ' || j.attempt,
		    updated_at = now()
		FROM expired
		WHERE j.id = expired.id AND j.state = 'running' AND j.lease_expires_at < now()
		RETURNING j.id, j.attempt, expired.worker_id, j.state`,
		maxAttempts, batch)
	if err != nil {
		return nil, fmt.Errorf("reap: %w", err)
	}
	reaped, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Reaped, error) {
		var x Reaped
		err := r.Scan(&x.ID, &x.Attempt, &x.WorkerID, &x.State)
		return x, err
	})
	if err != nil {
		return nil, fmt.Errorf("reap: %w", err)
	}
	return reaped, nil
}
