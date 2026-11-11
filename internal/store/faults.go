package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Deliberately broken variants of Claim and Complete.
//
// They exist only for chaos mutation runs (worker FAULT=unsafeclaim or
// FAULT=nofence). A harness run with one of them enabled must FAIL; if it
// passes, the harness cannot see that class of bug. Never use them otherwise.

// ClaimUnsafe is the classic naive claim: read a queued job without locking it,
// then write it back with an attempt computed from that stale read and no
// re-check of its state. Two workers can read the same row and both claim it
// with the same attempt number.
func (s *Store) ClaimUnsafe(ctx context.Context, workerID string, lease time.Duration) (Job, error) {
	var id int64
	var attempt int
	err := s.pool.QueryRow(ctx,
		`SELECT id, attempt FROM jobs WHERE state = 'queued' ORDER BY id LIMIT 1`).Scan(&id, &attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoJob
	}
	if err != nil {
		return Job{}, fmt.Errorf("unsafe claim: %w", err)
	}

	row := s.pool.QueryRow(ctx, `
		WITH claimed AS (
			UPDATE jobs
			SET state = 'running',
			    attempt = $2,
			    worker_id = $3,
			    lease_expires_at = now() + make_interval(secs => $4),
			    updated_at = now()
			WHERE id = $1
			RETURNING *
		), run AS (
			INSERT INTO job_runs (job_id, attempt, worker_id)
			SELECT id, attempt, worker_id FROM claimed
		)
		SELECT `+jobColumns+` FROM claimed`,
		id, attempt+1, workerID, lease.Seconds())
	job, err := scanJob(row)
	if err != nil {
		return Job{}, fmt.Errorf("unsafe claim: %w", err)
	}
	return job, nil
}

// CompleteUnfenced completes a job without checking attempt or state. A zombie
// worker overwrites the result of the worker that took over its lease, and a
// completion can overwrite a cancel.
func (s *Store) CompleteUnfenced(ctx context.Context, id int64, attempt int, workerID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH done AS (
			UPDATE jobs
			SET state = 'succeeded',
			    lease_expires_at = NULL,
			    finished_at = now(),
			    updated_at = now()
			WHERE id = $1
			RETURNING id
		)
		INSERT INTO executions (job_id, attempt, worker_id)
		SELECT id, $2, $3 FROM done`,
		id, attempt, workerID)
	if err != nil {
		return false, fmt.Errorf("unfenced complete job %d: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}
