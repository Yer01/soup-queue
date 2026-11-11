package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// CreateJob inserts a queued job, or returns the existing job with the same
// idempotency key. created is true only for the call that inserted the row.
//
// If the key exists with a different payload it returns ErrIdempotencyConflict
// together with the existing job. Payloads are compared as jsonb, so key order
// and whitespace do not matter.
//
// Concurrent calls with the same key are safe: ON CONFLICT waits for the
// competing insert to commit, and the follow-up SELECT runs with a fresh READ
// COMMITTED snapshot, so it sees that row. Jobs are never deleted.
func (s *Store) CreateJob(ctx context.Context, key string, payload json.RawMessage) (Job, bool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (idempotency_key, payload)
		VALUES ($1, $2)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+jobColumns,
		key, payload)
	job, err := scanJob(row)
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	var samePayload bool
	row = s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+`, payload = $2::jsonb
		FROM jobs
		WHERE idempotency_key = $1`,
		key, payload)
	job, err = scanJob(row, &samePayload)
	if err != nil {
		// ErrNoRows here would mean the conflicting row vanished, which cannot
		// happen while jobs are never deleted. Surface it rather than retry.
		return Job{}, false, fmt.Errorf("load job by idempotency key: %w", err)
	}
	if !samePayload {
		return job, false, ErrIdempotencyConflict
	}
	return job, false, nil
}

// GetJob returns the job with the given id, or ErrNotFound.
func (s *Store) GetJob(ctx context.Context, id int64) (Job, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job %d: %w", id, err)
	}
	return job, nil
}

// ListParams selects one page of jobs.
type ListParams struct {
	State   State // empty means all states
	AfterID int64 // exclusive cursor; 0 starts at the beginning
	Limit   int   // page size, must be > 0
}

// Page is one page of jobs ordered by id ascending.
type Page struct {
	Jobs []Job
	// NextCursor is the id to pass as AfterID for the next page,
	// or nil if this is the last page.
	NextCursor *int64
}

// ListJobs returns jobs with id > AfterID in id order (keyset pagination).
//
// Keyset rather than OFFSET: rows inserted while a client pages through the
// list never shift the pages, so nothing is returned twice. A row whose id was
// allocated before the cursor but whose insert commits later is not seen; the
// listing is a live view, not a snapshot.
func (s *Store) ListJobs(ctx context.Context, p ListParams) (Page, error) {
	if p.Limit <= 0 {
		return Page{}, fmt.Errorf("list jobs: limit must be positive, got %d", p.Limit)
	}

	// Two separate queries instead of "$1 IS NULL OR state = $1", so the planner
	// can use the (state, id) index when a state is given.
	var (
		rows pgx.Rows
		err  error
	)
	if p.State == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT `+jobColumns+`
			FROM jobs
			WHERE id > $1
			ORDER BY id
			LIMIT $2`,
			p.AfterID, p.Limit+1)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT `+jobColumns+`
			FROM jobs
			WHERE state = $1 AND id > $2
			ORDER BY id
			LIMIT $3`,
			p.State, p.AfterID, p.Limit+1)
	}
	if err != nil {
		return Page{}, fmt.Errorf("list jobs: %w", err)
	}
	jobs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Job, error) {
		return scanJob(r)
	})
	if err != nil {
		return Page{}, fmt.Errorf("list jobs: %w", err)
	}

	page := Page{Jobs: jobs}
	if len(jobs) > p.Limit {
		page.Jobs = jobs[:p.Limit]
		next := page.Jobs[p.Limit-1].ID
		page.NextCursor = &next
	}
	return page, nil
}

// CancelJob moves a queued or running job to cancelled immediately.
//
// A running job's worker is not told directly. Its next fenced write
// (heartbeat or complete, guarded by state = 'running') matches 0 rows, so it
// stops and its result and side effect are discarded.
//
// Cancelling an already cancelled job succeeds with alreadyCancelled = true,
// so retries are safe. A succeeded or failed job returns ErrNotCancellable
// together with the job; a missing job returns ErrNotFound.
func (s *Store) CancelJob(ctx context.Context, id int64) (job Job, alreadyCancelled bool, err error) {
	// The loop only repeats in one edge case: the UPDATE found no row because the
	// job's insert had not committed yet, and the GetJob that follows sees it
	// as queued. Terminal states never change, so any other outcome is final.
	for range 3 {
		row := s.pool.QueryRow(ctx, `
			UPDATE jobs
			SET state = 'cancelled',
			    lease_expires_at = NULL,
			    finished_at = now(),
			    updated_at = now()
			WHERE id = $1 AND state IN ('queued', 'running')
			RETURNING `+jobColumns,
			id)
		job, err = scanJob(row)
		if err == nil {
			return job, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return Job{}, false, fmt.Errorf("cancel job %d: %w", id, err)
		}

		job, err = s.GetJob(ctx, id)
		if err != nil {
			return Job{}, false, err
		}
		switch job.State {
		case StateCancelled:
			return job, true, nil
		case StateSucceeded, StateFailed:
			return job, false, ErrNotCancellable
		}
	}
	return Job{}, false, fmt.Errorf("cancel job %d: state kept changing, giving up", id)
}
