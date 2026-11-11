// Package store is the Postgres access layer shared by the API and the workers.
//
// # Concurrency contract
//
// Every state transition is a single conditional UPDATE run at the default
// READ COMMITTED isolation level. The UPDATE takes a row lock. If another
// transaction changed the row first, Postgres waits for it to finish and then
// re-checks the WHERE clause against the newest row version. So two competing
// transitions on the same job (cancel vs complete, live worker vs zombie)
// can never both succeed: the loser matches 0 rows.
//
// Rules every worker write must follow:
//
//   - Claim bumps attempt (the fencing token) and sets worker_id and the lease.
//   - Heartbeat, Complete and Fail are guarded by
//     WHERE id = $1 AND attempt = $2 AND state = 'running'.
//     A worker whose lease expired (job re-queued or re-claimed) or whose job
//     was cancelled matches 0 rows and must abandon the job.
//   - Complete writes the side effect (INSERT INTO executions) in the same
//     transaction as the fenced UPDATE, and only if that UPDATE hit exactly 1 row.
//   - Every timestamp comes from the database clock (now()), never from the Go
//     process, so clock skew between hosts cannot stretch or shrink a lease.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrNotFound means no job has the given id.
	ErrNotFound = errors.New("job not found")
	// ErrIdempotencyConflict means the idempotency key is already used by a job
	// with a different payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrNotCancellable means the job already finished as succeeded or failed.
	ErrNotCancellable = errors.New("job already finished and cannot be cancelled")
)

// State is a job lifecycle state.
type State string

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// Valid reports whether s is one of the five known states.
func (s State) Valid() bool {
	switch s {
	case StateQueued, StateRunning, StateSucceeded, StateFailed, StateCancelled:
		return true
	}
	return false
}

// Terminal reports whether s is a final state. Terminal states never change.
func (s State) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCancelled
}

// Job mirrors a row of the jobs table.
type Job struct {
	ID             int64
	IdempotencyKey string
	Payload        json.RawMessage
	State          State
	Attempt        int
	WorkerID       *string
	LeaseExpiresAt *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	FinishedAt     *time.Time
}

// Store runs queries against a connection pool. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping checks that the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// jobColumns is the column list every job query selects, in scanJob order.
const jobColumns = `id, idempotency_key, payload, state, attempt, worker_id,
	lease_expires_at, last_error, created_at, updated_at, finished_at`

type scanner interface {
	Scan(dest ...any) error
}

// scanJob scans the jobColumns of one row, followed by any extra columns.
func scanJob(row scanner, extra ...any) (Job, error) {
	var j Job
	dest := []any{
		&j.ID, &j.IdempotencyKey, &j.Payload, &j.State, &j.Attempt, &j.WorkerID,
		&j.LeaseExpiresAt, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt,
	}
	err := row.Scan(append(dest, extra...)...)
	return j, err
}
