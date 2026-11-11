package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"soup-queue/internal/store"
	"soup-queue/internal/testdb"
)

func testConfig(id string) Config {
	return Config{
		ID:             id,
		Lease:          2 * time.Second,
		HeartbeatEvery: 100 * time.Millisecond,
		PollEvery:      20 * time.Millisecond,
		ReapEvery:      100 * time.Millisecond,
		ReapBatch:      100,
		MaxAttempts:    5,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// startWorker runs a worker in the background. The returned stop function
// cancels it and waits for Run to return.
func startWorker(t *testing.T, s *store.Store, cfg Config) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, s, cfg) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("worker %s: %v", cfg.ID, err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func create(t *testing.T, s *store.Store, key string, payload any) store.Job {
	t.Helper()
	raw, _ := json.Marshal(payload)
	job, _, err := s.CreateJob(context.Background(), key, raw)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func jobState(t *testing.T, s *store.Store, id int64) store.Job {
	t.Helper()
	j, err := s.GetJob(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestWorkersDrainQueue(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)

	const jobs = 200
	for i := range jobs {
		create(t, s, fmt.Sprintf("k%d", i), map[string]any{"duration_ms": i % 20})
	}
	for i := range 4 {
		startWorker(t, s, testConfig(fmt.Sprintf("w%d", i)))
	}

	waitFor(t, 30*time.Second, "all jobs to finish", func() bool {
		return count(t, pool, `SELECT count(*) FROM jobs WHERE state IN ('queued','running')`) == 0
	})
	if n := count(t, pool, `SELECT count(*) FROM jobs WHERE state = 'succeeded'`); n != jobs {
		t.Errorf("succeeded = %d, want %d", n, jobs)
	}
	if n := count(t, pool, `SELECT count(*) FROM executions`); n != jobs {
		t.Errorf("executions = %d, want %d", n, jobs)
	}
	if n := count(t, pool, `SELECT count(DISTINCT worker_id) FROM job_runs`); n < 2 {
		t.Errorf("only %d worker(s) did any work, want the load spread", n)
	}
}

func TestFailedJob(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	j := create(t, s, "k", map[string]any{"duration_ms": 1, "fail": true})
	startWorker(t, s, testConfig("w1"))

	waitFor(t, 5*time.Second, "job to fail", func() bool { return jobState(t, s, j.ID).State == store.StateFailed })
	if got := jobState(t, s, j.ID); got.LastError == nil || *got.LastError != ErrJobFailed.Error() {
		t.Errorf("last_error = %v, want %q", got.LastError, ErrJobFailed)
	}
	if n := count(t, pool, `SELECT count(*) FROM executions`); n != 0 {
		t.Errorf("executions = %d, want 0", n)
	}
}

// TestCancelAbortsRunningWork: cancelling a running job stops the work at the
// next heartbeat, and no side effect is written.
func TestCancelAbortsRunningWork(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	j := create(t, s, "k", map[string]any{"duration_ms": 10_000})

	workEnded := make(chan error, 1)
	cfg := testConfig("w1")
	cfg.Work = func(ctx context.Context, job store.Job) error {
		err := Emulate(ctx, job)
		workEnded <- err
		return err
	}
	startWorker(t, s, cfg)

	waitFor(t, 5*time.Second, "job to start", func() bool { return jobState(t, s, j.ID).State == store.StateRunning })
	if _, _, err := s.CancelJob(context.Background(), j.ID); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-workEnded:
		if !errors.Is(err, errLeaseLost) {
			t.Errorf("work ended with %v, want errLeaseLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("work still running 2s after cancel")
	}
	if got := jobState(t, s, j.ID); got.State != store.StateCancelled {
		t.Errorf("state = %s, want cancelled", got.State)
	}
	if n := count(t, pool, `SELECT count(*) FROM executions`); n != 0 {
		t.Errorf("executions = %d, want 0", n)
	}
}

func TestShutdownReleasesJob(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	j := create(t, s, "k", map[string]any{"duration_ms": 10_000})

	stop := startWorker(t, s, testConfig("w1"))
	waitFor(t, 5*time.Second, "job to start", func() bool { return jobState(t, s, j.ID).State == store.StateRunning })
	stop()

	got := jobState(t, s, j.ID)
	if got.State != store.StateQueued || got.WorkerID != nil || got.Attempt != 1 {
		t.Errorf("after shutdown: state=%s worker=%v attempt=%d, want queued, no worker, attempt 1",
			got.State, got.WorkerID, got.Attempt)
	}
}

// TestDeadWorkerJobIsRecovered: a job claimed by a worker that then died (never
// heartbeats) is reaped after its lease and finished by a live worker. Only the
// live worker's attempt leaves a side effect.
func TestDeadWorkerJobIsRecovered(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	j := create(t, s, "k", map[string]any{"duration_ms": 1})

	if _, err := s.Claim(context.Background(), "dead-worker", 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	startWorker(t, s, testConfig("w1"))

	waitFor(t, 5*time.Second, "job to succeed", func() bool { return jobState(t, s, j.ID).State == store.StateSucceeded })
	got := jobState(t, s, j.ID)
	if got.Attempt != 2 || got.WorkerID == nil || *got.WorkerID != "w1" {
		t.Errorf("finished by %v at attempt %d, want w1 at attempt 2", got.WorkerID, got.Attempt)
	}
	if n := count(t, pool, `SELECT count(*) FROM executions WHERE job_id = $1 AND attempt = 2`, j.ID); n != 1 {
		t.Errorf("executions for attempt 2 = %d, want 1", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM job_runs WHERE job_id = $1`, j.ID); n != 2 {
		t.Errorf("job_runs = %d, want 2 (dead worker + w1)", n)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := testConfig("w1")
	cfg.HeartbeatEvery = cfg.Lease
	if err := cfg.validate(); err == nil {
		t.Error("heartbeat interval equal to the lease was accepted")
	}
	cfg = testConfig("w1")
	cfg.Fault = "typo"
	if err := cfg.validate(); err == nil {
		t.Error("unknown fault was accepted")
	}
}
