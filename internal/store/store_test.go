package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"soup-queue/internal/store"
	"soup-queue/internal/testdb"
)

func mustCreate(t *testing.T, s *store.Store, key, payload string) store.Job {
	t.Helper()
	job, _, err := s.CreateJob(context.Background(), key, json.RawMessage(payload))
	if err != nil {
		t.Fatalf("create %q: %v", key, err)
	}
	return job
}

const testLease = 30 * time.Second

// mustClaim claims the oldest queued job and checks it is the expected one.
// Tests keep at most one job queued at a time, so the expected job is the oldest.
func mustClaim(t *testing.T, s *store.Store, worker string, wantID int64) store.Job {
	t.Helper()
	job, err := s.Claim(context.Background(), worker, testLease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job.ID != wantID {
		t.Fatalf("claimed job %d, want %d", job.ID, wantID)
	}
	return job
}

// expireLease moves a running job's lease into the past, as if its worker froze.
func expireLease(t *testing.T, pool *pgxpool.Pool, id int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

// jitter sleeps up to max, so racing goroutines take turns arriving first.
func jitter(max time.Duration) {
	time.Sleep(time.Duration(rand.Int64N(int64(max))))
}

func countExecutions(t *testing.T, pool *pgxpool.Pool, id int64) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM executions WHERE job_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count executions: %v", err)
	}
	return n
}

func TestCreateJobIdempotentUnderConcurrency(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	const callers = 100
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
		ids     = map[int64]bool{}
		start   = make(chan struct{})
	)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			job, c, err := s.CreateJob(ctx, "same-key", json.RawMessage(`{"n":1}`))
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			ids[job.ID] = true
			if c {
				created++
			}
		}()
	}
	close(start)
	wg.Wait()

	if created != 1 {
		t.Errorf("created = %d, want exactly 1", created)
	}
	if len(ids) != 1 {
		t.Errorf("got %d distinct ids, want 1: %v", len(ids), ids)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key = 'same-key'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("rows in db = %d, want 1", rows)
	}
}

func TestCreateJobPayloadComparison(t *testing.T) {
	s := store.New(testdb.Pool(t))
	ctx := context.Background()

	first := mustCreate(t, s, "k", `{"a":1,"b":[1,2]}`)

	// Same JSON, different key order and whitespace: a replay, not a conflict.
	job, created, err := s.CreateJob(ctx, "k", json.RawMessage(`{ "b":[1,2], "a":1 }`))
	if err != nil || created || job.ID != first.ID {
		t.Fatalf("replay: id=%d created=%v err=%v, want id=%d created=false err=nil", job.ID, created, err, first.ID)
	}

	job, created, err = s.CreateJob(ctx, "k", json.RawMessage(`{"a":2}`))
	if !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("different payload: err=%v, want ErrIdempotencyConflict", err)
	}
	if created || job.ID != first.ID {
		t.Errorf("conflict should return the existing job: id=%d created=%v", job.ID, created)
	}
}

func TestGetJobNotFound(t *testing.T) {
	s := store.New(testdb.Pool(t))
	if _, err := s.GetJob(context.Background(), 12345); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestListJobsPagination(t *testing.T) {
	s := store.New(testdb.Pool(t))
	ctx := context.Background()

	const total = 103
	for i := range total {
		mustCreate(t, s, fmt.Sprintf("k%03d", i), `{}`)
	}

	var (
		seen    []int64
		pages   int
		afterID int64
	)
	for {
		page, err := s.ListJobs(ctx, store.ListParams{AfterID: afterID, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, j := range page.Jobs {
			seen = append(seen, j.ID)
		}
		if page.NextCursor == nil {
			break
		}
		afterID = *page.NextCursor
	}

	if pages != 11 {
		t.Errorf("pages = %d, want 11", pages)
	}
	if len(seen) != total {
		t.Fatalf("saw %d jobs, want %d", len(seen), total)
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("ids not strictly increasing at %d: %d then %d", i, seen[i-1], seen[i])
		}
	}
}

func TestListJobsStateFilter(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	var cancelled []int64
	for i := range 20 {
		j := mustCreate(t, s, fmt.Sprintf("k%d", i), `{}`)
		if i%3 == 0 {
			if _, _, err := s.CancelJob(ctx, j.ID); err != nil {
				t.Fatal(err)
			}
			cancelled = append(cancelled, j.ID)
		}
	}

	page, err := s.ListJobs(ctx, store.ListParams{State: store.StateCancelled, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Jobs) != len(cancelled) {
		t.Fatalf("got %d cancelled jobs, want %d", len(page.Jobs), len(cancelled))
	}
	for i, j := range page.Jobs {
		if j.State != store.StateCancelled || j.ID != cancelled[i] {
			t.Errorf("item %d: id=%d state=%s, want id=%d cancelled", i, j.ID, j.State, cancelled[i])
		}
	}
}

func TestCancelJobStates(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	t.Run("queued", func(t *testing.T) {
		j := mustCreate(t, s, "queued", `{}`)
		got, already, err := s.CancelJob(ctx, j.ID)
		if err != nil || already || got.State != store.StateCancelled || got.FinishedAt == nil {
			t.Fatalf("got state=%s already=%v finished=%v err=%v", got.State, already, got.FinishedAt, err)
		}
		got, already, err = s.CancelJob(ctx, j.ID)
		if err != nil || !already || got.State != store.StateCancelled {
			t.Fatalf("second cancel: state=%s already=%v err=%v", got.State, already, err)
		}
	})

	t.Run("running", func(t *testing.T) {
		j := mustCreate(t, s, "running", `{}`)
		claimed := mustClaim(t, s, "w1", j.ID)
		got, _, err := s.CancelJob(ctx, j.ID)
		if err != nil || got.State != store.StateCancelled || got.LeaseExpiresAt != nil {
			t.Fatalf("got state=%s lease=%v err=%v", got.State, got.LeaseExpiresAt, err)
		}
		// The worker that was running it is now fenced out.
		if ok, err := s.Heartbeat(ctx, j.ID, claimed.Attempt, testLease); err != nil || ok {
			t.Fatalf("heartbeat after cancel: ok=%v err=%v, want rejected", ok, err)
		}
		won, err := s.Complete(ctx, j.ID, claimed.Attempt, "w1")
		if err != nil || won {
			t.Fatalf("worker complete after cancel: won=%v err=%v, want rejected", won, err)
		}
		if n := countExecutions(t, pool, j.ID); n != 0 {
			t.Errorf("executions = %d, want 0", n)
		}
	})

	t.Run("succeeded", func(t *testing.T) {
		j := mustCreate(t, s, "succeeded", `{}`)
		claimed := mustClaim(t, s, "w1", j.ID)
		if won, err := s.Complete(ctx, j.ID, claimed.Attempt, "w1"); err != nil || !won {
			t.Fatalf("complete: won=%v err=%v", won, err)
		}
		got, _, err := s.CancelJob(ctx, j.ID)
		if !errors.Is(err, store.ErrNotCancellable) || got.State != store.StateSucceeded {
			t.Fatalf("err=%v state=%s, want ErrNotCancellable with succeeded job", err, got.State)
		}
	})

	t.Run("missing", func(t *testing.T) {
		if _, _, err := s.CancelJob(ctx, 999999); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
}

// TestCancelCompleteRace races a cancel against a worker completion on the
// same running job. Exactly one must win, and the side effect must exist if
// and only if the job ended succeeded.
func TestCancelCompleteRace(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	const iterations = 500
	var completeWins, cancelWins int

	for i := range iterations {
		j := mustCreate(t, s, fmt.Sprintf("race-%d", i), `{}`)
		claimed := mustClaim(t, s, "w1", j.ID)

		// Both sides are one statement and one round trip, and the jitter lets
		// each arrive first some of the time. (An earlier version completed in a
		// multi-statement transaction; its extra BEGIN round trip meant cancel
		// always arrived first, 500 of 500, so nothing actually raced.)
		var (
			wg          sync.WaitGroup
			start       = make(chan struct{})
			workerWon   bool
			workerErr   error
			cancelJob   store.Job
			alreadyDone bool
			cancelErr   error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			jitter(300 * time.Microsecond)
			workerWon, workerErr = s.Complete(ctx, j.ID, claimed.Attempt, "w1")
		}()
		go func() {
			defer wg.Done()
			<-start
			jitter(300 * time.Microsecond)
			cancelJob, alreadyDone, cancelErr = s.CancelJob(ctx, j.ID)
		}()
		close(start)
		wg.Wait()

		if workerErr != nil {
			t.Fatalf("job %d: worker error: %v", j.ID, workerErr)
		}
		cancelWon := cancelErr == nil && !alreadyDone
		if cancelErr != nil && !errors.Is(cancelErr, store.ErrNotCancellable) {
			t.Fatalf("job %d: cancel error: %v", j.ID, cancelErr)
		}
		if workerWon == cancelWon {
			t.Fatalf("job %d: workerWon=%v cancelWon=%v, want exactly one winner", j.ID, workerWon, cancelWon)
		}

		final, err := s.GetJob(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		execs := countExecutions(t, pool, j.ID)
		switch {
		case workerWon && (final.State != store.StateSucceeded || execs != 1):
			t.Fatalf("job %d: worker won but state=%s executions=%d", j.ID, final.State, execs)
		case cancelWon && (final.State != store.StateCancelled || execs != 0 || cancelJob.State != store.StateCancelled):
			t.Fatalf("job %d: cancel won but state=%s executions=%d", j.ID, final.State, execs)
		}
		if workerWon {
			completeWins++
		} else {
			cancelWins++
		}
	}
	t.Logf("%d iterations: complete won %d, cancel won %d", iterations, completeWins, cancelWins)
	// If one side always wins, the two never overlapped and the test proved nothing.
	if completeWins == 0 || cancelWins == 0 {
		t.Fatalf("race never went both ways (complete %d, cancel %d): test is not exercising the race",
			completeWins, cancelWins)
	}
}

// TestZombieWriteRejected walks the "worker froze and came back" story: w1
// claims, freezes past its lease, the reaper requeues the job, w2 claims it.
// When w1 wakes up, its heartbeat and its result must both be rejected.
func TestZombieWriteRejected(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	j := mustCreate(t, s, "zombie", `{}`)
	first := mustClaim(t, s, "w1", j.ID)
	expireLease(t, pool, j.ID)

	reaped, err := s.ReapExpired(ctx, 5, 10)
	if err != nil || len(reaped) != 1 || reaped[0].State != store.StateQueued ||
		reaped[0].WorkerID == nil || *reaped[0].WorkerID != "w1" {
		t.Fatalf("reap: %+v err=%v, want job requeued from w1", reaped, err)
	}
	second := mustClaim(t, s, "w2", j.ID)
	if second.Attempt != first.Attempt+1 {
		t.Fatalf("second claim attempt = %d, want %d", second.Attempt, first.Attempt+1)
	}

	if ok, err := s.Heartbeat(ctx, j.ID, first.Attempt, testLease); err != nil || ok {
		t.Fatalf("zombie heartbeat: ok=%v err=%v, want rejected", ok, err)
	}
	if won, err := s.Complete(ctx, j.ID, first.Attempt, "w1"); err != nil || won {
		t.Fatalf("zombie complete: won=%v err=%v, want rejected", won, err)
	}
	if n := countExecutions(t, pool, j.ID); n != 0 {
		t.Fatalf("executions after zombie write = %d, want 0", n)
	}
	got, _ := s.GetJob(ctx, j.ID)
	if got.State != store.StateRunning || got.Attempt != second.Attempt || *got.WorkerID != "w2" {
		t.Fatalf("job changed by zombie: state=%s attempt=%d worker=%v", got.State, got.Attempt, *got.WorkerID)
	}

	if won, err := s.Complete(ctx, j.ID, second.Attempt, "w2"); err != nil || !won {
		t.Fatalf("w2 complete: won=%v err=%v", won, err)
	}
	var execAttempt int
	if err := pool.QueryRow(ctx, `SELECT attempt FROM executions WHERE job_id = $1`, j.ID).Scan(&execAttempt); err != nil {
		t.Fatal(err)
	}
	if execAttempt != second.Attempt {
		t.Fatalf("execution recorded attempt %d, want %d", execAttempt, second.Attempt)
	}
}

func TestSchemaRejectsInconsistentStates(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()
	j := mustCreate(t, s, "k", `{}`)

	cases := []struct {
		name, sql, constraint string
	}{
		{"running without lease", `UPDATE jobs SET state = 'running', worker_id = 'w' WHERE id = $1`, "lease_iff_running"},
		{"running without worker", `UPDATE jobs SET state = 'running', lease_expires_at = now() WHERE id = $1`, "running_has_worker"},
		{"queued with lease", `UPDATE jobs SET lease_expires_at = now() WHERE id = $1`, "lease_iff_running"},
		{"terminal without finished_at", `UPDATE jobs SET state = 'succeeded' WHERE id = $1`, "terminal_has_finished_at"},
		{"queued with finished_at", `UPDATE jobs SET finished_at = now() WHERE id = $1`, "terminal_has_finished_at"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, tc.sql, j.ID)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.ConstraintName != tc.constraint {
				t.Fatalf("err = %v, want violation of %s", err, tc.constraint)
			}
		})
	}
}
