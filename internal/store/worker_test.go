package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"soup-queue/internal/store"
	"soup-queue/internal/testdb"
)

func TestClaimEmptyQueue(t *testing.T) {
	s := store.New(testdb.Pool(t))
	if _, err := s.Claim(context.Background(), "w1", testLease); !errors.Is(err, store.ErrNoJob) {
		t.Fatalf("err = %v, want ErrNoJob", err)
	}
}

func TestClaimSetsLeaseAndRunLog(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	a := mustCreate(t, s, "a", `{}`)
	b := mustCreate(t, s, "b", `{}`)

	got := mustClaim(t, s, "w1", a.ID) // oldest first
	if got.State != store.StateRunning || got.Attempt != 1 || got.WorkerID == nil || *got.WorkerID != "w1" {
		t.Fatalf("claimed job: state=%s attempt=%d worker=%v", got.State, got.Attempt, got.WorkerID)
	}

	// The lease is measured on the database clock.
	var leaseLeft time.Duration
	if err := pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM lease_expires_at - now())::float8 * 1e9 FROM jobs WHERE id = $1`, a.ID,
	).Scan((*float64Duration)(&leaseLeft)); err != nil {
		t.Fatal(err)
	}
	if leaseLeft < testLease-5*time.Second || leaseLeft > testLease {
		t.Errorf("lease left = %s, want about %s", leaseLeft, testLease)
	}

	var runs int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM job_runs WHERE job_id = $1 AND attempt = 1 AND worker_id = 'w1'`, a.ID).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 1 {
		t.Errorf("job_runs rows = %d, want 1", runs)
	}

	mustClaim(t, s, "w2", b.ID)
	if _, err := s.Claim(ctx, "w3", testLease); !errors.Is(err, store.ErrNoJob) {
		t.Fatalf("third claim: err = %v, want ErrNoJob", err)
	}
}

// float64Duration scans a float8 number of nanoseconds into a time.Duration.
type float64Duration time.Duration

func (d *float64Duration) Scan(src any) error {
	f, ok := src.(float64)
	if !ok {
		return fmt.Errorf("want float64, got %T", src)
	}
	*d = float64Duration(time.Duration(f))
	return nil
}

// TestClaimConcurrentEachJobOnce: many workers claiming at once must never get
// the same job.
func TestClaimConcurrentEachJobOnce(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	const jobs, workers = 200, 20
	for i := range jobs {
		mustCreate(t, s, fmt.Sprintf("k%d", i), `{}`)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		claimed = map[int64]int{}
		start   = make(chan struct{})
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				job, err := s.Claim(ctx, fmt.Sprintf("w%d", w), testLease)
				if errors.Is(err, store.ErrNoJob) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				claimed[job.ID]++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(claimed) != jobs {
		t.Errorf("claimed %d distinct jobs, want %d", len(claimed), jobs)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Errorf("job %d claimed %d times", id, n)
		}
	}
	var runs, dupes int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT 1 FROM job_runs GROUP BY job_id, attempt HAVING count(*) > 1
		) d`).Scan(&dupes); err != nil {
		t.Fatal(err)
	}
	if runs != jobs || dupes != 0 {
		t.Errorf("job_runs rows = %d (want %d), duplicate (job, attempt) = %d (want 0)", runs, jobs, dupes)
	}
}

func TestHeartbeatExtendsLeaseOnlyForOwner(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	j := mustCreate(t, s, "k", `{}`)
	c := mustClaim(t, s, "w1", j.ID)
	expireLease(t, pool, j.ID)

	if ok, err := s.Heartbeat(ctx, j.ID, c.Attempt+1, testLease); err != nil || ok {
		t.Fatalf("heartbeat with wrong attempt: ok=%v err=%v, want rejected", ok, err)
	}
	// Expired but not yet reaped: nobody else owns it, so the owner may keep it.
	if ok, err := s.Heartbeat(ctx, j.ID, c.Attempt, testLease); err != nil || !ok {
		t.Fatalf("owner heartbeat: ok=%v err=%v, want accepted", ok, err)
	}
	if reaped, err := s.ReapExpired(ctx, 5, 10); err != nil || len(reaped) != 0 {
		t.Fatalf("reap after heartbeat: %+v err=%v, want nothing reaped", reaped, err)
	}
}

func TestReapExpired(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	expired := mustCreate(t, s, "expired", `{}`)
	mustClaim(t, s, "w1", expired.ID)
	expireLease(t, pool, expired.ID)

	alive := mustCreate(t, s, "alive", `{}`)
	mustClaim(t, s, "w2", alive.ID)

	reaped, err := s.ReapExpired(ctx, 5, 10)
	if err != nil || len(reaped) != 1 || reaped[0].ID != expired.ID {
		t.Fatalf("reaped %+v err=%v, want only job %d", reaped, err, expired.ID)
	}

	got, _ := s.GetJob(ctx, expired.ID)
	if got.State != store.StateQueued || got.Attempt != 1 || got.WorkerID != nil ||
		got.LeaseExpiresAt != nil || got.LastError == nil {
		t.Errorf("requeued job: state=%s attempt=%d worker=%v lease=%v last_error=%v",
			got.State, got.Attempt, got.WorkerID, got.LeaseExpiresAt, got.LastError)
	}
	if got, _ := s.GetJob(ctx, alive.ID); got.State != store.StateRunning {
		t.Errorf("job with a live lease was touched: state=%s", got.State)
	}
}

func TestReapFailsJobAfterMaxAttempts(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	const maxAttempts = 3
	j := mustCreate(t, s, "poison", `{}`)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c := mustClaim(t, s, fmt.Sprintf("w%d", attempt), j.ID)
		if c.Attempt != attempt {
			t.Fatalf("attempt = %d, want %d", c.Attempt, attempt)
		}
		expireLease(t, pool, j.ID)
		reaped, err := s.ReapExpired(ctx, maxAttempts, 10)
		if err != nil || len(reaped) != 1 {
			t.Fatalf("reap %d: %+v err=%v", attempt, reaped, err)
		}
		want := store.StateQueued
		if attempt == maxAttempts {
			want = store.StateFailed
		}
		if reaped[0].State != want {
			t.Fatalf("after attempt %d: state=%s, want %s", attempt, reaped[0].State, want)
		}
	}

	got, _ := s.GetJob(ctx, j.ID)
	if got.State != store.StateFailed || got.FinishedAt == nil || got.LastError == nil {
		t.Fatalf("poison job: state=%s finished=%v last_error=%v", got.State, got.FinishedAt, got.LastError)
	}
	if _, err := s.Claim(ctx, "w9", testLease); !errors.Is(err, store.ErrNoJob) {
		t.Fatalf("failed job was claimable again: err=%v", err)
	}
}

func TestFailAndReleaseAreFenced(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	t.Run("fail", func(t *testing.T) {
		j := mustCreate(t, s, "fail", `{}`)
		c := mustClaim(t, s, "w1", j.ID)
		if ok, err := s.Fail(ctx, j.ID, c.Attempt+1, "stale"); err != nil || ok {
			t.Fatalf("stale fail: ok=%v err=%v, want rejected", ok, err)
		}
		if ok, err := s.Fail(ctx, j.ID, c.Attempt, "boom"); err != nil || !ok {
			t.Fatalf("fail: ok=%v err=%v", ok, err)
		}
		got, _ := s.GetJob(ctx, j.ID)
		if got.State != store.StateFailed || got.LastError == nil || *got.LastError != "boom" || countExecutions(t, pool, j.ID) != 0 {
			t.Fatalf("failed job: state=%s last_error=%v", got.State, got.LastError)
		}
	})

	t.Run("release", func(t *testing.T) {
		j := mustCreate(t, s, "release", `{}`)
		c := mustClaim(t, s, "w1", j.ID)
		if ok, err := s.Release(ctx, j.ID, c.Attempt+1); err != nil || ok {
			t.Fatalf("stale release: ok=%v err=%v, want rejected", ok, err)
		}
		if ok, err := s.Release(ctx, j.ID, c.Attempt); err != nil || !ok {
			t.Fatalf("release: ok=%v err=%v", ok, err)
		}
		again := mustClaim(t, s, "w2", j.ID)
		if again.Attempt != c.Attempt+1 {
			t.Fatalf("reclaim attempt = %d, want %d", again.Attempt, c.Attempt+1)
		}
		if won, err := s.Complete(ctx, j.ID, c.Attempt, "w1"); err != nil || won {
			t.Fatalf("released worker completed anyway: won=%v err=%v", won, err)
		}
	})
}

// TestReapHeartbeatRace races the reaper against a late heartbeat on a job
// whose lease just expired. Exactly one must win: either the lease is extended
// and the job stays with its worker, or the job is requeued and the heartbeat
// is rejected.
func TestReapHeartbeatRace(t *testing.T) {
	pool := testdb.Pool(t)
	s := store.New(pool)
	ctx := context.Background()

	const iterations = 300
	var heartbeatWins, reapWins int
	for i := range iterations {
		j := mustCreate(t, s, fmt.Sprintf("race-%d", i), `{}`)
		c := mustClaim(t, s, "w1", j.ID)
		expireLease(t, pool, j.ID)

		var (
			wg      sync.WaitGroup
			start   = make(chan struct{})
			hbOK    bool
			hbErr   error
			reaped  []store.Reaped
			reapErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			jitter(300 * time.Microsecond)
			hbOK, hbErr = s.Heartbeat(ctx, j.ID, c.Attempt, testLease)
		}()
		go func() {
			defer wg.Done()
			<-start
			jitter(300 * time.Microsecond)
			reaped, reapErr = s.ReapExpired(ctx, 5, 10)
		}()
		close(start)
		wg.Wait()

		if hbErr != nil || reapErr != nil {
			t.Fatalf("job %d: heartbeat err=%v reap err=%v", j.ID, hbErr, reapErr)
		}
		reapWon := len(reaped) == 1
		if hbOK == reapWon {
			t.Fatalf("job %d: heartbeat ok=%v reaped=%v, want exactly one winner", j.ID, hbOK, reapWon)
		}
		got, _ := s.GetJob(ctx, j.ID)
		if hbOK && got.State != store.StateRunning || reapWon && got.State != store.StateQueued {
			t.Fatalf("job %d: heartbeat ok=%v reaped=%v but state=%s", j.ID, hbOK, reapWon, got.State)
		}

		// Leave the queue empty for the next iteration.
		if _, _, err := s.CancelJob(ctx, j.ID); err != nil {
			t.Fatal(err)
		}
		if hbOK {
			heartbeatWins++
		} else {
			reapWins++
		}
	}
	t.Logf("%d iterations: heartbeat won %d, reaper won %d", iterations, heartbeatWins, reapWins)
	if heartbeatWins == 0 || reapWins == 0 {
		t.Fatalf("race never went both ways (heartbeat %d, reaper %d): test is not exercising the race",
			heartbeatWins, reapWins)
	}
}
