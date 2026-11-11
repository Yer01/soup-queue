// Package worker runs the claim / heartbeat / complete loop and the lease reaper.
//
// Correctness rests only on the database fence (see package store): every
// write a worker makes is guarded by attempt and state. Everything a worker
// decides locally, such as aborting work when a heartbeat is rejected, only
// saves wasted work; it is never needed for correctness.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"soup-queue/internal/store"
)

// Fault names for chaos mutation runs. See store/faults.go.
const (
	FaultNone        = ""
	FaultNoFence     = "nofence"     // complete without the attempt/state guard
	FaultUnsafeClaim = "unsafeclaim" // claim without row lock or state re-check
)

// Config tunes one worker.
type Config struct {
	ID             string        // unique per process; a restarted process gets a new one
	Lease          time.Duration // how long a claim or heartbeat keeps the job
	HeartbeatEvery time.Duration // must be well below Lease
	PollEvery      time.Duration // pause between claims when the queue is empty
	ReapEvery      time.Duration // how often this worker looks for expired leases
	ReapBatch      int           // max jobs reaped per pass
	MaxAttempts    int           // a job whose lease expires this many times fails
	Fault          string        // FaultNone except in chaos mutation runs
	Work           WorkFunc      // defaults to Emulate
	Logger         *slog.Logger  // defaults to slog.Default()
}

func (c *Config) validate() error {
	switch {
	case c.ID == "":
		return errors.New("worker id is required")
	case c.Lease <= 0 || c.HeartbeatEvery <= 0 || c.PollEvery <= 0 || c.ReapEvery <= 0:
		return errors.New("lease and intervals must be positive")
	case c.HeartbeatEvery*2 > c.Lease:
		return fmt.Errorf("heartbeat interval %s must be at most half the lease %s", c.HeartbeatEvery, c.Lease)
	case c.ReapBatch <= 0 || c.MaxAttempts <= 0:
		return errors.New("reap batch and max attempts must be positive")
	case c.Fault != FaultNone && c.Fault != FaultNoFence && c.Fault != FaultUnsafeClaim:
		return fmt.Errorf("unknown fault %q", c.Fault)
	}
	if c.Work == nil {
		c.Work = Emulate
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

var (
	// errLeaseLost: a heartbeat matched no row. The job was cancelled, or the
	// lease expired and the job was requeued or taken by another worker.
	errLeaseLost = errors.New("lease lost: job cancelled or taken over")
	// errLeaseUnknown: heartbeats kept failing for longer than the lease, so
	// the lease has most likely expired already.
	errLeaseUnknown = errors.New("lease state unknown: heartbeats failing for longer than the lease")
)

type worker struct {
	store *store.Store
	cfg   Config
	log   *slog.Logger
}

// Run claims and executes jobs until ctx is cancelled. On cancellation the job
// in progress is released back to the queue before Run returns.
func Run(ctx context.Context, s *store.Store, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	w := &worker{store: s, cfg: cfg, log: cfg.Logger.With("worker", cfg.ID)}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.reapLoop(ctx)
	}()
	w.claimLoop(ctx)
	wg.Wait()
	return nil
}

func (w *worker) claimLoop(ctx context.Context) {
	for ctx.Err() == nil {
		job, err := w.claim(ctx)
		switch {
		case errors.Is(err, store.ErrNoJob):
			sleep(ctx, jitter(w.cfg.PollEvery))
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			w.log.Error("claim failed", "event", "claim_error", "err", err)
			sleep(ctx, jitter(time.Second))
		default:
			w.process(ctx, job)
		}
	}
}

func (w *worker) claim(ctx context.Context) (store.Job, error) {
	if w.cfg.Fault == FaultUnsafeClaim {
		return w.store.ClaimUnsafe(ctx, w.cfg.ID, w.cfg.Lease)
	}
	return w.store.Claim(ctx, w.cfg.ID, w.cfg.Lease)
}

func (w *worker) complete(ctx context.Context, job store.Job) (bool, error) {
	if w.cfg.Fault == FaultNoFence {
		return w.store.CompleteUnfenced(ctx, job.ID, job.Attempt, w.cfg.ID)
	}
	return w.store.Complete(ctx, job.ID, job.Attempt, w.cfg.ID)
}

// process runs one claimed job to its end.
func (w *worker) process(ctx context.Context, job store.Job) {
	log := w.log.With("job", job.ID, "attempt", job.Attempt)
	log.Debug("claimed", "event", "claimed")

	jobCtx, stopJob := context.WithCancelCause(ctx)
	hbDone := make(chan struct{})
	go w.heartbeat(jobCtx, stopJob, job, hbDone, log)

	workErr := w.cfg.Work(jobCtx, job)
	stopJob(nil)
	<-hbDone

	// The final write must not be cut off by shutdown, so it gets its own
	// context. If the process is killed here instead, the lease expires and the
	// job runs again; the fence keeps the effect single.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	switch {
	case workErr == nil:
		// Always try to complete, even if a heartbeat was rejected meanwhile:
		// the fence in Complete is what decides, not local state.
		ok, err := w.complete(wctx, job)
		switch {
		case err != nil:
			log.Error("complete failed; lease will expire and the job will run again", "event", "complete_error", "err", err)
		case !ok:
			log.Info("result rejected: job cancelled or taken over", "event", "fenced", "reason", "complete")
		default:
			log.Debug("completed", "event", "completed")
		}

	case errors.Is(workErr, ErrJobFailed):
		ok, err := w.store.Fail(wctx, job.ID, job.Attempt, workErr.Error())
		switch {
		case err != nil:
			log.Error("fail failed; lease will expire and the job will run again", "event", "fail_error", "err", err)
		case !ok:
			log.Info("failure rejected: job cancelled or taken over", "event", "fenced", "reason", "fail")
		default:
			log.Debug("failed", "event", "failed")
		}

	case errors.Is(workErr, errLeaseLost):
		log.Info("work aborted: job cancelled or taken over", "event", "fenced", "reason", "heartbeat")

	case ctx.Err() != nil:
		ok, err := w.store.Release(wctx, job.ID, job.Attempt)
		log.Info("released on shutdown", "event", "released", "ok", ok, "err", err)

	default:
		log.Warn("work interrupted; lease will expire", "event", "abandoned", "err", workErr)
	}
}

// heartbeat extends the lease until ctx ends. If the database says the worker
// no longer owns the job, it cancels the work with errLeaseLost.
func (w *worker) heartbeat(ctx context.Context, stop context.CancelCauseFunc, job store.Job, done chan<- struct{}, log *slog.Logger) {
	defer close(done)
	t := time.NewTicker(w.cfg.HeartbeatEvery)
	defer t.Stop()
	lastOK := time.Now() // monotonic: wall-clock jumps on this host do not affect it

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		hbCtx, cancel := context.WithTimeout(ctx, w.cfg.HeartbeatEvery)
		ok, err := w.store.Heartbeat(hbCtx, job.ID, job.Attempt, w.cfg.Lease)
		cancel()
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			log.Warn("heartbeat failed", "event", "heartbeat_error", "err", err)
			if time.Since(lastOK) > w.cfg.Lease {
				stop(errLeaseUnknown)
				return
			}
		case !ok:
			stop(errLeaseLost)
			return
		default:
			lastOK = time.Now()
		}
	}
}

func (w *worker) reapLoop(ctx context.Context) {
	for sleep(ctx, jitter(w.cfg.ReapEvery)) {
		reaped, err := w.store.ReapExpired(ctx, w.cfg.MaxAttempts, w.cfg.ReapBatch)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Error("reap failed", "event", "reap_error", "err", err)
			}
			continue
		}
		for _, r := range reaped {
			lostBy := ""
			if r.WorkerID != nil {
				lostBy = *r.WorkerID
			}
			w.log.Info("lease expired", "event", "reaped",
				"job", r.ID, "attempt", r.Attempt, "lost_by", lostBy, "new_state", r.State)
		}
	}
}

// jitter returns a random duration in [d/2, 3d/2), so workers do not poll in lockstep.
func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d)
}

// sleep waits for d or until ctx ends. It reports whether the full wait happened.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
