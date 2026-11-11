package worker

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	"soup-queue/internal/store"
)

// ErrJobFailed is returned by a WorkFunc when the job itself failed and must
// not be retried. Any other error means the work was interrupted.
var ErrJobFailed = errors.New("job failed")

// WorkFunc runs one job. It must return promptly once ctx is cancelled.
type WorkFunc func(ctx context.Context, job store.Job) error

// emulatedPayload is the part of a job payload the emulated work understands.
type emulatedPayload struct {
	DurationMS *int `json:"duration_ms"`
	Fail       bool `json:"fail"`
}

const maxEmulatedDuration = 10 * time.Minute

// Emulate stands in for a training run. It sleeps for payload.duration_ms
// (default: random 50-150 ms) and returns ErrJobFailed if payload.fail is true.
// The real side effect is written by the worker when it completes the job.
func Emulate(ctx context.Context, job store.Job) error {
	var p emulatedPayload
	_ = json.Unmarshal(job.Payload, &p) // unknown or malformed fields fall back to defaults

	d := 50*time.Millisecond + rand.N(100*time.Millisecond)
	if p.DurationMS != nil {
		d = min(max(time.Duration(*p.DurationMS)*time.Millisecond, 0), maxEmulatedDuration)
	}

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
	}
	if p.Fail {
		return ErrJobFailed
	}
	return nil
}
