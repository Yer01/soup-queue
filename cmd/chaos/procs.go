package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os/exec"
	"sort"
	"sync"
	"time"
)

// supervisor runs the worker processes and does the killing and pausing.
type supervisor struct {
	bin    string
	env    []string // base environment for every worker
	runTag string   // worker ids are <runTag>-w<slot>-g<generation>
	logs   *workerLogs
	rep    *reporter

	mu              sync.Mutex
	procs           []*proc // indexed by slot
	gen             int
	stopping        bool
	ids             map[string]bool // every worker id ever started
	kills           int
	pauses          int
	respawns        int
	unexpectedExits int
	wg              sync.WaitGroup
}

type proc struct {
	slot     int
	id       string
	cmd      *exec.Cmd
	exited   chan struct{}
	killed   bool // set before we SIGKILL it
	paused   bool
	stopping bool
}

func newSupervisor(bin string, env []string, runTag string, workers int, logs *workerLogs, rep *reporter) *supervisor {
	return &supervisor{
		bin: bin, env: env, runTag: runTag, logs: logs, rep: rep,
		procs: make([]*proc, workers),
		ids:   map[string]bool{},
	}
}

func (s *supervisor) startAll() error {
	for slot := range s.procs {
		if err := s.start(slot); err != nil {
			return err
		}
	}
	return nil
}

func (s *supervisor) start(slot int) error {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	s.gen++
	id := fmt.Sprintf("%s-w%d-g%d", s.runTag, slot, s.gen)
	s.mu.Unlock()

	cmd := exec.Command(s.bin)
	cmd.Env = append(append([]string{}, s.env...), "WORKER_ID="+id)
	lw := s.logs.writer()
	cmd.Stdout = lw // same writer for both: exec then uses one pipe and one copier
	cmd.Stderr = lw
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start worker %s: %w", id, err)
	}

	p := &proc{slot: slot, id: id, cmd: cmd, exited: make(chan struct{})}
	s.mu.Lock()
	s.procs[slot] = p
	s.ids[id] = true
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := cmd.Wait()
		lw.flush()
		close(p.exited)

		s.mu.Lock()
		unexpected := !p.killed && !p.stopping
		if unexpected {
			s.unexpectedExits++
		}
		s.mu.Unlock()
		if unexpected {
			s.rep.eventf("worker %s exited on its own: %v (respawning)", id, err)
			time.Sleep(200 * time.Millisecond)
			if err := s.start(slot); err != nil {
				s.rep.eventf("respawn failed: %v", err)
			}
		}
	}()
	return nil
}

// pick returns a random worker that is running and not paused, or nil.
func (s *supervisor) pick(rng *rand.Rand) *proc {
	var live []*proc
	for _, p := range s.procs {
		if p != nil && !p.killed && !p.paused && !p.stopping {
			select {
			case <-p.exited:
			default:
				live = append(live, p)
			}
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live[rng.IntN(len(live))]
}

// killLoop SIGKILLs a random worker about every `every` (±50%) and respawns
// its slot after a short delay, until ctx ends.
func (s *supervisor) killLoop(ctx context.Context, every time.Duration, rng *rand.Rand) {
	for sleepCtx(ctx, every/2+time.Duration(rng.Int64N(int64(every)))) {
		s.mu.Lock()
		p := s.pick(rng)
		if p != nil {
			p.killed = true
			s.kills++
		}
		s.mu.Unlock()
		if p == nil {
			continue
		}

		err := p.cmd.Process.Kill() // SIGKILL on Unix: no cleanup, no release
		s.rep.eventf("kill -9 %s (pid %d) err=%v", p.id, p.cmd.Process.Pid, err)
		select {
		case <-p.exited:
		case <-time.After(5 * time.Second):
			s.rep.eventf("worker %s did not exit 5s after SIGKILL", p.id)
		}
		time.Sleep(200 * time.Millisecond)
		if ctx.Err() != nil {
			// Chaos is over and workers are about to be stopped. Respawning here
			// started a worker that was SIGTERMed before it even connected
			// (seed 1, first batch): harmless, but noise in the counts.
			return
		}

		s.mu.Lock()
		s.respawns++
		s.mu.Unlock()
		if err := s.start(p.slot); err != nil {
			s.rep.eventf("respawn failed: %v", err)
		}
	}
}

// pauseLoop freezes a random worker with SIGSTOP for `pauseFor` (longer than
// the lease), then resumes it with SIGCONT. The resumed worker is a zombie:
// its job was reaped and probably finished by another worker meanwhile.
func (s *supervisor) pauseLoop(ctx context.Context, every, pauseFor time.Duration, rng *rand.Rand) {
	for sleepCtx(ctx, every/2+time.Duration(rng.Int64N(int64(every)))) {
		s.mu.Lock()
		p := s.pick(rng)
		if p != nil {
			p.paused = true
			s.pauses++
		}
		s.mu.Unlock()
		if p == nil {
			continue
		}

		if err := suspend(p.cmd.Process); err != nil {
			s.rep.eventf("SIGSTOP %s failed: %v", p.id, err)
		} else {
			s.rep.eventf("SIGSTOP %s (pid %d) for %s", p.id, p.cmd.Process.Pid, pauseFor)
			sleepCtx(ctx, pauseFor) // resume early if the run is ending
			err := resume(p.cmd.Process)
			s.rep.eventf("SIGCONT %s err=%v", p.id, err)
		}

		s.mu.Lock()
		p.paused = false
		s.mu.Unlock()
	}
}

// stopAll resumes paused workers, sends SIGTERM (graceful: release the job in
// progress) and SIGKILLs anything still alive after the timeout.
func (s *supervisor) stopAll(timeout time.Duration) {
	s.mu.Lock()
	s.stopping = true
	var live []*proc
	for _, p := range s.procs {
		if p != nil {
			p.stopping = true
			live = append(live, p)
		}
	}
	s.mu.Unlock()

	for _, p := range live {
		_ = resume(p.cmd.Process)
		_ = terminate(p.cmd.Process)
	}
	deadline := time.After(timeout)
	for _, p := range live {
		select {
		case <-p.exited:
		case <-deadline:
			s.rep.eventf("worker %s ignored SIGTERM for %s, killing", p.id, timeout)
			_ = p.cmd.Process.Kill()
			<-p.exited
		}
	}
	s.wg.Wait()
}

func (s *supervisor) workerIDs() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make(map[string]bool, len(s.ids))
	for id := range s.ids {
		ids[id] = true
	}
	return ids
}

// workerLogs collects every worker's JSON log lines into one file and counts
// events (fenced, reaped, released, ...) as evidence of which paths ran.
type workerLogs struct {
	mu     sync.Mutex
	out    io.Writer
	counts map[string]int
}

func newWorkerLogs(out io.Writer) *workerLogs {
	return &workerLogs{out: out, counts: map[string]int{}}
}

func (l *workerLogs) writer() *lineWriter { return &lineWriter{logs: l} }

func (l *workerLogs) line(b []byte) {
	var ev struct {
		Event  string `json:"event"`
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(b, &ev)
	key := ev.Event
	if ev.Reason != "" {
		key += "/" + ev.Reason
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(b)
	if key != "" {
		l.counts[key]++
	}
}

func (l *workerLogs) eventCounts() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.counts))
	for k, n := range l.counts {
		out = append(out, fmt.Sprintf("%s=%d", k, n))
	}
	sort.Strings(out)
	return out
}

func (l *workerLogs) count(key string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[key]
}

// lineWriter hands complete lines to workerLogs, so lines from different
// processes never interleave. exec calls Write from a single goroutine.
type lineWriter struct {
	logs *workerLogs
	buf  []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.logs.line(w.buf[:i+1])
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

func (w *lineWriter) flush() {
	if len(w.buf) > 0 {
		w.logs.line(append(w.buf, '\n'))
		w.buf = nil
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
