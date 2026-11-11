package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// snapshot is the database after the run, loaded once so every check sees the
// same data.
type snapshot struct {
	jobs  map[string]*dbJob // by idempotency key
	byID  map[int64]*dbJob
	execs map[int64][]execRow
	runs  map[int64][]runRow
}

type dbJob struct {
	id          int64
	key         string
	state       string
	attempt     int
	finished    bool
	leaseNull   bool
	lastError   string
	conflictPay bool // stored payload is the conflicting one
}

type execRow struct {
	attempt int
	worker  string
}

type runRow struct {
	attempt int
	worker  string
}

func loadSnapshot(ctx context.Context, pool *pgxpool.Pool) (*snapshot, error) {
	s := &snapshot{
		jobs:  map[string]*dbJob{},
		byID:  map[int64]*dbJob{},
		execs: map[int64][]execRow{},
		runs:  map[int64][]runRow{},
	}

	rows, err := pool.Query(ctx, `
		SELECT id, idempotency_key, state, attempt, finished_at IS NOT NULL,
		       lease_expires_at IS NULL, coalesce(last_error, ''),
		       payload @> '{"conflict": true}'
		FROM jobs`)
	if err != nil {
		return nil, fmt.Errorf("load jobs: %w", err)
	}
	for rows.Next() {
		j := &dbJob{}
		if err := rows.Scan(&j.id, &j.key, &j.state, &j.attempt, &j.finished, &j.leaseNull, &j.lastError, &j.conflictPay); err != nil {
			rows.Close()
			return nil, err
		}
		s.jobs[j.key] = j
		s.byID[j.id] = j
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = pool.Query(ctx, `SELECT job_id, attempt, worker_id FROM executions`)
	if err != nil {
		return nil, fmt.Errorf("load executions: %w", err)
	}
	for rows.Next() {
		var id int64
		var e execRow
		if err := rows.Scan(&id, &e.attempt, &e.worker); err != nil {
			rows.Close()
			return nil, err
		}
		s.execs[id] = append(s.execs[id], e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = pool.Query(ctx, `SELECT job_id, attempt, worker_id FROM job_runs`)
	if err != nil {
		return nil, fmt.Errorf("load job_runs: %w", err)
	}
	for rows.Next() {
		var id int64
		var r runRow
		if err := rows.Scan(&id, &r.attempt, &r.worker); err != nil {
			rows.Close()
			return nil, err
		}
		s.runs[id] = append(s.runs[id], r)
	}
	return s, rows.Err()
}

type checkResult struct {
	id         string
	desc       string
	violations []string
}

func (c *checkResult) addf(format string, args ...any) {
	c.violations = append(c.violations, fmt.Sprintf(format, args...))
}

func (c *checkResult) passed() bool { return len(c.violations) == 0 }

// runChecks evaluates every invariant against the database snapshot and what
// the client observed. Each check lists every offending job, not just a count.
func runChecks(snap *snapshot, keys []*keyInfo, obs []observation, workerIDs map[string]bool, unexpectedExits int) []*checkResult {
	byKey := map[int][]observation{}
	for _, ob := range obs {
		byKey[ob.key] = append(byKey[ob.key], ob)
	}
	ourKeys := map[string]int{}
	for i, k := range keys {
		ourKeys[k.key] = i
	}

	i1 := &checkResult{id: "I1", desc: "no job lost: every acknowledged create exists with the id the client got; no unknown jobs"}
	a1 := &checkResult{id: "A1", desc: "every key was acknowledged (some create got 200/201)"}
	for i, k := range keys {
		ackID := int64(0)
		for _, ob := range byKey[i] {
			if (ob.kind == opCreate || ob.kind == opCreateLossy) && (ob.status == 200 || ob.status == 201) {
				ackID = ob.id
				break
			}
		}
		j := snap.jobs[k.key]
		switch {
		case ackID == 0:
			a1.addf("key %s: no create was acknowledged (in db: %v)", k.key, j != nil)
		case j == nil:
			i1.addf("key %s: acknowledged as job %d but missing from db", k.key, ackID)
		case j.id != ackID:
			i1.addf("key %s: client got id %d, db has id %d", k.key, ackID, j.id)
		}
	}
	for key, j := range snap.jobs {
		if _, ok := ourKeys[key]; !ok {
			i1.addf("job %d: key %q was never sent by this run", j.id, key)
		}
	}

	i2 := &checkResult{id: "I2", desc: "at most one side effect per job"}
	i3 := &checkResult{id: "I3", desc: "no job left queued or running after drain"}
	i4 := &checkResult{id: "I4", desc: "final states consistent: succeeded <=> exactly 1 effect from the final attempt; terminal => finished_at set, no lease; payload unchanged"}
	i6 := &checkResult{id: "I6", desc: "no double claim: each (job, attempt) claimed once, attempts 1..n with no gaps"}
	i8 := &checkResult{id: "I8", desc: "outcome matches payload: fail=true => failed, otherwise succeeded (unless cancelled or failed by lease expiry)"}
	for _, j := range snap.byID {
		ex := snap.execs[j.id]
		if len(ex) > 1 {
			i2.addf("job %d: %d effects (attempts %s)", j.id, len(ex), execAttempts(ex))
		}
		switch j.state {
		case "queued", "running":
			i3.addf("job %d: still %s (attempt %d)", j.id, j.state, j.attempt)
		case "succeeded":
			if len(ex) != 1 || ex[0].attempt != j.attempt {
				i4.addf("job %d: succeeded at attempt %d but effects are [%s]", j.id, j.attempt, execAttempts(ex))
			}
		case "failed", "cancelled":
			if len(ex) != 0 {
				i4.addf("job %d: %s but has %d effect(s) (attempts %s)", j.id, j.state, len(ex), execAttempts(ex))
			}
		}
		if j.state != "queued" && j.state != "running" && (!j.finished || !j.leaseNull) {
			i4.addf("job %d: %s but finished_at set=%v, lease cleared=%v", j.id, j.state, j.finished, j.leaseNull)
		}
		if j.conflictPay {
			i4.addf("job %d: stored payload was overwritten by a conflicting POST", j.id)
		}

		seen := map[int]int{}
		for _, r := range snap.runs[j.id] {
			seen[r.attempt]++
		}
		for a, n := range seen {
			if n > 1 {
				i6.addf("job %d: attempt %d claimed %d times", j.id, a, n)
			}
		}
		if len(seen) != j.attempt {
			i6.addf("job %d: attempt=%d but claims recorded for %d distinct attempts", j.id, j.attempt, len(seen))
		}
		for a := 1; a <= j.attempt; a++ {
			if seen[a] == 0 {
				i6.addf("job %d: no claim recorded for attempt %d", j.id, a)
			}
		}

		if idx, ok := ourKeys[j.key]; ok && j.state != "cancelled" && j.state != "queued" && j.state != "running" {
			wantFail := keys[idx].fail
			switch {
			case wantFail && j.state != "failed":
				i8.addf("job %d: payload fail=true but ended %s", j.id, j.state)
			case !wantFail && j.state == "failed" && !strings.HasPrefix(j.lastError, "lease expired"):
				i8.addf("job %d: payload fail=false but failed: %s", j.id, j.lastError)
			}
		}
	}

	i5 := &checkResult{id: "I5", desc: "idempotent from the client's view: one id per key, at most one 201, conflicting payloads get 409"}
	i7 := &checkResult{id: "I7", desc: "cancel responses truthful: 200 => final cancelled; 409 => final state equals the 409 body"}
	for i, k := range keys {
		ids := map[int64]bool{}
		created := 0
		for _, ob := range byKey[i] {
			switch ob.kind {
			case opCreate, opCreateLossy:
				if ob.status == 200 || ob.status == 201 {
					ids[ob.id] = true
				}
				if ob.status == 201 {
					created++
				}
				if ob.status == http.StatusConflict {
					i5.addf("key %s: same payload got 409 %s", k.key, ob.errCode)
				}
			case opCreateConflict:
				if ob.status != 0 && ob.status != http.StatusConflict && ob.status < 500 {
					i5.addf("key %s: conflicting payload got %d, want 409", k.key, ob.status)
				}
				if ob.status == http.StatusConflict {
					ids[ob.id] = true
				}
			case opCancel:
				j := snap.jobs[k.key]
				if j == nil || ob.status == 0 || ob.status >= 500 {
					continue
				}
				switch ob.status {
				case http.StatusOK:
					if j.state != "cancelled" {
						i7.addf("job %d: cancel returned 200 but final state is %s", j.id, j.state)
					}
				case http.StatusConflict:
					if ob.state != j.state || (j.state != "succeeded" && j.state != "failed") {
						i7.addf("job %d: cancel returned 409 with state %q but final state is %s", j.id, ob.state, j.state)
					}
				default:
					i7.addf("job %d: cancel returned unexpected %d %s", j.id, ob.status, ob.errCode)
				}
			}
		}
		if len(ids) > 1 {
			i5.addf("key %s: responses carried %d different ids %v", k.key, len(ids), sortedIDs(ids))
		}
		if created > 1 {
			i5.addf("key %s: %d responses were 201 Created", k.key, created)
		}
	}

	w1 := &checkResult{id: "W1", desc: "only this run's workers claimed jobs, and none crashed on its own"}
	foreign := map[string]bool{}
	for _, runs := range snap.runs {
		for _, r := range runs {
			if !workerIDs[r.worker] {
				foreign[r.worker] = true
			}
		}
	}
	for id := range foreign {
		w1.addf("worker %q is not one of the harness workers (is the compose worker service running?)", id)
	}
	if unexpectedExits > 0 {
		w1.addf("%d worker process(es) exited without being killed", unexpectedExits)
	}

	a2 := &checkResult{id: "A2", desc: "API health: no 5xx and no unexpected transport errors"}
	for _, ob := range obs {
		switch {
		case ob.status >= 500:
			a2.addf("%s key=%s: HTTP %d %s", ob.kind, keys[ob.key].key, ob.status, ob.errCode)
		case ob.status == 0 && ob.kind != opCreateLossy && ob.errKind != "no_id":
			a2.addf("%s key=%s: no response (%s) after %s", ob.kind, keys[ob.key].key, ob.errKind, ob.latency)
		}
	}

	results := []*checkResult{i1, i2, i3, i4, i5, i6, i7, i8, w1, a1, a2}
	for _, r := range results {
		sort.Strings(r.violations)
	}
	return results
}

func execAttempts(ex []execRow) string {
	parts := make([]string, len(ex))
	for i, e := range ex {
		parts[i] = fmt.Sprintf("%d by %s", e.attempt, e.worker)
	}
	return strings.Join(parts, ", ")
}

func sortedIDs(ids map[int64]bool) []int64 {
	out := make([]int64, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}
