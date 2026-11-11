# Design note

A training-job queue: an HTTP API, Postgres, and N worker processes. This note covers how a worker claims a job, how a dead worker is detected, what happens when a running job is cancelled, which guarantee the system gives, how each silent failure was checked, the chaos results, and the verdict.

## Storage: Postgres, not SQLite

Several worker processes claim jobs concurrently, and the chaos test kills them with `kill -9`.

- Postgres has `SELECT … FOR UPDATE SKIP LOCKED`, so workers claim different rows without blocking each other.
- It has row-level locks, so a cancel, a completion and a reaper on the same row serialize instead of overwriting each other.
- It handles many concurrent writers from separate processes.

SQLite has one writer lock per database file and no `SKIP LOCKED`. Every claim, heartbeat, cancel and completion would queue behind that lock, and a writer killed while holding it is a failure mode that does not exist with Postgres connections.

## Data model

`queued → running → succeeded | failed | cancelled`. A lease expiry or a graceful shutdown sends `running` back to `queued`. Terminal states never change.

| Column / table | Role |
|---|---|
| `jobs.idempotency_key` UNIQUE | Deduplicates create requests. |
| `jobs.attempt` | **Fencing token.** Every claim increments it, and every worker write must match it. |
| `jobs.worker_id`, `jobs.lease_expires_at` | Who owns a running job, and until when (database clock). |
| `job_runs` | One row per claim, written in the same statement as the claim. Rows per job = how many times the job really started. **Not** unique on `(job_id, attempt)`, so a double claim is visible as two rows. |
| `executions` | The emulated side effect, one row per committed result. **Not** unique on `job_id`, so a double effect is a countable second row and not an insert error the harness could never see. Production should add the unique constraint back as defense in depth. |

CHECK constraints make inconsistent rows impossible instead of silent:
- `lease_iff_running`: a lease exists exactly while running.
- `running_has_worker`: a running job has an owner.
- `terminal_has_finished_at`: `finished_at` is set exactly in terminal states.

## The one concurrency rule

Every state change is **one conditional SQL statement** at the default **READ COMMITTED** isolation level. Where two tables change (claim + `job_runs`, complete + `executions`), it is one statement with a data-modifying CTE, so there is no multi-round-trip transaction for a crash to cut in half.

The statement takes a row lock. If another transaction changed the row first, Postgres waits for it, then re-checks the `WHERE` clause against the newest row version. Two competing transitions therefore cannot both succeed: the loser matches 0 rows and the code acts on that.

Every worker write is guarded by:

```sql
WHERE id = $1 AND attempt = $my_attempt AND state = 'running'
```

## How a worker claims a job

```sql
WITH picked AS (SELECT id FROM jobs WHERE state = 'queued' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED),
     claimed AS (UPDATE jobs SET state = 'running', attempt = attempt + 1, worker_id = $me,
                        lease_expires_at = now() + $lease ... RETURNING ...),
     run AS (INSERT INTO job_runs ... SELECT ... FROM claimed)
SELECT ... FROM claimed
```

- `SKIP LOCKED` lets 8 workers take 8 different rows at once.
- The claim and its `job_runs` row commit together.
- The lease end comes from the database clock.
- The queue is FIFO by id.
- An idle worker polls every ~150 ms with jitter. `LISTEN/NOTIFY` would cut idle queries, but polling is simpler and loses nothing when a notification is missed.

## How a dead worker is detected

**Lease + heartbeat + reaper.**

- The lease is 5 s. A worker extends it every 1 s with a fenced `Heartbeat`.
- A killed, frozen or partitioned worker stops heartbeating, and the lease expires.
- **Every worker** runs a reaper every ~1 s:

  ```sql
  UPDATE ... WHERE state = 'running' AND lease_expires_at < now() ... FOR UPDATE SKIP LOCKED LIMIT 100
  ```

  It moves the job back to `queued`. Once a job has used `MAX_ATTEMPTS` (5) claims, it goes to `failed` with `last_error = 'lease expired on attempt N'`, so a job that crashes every worker cannot loop forever.
- There is no reaper process to lose, and concurrent reapers are safe: `SKIP LOCKED` splits the rows, and a heartbeat that commits first moves the lease forward, so the row no longer matches on re-check.
- Detection time is between the lease (5 s) and lease + reap interval (~6.5 s).
- The worker also gives up locally if heartbeats fail with errors for longer than the lease. It measures this with Go's monotonic clock. This only saves wasted work; the database fence decides correctness.

**"A worker froze for 5 minutes and came back":**
1. Its lease expired after 5 s, a reaper requeued the job, and another worker claimed it with `attempt + 1`.
2. When the frozen worker wakes, its next heartbeat or its completion carries the old attempt and matches 0 rows, so it logs `event=fenced` and drops the result.
3. Its emulated work may already have run to the end, so the work was done twice, but only the current attempt's result is recorded.

`TestZombieWriteRejected` walks exactly this sequence, and the chaos harness reproduces it with `SIGSTOP`/`SIGCONT`.

## What happens when a running job is cancelled

`POST /jobs/{id}/cancel` moves a `queued` **or `running`** job to `cancelled` in one UPDATE, effective immediately, and the API's answer is final.

Nothing signals the worker. Its next heartbeat (≤ 1 s) matches 0 rows, it cancels the work's context and logs `fenced/heartbeat`. If it finishes before that heartbeat, its `Complete` matches 0 rows instead. Either way no side effect is written. This is the same fencing path that stops a zombie, so there is one mechanism, not two.

**Cancel vs. complete race:** both are single statements on the same row. Whichever takes the row lock first wins:
- If complete wins, cancel re-checks `state IN ('queued','running')`, finds `succeeded`, and the API returns 409 with the job.
- If cancel wins, `Complete` matches 0 rows.

`TestCancelCompleteRace` forces 500 of these races per run and fails unless both sides win some of them. One recorded run split 243 / 257.

**Cost:** after a cancel the worker keeps computing for up to one heartbeat interval. The work is wasted but never recorded.

## The guarantee: at-least-once execution, exactly-once effect

- **Execution is at-least-once.** A worker killed mid-job leaves the job running until its lease expires. The job is then re-claimed and the work runs again. The harness counts this from `job_runs`, and every run below had jobs that ran 2+ times.
- **The recorded effect is exactly-once.** The `executions` row is inserted in the *same statement* as the fenced `running → succeeded` update, so it commits if and only if that update wins. At most one attempt can win, because the winner moves the job out of `running`. A job that is never acknowledged as succeeded has no effect.
- **Why not at-most-once:** that would mean never re-running a job whose worker died. With `kill -9` every 2 s, many jobs would be lost; losing a training job is worse than running it twice.
- **The honest limit:** exactly-once holds only because the effect lives in the same database transaction as the state change. A real training job writes outside the database (checkpoints to S3, metrics, a webhook). Those effects would be at-least-once, and making them safe needs an idempotent sink keyed by `job_id` (or `job_id, attempt` plus a final pointer switched by the fenced completion).

## API

| Route | Behavior |
|---|---|
| `POST /jobs` | Requires the `Idempotency-Key` header (1–255 bytes) and a body `{"payload": {…}}`. Returns 201 with `Location` for a new job, 200 for a replay with the same payload, and 409 `idempotency_conflict` (including the existing job) for the same key with a different payload. |
| `GET /jobs/{id}` | 200 or 404. |
| `GET /jobs?limit=&cursor=&state=` | Keyset pagination by id. Returns `{"items": […], "next_cursor": "…" \| null}`. |
| `POST /jobs/{id}/cancel` | 200 with the cancelled job, also when it was already cancelled. 409 `not_cancellable` if the job succeeded or failed. 404 if it does not exist. |
| `GET /healthz` | Database ping. |

**Idempotency**
- `INSERT … ON CONFLICT (idempotency_key) DO NOTHING`. If nothing comes back, the code selects the existing row and compares payloads as `jsonb`, so key order and whitespace do not matter.
- `ON CONFLICT` waits for a concurrent inserter to commit, so 100 simultaneous POSTs with one key produce one row, one 201 and 99 × 200 (tested).
- A replay returns the job's **current** state, not a stored copy of the first response.
- Keys never expire.

**Pagination:** keyset (`id > cursor`), not OFFSET, so inserts never shift pages. A row whose sequence id is below the cursor but which commits after that page was read is never seen by that walk. The list is a live view, not a snapshot. The harness checks invariants against the database after writes stop, never by paging the API.

## Silent failures and how each was checked

Each mechanism has a deterministic test, an invariant checked under chaos, and where possible a mutation run that disables the mechanism and must make the harness fail.

| Failure | Mechanism | Evidence |
|---|---|---|
| **Double claim** | `FOR UPDATE SKIP LOCKED` plus the `state = 'queued'` re-check in one statement. The claim bumps `attempt` itself, never from an application-side read. | `TestClaimConcurrentEachJobOnce` (20 claimers, 200 jobs, each claimed once). Chaos **I6**: no duplicate `(job, attempt)` in `job_runs`, attempts 1..n with no gaps. Mutation `unsafeclaim` (read without lock, write `attempt = read + 1`): see results. |
| **Zombie writes a stale result** | Lease + reaper, and every worker write fenced on `attempt` and `state`. | `TestZombieWriteRejected`, `TestDeadWorkerJobIsRecovered`. Chaos: `SIGSTOP` a worker past its lease, then `SIGCONT`. **I4** requires the single effect to come from the job's *final* attempt. Worker logs count `fenced/*` events, which shows the path actually ran. Mutation `nofence`: see results. |
| **Cancel / complete race** | Both are single conditional statements on one row; the row lock decides. | `TestCancelCompleteRace` (500 forced races, must go both ways), `TestCancelAbortsRunningWork`. Chaos **I7**: a 200 cancel means the final state is `cancelled` with 0 effects; a 409 means the final state equals the 409 body. Half the chaos cancels land within 150 ms of creation. |
| **Reaper / heartbeat race** | Same rule: whoever locks first wins, and the other re-checks. | `TestReapHeartbeatRace` (300 forced races, must go both ways; one recorded run split 104 / 196). |
| **Duplicate on request retry** | Unique key + `ON CONFLICT DO NOTHING` + payload comparison. | `TestCreateIdempotentUnderConcurrency` (100 concurrent POSTs). Chaos **I1/I5**: 30% of keys POSTed twice (half concurrently), 5% sent first with a 1–20 ms timeout (lost response) then retried, 1% re-POSTed with a different payload (must get 409). The client checks it got one id per key and at most one 201. |
| **Transaction isolation level** | Correct at READ COMMITTED *because* every transition is a single statement that is re-checked under the row lock. The harness records `default_transaction_isolation` in every report. | Experiment: the same test suite with `default_transaction_isolation=repeatable read` (see results). Higher levels turn these races into serialization errors (40001), which the code does not retry: loud failures, not silent ones. |
| **Clock skew** | Every timestamp is the database's `now()`. Store functions take *durations*, never times, and the only Go clock use is monotonic elapsed time inside one process. | `grep` over `internal/`: no Go time is ever written to the database. `TestClaimSetsLeaseAndRunLog` measures the lease against the database clock. Residual risk: a clock step **on the database host** moves every lease at once. Forward: mass reaping, zombies fenced, safe but wasteful. Backward: slower dead-worker detection. |
| **Constraint-level corruption** | CHECK constraints on state, lease, owner and finished_at. | `TestSchemaRejectsInconsistentStates`. |

## Chaos harness

`cmd/chaos` runs inside the compose network (`docker compose --profile chaos run --rm chaos`) and drives the real API over HTTP. It does **not** use the compose worker service: it spawns its own worker processes so it can kill them with a real `SIGKILL` and control respawn exactly.

- **Load:** 10,000 jobs at ~100 requests/s, open loop. 98% of jobs take 10–100 ms and 2% take 1–3 s. 2% are told to fail.
- **Client behavior:**
  - 30% of keys are POSTed twice.
  - 5% of keys have their first POST timed out after 1–20 ms (the response is "lost") and are then retried.
  - 1% of keys are re-POSTed with a different payload.
  - 10% of keys are cancelled, half within 150 ms and half within 3 s; 20% of those cancels are sent twice.
  - Creates and cancels are retried on transport errors and 5xx.
- **Chaos:**
  - `kill -9` on a random worker every 2 s ± 50%, respawned 200 ms later with a new identity.
  - `SIGSTOP` on a random worker for lease + 3 s every ~15 s, then `SIGCONT`.
  - 8 worker processes at all times, apart from the 200 ms respawn gap.
- **End of run:** the queue drains, workers get `SIGTERM`, the database is snapshotted, and every invariant is checked against that snapshot *and* against every HTTP response the client recorded.

| # | Invariant |
|---|---|
| I1 | No job lost: every acknowledged create exists, with the id the client was given; no unknown jobs. |
| I2 | At most one side effect per job. |
| I3 | Nothing left `queued` or `running` after drain. |
| I4 | Consistent final states: succeeded ⇔ exactly one effect, *from the final attempt*; cancelled/failed ⇒ no effect; terminal ⇒ `finished_at` set and lease cleared; stored payload never overwritten. |
| I5 | Idempotent from the client's view: one id per key, at most one 201, conflicting payload ⇒ 409. |
| I6 | No double claim: each `(job, attempt)` claimed once, attempts 1..n with no gaps. |
| I7 | Cancel answers are truthful (200 ⇒ final cancelled; 409 ⇒ final state equals the 409 body). |
| I8 | Outcome matches payload (`fail=true` ⇒ failed; otherwise succeeded unless cancelled or failed by lease expiry). |
| W1 | Only this run's workers claimed jobs; no worker exited on its own. |
| A1 | Every key was eventually acknowledged. |
| A2 | No 5xx and no unexpected transport errors. |

Each run writes `runs/<run-id>/report.txt` (timeline, stats, checks with offending job ids) and `runs/<run-id>/workers.log` (every worker log line), and appends a line to `runs/index.tsv`. Nothing is deleted, including failed runs.

### What the harness does not catch

- **Database failures:** a Postgres crash, failover, replica lag, or disk full. Every guarantee above assumes one healthy primary.
- **API failures:** the API process is never killed, and there is only one instance. A crash between commit and response is approximated only by client timeouts.
- **Real network partitions:** a worker that can reach the database but not the reverse is approximated by `SIGSTOP`.
- **A clock step on the database host.**
- **External side effects** (see the guarantee section).
- **Scale:** 10,000 rows, 8 workers. Index bloat, vacuum, connection limits with hundreds of workers, and long jobs (hours) with heartbeats over flaky links are not exercised.
- **Rare interleavings:** coverage is statistical, and timing is random, not systematic. The forced-race unit tests and the mutation runs narrow this gap; they do not close it. Ten thousand jobs gives many hundreds of kills and requeues per run, but not every point inside a statement.

## Things that are easy to get wrong

- **A race test that never races proves nothing.** The first cancel/complete test passed 500 of 500, but cancel won every time. The completion ran as a multi-statement transaction, and the extra `BEGIN` round trip meant the two sides never overlapped. The race tests now fail unless both sides win sometimes.
- **A chaos scenario that rarely happens proves little.** In the first harness smoke run, 87% of cancels arrived after the job had already finished, because short jobs end long before a cancel sent 0–3 s later arrives. Half the cancels now come within 150 ms.
- **The harness can be stale too.** `docker compose run` never rebuilds an existing image, so eight runs used a harness older than its source. A missing output file gave it away. Each report now prints the hashes of the binaries that actually ran.
- **Ids have gaps.** `ON CONFLICT DO NOTHING` still uses up a sequence value, so every replayed POST burns an id. Never infer job counts from `max(id)`.
- **A unique constraint can hide what a test should measure.** Keeping `executions` unique on `job_id` would have turned a double effect into an error inside the worker. The harness could then report "0 duplicates" even with broken fencing.

## Results

All raw output is in `runs/`:
- one folder per run, with `report.txt`, `workers.log`, and (for harness v2) `requests.tsv`;
- one line per run in `runs/index.tsv`.

Every run is listed here, including the failed ones and the ones made with a stale harness.

**Harness versions.** The code under test (API, worker, store, schema) was identical in every run; only the harness changed.
- **v1** is the image built for the smoke run. It sends cancels 0–3 s after creation, writes no per-request log, and can respawn a worker during shutdown. `scripts/chaos.sh` never rebuilt it (see *Anomalies*), so every run until the v2 rerun used it.
- **v2** is the current source: `chaos sha256:ae42030ca3e2`, `worker sha256:76a70f73c33f`, printed at the top of each report. It sends half the cancels within 150 ms, writes `requests.tsv`, and never respawns during shutdown.

| Run | Harness | Purpose | kill -9 | Freezes | Jobs run 2+ times | Writes fenced | Reaped | Create p99 | Result |
|---|---|---|---|---|---|---|---|---|---|
| seed100 (1,000 jobs) | v1 | smoke test | 11 | 1 | 5 | 2 | 5 | 81 ms | PASS |
| seed1 | v1 | main | 70 | 6 | 49 | 43 | 53 | 48 ms | PASS |
| seed2 | v1 | main | 72 | 7 | 66 | 37 | 67 | 36 ms | PASS |
| seed3 | v1 | main | 71 | 7 | 48 | 37 | 50 | 15 ms | PASS |
| seed4 | v1 | main | 63 | 8 | 56 | 35 | 56 | 42 ms | PASS |
| seed5 | v1 | main | 68 | 6 | 56 | 42 | 56 | 11 ms | PASS |
| seed6 `-fault nofence` | v1 | mutation | 69 | 6 | 57 | 4 | 63 | 1,030 ms | **FAIL** I2, I4, I7 (expected) |
| seed7 `-fault unsafeclaim` | v1 | mutation | 122 | 12 | 5,565 | 9,960 | 24 | 42 ms | **FAIL** I6 (expected) |
| seed8 `-rate 1000` | v1 | 10x load | 55 | 5 | 57 | 7 | 57 | 136 ms | PASS |
| seed6 `-fault nofence` | v1 | rerun: does the stall repeat? | 70 | 6 | 58 | 5 | 63 | 11 ms | **FAIL** I2, I4, I7 (expected) |
| seed1 | v2 | main | 70 | 6 | 54 | 170 | 54 | 11 ms | PASS |
| seed2 | v2 | main | 72 | 7 | 60 | 121 | 67 | 63 ms | PASS |
| seed3 | v2 | main | 71 | 7 | 57 | 122 | 60 | 42 ms | PASS |
| seed4 | v2 | main | 63 | 8 | 48 | 162 | 51 | 4 ms | PASS |
| seed5 | v2 | main | 68 | 6 | 52 | 173 | 60 | 4 ms | PASS |

**15 harness runs in total:**
- 10 of the main configuration, all PASS;
- 3 mutation runs, all FAIL as intended;
- 1 at 10x load, PASS;
- 1 smoke test, PASS.

"Writes fenced" counts stale or cancelled results and heartbeats that a worker tried to write and the fence rejected (`fenced/*` in `workers.log`). Kill and freeze counts are the same for a seed in v1 and v2, because the killer and the pauser draw from their own seeded random streams.

### Main configuration

10 runs × 10,000 jobs (5 per harness version), 8 workers, ~97 requests/s, ~2.5 minutes each.

- **Chaos:** 688 `kill -9`s and 68 freezes past the lease.
- **Re-runs:** 546 jobs were executed more than once. At-least-once execution is observed, not just assumed.
- **Rejected writes:** 942 stale or cancelled writes were rejected by the fence, 194 in v1 and 748 in v2.
  - v2's early cancels catch jobs while they run: 419–585 cancels per run landed on a live job, against 131–298 in v1.
  - So the cancel/complete race runs well over a hundred times per v2 run.
- **Every check passed in every run:** 0 lost jobs, 0 jobs with two effects, 0 stuck, 0 untruthful cancel answers, 0 double claims, 0 HTTP 5xx.
- **API latency:** create p50 2–4 ms, p99 4–63 ms.
- **Workers kept up:** the queue was empty ≤ 4.5 s after the last request.
- **Rate shortfall:** the achieved ~96–97 rather than 100 req/s is the schedule's tail of late retries and cancels, not the server. The client's dispatch lag stayed ≤ 155 ms.

No run of the unmodified code failed. That is evidence, not proof; see *What the harness does not catch*.

### The mutation runs: can the harness see the bugs it is looking for?

These ran on harness v1. Its cancel timing makes them a weaker test of the cancel race, but they still failed.

- **`nofence`** (completion without the attempt/state guard): **caught, twice** (two runs, same seed).
  - I2: 6 and 5 jobs got two effects.
  - I4: 6 and 6 got an effect from a stale attempt.
  - I7: 26 and 29 cancels were answered 200 and then overwritten to `succeeded` by a late completion.
- **`unsafeclaim`** (read the oldest queued row without a lock, write `attempt = read + 1`): **caught by I6 only.**
  - 5,556 jobs were claimed twice with the same attempt. Eight workers polling the same oldest row collide constantly.
  - No job got two effects. The second claimer shared the attempt number, so the `state = 'running'` half of the fence rejected whichever finished second (9,743 rejected completions).
  - The cost showed up as throughput: 5,565 jobs ran twice, and the queue took 115 s to drain instead of ≤ 4.5 s.
  - The `terminal_has_finished_at` constraint stopped the broken claim from reviving a finished job 3 times (`claim_error` in `workers.log`).
  - Without the `job_runs` log, this bug would have been invisible to every other check.
- **Isolation experiment** (`runs/experiments/isolation-repeatable-read.txt`): the store tests with `default_transaction_isolation = repeatable read`.
  - 3 tests fail: cancel/complete race, concurrent claim, reap/heartbeat race.
  - There are 19 `40001 could not serialize access` errors. The conflicting statement errors instead of re-checking the newer row.
  - No duplicate claim or double effect appeared. A higher isolation level makes this code fail loudly (it would need retries), not silently. At READ COMMITTED the same tests pass.

### Anomalies, explained (and one not)

- **Eight runs used a stale harness.** After the `nofence` rerun, `requests.tsv` was missing even though the source wrote it.
  - The cause: `docker compose run` reuses an existing image and never rebuilds it, and `scripts/chaos.sh` rebuilt only `db` and `api`. The harness image was still the one built for the smoke run.
  - File times show only `cmd/chaos` changed after that build, so the system under test was the same. Those runs did, however, have the old cancel timing.
  - Fix: the script now builds the harness image, every report prints the SHA-256 of both binaries, and the main configuration was rerun as v2.
- **Short stalls of the whole request path: not explained.**
  - The first `nofence` run had create p99 1,030 ms (max 2.19 s). About 1% of every request type took over 1 s, which matches one ~2 s pause.
  - The rerun with the same seed and fault had p99 11 ms, so the fault does not cause it. The first run cannot be placed in time, because v1 kept no per-request log.
  - With v2's `requests.tsv`, seed2 shows a smaller one: 44 requests over 100 ms (max 607 ms), all between 69.7 s and 70.4 s. It does not line up with any kill or freeze.
  - Most likely the API, Postgres or the Docker Desktop VM paused briefly. Without server-side timings it cannot be pinned down.
  - Correctness was unaffected, but it matters for any latency target, and it is a reminder of why the lease (5 s) needs a wide margin.
- **`unsafeclaim`: one `complete_error: context deadline exceeded`.** Worker `w7-g15` was frozen with SIGSTOP from 16.96 s to 24.96 s, in the middle of its `Complete` call. When it woke, its own 5 s deadline had passed. The job's lease expired and it ran again, as designed.
- **seed1 (v1): `started=77` for 78 processes.** A kill 140 ms before the drain finished led to a respawn during shutdown, and that worker was stopped before it connected. It never touched a job. v2 does not respawn once chaos has stopped.

### 10x load (`-rate 1000`, harness v1)

- **The API was fine.** It served ~756 req/s (schedule-limited; dispatch lag ≤ 135 ms), with create p50 2 ms, p99 136 ms and 0 errors. All 11 checks passed.
- **The workers were the first thing to break.** 8 workers finished ~75 jobs/s (77/s measured between 30 s and 105 s), and jobs arrived at ~650/s. The backlog peaked at ~6,700 queued, and the queue emptied 104 s after the last request.
- **Nothing was incorrect, only late.** Queue wait grows without bound while arrivals exceed worker capacity, and nothing in the system reports it except the queue length.
- **Side effect on cancels:** at this load, 1,203 of 1,230 cancels found the job still queued.

Untested reasoning about what breaks next once workers are scaled out:
- **Connections:** each worker holds up to 4, and the API 20. Postgres's default `max_connections = 100` runs out at about 20 workers, so a pooler (PgBouncer) or smaller pools is needed.
- **Polling:** idle polling grows linearly with the number of workers.
- **Table growth:** at 100 creates/s the `jobs` table grows by millions of rows a day with no retention.

### How do we know 10,000 jobs was enough?

We don't, in general. But per v2 run there are 63–72 kills, 6–8 freezes, 51–67 requeues, 121–173 rejected stale writes and ~1,200 cancels (419–585 of them landing on live jobs). So every failure path ran dozens to hundreds of times per run, and hundreds to thousands of times across the ten main runs.

Both deliberately broken variants were caught in every run that used them. A bug at least as frequent as those would very likely have shown up. A bug that needs a rarer interleaving, such as a kill landing between two specific instructions, may not have. That is what the forced-race unit tests are for, and why they assert that both sides of each race actually won.

## Verdict at 100 requests per second: SHIP, with conditions

At 100 req/s every correctness check held in all ten main runs (688 `kill -9`s in total), under a failure rate far beyond anything production should see. API create p99 stayed at or below 63 ms with no errors, and the workers kept the queue empty. Both deliberately broken builds made the harness fail, so the passing runs mean something.

The latency picture has one unexplained blemish: brief stalls of the whole request path (0.6 s and ~2 s were seen). They did not affect correctness, but they need explaining before anyone promises a latency target.

What to fix first, in order, before real training jobs depend on it:

1. **External side effects.** The exactly-once claim holds only for the in-database effect. A real training job's checkpoints and metrics need an idempotent sink keyed by job (and attempt), or duplicate runs will produce duplicate outputs.
2. **Visibility.** Queue depth and age, reap rate, fenced writes and claim errors exist only in logs. Add metrics and alerts on them; at 10x the only symptom is a silently growing queue.
3. **Retention.** Archive finished jobs, `job_runs` and `executions`, and expire idempotency keys. Without that the tables grow without bound.
4. **Scaling the workers**, with connection pooling, before the load reaches what 8 workers can do (~75 jobs/s for jobs of this length).

## Surprises and open concerns

The biggest surprise was `unsafeclaim`. A claim broken badly enough to double-claim more than half of all jobs produced **zero** wrong outputs, because the completion fence covered for it. The only signs were the claim log and the halved throughput. A queue can be badly broken while every result looks right, which is why the harness checks how the work happened, not only what came out.

The second surprise was how easily a test can pass without testing anything:
- the first race test never raced;
- the first harness version almost never cancelled a job that was still running;
- a uniqueness constraint would have hidden double effects behind errors;
- eight runs reported results for a harness binary that no longer matched its source.

What still concerns me:
- **Real-world side effects** are only at-least-once;
- **One Postgres primary** carries every guarantee, and failover was never tested.
- **Unexplained stalls.** Short stalls of the request path (up to ~2 s) happened twice and remain unexplained.
- **A 5 s lease** means any stall longer than that (GC, network, or a longer version of those stalls) makes a job run twice. That is safe, but expensive for hour-long training jobs. The lease must be tuned against job length and heartbeat reliability.
- **Graceful releases count toward `MAX_ATTEMPTS`**, so frequent deploys could push an innocent job to `failed` after a single later lease expiry.
