# soup-queue

Training-job queue service with multiple workers (Soup backend take-home).

The design, the guarantee, how each silent failure was checked, the chaos results and the verdict are in **[docs/design.md](docs/design.md)**.

## Run

    docker compose up --build

This starts Postgres (`localhost:15432`), the API (`http://localhost:8080`) and 4 workers.

The schema in `migrations/` is applied only when the database volume is first created. After changing it, run:

    docker compose down -v

## API

    POST /jobs                 Idempotency-Key: <key>   body: {"payload": {...}}
    GET  /jobs/{id}
    GET  /jobs?limit=50&cursor=<next_cursor>&state=<state>
    POST /jobs/{id}/cancel
    GET  /healthz

Example (on Windows use `curl.exe`, not the PowerShell `curl` alias):

    curl -i -X POST localhost:8080/jobs -H "Idempotency-Key: k1" -d '{"payload":{"duration_ms":200}}'

The emulated work reads two optional payload fields:
- `duration_ms`: how long the job runs (default: random 50–150 ms).
- `fail`: `true` makes the job fail.

## Worker settings (environment)

| Variable | Default | Meaning |
|---|---|---|
| `LEASE` | `5s` | How long a claim or heartbeat keeps a job. |
| `HEARTBEAT_EVERY` | lease / 5 | How often a worker extends its lease. |
| `MAX_ATTEMPTS` | `5` | Lease expiries before a job is failed. |
| `WORKER_ID` | hostname-pid-random | Unique per process. |
| `LOG_LEVEL` | `info` | `debug` also logs every claim and completion. |

## Chaos test

    scripts/chaos.sh 1 2 3 4 5                 # five runs, seeds 1-5
    scripts/chaos.sh 6 -- -fault nofence       # mutation run: expected to FAIL
    scripts/chaos.sh 7 -- -rate 1000           # 10x load

The script stops the compose workers before every seed, because the harness runs its own 8 worker processes and kills them. The harness also refuses to start while any other worker is connected to the database. Each run writes `runs/<run-id>/report.txt` and `runs/<run-id>/workers.log` and appends one line to `runs/index.tsv`. Run `/app/chaos -h` for all flags.

## Tests

The tests run against a real Postgres (the compose `db`) and are skipped when `TEST_DATABASE_URL` is unset. They truncate the tables, so don't run them during a chaos run.

    TEST_DATABASE_URL=postgres://soup:soup@localhost:15432/soup?sslmode=disable go test -p 1 ./...

## Layout

    cmd/api/            HTTP API entrypoint
    cmd/worker/         worker process entrypoint
    cmd/chaos/          chaos harness
    internal/api/       HTTP handlers
    internal/store/     Postgres access: jobs (API side) and worker.go (claim, heartbeat, complete, reap)
    internal/worker/    worker loop, heartbeat, reaper, emulated work
    internal/bootstrap/ env and database connection helpers
    internal/testdb/    test helper: connect to the test database and reset it
    migrations/         SQL schema
    scripts/chaos.sh    runs the harness for a list of seeds
    docs/design.md      design note, silent-failure checks, results, verdict
    runs/               raw output of every chaos run, including failed ones
