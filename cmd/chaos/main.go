// Command chaos is the chaos-test harness. It submits jobs through the HTTP
// API while running its own worker processes, kills a random worker with
// SIGKILL every couple of seconds, freezes others past their lease, retries and
// cancels requests in parallel, then checks the invariants against the
// database and what the client saw.
//
// It is meant to run in the Linux container (docker compose --profile chaos
// run --rm chaos). Every run writes report.txt and workers.log under RUNS_DIR
// and appends a line to RUNS_DIR/index.tsv. Exit code: 0 all checks passed,
// 1 some check failed, 2 the harness itself could not run.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"soup-queue/internal/bootstrap"
)

type options struct {
	jobs         int
	workers      int
	killEvery    time.Duration
	pauseEvery   time.Duration
	pauseFor     time.Duration
	lease        time.Duration
	rate         float64
	dup          float64
	lossy        float64
	conflict     float64
	cancel       float64
	longFrac     float64
	failFrac     float64
	seed         uint64
	timeout      time.Duration
	drainTimeout time.Duration
	fault        string
	dbURL        string
	apiURL       string
	workerBin    string
	runsDir      string
}

func parseFlags() options {
	var o options
	flag.IntVar(&o.jobs, "jobs", 10000, "number of distinct jobs (idempotency keys)")
	flag.IntVar(&o.workers, "workers", 8, "worker processes")
	flag.DurationVar(&o.killEvery, "kill-every", 2*time.Second, "mean time between kill -9 (0 disables)")
	flag.DurationVar(&o.pauseEvery, "pause-every", 15*time.Second, "mean time between SIGSTOP pauses (0 disables)")
	flag.DurationVar(&o.pauseFor, "pause-for", 0, "how long a paused worker stays frozen (default lease + 3s)")
	flag.DurationVar(&o.lease, "lease", 5*time.Second, "worker lease length")
	flag.Float64Var(&o.rate, "rate", 100, "target total HTTP requests per second")
	flag.Float64Var(&o.dup, "dup", 0.3, "fraction of keys POSTed a second time (half concurrently, half later)")
	flag.Float64Var(&o.lossy, "lossy", 0.05, "fraction of keys whose first POST uses a 1-20ms timeout (lost response), then retried")
	flag.Float64Var(&o.conflict, "conflict", 0.01, "fraction of keys also POSTed with a different payload (must get 409)")
	flag.Float64Var(&o.cancel, "cancel", 0.1, "fraction of keys cancelled after creation: half within 150ms, half within 3s (20% of them twice)")
	flag.Float64Var(&o.longFrac, "long", 0.02, "fraction of jobs that run 1-3s instead of 10-100ms")
	flag.Float64Var(&o.failFrac, "fail", 0.02, "fraction of jobs whose payload says fail")
	flag.Uint64Var(&o.seed, "seed", 1, "random seed for the schedule and the chaos")
	flag.DurationVar(&o.timeout, "timeout", 20*time.Minute, "abort the whole run after this long")
	flag.DurationVar(&o.drainTimeout, "drain-timeout", 3*time.Minute, "how long to wait for the queue to empty after the last request")
	flag.StringVar(&o.fault, "fault", "", "mutation run: nofence or unsafeclaim (the run is expected to FAIL)")
	flag.StringVar(&o.dbURL, "db", bootstrap.Getenv("DATABASE_URL", bootstrap.DefaultDatabaseURL), "database url")
	flag.StringVar(&o.apiURL, "api", bootstrap.Getenv("API_URL", "http://localhost:8080"), "api base url")
	flag.StringVar(&o.workerBin, "worker-bin", bootstrap.Getenv("WORKER_BIN", "/app/worker"), "worker binary")
	flag.StringVar(&o.runsDir, "runs-dir", bootstrap.Getenv("RUNS_DIR", "runs"), "where run output goes")
	flag.Parse()
	if o.pauseFor == 0 {
		o.pauseFor = o.lease + 3*time.Second
	}
	return o
}

func main() {
	os.Exit(run(parseFlags()))
}

// reporter writes timestamped events to stdout and the run's report file.
type reporter struct {
	mu    sync.Mutex
	w     io.Writer
	start time.Time
}

func (r *reporter) eventf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintf(r.w, "[%8.3fs] %s\n", time.Since(r.start).Seconds(), fmt.Sprintf(format, args...))
}

func (r *reporter) printf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintf(r.w, format, args...)
}

func run(o options) int {
	started := time.Now().UTC()
	runID := started.Format("20060102T150405Z") + fmt.Sprintf("-seed%d", o.seed)
	if o.fault != "" {
		runID += "-" + o.fault
	}
	runTag := fmt.Sprintf("r%d", started.Unix()%100000) // short prefix for keys and worker ids
	dir := filepath.Join(o.runsDir, runID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "create run dir:", err)
		return 2
	}
	reportFile, err := os.Create(filepath.Join(dir, "report.txt"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer reportFile.Close()
	workerLogFile, err := os.Create(filepath.Join(dir, "workers.log"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	defer workerLogFile.Close()

	rep := &reporter{w: io.MultiWriter(os.Stdout, reportFile), start: time.Now()}
	rep.printf("== soup-queue chaos run %s ==\n", runID)
	self, _ := os.Executable()
	rep.printf("binaries: chaos sha256:%s worker sha256:%s\n", fileHash(self), fileHash(o.workerBin))
	rep.printf("config: jobs=%d workers=%d kill-every=%s pause-every=%s pause-for=%s lease=%s rate=%.0f/s dup=%.2f lossy=%.2f conflict=%.2f cancel=%.2f long=%.2f fail=%.2f fault=%q seed=%d\n",
		o.jobs, o.workers, o.killEvery, o.pauseEvery, o.pauseFor, o.lease, o.rate, o.dup, o.lossy, o.conflict, o.cancel, o.longFrac, o.failFrac, o.fault, o.seed)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, o.timeout)
	defer cancelTimeout()

	pool, err := bootstrap.ConnectDB(ctx, o.dbURL, "soup-chaos", 8)
	if err != nil {
		rep.printf("HARNESS ERROR: %v\n", err)
		return 2
	}
	defer pool.Close()

	var version, isolation string
	_ = pool.QueryRow(ctx, `SELECT version()`).Scan(&version)
	_ = pool.QueryRow(ctx, `SHOW default_transaction_isolation`).Scan(&isolation)
	rep.printf("postgres: %s\ndefault_transaction_isolation: %s\n", version, isolation)
	if err := waitForAPI(ctx, o.apiURL); err != nil {
		rep.printf("HARNESS ERROR: %v\n", err)
		return 2
	}
	if err := checkNoOtherWorkers(ctx, pool, rep); err != nil {
		rep.printf("HARNESS ERROR: %v\n", err)
		return 2
	}

	rep.printf("-- timeline --\n")
	if _, err := pool.Exec(ctx, `TRUNCATE jobs, job_runs, executions RESTART IDENTITY CASCADE`); err != nil {
		rep.printf("HARNESS ERROR: reset tables: %v\n", err)
		return 2
	}
	rep.eventf("tables reset")

	rng := rand.New(rand.NewPCG(o.seed, 0)) // streams 1 and 2 drive the killer and the pauser
	keys, ops := buildSchedule(o, runTag, rng)
	rep.eventf("schedule: %d keys, %d requests over ~%.0fs", len(keys), len(ops), ops[len(ops)-1].at.Seconds())

	env := append(os.Environ(),
		"DATABASE_URL="+o.dbURL,
		"LEASE="+o.lease.String(),
		"FAULT="+o.fault,
		"LOG_LEVEL=info",
	)
	logs := newWorkerLogs(workerLogFile)
	sup := newSupervisor(o.workerBin, env, runTag, o.workers, logs, rep)
	if err := sup.startAll(); err != nil {
		rep.printf("HARNESS ERROR: %v\n", err)
		sup.stopAll(5 * time.Second)
		return 2
	}
	rep.eventf("started %d workers", o.workers)

	chaosCtx, stopChaos := context.WithCancel(ctx)
	var chaosWG sync.WaitGroup
	if o.killEvery > 0 {
		chaosWG.Add(1)
		go func() {
			defer chaosWG.Done()
			sup.killLoop(chaosCtx, o.killEvery, rand.New(rand.NewPCG(o.seed, 1)))
		}()
	}
	if o.pauseEvery > 0 {
		if canPause {
			chaosWG.Add(1)
			go func() {
				defer chaosWG.Done()
				sup.pauseLoop(chaosCtx, o.pauseEvery, o.pauseFor, rand.New(rand.NewPCG(o.seed, 2)))
			}()
		} else {
			rep.eventf("pauses disabled: SIGSTOP is not available on this OS")
		}
	}
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		sampleQueue(chaosCtx, pool, rep)
	}()

	cl := newClient(o.apiURL, keys, ops, rep)
	cl.run(ctx)
	rep.eventf("client finished: %d requests in %.1fs (max dispatch lag %s)", len(cl.observations()), cl.duration.Seconds(), cl.maxLag.Round(time.Millisecond))

	drainStart := time.Now()
	drained := waitForDrain(ctx, pool, o.drainTimeout)
	drainTime := time.Since(drainStart)
	if drained {
		rep.eventf("queue drained %.1fs after the last request", drainTime.Seconds())
	} else {
		rep.eventf("queue NOT drained after %s", o.drainTimeout)
	}

	stopChaos()
	chaosWG.Wait()
	sup.stopAll(10 * time.Second)
	rep.eventf("workers stopped")

	snap, err := loadSnapshot(context.Background(), pool)
	if err != nil {
		rep.printf("HARNESS ERROR: %v\n", err)
		return 2
	}
	obs := cl.observations()
	if err := writeRequests(filepath.Join(dir, "requests.tsv"), keys, obs); err != nil {
		rep.eventf("could not write requests.tsv: %v", err)
	}
	results := runChecks(snap, keys, obs, sup.workerIDs(), sup.unexpectedExits)

	stats := summarize(rep, o, cl, obs, snap, sup, logs, drainTime)
	failed := printChecks(rep, results)

	verdict := "PASS"
	if len(failed) > 0 {
		verdict = "FAIL"
	}
	rep.printf("\nRESULT: %s (%d of %d checks failed%s)\n", verdict, len(failed), len(results), listOrEmpty(failed))
	if o.fault != "" {
		rep.printf("note: mutation run with fault %q; a FAIL here means the harness caught the injected bug\n", o.fault)
	}

	appendIndex(o, runID, started, verdict, failed, stats, drainTime)
	if verdict == "FAIL" {
		return 1
	}
	return 0
}

func waitForAPI(ctx context.Context, api string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		res, err := http.Get(api + "/healthz")
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("api %s not healthy after 30s: %v", api, err)
		}
		if !sleepCtx(ctx, time.Second) {
			return ctx.Err()
		}
	}
}

// checkNoOtherWorkers refuses to start a run while a worker that is not part
// of this harness is connected: it would claim this run's jobs. Check W1
// catches that only after the run, as it did in four runs where the compose
// workers were restarted while a batch was in progress.
//
// A worker is recognized by its application_name, or, for worker binaries
// built before they set one, by its last query being a claim or a reap (both
// use FOR UPDATE SKIP LOCKED). Workers that are still shutting down get 10s.
func checkNoOtherWorkers(ctx context.Context, pool *pgxpool.Pool, rep *reporter) error {
	deadline := time.Now().Add(10 * time.Second)
	reported := false
	for {
		var sessions int
		var who string
		err := pool.QueryRow(ctx, `
			SELECT count(*),
			       coalesce(string_agg(DISTINCT coalesce(nullif(application_name, ''), 'unnamed')
			                || '@' || coalesce(host(client_addr), 'local'), ', '), '')
			FROM pg_stat_activity
			WHERE datname = current_database()
			  AND pid <> pg_backend_pid()
			  AND (application_name LIKE 'soup-worker%' OR query ILIKE '%skip locked%')`).Scan(&sessions, &who)
		if err != nil {
			return fmt.Errorf("check for other workers: %w", err)
		}
		if sessions == 0 {
			rep.eventf("preflight: no other workers connected")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d database session(s) belong to workers outside this harness (%s); "+
				"they would claim this run's jobs. Stop them first: docker compose stop worker", sessions, who)
		}
		if !reported {
			rep.eventf("preflight: waiting up to 10s for other workers to disconnect: %s", who)
			reported = true
		}
		if !sleepCtx(ctx, time.Second) {
			return ctx.Err()
		}
	}
}

func stateCounts(ctx context.Context, pool *pgxpool.Pool) (map[string]int, error) {
	rows, err := pool.Query(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		counts[s] = n
	}
	return counts, rows.Err()
}

func sampleQueue(ctx context.Context, pool *pgxpool.Pool, rep *reporter) {
	for sleepCtx(ctx, 5*time.Second) {
		c, err := stateCounts(ctx, pool)
		if err != nil {
			continue
		}
		rep.eventf("queue: queued=%d running=%d succeeded=%d failed=%d cancelled=%d",
			c["queued"], c["running"], c["succeeded"], c["failed"], c["cancelled"])
	}
}

func waitForDrain(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := stateCounts(ctx, pool)
		if err == nil && c["queued"]+c["running"] == 0 {
			return true
		}
		if !sleepCtx(ctx, 500*time.Millisecond) {
			return false
		}
	}
	return false
}

type runStats struct {
	kills, pauses  int
	createP99      time.Duration
	ranTwiceOrMore int
	fenced, reaped int
	requests       int
	achievedRate   float64
}

func summarize(rep *reporter, o options, cl *client, obs []observation, snap *snapshot, sup *supervisor, logs *workerLogs, drainTime time.Duration) runStats {
	var st runStats

	rep.printf("\n-- client --\n")
	statusCounts := map[string]int{}
	latencies := map[opKind][]time.Duration{}
	for _, ob := range obs {
		label := fmt.Sprintf("%d", ob.status)
		if ob.status == 0 {
			label = ob.errKind
		}
		statusCounts[fmt.Sprintf("%-15s %s", ob.kind, label)]++
		if ob.status != 0 && ob.status < 500 && ob.kind != opCreateLossy {
			latencies[ob.kind] = append(latencies[ob.kind], ob.latency)
		}
	}
	for _, line := range sortedKeys(statusCounts) {
		rep.printf("  %-28s %6d\n", line, statusCounts[line])
	}
	for _, k := range []opKind{opCreate, opCreateConflict, opCancel} {
		l := latencies[k]
		if len(l) == 0 {
			continue
		}
		sort.Slice(l, func(a, b int) bool { return l[a] < l[b] })
		pct := func(q float64) time.Duration { return l[int(q*float64(len(l)-1))].Round(100 * time.Microsecond) }
		rep.printf("  latency %-15s n=%d p50=%s p95=%s p99=%s max=%s\n", k, len(l), pct(0.5), pct(0.95), pct(0.99), l[len(l)-1].Round(100*time.Microsecond))
		if k == opCreate {
			st.createP99 = pct(0.99)
		}
	}
	st.requests = len(obs)
	st.achievedRate = float64(len(obs)) / cl.duration.Seconds()
	rep.printf("  achieved rate %.1f req/s (target %.0f), max dispatch lag %s\n", st.achievedRate, o.rate, cl.maxLag.Round(time.Millisecond))

	rep.printf("\n-- chaos and workers --\n")
	sup.mu.Lock()
	st.kills, st.pauses = sup.kills, sup.pauses
	rep.printf("  kills=%d pauses=%d respawns=%d unexpected_exits=%d worker_processes=%d\n",
		sup.kills, sup.pauses, sup.respawns, sup.unexpectedExits, len(sup.ids))
	sup.mu.Unlock()
	rep.printf("  worker events: %s\n", strings.Join(logs.eventCounts(), " "))
	st.fenced = logs.count("fenced/complete") + logs.count("fenced/heartbeat") + logs.count("fenced/fail")
	st.reaped = logs.count("reaped")

	runsHist := map[int]int{}
	states := map[string]int{}
	leaseFailures := 0
	for _, j := range snap.byID {
		runsHist[len(snap.runs[j.id])]++
		states[j.state]++
		if j.state == "failed" && strings.HasPrefix(j.lastError, "lease expired") {
			leaseFailures++
		}
		if len(snap.runs[j.id]) >= 2 {
			st.ranTwiceOrMore++
		}
	}
	var hist []string
	for n := range runsHist {
		hist = append(hist, fmt.Sprintf("%d run(s): %d", n, runsHist[n]))
	}
	sort.Strings(hist)
	effects := 0
	for _, ex := range snap.execs {
		effects += len(ex)
	}
	rep.printf("  runs per job: %s\n", strings.Join(hist, ", "))
	rep.printf("  jobs that ran more than once: %d; total side effects: %d\n", st.ranTwiceOrMore, effects)
	rep.printf("  final states: %v (failed by lease expiry: %d)\n", states, leaseFailures)
	rep.printf("  drain time after last request: %.1fs\n", drainTime.Seconds())
	return st
}

func printChecks(rep *reporter, results []*checkResult) []string {
	rep.printf("\n-- checks --\n")
	var failed []string
	for _, r := range results {
		status := "PASS"
		if !r.passed() {
			status = "FAIL"
			failed = append(failed, r.id)
		}
		rep.printf("[%s] %s %s", status, r.id, r.desc)
		if !r.passed() {
			rep.printf(" (%d violations)", len(r.violations))
		}
		rep.printf("\n")
		for i, v := range r.violations {
			if i == 20 {
				rep.printf("      ... and %d more\n", len(r.violations)-20)
				break
			}
			rep.printf("      %s\n", v)
		}
	}
	return failed
}

func appendIndex(o options, runID string, started time.Time, verdict string, failed []string, st runStats, drain time.Duration) {
	path := filepath.Join(o.runsDir, "index.tsv")
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "index.tsv:", err)
		return
	}
	defer f.Close()
	if os.IsNotExist(statErr) {
		fmt.Fprintln(f, "run_id\tstarted_utc\tseed\tfault\tjobs\tworkers\trate\tachieved_rate\tkills\tpauses\tresult\tfailed_checks\tcreate_p99_ms\tdrain_s\tjobs_ran_2plus\tfenced\treaped")
	}
	fault := o.fault
	if fault == "" {
		fault = "-"
	}
	failedList := strings.Join(failed, ",")
	if failedList == "" {
		failedList = "-"
	}
	fmt.Fprintf(f, "%s\t%s\t%d\t%s\t%d\t%d\t%.0f\t%.1f\t%d\t%d\t%s\t%s\t%.1f\t%.1f\t%d\t%d\t%d\n",
		runID, started.Format(time.RFC3339), o.seed, fault, o.jobs, o.workers, o.rate, st.achievedRate,
		st.kills, st.pauses, verdict, failedList, float64(st.createP99.Microseconds())/1000, drain.Seconds(),
		st.ranTwiceOrMore, st.fenced, st.reaped)
}

// writeRequests saves every request the client made, in start order, so a
// latency spike or an odd response can be traced to the moment it happened.
// (Added after a run showed a ~2s stall that the aggregated numbers could not
// place in time.)
func writeRequests(path string, keys []*keyInfo, obs []observation) error {
	sorted := append([]observation(nil), obs...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a].start < sorted[b].start })

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprintln(f, "start_ms\tkind\tkey\ttry\tstatus\terror\tlatency_ms\tid\tstate\terror_code")
	for _, ob := range sorted {
		fmt.Fprintf(f, "%.1f\t%s\t%s\t%d\t%d\t%s\t%.2f\t%d\t%s\t%s\n",
			float64(ob.start.Microseconds())/1000, ob.kind, keys[ob.key].key, ob.try, ob.status,
			ob.errKind, float64(ob.latency.Microseconds())/1000, ob.id, ob.state, ob.errCode)
	}
	return nil
}

// fileHash identifies exactly which binary ran: the first 12 hex digits of its
// SHA-256. Every report prints it, so a stale image cannot go unnoticed.
func fileHash(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return "unavailable(" + err.Error() + ")"
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "unavailable(" + err.Error() + ")"
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

func sortedKeys(m map[string]int) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func listOrEmpty(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	return ": " + strings.Join(ids, ", ")
}
