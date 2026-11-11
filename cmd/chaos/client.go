package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type opKind int

const (
	opCreate         opKind = iota // POST /jobs; retried on transport errors and 5xx
	opCreateLossy                  // POST /jobs with a tiny timeout: the response is "lost"
	opCreateConflict               // POST /jobs, same key, different payload: must get 409
	opCancel                       // POST /jobs/{id}/cancel; retried on transport errors and 5xx
)

func (k opKind) String() string {
	return [...]string{"create", "create_lossy", "create_conflict", "cancel"}[k]
}

const requestTimeout = 10 * time.Second

// keyInfo is one idempotency key, i.e. one logical job the client submits.
type keyInfo struct {
	key             string
	payload         []byte
	conflictPayload []byte
	fail            bool

	once    sync.Once
	idReady chan struct{}
	id      atomic.Int64
}

func (k *keyInfo) setID(id int64) {
	k.once.Do(func() {
		k.id.Store(id)
		close(k.idReady)
	})
}

// waitID blocks until a create for this key succeeded, so cancels and
// conflicting POSTs always target a job that exists.
func (k *keyInfo) waitID(ctx context.Context, timeout time.Duration) (int64, bool) {
	select {
	case <-k.idReady:
		return k.id.Load(), true
	case <-ctx.Done():
	case <-time.After(timeout):
	}
	return 0, false
}

type op struct {
	at      time.Duration // offset from the start of the client phase
	kind    opKind
	key     int
	timeout time.Duration // for opCreateLossy
}

// observation is one HTTP request as the client saw it.
type observation struct {
	kind    opKind
	key     int
	try     int
	start   time.Duration
	latency time.Duration
	status  int    // 0 when no response arrived
	errKind string // "" | "timeout" | "error" | "no_id"
	id      int64  // job id from the body (also from error.job on 409)
	state   string // job state from the body
	errCode string // error.code from the body
}

type client struct {
	api  string
	http *http.Client
	keys []*keyInfo
	ops  []op
	rep  *reporter

	mu       sync.Mutex
	obs      []observation
	start    time.Time
	maxLag   time.Duration
	duration time.Duration
}

// buildSchedule decides every request up front from the seed: payloads, which
// keys get duplicate, lost-response, conflicting and cancel requests, and when.
// Requests are spread so the total rate is about o.rate per second.
func buildSchedule(o options, runTag string, rng *rand.Rand) ([]*keyInfo, []op) {
	perKey := 1 + o.dup + o.lossy + o.conflict + o.cancel*1.2
	spacing := time.Duration(float64(time.Second) * perKey / o.rate)
	uniform := func(max time.Duration) time.Duration { return time.Duration(rng.Int64N(int64(max))) }

	keys := make([]*keyInfo, o.jobs)
	var ops []op
	for i := range o.jobs {
		dur := 10 + rng.IntN(91)
		if rng.Float64() < o.longFrac {
			dur = 1000 + rng.IntN(2001)
		}
		fail := rng.Float64() < o.failFrac
		payload := map[string]any{"duration_ms": dur, "fail": fail, "n": i}
		body, _ := json.Marshal(map[string]any{"payload": payload})
		payload["conflict"] = true
		conflictBody, _ := json.Marshal(map[string]any{"payload": payload})

		keys[i] = &keyInfo{
			key:             fmt.Sprintf("%s-%05d", runTag, i),
			payload:         body,
			conflictPayload: conflictBody,
			fail:            fail,
			idReady:         make(chan struct{}),
		}

		t0 := time.Duration(i) * spacing
		if rng.Float64() < o.lossy {
			ops = append(ops, op{at: t0, kind: opCreateLossy, key: i, timeout: time.Millisecond + uniform(19*time.Millisecond)})
			t0 += 20 * time.Millisecond // the normal create below is the client's retry
		}
		ops = append(ops, op{at: t0, kind: opCreate, key: i})
		if rng.Float64() < o.dup {
			if rng.IntN(2) == 0 {
				ops = append(ops, op{at: t0, kind: opCreate, key: i}) // concurrent duplicate
			} else {
				ops = append(ops, op{at: t0 + uniform(5*time.Second), kind: opCreate, key: i}) // late retry
			}
		}
		if rng.Float64() < o.conflict {
			ops = append(ops, op{at: t0 + uniform(2*time.Second), kind: opCreateConflict, key: i})
		}
		if rng.Float64() < o.cancel {
			// Half the cancels come within 150ms, while the job is usually still
			// queued or running; the rest within 3s, usually after it finished.
			// (With only the 3s window, 87% of cancels in the first smoke run
			// arrived after the job was done, so the cancel/complete race was
			// barely exercised.)
			c := t0 + uniform(3*time.Second)
			if rng.IntN(2) == 0 {
				c = t0 + uniform(150*time.Millisecond)
			}
			ops = append(ops, op{at: c, kind: opCancel, key: i})
			if rng.Float64() < 0.2 {
				ops = append(ops, op{at: c + uniform(500*time.Millisecond), kind: opCancel, key: i})
			}
		}
	}
	sort.SliceStable(ops, func(a, b int) bool { return ops[a].at < ops[b].at })
	return keys, ops
}

func newClient(api string, keys []*keyInfo, ops []op, rep *reporter) *client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 512
	tr.MaxIdleConnsPerHost = 512
	return &client{api: api, http: &http.Client{Transport: tr}, keys: keys, ops: ops, rep: rep}
}

// run fires every scheduled request at its time (open loop: a slow server does
// not slow the schedule down until 512 requests are in flight) and waits for
// all of them.
func (c *client) run(ctx context.Context) {
	c.start = time.Now()
	sem := make(chan struct{}, 512)
	var wg sync.WaitGroup
	for _, o := range c.ops {
		if d := time.Until(c.start.Add(o.at)); d > 0 && !sleepCtx(ctx, d) {
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		if lag := time.Since(c.start.Add(o.at)); lag > c.maxLag {
			c.maxLag = lag
		}
		wg.Add(1)
		go func(o op) {
			defer wg.Done()
			defer func() { <-sem }()
			c.do(ctx, o)
		}(o)
	}
	wg.Wait()
	c.duration = time.Since(c.start)
}

func (c *client) do(ctx context.Context, o op) {
	k := c.keys[o.key]
	switch o.kind {
	case opCreateLossy:
		ob := c.post(ctx, "/jobs", k.key, k.payload, o.timeout)
		ob.kind, ob.key = o.kind, o.key
		if ob.status == http.StatusOK || ob.status == http.StatusCreated {
			k.setID(ob.id)
		}
		c.record(ob)

	case opCreate:
		c.withRetry(ctx, o, func() observation {
			ob := c.post(ctx, "/jobs", k.key, k.payload, requestTimeout)
			if ob.status == http.StatusOK || ob.status == http.StatusCreated {
				k.setID(ob.id)
			}
			return ob
		})

	case opCreateConflict, opCancel:
		id, ok := k.waitID(ctx, time.Minute)
		if !ok {
			c.record(observation{kind: o.kind, key: o.key, start: time.Since(c.start), errKind: "no_id"})
			return
		}
		c.withRetry(ctx, o, func() observation {
			if o.kind == opCancel {
				return c.post(ctx, fmt.Sprintf("/jobs/%d/cancel", id), "", nil, requestTimeout)
			}
			return c.post(ctx, "/jobs", k.key, k.conflictPayload, requestTimeout)
		})
	}
}

// withRetry repeats a request on transport errors and 5xx, like a real client,
// recording every try.
func (c *client) withRetry(ctx context.Context, o op, try func() observation) {
	backoff := 100 * time.Millisecond
	for n := 0; n < 6; n++ {
		ob := try()
		ob.kind, ob.key, ob.try = o.kind, o.key, n
		c.record(ob)
		if (ob.status != 0 && ob.status < 500) || !sleepCtx(ctx, backoff) {
			return
		}
		backoff *= 2
	}
}

type apiBody struct {
	ID    int64  `json:"id"`
	State string `json:"state"`
	Error *struct {
		Code string `json:"code"`
		Job  *struct {
			ID    int64  `json:"id"`
			State string `json:"state"`
		} `json:"job"`
	} `json:"error"`
}

func (c *client) post(ctx context.Context, path, key string, body []byte, timeout time.Duration) observation {
	ob := observation{start: time.Since(c.start)}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodPost, c.api+path, bytes.NewReader(body))
	if err != nil {
		ob.errKind = "error"
		return ob
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	t := time.Now()
	res, err := c.http.Do(req)
	if err == nil {
		var raw []byte
		raw, err = io.ReadAll(res.Body)
		res.Body.Close()
		if err == nil {
			ob.status = res.StatusCode
			var b apiBody
			_ = json.Unmarshal(raw, &b)
			ob.id, ob.state = b.ID, b.State
			if b.Error != nil {
				ob.errCode = b.Error.Code
				if b.Error.Job != nil {
					ob.id, ob.state = b.Error.Job.ID, b.Error.Job.State
				}
			}
		}
	}
	ob.latency = time.Since(t)
	if err != nil {
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
			ob.errKind = "timeout"
		} else {
			ob.errKind = "error"
		}
	}
	return ob
}

func (c *client) record(ob observation) {
	c.mu.Lock()
	c.obs = append(c.obs, ob)
	c.mu.Unlock()
}

func (c *client) observations() []observation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]observation(nil), c.obs...)
}
