package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"soup-queue/internal/api"
	"soup-queue/internal/store"
	"soup-queue/internal/testdb"
)

type jobBody struct {
	ID    int64  `json:"id"`
	State string `json:"state"`
}

type listBody struct {
	Items      []jobBody `json:"items"`
	NextCursor *string   `json:"next_cursor"`
}

type errBody struct {
	Error struct {
		Code string   `json:"code"`
		Job  *jobBody `json:"job"`
	} `json:"error"`
}

func newServer(t *testing.T) (*httptest.Server, *pgxpool.Pool) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pool := testdb.Pool(t)
	srv := httptest.NewServer(api.New(store.New(pool), api.Config{}).Handler())
	t.Cleanup(srv.Close)
	return srv, pool
}

// do sends a request and decodes the JSON response into out (if non-nil).
func do(t *testing.T, method, url, key, body string, out any) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, url, raw, err)
		}
	}
	return resp
}

func TestCreateIdempotentUnderConcurrency(t *testing.T) {
	srv, pool := newServer(t)

	const callers = 100
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		status = map[int]int{}
		ids    = map[int64]bool{}
		start  = make(chan struct{})
	)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/jobs", strings.NewReader(`{"payload":{"n":1}}`))
			req.Header.Set("Idempotency-Key", "same")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			var j jobBody
			_ = json.NewDecoder(resp.Body).Decode(&j)
			mu.Lock()
			defer mu.Unlock()
			status[resp.StatusCode]++
			ids[j.ID] = true
		}()
	}
	close(start)
	wg.Wait()

	if status[http.StatusCreated] != 1 || status[http.StatusOK] != callers-1 {
		t.Errorf("status counts = %v, want one 201 and %d 200", status, callers-1)
	}
	if len(ids) != 1 {
		t.Errorf("distinct ids = %v, want 1", ids)
	}
	var rows int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("rows in db = %d, want 1", rows)
	}
}

func TestCreateValidationAndConflict(t *testing.T) {
	srv, _ := newServer(t)
	url := srv.URL + "/jobs"

	var created jobBody
	resp := do(t, "POST", url, "k1", `{"payload":{"a":1}}`, &created)
	if resp.StatusCode != http.StatusCreated || resp.Header.Get("Location") != fmt.Sprintf("/jobs/%d", created.ID) {
		t.Fatalf("create: status=%d location=%q", resp.StatusCode, resp.Header.Get("Location"))
	}

	var eb errBody
	if resp := do(t, "POST", url, "k1", `{"payload":{"a":2}}`, &eb); resp.StatusCode != http.StatusConflict ||
		eb.Error.Code != "idempotency_conflict" || eb.Error.Job == nil || eb.Error.Job.ID != created.ID {
		t.Errorf("conflict: status=%d body=%+v", resp.StatusCode, eb)
	}

	cases := []struct {
		name, key, body string
		want            int
	}{
		{"missing key", "", `{"payload":{}}`, 400},
		{"key too long", strings.Repeat("x", 256), `{"payload":{}}`, 400},
		{"empty body", "k2", ``, 400},
		{"not json", "k2", `nope`, 400},
		{"payload missing", "k2", `{}`, 400},
		{"payload null", "k2", `{"payload":null}`, 400},
		{"payload array", "k2", `{"payload":[1]}`, 400},
		{"unknown field", "k2", `{"payload":{},"extra":1}`, 400},
		{"trailing data", "k2", `{"payload":{}} {}`, 400},
		{"too large", "k2", `{"payload":{"x":"` + strings.Repeat("a", 1<<20) + `"}}`, 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if resp := do(t, "POST", url, tc.key, tc.body, nil); resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	srv, _ := newServer(t)
	var created jobBody
	do(t, "POST", srv.URL+"/jobs", "k", `{"payload":{}}`, &created)

	var got jobBody
	if resp := do(t, "GET", fmt.Sprintf("%s/jobs/%d", srv.URL, created.ID), "", "", &got); resp.StatusCode != 200 ||
		got.ID != created.ID || got.State != "queued" {
		t.Errorf("get: status=%d body=%+v", resp.StatusCode, got)
	}
	if resp := do(t, "GET", srv.URL+"/jobs/999999", "", "", nil); resp.StatusCode != 404 {
		t.Errorf("missing: status = %d, want 404", resp.StatusCode)
	}
	for _, bad := range []string{"abc", "0", "-1", "99999999999999999999"} {
		if resp := do(t, "GET", srv.URL+"/jobs/"+bad, "", "", nil); resp.StatusCode != 400 {
			t.Errorf("id %q: status = %d, want 400", bad, resp.StatusCode)
		}
	}
}

// pageAll follows next_cursor until the end and returns every id seen.
func pageAll(t *testing.T, base string, limit int) (ids []int64, pages int) {
	t.Helper()
	cursor := ""
	for {
		url := fmt.Sprintf("%s/jobs?limit=%d", base, limit)
		if cursor != "" {
			url += "&cursor=" + cursor
		}
		var lb listBody
		if resp := do(t, "GET", url, "", "", &lb); resp.StatusCode != 200 {
			t.Fatalf("list: status %d", resp.StatusCode)
		}
		pages++
		for _, j := range lb.Items {
			ids = append(ids, j.ID)
		}
		if lb.NextCursor == nil {
			return ids, pages
		}
		cursor = *lb.NextCursor
	}
}

func assertStrictlyIncreasing(t *testing.T, ids []int64) {
	t.Helper()
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ids not strictly increasing at %d: %d then %d (duplicate or reordering)", i, ids[i-1], ids[i])
		}
	}
}

func TestListPagination(t *testing.T) {
	srv, _ := newServer(t)
	for i := range 103 {
		do(t, "POST", srv.URL+"/jobs", fmt.Sprintf("k%d", i), `{"payload":{}}`, nil)
	}

	ids, pages := pageAll(t, srv.URL, 10)
	if pages != 11 || len(ids) != 103 {
		t.Errorf("pages=%d ids=%d, want 11 pages and 103 ids", pages, len(ids))
	}
	assertStrictlyIncreasing(t, ids)

	var empty listBody
	do(t, "GET", srv.URL+"/jobs?state=running", "", "", &empty)
	if empty.Items == nil || len(empty.Items) != 0 || empty.NextCursor != nil {
		t.Errorf("empty filter: %+v, want items=[] next_cursor=null", empty)
	}

	for _, q := range []string{"limit=0", "limit=501", "limit=x", "cursor=-1", "cursor=x", "state=done"} {
		if resp := do(t, "GET", srv.URL+"/jobs?"+q, "", "", nil); resp.StatusCode != 400 {
			t.Errorf("%s: status = %d, want 400", q, resp.StatusCode)
		}
	}
}

// TestListDuringConcurrentInserts pages through the list while other jobs are
// being created. Keyset pagination must never return an id twice, and every
// job committed before paging started must be seen.
func TestListDuringConcurrentInserts(t *testing.T) {
	srv, _ := newServer(t)
	const before = 200
	for i := range before {
		do(t, "POST", srv.URL+"/jobs", fmt.Sprintf("pre-%d", i), `{"payload":{}}`, nil)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			req, _ := http.NewRequest("POST", srv.URL+"/jobs", strings.NewReader(`{"payload":{}}`))
			req.Header.Set("Idempotency-Key", fmt.Sprintf("during-%d", i))
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}
	}()

	ids, _ := pageAll(t, srv.URL, 7)
	close(stop)
	<-done

	assertStrictlyIncreasing(t, ids)
	if len(ids) < before {
		t.Fatalf("saw %d ids, want at least the %d created before paging", len(ids), before)
	}
	for i := range before {
		if ids[i] != int64(i+1) {
			t.Fatalf("pre-existing job %d missing: got id %d at position %d", i+1, ids[i], i)
		}
	}
}

func TestCancel(t *testing.T) {
	srv, pool := newServer(t)
	var j jobBody
	do(t, "POST", srv.URL+"/jobs", "k", `{"payload":{}}`, &j)
	cancelURL := fmt.Sprintf("%s/jobs/%d/cancel", srv.URL, j.ID)

	for i := range 2 {
		var got jobBody
		if resp := do(t, "POST", cancelURL, "", "", &got); resp.StatusCode != 200 || got.State != "cancelled" {
			t.Errorf("cancel #%d: status=%d state=%s", i+1, resp.StatusCode, got.State)
		}
	}

	var done jobBody
	do(t, "POST", srv.URL+"/jobs", "k-done", `{"payload":{}}`, &done)
	if _, err := pool.Exec(context.Background(),
		`UPDATE jobs SET state = 'succeeded', finished_at = now() WHERE id = $1`, done.ID); err != nil {
		t.Fatal(err)
	}
	var eb errBody
	if resp := do(t, "POST", fmt.Sprintf("%s/jobs/%d/cancel", srv.URL, done.ID), "", "", &eb); resp.StatusCode != 409 ||
		eb.Error.Code != "not_cancellable" || eb.Error.Job == nil || eb.Error.Job.State != "succeeded" {
		t.Errorf("cancel succeeded job: status=%d body=%+v", resp.StatusCode, eb)
	}

	if resp := do(t, "POST", srv.URL+"/jobs/999999/cancel", "", "", nil); resp.StatusCode != 404 {
		t.Errorf("cancel missing: status = %d, want 404", resp.StatusCode)
	}
}

func TestHealthzAndUnknownRoute(t *testing.T) {
	srv, _ := newServer(t)
	if resp := do(t, "GET", srv.URL+"/healthz", "", "", nil); resp.StatusCode != 200 {
		t.Errorf("healthz: status = %d", resp.StatusCode)
	}
	var eb errBody
	if resp := do(t, "GET", srv.URL+"/nope", "", "", &eb); resp.StatusCode != 404 || eb.Error.Code != "not_found" {
		t.Errorf("unknown route: status=%d code=%q", resp.StatusCode, eb.Error.Code)
	}
}
