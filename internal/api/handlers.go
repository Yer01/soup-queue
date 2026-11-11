package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"

	"soup-queue/internal/store"
)

const (
	maxBodyBytes   = 1 << 20 // 1 MiB
	maxKeyBytes    = 255
	defaultLimit   = 50
	maxLimit       = 500
	requestTimeout = 5 * time.Second
)

// dbContext bounds a request's database work. It is also cancelled when the
// client goes away.
func dbContext(c *gin.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Request.Context(), requestTimeout)
}

func internalError(c *gin.Context, op string, err error) {
	log.Printf("%s: %v", op, err)
	writeError(c, http.StatusInternalServerError, "internal", "internal error", nil)
}

func (a *App) healthz(c *gin.Context) {
	ctx, cancel := dbContext(c)
	defer cancel()
	if err := a.store.Ping(ctx); err != nil {
		log.Printf("healthz: %v", err)
		writeError(c, http.StatusServiceUnavailable, "db_unavailable", "database unreachable", nil)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

type createJobRequest struct {
	Payload json.RawMessage `json:"payload"`
}

// createJob handles POST /jobs.
//
// A replay (same Idempotency-Key, same payload) returns 200 with the job's
// current state, not a copy of the first response.
func (a *App) createJob(c *gin.Context) {
	key := c.GetHeader("Idempotency-Key")
	if key == "" || len(key) > maxKeyBytes || !utf8.ValidString(key) {
		writeError(c, http.StatusBadRequest, "invalid_idempotency_key",
			"Idempotency-Key header is required: 1-255 bytes of UTF-8", nil)
		return
	}

	payload, status, msg := decodeCreateBody(c)
	if msg != "" {
		code := "invalid_body"
		if status == http.StatusRequestEntityTooLarge {
			code = "body_too_large"
		}
		writeError(c, status, code, msg, nil)
		return
	}

	ctx, cancel := dbContext(c)
	defer cancel()
	job, created, err := a.store.CreateJob(ctx, key, payload)
	switch {
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(c, http.StatusConflict, "idempotency_conflict",
			"Idempotency-Key was already used with a different payload", &job)
		return
	case err != nil:
		internalError(c, "create job", err)
		return
	}

	if created {
		c.Header("Location", "/jobs/"+strconv.FormatInt(job.ID, 10))
		c.JSON(http.StatusCreated, toJobResponse(job))
		return
	}
	c.JSON(http.StatusOK, toJobResponse(job))
}

// decodeCreateBody reads {"payload": {...}}. On failure it returns an HTTP
// status and a non-empty message.
func decodeCreateBody(c *gin.Context) (json.RawMessage, int, string) {
	body := http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()

	var req createJobRequest
	if err := dec.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, http.StatusRequestEntityTooLarge, "body exceeds 1 MiB"
		}
		return nil, http.StatusBadRequest, "body must be a JSON object like {\"payload\": {...}}: " + err.Error()
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, http.StatusBadRequest, "body must contain a single JSON object"
	}
	if p := bytes.TrimSpace(req.Payload); len(p) == 0 || p[0] != '{' {
		return nil, http.StatusBadRequest, "payload is required and must be a JSON object"
	}
	return req.Payload, 0, ""
}

// getJob handles GET /jobs/:id.
func (a *App) getJob(c *gin.Context) {
	id, ok := parseID(c)
	if !ok {
		return
	}
	ctx, cancel := dbContext(c)
	defer cancel()

	job, err := a.store.GetJob(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(c, http.StatusNotFound, "not_found", "job not found", nil)
		return
	case err != nil:
		internalError(c, "get job", err)
		return
	}
	c.JSON(http.StatusOK, toJobResponse(job))
}

// listJobs handles GET /jobs?limit=&cursor=&state=.
func (a *App) listJobs(c *gin.Context) {
	params := store.ListParams{Limit: defaultLimit}

	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxLimit {
			writeError(c, http.StatusBadRequest, "invalid_limit", "limit must be an integer from 1 to 500", nil)
			return
		}
		params.Limit = n
	}
	if v := c.Query("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(c, http.StatusBadRequest, "invalid_cursor", "cursor must be a next_cursor value from a previous page", nil)
			return
		}
		params.AfterID = n
	}
	if v := c.Query("state"); v != "" {
		s := store.State(v)
		if !s.Valid() {
			writeError(c, http.StatusBadRequest, "invalid_state",
				"state must be one of queued, running, succeeded, failed, cancelled", nil)
			return
		}
		params.State = s
	}

	ctx, cancel := dbContext(c)
	defer cancel()
	page, err := a.store.ListJobs(ctx, params)
	if err != nil {
		internalError(c, "list jobs", err)
		return
	}

	resp := listResponse{Items: make([]jobResponse, 0, len(page.Jobs))}
	for _, j := range page.Jobs {
		resp.Items = append(resp.Items, toJobResponse(j))
	}
	if page.NextCursor != nil {
		next := strconv.FormatInt(*page.NextCursor, 10)
		resp.NextCursor = &next
	}
	c.JSON(http.StatusOK, resp)
}

// cancelJob handles POST /jobs/:id/cancel. Cancelling an already cancelled
// job returns 200, so clients can retry safely.
func (a *App) cancelJob(c *gin.Context) {
	id, ok := parseID(c)
	if !ok {
		return
	}
	ctx, cancel := dbContext(c)
	defer cancel()

	job, _, err := a.store.CancelJob(ctx, id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(c, http.StatusNotFound, "not_found", "job not found", nil)
		return
	case errors.Is(err, store.ErrNotCancellable):
		writeError(c, http.StatusConflict, "not_cancellable",
			"job already finished as "+string(job.State), &job)
		return
	case err != nil:
		internalError(c, "cancel job", err)
		return
	}
	c.JSON(http.StatusOK, toJobResponse(job))
}

func parseID(c *gin.Context) (int64, bool) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(c, http.StatusBadRequest, "invalid_id", "job id must be a positive integer", nil)
		return 0, false
	}
	return id, true
}
