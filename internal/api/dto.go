package api

import (
	"encoding/json"
	"time"

	"github.com/gin-gonic/gin"

	"soup-queue/internal/store"
)

type jobResponse struct {
	ID             int64           `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	State          store.State     `json:"state"`
	Payload        json.RawMessage `json:"payload"`
	Attempt        int             `json:"attempt"`
	WorkerID       *string         `json:"worker_id"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at"`
	LastError      *string         `json:"last_error"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     *time.Time      `json:"finished_at"`
}

func toJobResponse(j store.Job) jobResponse {
	return jobResponse{
		ID:             j.ID,
		IdempotencyKey: j.IdempotencyKey,
		State:          j.State,
		Payload:        j.Payload,
		Attempt:        j.Attempt,
		WorkerID:       j.WorkerID,
		LeaseExpiresAt: j.LeaseExpiresAt,
		LastError:      j.LastError,
		CreatedAt:      j.CreatedAt,
		UpdatedAt:      j.UpdatedAt,
		FinishedAt:     j.FinishedAt,
	}
}

type listResponse struct {
	Items      []jobResponse `json:"items"`
	NextCursor *string       `json:"next_cursor"`
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// Job is the current job, for errors where it helps the client decide what
	// to do next (409 on an idempotency conflict or an uncancellable job).
	Job *jobResponse `json:"job,omitempty"`
}

func writeError(c *gin.Context, status int, code, message string, job *store.Job) {
	detail := errorDetail{Code: code, Message: message}
	if job != nil {
		jr := toJobResponse(*job)
		detail.Job = &jr
	}
	c.AbortWithStatusJSON(status, errorResponse{Error: detail})
}
