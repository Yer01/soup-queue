// Package api is the HTTP interface of the job queue.
package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"soup-queue/internal/store"
)

// Config tunes the HTTP layer.
type Config struct {
	// AccessLog enables one log line per request.
	AccessLog bool
}

// App holds the routes and their dependencies.
type App struct {
	store  *store.Store
	router *gin.Engine
}

// New builds the router. The caller owns the http.Server and its lifecycle.
func New(s *store.Store, cfg Config) *App {
	r := gin.New()
	if cfg.AccessLog {
		r.Use(gin.Logger())
	}
	r.Use(gin.Recovery())

	a := &App{store: s, router: r}

	r.GET("/healthz", a.healthz)
	r.POST("/jobs", a.createJob)
	r.GET("/jobs", a.listJobs)
	r.GET("/jobs/:id", a.getJob)
	r.POST("/jobs/:id/cancel", a.cancelJob)

	r.NoRoute(func(c *gin.Context) {
		writeError(c, http.StatusNotFound, "not_found", "no such route", nil)
	})
	return a
}

// Handler returns the http.Handler serving all routes.
func (a *App) Handler() http.Handler {
	return a.router
}
