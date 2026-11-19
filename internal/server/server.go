
package server

import (
    "context"
    "errors"
    "log/slog"
    "net/http"
    "time"
)

type Server struct {
    httpServer *http.Server
}

func New(handler http.Handler, port string) *Server {
    return &Server{
        httpServer: &http.Server{
            Addr:         ":" + port,
            Handler:      handler,
            ReadTimeout:  15 * time.Second,
            WriteTimeout: 15 * time.Second,
            IdleTimeout:  60 * time.Second,
        },
    }
}

// Start starts the HTTP server and blocks until it stops.
func (s *Server) Start() error {
    slog.Info("Starting HTTP server", "address", s.httpServer.Addr)

    err := s.httpServer.ListenAndServe()

    if errors.Is(err, http.ErrServerClosed) {
        return nil
    }

    return err
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
    slog.Info("Gracefully shutting down HTTP server")

    if err := s.httpServer.Shutdown(ctx); err != nil {
        slog.Error("Graceful shutdown failed", "error", err)
        
        return s.httpServer.Close()
    }

    slog.Info("HTTP server stopped successfully")
    return nil
}
