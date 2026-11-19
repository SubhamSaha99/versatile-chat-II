package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"versatile-chat-II/internal/config"
	"versatile-chat-II/internal/database"
	"versatile-chat-II/internal/modules/auth"
	appserver "versatile-chat-II/internal/server"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Connect Database
func connectDatabase(cfg *config.EnvConfig) (
	*mongo.Client,
	*mongo.Database,
	error,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	client, db, err := database.ConnectMongo(
		ctx,
		cfg.MongoURI,
		cfg.MongoDatabase,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"initialize database: %w",
			err,
		)
	}

	slog.Info("MongoDB connected successfully")

	return client, db, nil
}

// Disconnect database
func disconnectDatabase(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	if err := database.DisconnectMongo(ctx, client); err != nil {
		slog.Error("MongoDB disconnect failed", "error", err)
		return
	}

	slog.Info("MongoDB disconnected successfully")
}

// Register routes
func registerRoutes(
	cfg *config.EnvConfig,
	db *mongo.Database,
) (*gin.Engine, error) {
	app := gin.Default()

	authModule, err := auth.NewAuthModule(
		context.Background(),
		db,
		cfg.JWTSecret,
		cfg.JWTExpiration,
	)
	if err != nil {
		return nil, fmt.Errorf("initialize auth module %w", err)
	}

	app.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	api := app.Group("/api/v1")

	auth.RegisterRoutes(api, authModule.Controller)

	return app, nil
}

// Run server
func runServer(
	httpServer *appserver.Server,
	mongoClient *mongo.Client,
) {
	serverErrors := make(chan error, 1)

	go func() {
		serverErrors <- httpServer.Start()
	}()

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	select {
	case err := <-serverErrors:
		// Server stopped or failed before receiving a shutdown signal.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", "error", err)
		}

	case <-signalCtx.Done():
		slog.Info("Shutdown signal received")

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second,
		)

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown failed", "error", err)
		}

		cancel()

		// Wait for ListenAndServe to return.
		<-serverErrors
	}

	// Disconnect MongoDB only after the HTTP server has stopped.
	disconnectDatabase(mongoClient)
}

// Main func
func main() {
	cfg, err := config.LoadEnv()
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		return
	}

	mongoClient, db, err := connectDatabase(cfg)
	if err != nil {
		slog.Error("Database initialization failed", "error", err)
		return
	}

	app, err := registerRoutes(cfg, db)
	if err != nil {
		slog.Error("Error registering routes", "error", err)
	}

	httpServer := appserver.New(app, cfg.Port)

	runServer(httpServer, mongoClient)

	slog.Info("Application stopped")
}
