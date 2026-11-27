package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"versatile-chat-II/internal/config"
	"versatile-chat-II/internal/database"
	"versatile-chat-II/internal/kafka"
	"versatile-chat-II/internal/modules/auth"
	"versatile-chat-II/internal/modules/message"
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
	kafkaProducer *kafka.KafkaProducer,
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

	messageService := message.NewMessageService(kafkaProducer)
	messageGateway := message.NewMessageGateway(messageService)
	message.RegisterRoutes(api, messageGateway, cfg.JWTSecret)

	return app, nil
}

// Run server
func runServer(
	httpServer *appserver.Server,
	mongoClient *mongo.Client,
	kafkaProducer *kafka.KafkaProducer,
	messageConsumer *kafka.MessageConsumer,
) {
	serverErrors := make(chan error, 1)
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	consumerErrors := make(chan error, 1)

	go func() {
		serverErrors <- httpServer.Start()
	}()
	go func() {
		consumerErrors <- messageConsumer.Run(consumerCtx)
	}()

	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	consumerStopped := false
	select {
	case err := <-serverErrors:
		// Server stopped or failed before receiving a shutdown signal.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", "error", err)
		}

	case err := <-consumerErrors:
		consumerStopped = true
		if err != nil {
			slog.Error("Kafka message consumer stopped", "error", err)
		}
		shutdownHTTPServer(httpServer)
		<-serverErrors

	case <-signalCtx.Done():
		slog.Info("Shutdown signal received")
		shutdownHTTPServer(httpServer)
		// Wait for ListenAndServe to return.
		<-serverErrors
	}

	cancelConsumer()
	if !consumerStopped {
		if err := <-consumerErrors; err != nil {
			slog.Error("Kafka message consumer stopped", "error", err)
		}
	}

	messageConsumer.Close()
	disconnectDatabase(mongoClient)
	kafkaProducer.Close()
}

func shutdownHTTPServer(httpServer *appserver.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown failed", "error", err)
	}
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

	kafkaProducer, err := kafka.NewKafkaProducer(cfg.KafkaBrokerUrl, cfg.KafkaTopic)
	if err != nil {
		slog.Error("Kafka producer initialization failed", "error", err)
		disconnectDatabase(mongoClient)
		return
	}

	flushIntervalMS, err := strconv.Atoi(cfg.KafkaFlushInterval)
	if err != nil || flushIntervalMS <= 0 {
		slog.Error("Invalid Kafka flush interval", "value", cfg.KafkaFlushInterval)
		kafkaProducer.Close()
		disconnectDatabase(mongoClient)
		return
	}
	batchSize, err := strconv.Atoi(cfg.KafkaBatchSize)
	if err != nil || batchSize <= 0 {
		slog.Error("Invalid Kafka batch size", "value", cfg.KafkaBatchSize)
		kafkaProducer.Close()
		disconnectDatabase(mongoClient)
		return
	}

	messageRepository := message.NewMessageRepository(db)
	messageConsumer, err := kafka.NewMessageConsumer(
		cfg.KafkaBrokerUrl,
		cfg.KafkaTopic,
		cfg.KafkaConsumerGroup,
		messageRepository,
		time.Duration(flushIntervalMS)*time.Millisecond,
		batchSize,
	)
	if err != nil {
		slog.Error("Kafka message consumer initialization failed", "error", err)
		kafkaProducer.Close()
		disconnectDatabase(mongoClient)
		return
	}

	app, err := registerRoutes(cfg, db, kafkaProducer)
	if err != nil {
		slog.Error("Error registering routes", "error", err)
		messageConsumer.Close()
		kafkaProducer.Close()
		disconnectDatabase(mongoClient)
		return
	}

	httpServer := appserver.New(app, cfg.Port)

	runServer(httpServer, mongoClient, kafkaProducer, messageConsumer)

	slog.Info("Application stopped")
}
