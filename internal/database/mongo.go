package database

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ConnectMongo creates a MongoDB client and verifies that the server is reachable.
func ConnectMongo(ctx context.Context, uri, databaseName string) (*mongo.Client, *mongo.Database, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, nil, fmt.Errorf("create MongoDB client: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, nil, fmt.Errorf("ping MongoDB: %w", err)
	}

	return client, client.Database(databaseName), nil
}

// PingMongo verifies that the selected MongoDB database is reachable.
func PingMongo(ctx context.Context, db *mongo.Database) error {
	return db.RunCommand(ctx, bson.D{{Key: "ping", Value: 1}}).Err()
}

// DisconnectMongo closes a MongoDB client.
func DisconnectMongo(ctx context.Context, client *mongo.Client) error {
	return client.Disconnect(ctx)
}
