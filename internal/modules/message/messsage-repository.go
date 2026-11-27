package message

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MessageRepository struct {
	collection *mongo.Collection
}

// Chat repository constructor
func NewMessageRepository(db *mongo.Database) *MessageRepository {
	return &MessageRepository{
		collection: db.Collection("messages"),
	}
}

// BulkUpsert stores messages idempotently by their stable Kafka message IDs.
func (r *MessageRepository) BulkUpsert(
	ctx context.Context,
	messages []Message,
) error {
	if len(messages) == 0 {
		return nil
	}

	operations := make([]mongo.WriteModel, len(messages))
	for i := range messages {
		message := messages[i]
		setOnInsert := bson.D{
			{Key: "sender_id", Value: message.SenderID},
			{Key: "receiver_id", Value: message.ReceiverID},
			{Key: "body", Value: message.Body},
			{Key: "created_at", Value: message.CreatedAt},
		}
		operations[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: message.ID}}).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: setOnInsert}}).
			SetUpsert(true)
	}

	if _, err := r.collection.BulkWrite(ctx, operations, options.BulkWrite().SetOrdered(false)); err != nil {
		return fmt.Errorf("upsert messages: %w", err)
	}

	return nil
}
