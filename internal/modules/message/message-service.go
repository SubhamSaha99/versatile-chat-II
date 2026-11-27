package message

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const maxMessageBodyBytes = 4000

type MessagePublisher interface {
	Publish(ctx context.Context, key string, payload []byte) error
}

type MessageService struct {
	producer MessagePublisher
}


// Message service constructor
func NewMessageService(producer MessagePublisher) *MessageService {
	return &MessageService{producer: producer}
}

// Function to create conversation key
func conversationKey(message Message) string {
	participants := []string{message.SenderID.Hex(), message.ReceiverID.Hex()}
	sort.Strings(participants)
	return participants[0] + ":" + participants[1]
}

// Send validates client input, adds trusted sender metadata, and publishes the
// resulting message before returning it to the gateway for acknowledgement.
func (s *MessageService) Send(
	ctx context.Context,
	senderID string,
	request sendMessageRequest,
) (Message, error) {
	senderObjectID, err := bson.ObjectIDFromHex(senderID)
	if err != nil {
		return Message{}, errors.New("invalid sender ID")
	}

	receiverID := strings.TrimSpace(request.ReceiverID)
	receiverObjectID, err := bson.ObjectIDFromHex(receiverID)
	if err != nil {
		return Message{}, errors.New("invalid receiver ID")
	}

	body := strings.TrimSpace(request.Body)
	if body == "" {
		return Message{}, errors.New("message body cannot be empty")
	}
	if len(body) > maxMessageBodyBytes {
		return Message{}, errors.New("message body exceeds 4000 bytes")
	}
	if senderObjectID == receiverObjectID {
		return Message{}, errors.New("cannot send a message to yourself")
	}

	message := Message{
		ID:         bson.NewObjectID(),
		SenderID:   senderObjectID,
		ReceiverID: receiverObjectID,
		Body:       body,
		CreatedAt:  time.Now().UTC(),
	}

	payload, err := json.Marshal(message)
	if err != nil {
		return Message{}, errors.New("could not encode message")
	}

	if err := s.producer.Publish(ctx, conversationKey(message), payload); err != nil {
		slog.Error("Failed to publish chat message", "error", err)
		return Message{}, errors.New("could not queue message")
	}

	return message, nil
}