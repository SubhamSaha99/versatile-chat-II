package message

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type Message struct {
	ID         bson.ObjectID `bson:"_id,omitempty" json:"id"`
	SenderID   bson.ObjectID `bson:"sender_id" json:"senderId"`
	ReceiverID bson.ObjectID `bson:"receiver_id" json:"receiverId"`
	Body       string        `bson:"body" json:"body"`
	CreatedAt  time.Time     `bson:"created_at" json:"createdAt"`
}
