package auth

import (
    "time"

    "go.mongodb.org/mongo-driver/v2/bson"
)

type User struct {
    ID           bson.ObjectID `bson:"_id,omitempty" json:"id"`
    Username     string        `bson:"username" json:"username"`
    Email        string        `bson:"email" json:"email"`
    PasswordHash string        `bson:"password_hash" json:"-"`
    CreatedAt    time.Time     `bson:"created_at" json:"created_at"`
    UpdatedAt    time.Time     `bson:"updated_at" json:"updated_at"`
}
