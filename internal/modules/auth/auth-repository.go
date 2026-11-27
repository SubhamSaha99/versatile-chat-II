package auth

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type AuthRepository struct {
	collection *mongo.Collection
}

// Auth repository constructor
func NewAuthRepository(db *mongo.Database) *AuthRepository {
	return &AuthRepository{
		collection: db.Collection("users"),
	}
}

// Creating indexs
func (r *AuthRepository) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    map[string]int{"email": 1},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys:    map[string]int{"username": 1},
			Options: options.Index().SetUnique(true),
		},
	}

	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		return fmt.Errorf("create user indexes: %w", err)
	}

	return nil
}

// Create new user
func (r *AuthRepository) CreateUser(
	ctx context.Context,
	user *User,
) error {
	result, err := r.collection.InsertOne(ctx, user)
	if err != nil {
		return fmt.Errorf("insert user: %w", err)
	}

	id, ok := result.InsertedID.(bson.ObjectID)
	if !ok {
		return fmt.Errorf("insert user: unexpected inserted ID type %T", result.InsertedID)
	}
	user.ID = id

	return nil
}

// Find user by email
func (r *AuthRepository) FindByEmail(
	ctx context.Context,
	email string,
) (*User, error) {
	var user User

	err := r.collection.FindOne(
		ctx,
		map[string]any{"email": email},
	).Decode(&user)

	if err != nil {
		return nil, err
	}

	return &user, nil
}
