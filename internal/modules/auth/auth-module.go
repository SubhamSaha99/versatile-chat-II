package auth

import (
    "context"
    "fmt"
    "time"

    "go.mongodb.org/mongo-driver/v2/mongo"
)

type Module struct {
    Controller *AuthController
    Service    *AuthService
    Repository *AuthRepository
}

func NewAuthModule(
    ctx context.Context,
    db *mongo.Database,
    jwtSecret string,
    tokenExpiry time.Duration,
) (*Module, error) {

    repository := NewAuthRepository(db)

    if err := repository.EnsureIndexes(ctx); err != nil {
        return nil, fmt.Errorf("initialize auth indexes: %w", err)
    }

    service := NewAuthService(
        repository,
        jwtSecret,
        tokenExpiry,
    )

    controller := NewAuthController(service)

    return &Module{
        Repository: repository,
        Service:    service,
        Controller: controller,
    }, nil
}