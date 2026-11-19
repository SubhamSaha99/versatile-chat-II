package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"golang.org/x/crypto/bcrypt"
)

type AuthService struct {
	repository  *AuthRepository
	jwtSecret   []byte
	tokenExpiry time.Duration
}

// Auth service constructor
func NewAuthService(
	repository *AuthRepository,
	jwtSecret string,
	tokenExpiry time.Duration,
) *AuthService {
	return &AuthService{
		repository:  repository,
		jwtSecret:   []byte(jwtSecret),
		tokenExpiry: tokenExpiry,
	}
}

// Register new User
func (s *AuthService) Register(
	ctx context.Context,
	req RegisterDTO,
) (*AuthResponseDTO, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))
	username := strings.TrimSpace(req.Username)

	// Check for existing user
	existingUser, err := s.repository.FindByEmail(ctx, email)
	if err == nil && existingUser != nil {
		return nil, errors.New("email already registered")
	}

	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("check existing user: %w", err)
	}

	// Hashing password
	passwordHash, err := bcrypt.GenerateFromPassword(
		[]byte(req.Password),
		bcrypt.DefaultCost,
	)
	if err != nil {
		return nil, fmt.Errorf("hash password: %w", err)
	}

	now := time.Now().UTC()

	user := &User{
		Username:     username,
		Email:        email,
		PasswordHash: string(passwordHash),
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	// Creating new user
	if err := s.repository.CreateUser(ctx, user); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil, errors.New("email or username already exists")
		}
		return nil, fmt.Errorf("create user: %w", err)
	}

	return s.buildAuthResponse(user)
}

// user login
func (s *AuthService) Login(
	ctx context.Context,
	req LoginDTO,
) (*AuthResponseDTO, error) {
	email := strings.ToLower(strings.TrimSpace(req.Email))

	user, err := s.repository.FindByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.New("invalid email or password")
		}
		return nil, fmt.Errorf("find user: %w", err)
	}

	if err := bcrypt.CompareHashAndPassword(
		[]byte(user.PasswordHash),
		[]byte(req.Password),
	); err != nil {
		return nil, errors.New("invalid email or password")
	}

	return s.buildAuthResponse(user)
}

func (s *AuthService) buildAuthResponse(
	user *User,
) (*AuthResponseDTO, error) {
	expiresAt := time.Now().Add(s.tokenExpiry)

	claims := jwt.RegisteredClaims{
		Subject:   user.ID.Hex(),
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(expiresAt),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	signedToken, err := token.SignedString(s.jwtSecret)
	if err != nil {
		return nil, fmt.Errorf("sign JWT: %w", err)
	}

	return &AuthResponseDTO{
		User: UserResponseDTO{
			ID:        user.ID.Hex(),
			Username:  user.Username,
			Email:     user.Email,
			CreatedAt: user.CreatedAt,
		},
		Token:     signedToken,
		TokenType: "Bearer",
	}, nil
}
