package constants

import "errors"

var (
    ErrEmailAlreadyRegistered = errors.New("email already registered")
    ErrAccountAlreadyExists   = errors.New("email or username already exists")
    ErrInvalidCredentials     = errors.New("invalid email or password")
)