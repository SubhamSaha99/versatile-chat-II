package auth

import "time"

type RegisterDTO struct {
    Username string `json:"username" binding:"required,min=3,max=30"`
    Email    string `json:"email" binding:"required,email"`
    Password string `json:"password" binding:"required,min=8"`
}

type LoginDTO struct {
    Email    string `json:"email" binding:"required,email"`
    Password string `json:"password" binding:"required"`
}

type AuthResponseDTO struct {
    User      UserResponseDTO `json:"user"`
    Token     string          `json:"access_token"`
    TokenType string          `json:"token_type"`
}

type UserResponseDTO struct {
    ID        string    `json:"id"`
    Username  string    `json:"username"`
    Email     string    `json:"email"`
    CreatedAt time.Time `json:"created_at"`
}
