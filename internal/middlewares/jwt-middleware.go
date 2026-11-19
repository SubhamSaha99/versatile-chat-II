
package middlewares

import (
    "net/http"
    "strings"

    "github.com/gin-gonic/gin"
    "github.com/golang-jwt/jwt/v5"
)

const AuthenticatedUserIDKey = "userID"

func AuthMiddleware(jwtSecret string) gin.HandlerFunc {
    return func(c *gin.Context) {
        authHeader := c.GetHeader("Authorization")

        parts := strings.SplitN(authHeader, " ", 2)

        if len(parts) != 2 ||
            !strings.EqualFold(parts[0], "Bearer") {
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
                "message": "Missing or invalid authorization header",
            })
            return
        }

        token, err := jwt.ParseWithClaims(
            parts[1],
            &jwt.RegisteredClaims{},
            func(token *jwt.Token) (any, error) {
                if token.Method != jwt.SigningMethodHS256 {
                    return nil, jwt.ErrSignatureInvalid
                }

                return []byte(jwtSecret), nil
            },
        )

        if err != nil || !token.Valid {
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
                "message": "Invalid or expired token",
            })
            return
        }

        claims, ok := token.Claims.(*jwt.RegisteredClaims)
        if !ok || claims.Subject == "" {
            c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
                "message": "Invalid token claims",
            })
            return
        }

        c.Set(AuthenticatedUserIDKey, claims.Subject)
        c.Next()
    }
}
