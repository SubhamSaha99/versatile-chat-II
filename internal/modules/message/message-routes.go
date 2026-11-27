package message

import (
	"versatile-chat-II/internal/middlewares"

	"github.com/gin-gonic/gin"
)

// RegisterRoutes mounts the message WebSocket endpoint behind JWT auth.
func RegisterRoutes(router *gin.RouterGroup, gateway *MessageGateway, jwtSecret string) {
	protectedMessages := router.Group("/messages")
	protectedMessages.Use(middlewares.AuthMiddleware(jwtSecret))
	protectedMessages.GET("/ws", gateway.Connect)
}
