package auth

import "github.com/gin-gonic/gin"

func RegisterRoutes(
    router *gin.RouterGroup,
    controller *AuthController,
) {
    authRoutes := router.Group("/auth")

    authRoutes.POST("/register", controller.Register)
    authRoutes.POST("/login", controller.Login)
}