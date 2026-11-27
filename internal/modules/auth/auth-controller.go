package auth

import (
	"errors"
	"net/http"
	"strings"
	"versatile-chat-II/internal/utils/constants"

	"github.com/gin-gonic/gin"
)

type AuthController struct {
	service *AuthService
}

// Auth controller constructor
func NewAuthController(service *AuthService) *AuthController {
	return &AuthController{
		service: service,
	}
}

// Register user controller
func (ctrl *AuthController) Register(c *gin.Context) {
	var req RegisterDTO

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid registration details",
			"error":   err.Error(),
		})
		return
	}

	result, err := ctrl.service.Register(
		c.Request.Context(),
		req,
	)
	if err != nil {
		status := http.StatusInternalServerError

		if strings.Contains(err.Error(), "already registered") ||
			strings.Contains(err.Error(), "already exists") {
			status = http.StatusConflict
		}

		c.JSON(status, gin.H{"message": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Registration successful",
		"data":    gin.H{"id": result},
	})
}

// User login controller
func (ctrl *AuthController) Login(c *gin.Context) {
	var req LoginDTO

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid login details",
			"error":   err.Error(),
		})
		return
	}

	result, err := ctrl.service.Login(
		c.Request.Context(),
		req,
	)
	if err != nil {
		if errors.Is(err, constants.ErrInvalidCredentials) {
			c.JSON(http.StatusUnauthorized, gin.H{
				"message": "Invalid email or password",
			})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Unable to login",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Login successful",
		"data":    result,
	})
}
