package message

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"

	"versatile-chat-II/internal/middlewares"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

// MessageGateway manages active WebSocket connections for authenticated users.
type MessageGateway struct {
	mu             sync.Mutex
	connections    map[string]map[*websocket.Conn]struct{}
	messageService *MessageService
}

// Message gateway constructor
func NewMessageGateway(messageService *MessageService) *MessageGateway {
	return &MessageGateway{
		connections:    make(map[string]map[*websocket.Conn]struct{}),
		messageService: messageService,
	}
}

// Function to send a structured response back to the client over the WebSocket.
func writeSocketResponse(ctx context.Context, conn *websocket.Conn, response socketResponse) error {
	payload, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

// Function to create new user into the connections map
func (g *MessageGateway) register(userID string, conn *websocket.Conn) {
	/* 
	 * A mutex makes the map update happen one goroutine at a time. Without it, 
	 * two connections for the same user could both see that the user has no map yet, 
	 * then both try to create and assign one. 
	 */ 
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.connections[userID] == nil {
		g.connections[userID] = make(map[*websocket.Conn]struct{})
	}
	g.connections[userID][conn] = struct{}{}
}

// Function to delete 
func (g *MessageGateway) unregister(userID string, conn *websocket.Conn) {
	g.mu.Lock()
	defer g.mu.Unlock()

	userConnections := g.connections[userID]
	delete(userConnections, conn)
	if len(userConnections) == 0 {
		delete(g.connections, userID)
	}
}

// Disconnect removes an active connection from the MessageGateway's registry.
func (g *MessageGateway) Disconnect(userID string, conn *websocket.Conn) {
	g.unregister(userID, conn)
	_ = conn.Close(websocket.StatusNormalClosure, "connection closed")
}

// Function to connect websocket
func (g *MessageGateway) Connect(c *gin.Context) {
	userID, ok := c.Get(middlewares.AuthenticatedUserIDKey)
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"message": "Authentication required",
		})
		return
	}

	authenticatedUserID, ok := userID.(string)
	if !ok || authenticatedUserID == "" {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"message": "Invalid authenticated user",
		})
		return
	}

	conn, err := websocket.Accept(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(64 * 1024)

	g.register(authenticatedUserID, conn)
	defer func() {
		g.unregister(authenticatedUserID, conn)
		_ = conn.Close(websocket.StatusNormalClosure, "connection closed")
	}()

	for {
		messageType, payload, err := conn.Read(c.Request.Context())
		if err != nil {
			return
		}
		if messageType != websocket.MessageText {
			if err := writeSocketResponse(c.Request.Context(), conn, socketResponse{
				Type:  "error",
				Error: "Only text messages are supported",
			}); err != nil {
				return
			}
			continue
		}

		var request sendMessageRequest
		if err := json.Unmarshal(payload, &request); err != nil {
			if err := writeSocketResponse(c.Request.Context(), conn, socketResponse{
				Type:  "error",
				Error: "Invalid message payload",
			}); err != nil {
				return
			}
			continue
		}

		message, err := g.messageService.Send(c.Request.Context(), authenticatedUserID, request)
		if err != nil {
			if err := writeSocketResponse(c.Request.Context(), conn, socketResponse{
				Type:  "error",
				Error: err.Error(),
			}); err != nil {
				return
			}
			continue
		}

		if err := writeSocketResponse(c.Request.Context(), conn, socketResponse{
			Type:    "message_queued",
			Message: &message,
		}); err != nil {
			return
		}
	}
}

