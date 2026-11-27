package message

type sendMessageRequest struct {
    ReceiverID string `json:"receiverId"`
    Body       string `json:"body"`
}
type socketResponse struct {
	Type    string   `json:"type"`
	Message *Message `json:"message,omitempty"`
	Error   string   `json:"error,omitempty"`
}