package connector

// PortalKey identifies a chat/conversation.
type PortalKey struct {
	ID       string `json:"id"`
	Receiver string `json:"receiver"`
}

// SenderInfo identifies the sender of a message.
type SenderInfo struct {
	ID       string `json:"id"`
	IsFromMe bool   `json:"is_from_me"`
}

// MediaInfo contains metadata about a media attachment.
type MediaInfo struct {
	ID       string `json:"id"`
	MimeType string `json:"mime_type,omitempty"`
	Filename string `json:"filename,omitempty"`
	Size     int64  `json:"size,omitempty"`
	Ready    bool   `json:"ready"`
}

// MessageContent holds the message content.
type MessageContent struct {
	MsgType  string  `json:"msgtype"`
	Body     string  `json:"body"`
	GeoURI   string  `json:"geo_uri,omitempty"`
	Altitude float64 `json:"altitude,omitempty"`
}

// MessageEvent represents an incoming message from the Garmin network.
type MessageEvent struct {
	Type       string         `json:"type"`
	PortalKey  PortalKey      `json:"portal_key"`
	Sender     SenderInfo     `json:"sender"`
	ID         string         `json:"id"`
	DeliveryID string         `json:"delivery_id"`
	Timestamp  int64          `json:"timestamp"`
	Content    MessageContent `json:"content"`
	Media      *MediaInfo     `json:"media,omitempty"`
}

// ReactionEvent represents an incoming reaction from the Garmin network.
type ReactionEvent struct {
	Type      string    `json:"type"`
	PortalKey PortalKey `json:"portal_key"`
	ID        string    `json:"id"`
	TargetID  string    `json:"target_id"`
	Emoji     string    `json:"emoji"`
	Operation string    `json:"operation"` // "add" or "remove"
	Timestamp int64     `json:"timestamp"`
}

// MediaReadyEvent is sent when media becomes available for download.
type MediaReadyEvent struct {
	Type       string    `json:"type"`
	DeliveryID string    `json:"delivery_id"`
	PortalKey  PortalKey `json:"portal_key"`
	Media      MediaInfo `json:"media"`
}

// ConnectedEvent is sent when SSE connection is established.
type ConnectedEvent struct {
	Type       string `json:"type"`
	ClientID   string `json:"client_id"`
	Timestamp  int64  `json:"timestamp"`
	BridgeMode string `json:"bridge_mode"`
}

// CapabilitiesResponse is the response from /capabilities endpoint.
type CapabilitiesResponse struct {
	OK           bool         `json:"ok"`
	Network      string       `json:"network"`
	Name         string       `json:"name"`
	Capabilities Capabilities `json:"capabilities"`
}

// Capabilities describes what the Garmin network supports.
type Capabilities struct {
	DisappearingMessages   bool     `json:"disappearing_messages"`
	Reactions              bool     `json:"reactions"`
	Replies                bool     `json:"replies"`
	Edits                  bool     `json:"edits"`
	Deletes                bool     `json:"deletes"`
	ReadReceipts           bool     `json:"read_receipts"`
	TypingNotifications    bool     `json:"typing_notifications"`
	Presence               bool     `json:"presence"`
	Captions               bool     `json:"captions"`
	LocationMessages       bool     `json:"location_messages"`
	ContactInfo            bool     `json:"contact_info"`
	MaxTextLength          int      `json:"max_text_length"`
	MediaTypes             []string `json:"media_types"`
}

// LoginVerifyResponse is the response from /login/verify endpoint.
type LoginVerifyResponse struct {
	OK                bool   `json:"ok"`
	LoggedIn          bool   `json:"logged_in"`
	UserID            string `json:"user_id,omitempty"`
	ConversationCount int    `json:"conversation_count,omitempty"`
	ADBConnected      bool   `json:"adb_connected,omitempty"`
	BridgeMode        string `json:"bridge_mode,omitempty"`
	Error             string `json:"error,omitempty"`
}

// PortalInfo describes a chat/conversation.
type PortalInfo struct {
	PortalKey     PortalKey `json:"portal_key"`
	Participants  []string  `json:"participants"`
	Name          *string   `json:"name"`
	IsDM          bool      `json:"is_dm"`
	LastMessageTS int64     `json:"last_message_ts"`
}

// ContactInfo describes a contact/user.
type ContactInfo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Identifier string `json:"identifier"`
}

// SendRequest is the request body for /send endpoint.
type SendRequest struct {
	PortalKey  *PortalKey      `json:"portal_key,omitempty"`
	PortalID   string          `json:"portal_id,omitempty"`
	Content    *MessageContent `json:"content,omitempty"`
	Text       string          `json:"text,omitempty"`
	EventID    string          `json:"event_id"`
	RoomID     string          `json:"room_id"`
	Recipients []string        `json:"recipients,omitempty"`
}

// SendResponse is the response from /send endpoint.
type SendResponse struct {
	OK        bool   `json:"ok"`
	JobID     int    `json:"job_id,omitempty"`
	Status    string `json:"status,omitempty"`
	PortalID  string `json:"portal_id,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	Error     string `json:"error,omitempty"`
}
