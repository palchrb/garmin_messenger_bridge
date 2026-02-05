// Package connector implements the mautrix-go bridgev2 NetworkConnector for Garmin Messenger.
//
// This connector communicates with a Python backend that handles:
// - ADB UI automation to interact with the Garmin Messenger Android app
// - Polling the Garmin Messenger SQLite database for new messages
// - Media file serving
//
// The Python backend exposes an HTTP API with SSE for event streaming.
package connector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

// GarminConnector implements bridgev2.NetworkConnector for Garmin Messenger.
type GarminConnector struct {
	bridge *bridgev2.Bridge
	log    zerolog.Logger
	config *Config

	// HTTP client for backend communication
	httpClient *http.Client

	// SSE connection management
	sseCancel context.CancelFunc
	sseMu     sync.Mutex

	// Active user login (we only support single-user for now)
	activeLogin   *bridgev2.UserLogin
	activeLoginMu sync.RWMutex
}

// Config holds the connector configuration.
type Config struct {
	// BackendURL is the URL of the Python backend API (e.g., "http://localhost:8808")
	BackendURL string `yaml:"backend_url"`

	// AuthToken is the bearer token for authenticating with the backend
	AuthToken string `yaml:"auth_token"`

	// UserID is the Garmin user identifier (phone number or email)
	UserID string `yaml:"user_id"`

	// ReconnectInterval is how long to wait before reconnecting SSE on disconnect
	ReconnectInterval time.Duration `yaml:"reconnect_interval"`
}

// Ensure GarminConnector implements NetworkConnector
var _ bridgev2.NetworkConnector = (*GarminConnector)(nil)

// NewGarminConnector creates a new Garmin Messenger connector.
func NewGarminConnector() *GarminConnector {
	return &GarminConnector{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		config: &Config{
			ReconnectInterval: 5 * time.Second,
		},
	}
}

// Init initializes the connector with the bridge instance.
func (gc *GarminConnector) Init(bridge *bridgev2.Bridge) {
	gc.bridge = bridge
	gc.log = bridge.Log.With().Str("connector", "garmin").Logger()
}

// Start starts the connector and begins listening for events.
func (gc *GarminConnector) Start(ctx context.Context) error {
	gc.log.Info().Msg("Starting Garmin connector")

	// Verify backend connectivity
	if err := gc.verifyBackend(ctx); err != nil {
		return fmt.Errorf("failed to verify backend: %w", err)
	}

	// Start SSE event listener in background
	go gc.runSSEListener(ctx)

	return nil
}

// GetName returns metadata about this network.
func (gc *GarminConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:      "Garmin Messenger",
		NetworkURL:       "https://www.garmin.com/en-US/p/878787",
		NetworkIcon:      "mdi:satellite-variant",
		NetworkID:        "garmin",
		BeeperBridgeType: "garmin",
		DefaultPort:      29340,
	}
}

// GetCapabilities returns the capabilities of this network.
func (gc *GarminConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{
		DisappearingMessages: false,
		AggressiveUpdateInfo: false,
	}
}

// GetConfig returns the config parts for the bridge.
func (gc *GarminConnector) GetConfig() (example string, data any, upgrader configupgrade.Upgrader) {
	return exampleConfig, gc.config, configupgrade.NoopUpgrader
}

// Example config for the connector.
const exampleConfig = `
# Garmin Messenger bridge configuration
garmin:
    # URL of the Python backend API
    backend_url: http://localhost:8808
    # Bearer token for authenticating with the backend
    auth_token: ""
    # Your Garmin user identifier (phone number or email)
    user_id: ""
    # How long to wait before reconnecting SSE on disconnect
    reconnect_interval: 5s
`

// GetDBMetaTypes returns database meta types for persisting login metadata.
func (gc *GarminConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		UserLogin: func() any { return &UserLoginMetadata{} },
	}
}

// GetBridgeInfoVersion returns version numbers for bridge info and room capabilities.
func (gc *GarminConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

// LoadUserLogin loads a user login from the database.
func (gc *GarminConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	// Load credentials from metadata
	meta, ok := login.Metadata.(*UserLoginMetadata)
	if ok && meta != nil {
		gc.config.BackendURL = meta.BackendURL
		gc.config.AuthToken = meta.AuthToken
		gc.config.UserID = meta.UserID
		gc.log.Info().
			Str("backend_url", meta.BackendURL).
			Str("user_id", meta.UserID).
			Msg("Loaded credentials from login metadata")
	}

	// Create a GarminClient for this login
	client := &GarminClient{
		connector: gc,
		userLogin: login,
		log:       gc.log.With().Str("user", string(login.ID)).Logger(),
	}
	login.Client = client

	// Track this as the active login
	gc.activeLoginMu.Lock()
	gc.activeLogin = login
	gc.activeLoginMu.Unlock()

	return nil
}

// getUserLogin returns the current active user login.
func (gc *GarminConnector) getUserLogin() *bridgev2.UserLogin {
	gc.activeLoginMu.RLock()
	defer gc.activeLoginMu.RUnlock()
	return gc.activeLogin
}

// GetLoginFlows returns the available login flows.
func (gc *GarminConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{
			Name:        "Backend Configuration",
			Description: "Configure the Python backend URL and authentication",
			ID:          "backend",
		},
	}
}

// CreateLogin creates a new login session.
func (gc *GarminConnector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	return &GarminLogin{
		connector: gc,
		user:      user,
		log:       gc.log.With().Str("flow", flowID).Logger(),
	}, nil
}

// verifyBackend checks if the Python backend is accessible and working.
func (gc *GarminConnector) verifyBackend(ctx context.Context) error {
	req, err := gc.newRequest(ctx, "GET", "/login/verify", nil)
	if err != nil {
		return err
	}

	resp, err := gc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("backend request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("backend returned status %d", resp.StatusCode)
	}

	var result struct {
		OK       bool   `json:"ok"`
		LoggedIn bool   `json:"logged_in"`
		Error    string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if !result.OK || !result.LoggedIn {
		return fmt.Errorf("backend not ready: %s", result.Error)
	}

	gc.log.Info().Msg("Backend verified successfully")
	return nil
}

// newRequest creates a new HTTP request to the backend.
func (gc *GarminConnector) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	u, err := url.JoinPath(gc.config.BackendURL, path)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, bodyReader)
	if err != nil {
		return nil, err
	}

	if gc.config.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+gc.config.AuthToken)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

// runSSEListener connects to the SSE event stream and processes events.
func (gc *GarminConnector) runSSEListener(ctx context.Context) {
	gc.sseMu.Lock()
	if gc.sseCancel != nil {
		gc.sseCancel()
	}
	sseCtx, cancel := context.WithCancel(ctx)
	gc.sseCancel = cancel
	gc.sseMu.Unlock()

	for {
		select {
		case <-sseCtx.Done():
			gc.log.Info().Msg("SSE listener stopped")
			return
		default:
			if err := gc.connectSSE(sseCtx); err != nil {
				gc.log.Error().Err(err).Msg("SSE connection error")
			}
			// Wait before reconnecting
			select {
			case <-sseCtx.Done():
				return
			case <-time.After(gc.config.ReconnectInterval):
			}
		}
	}
}

// connectSSE establishes an SSE connection and processes events.
func (gc *GarminConnector) connectSSE(ctx context.Context) error {
	req, err := gc.newRequest(ctx, "GET", "/events/stream", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// Use a client without timeout for SSE
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connection failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SSE returned status %d", resp.StatusCode)
	}

	gc.log.Info().Msg("SSE connected, listening for events")

	scanner := bufio.NewScanner(resp.Body)
	var eventType string
	var dataLines []string

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			// Empty line = end of event
			if eventType != "" && len(dataLines) > 0 {
				data := strings.Join(dataLines, "\n")
				gc.handleSSEEvent(ctx, eventType, data)
			}
			eventType = ""
			dataLines = nil
			continue
		}

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data:"))
		} else if strings.HasPrefix(line, ":") {
			// Comment (keep-alive), ignore
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("SSE read error: %w", err)
	}

	return nil
}

// handleSSEEvent processes a single SSE event.
func (gc *GarminConnector) handleSSEEvent(ctx context.Context, eventType, data string) {
	gc.log.Debug().Str("type", eventType).Msg("Received SSE event")

	switch eventType {
	case "connected":
		gc.log.Info().Msg("SSE connection confirmed by server")

	case "message":
		var event MessageEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			gc.log.Error().Err(err).Str("data", data).Msg("Failed to parse message event")
			return
		}
		gc.handleMessageEvent(ctx, &event)

	case "reaction":
		var event ReactionEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			gc.log.Error().Err(err).Str("data", data).Msg("Failed to parse reaction event")
			return
		}
		gc.handleReactionEvent(ctx, &event)

	case "media_ready":
		var event MediaReadyEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			gc.log.Error().Err(err).Str("data", data).Msg("Failed to parse media_ready event")
			return
		}
		gc.handleMediaReadyEvent(ctx, &event)

	default:
		gc.log.Warn().Str("type", eventType).Msg("Unknown event type")
	}
}

// handleMessageEvent processes an incoming message event.
func (gc *GarminConnector) handleMessageEvent(ctx context.Context, evt *MessageEvent) {
	gc.log.Info().
		Str("portal_id", evt.PortalKey.ID).
		Str("message_id", evt.ID).
		Bool("is_from_me", evt.Sender.IsFromMe).
		Msg("Processing message event")

	// Get the user login to queue the event
	userLogin := gc.getUserLogin()
	if userLogin == nil {
		gc.log.Warn().Msg("No user login found, cannot process message")
		return
	}

	// Determine sender info
	var sender bridgev2.EventSender
	if evt.Sender.IsFromMe {
		sender = bridgev2.EventSender{
			IsFromMe: true,
			Sender:   MakeUserID(gc.config.UserID),
		}
	} else {
		sender = bridgev2.EventSender{
			IsFromMe: false,
			Sender:   MakeUserID(evt.Sender.ID),
		}
	}

	// Build message content
	var msgType event.MessageType
	var content *event.MessageEventContent

	switch evt.Content.MsgType {
	case "m.text":
		msgType = event.MsgText
		content = &event.MessageEventContent{
			MsgType: msgType,
			Body:    evt.Content.Body,
		}
	case "m.location":
		msgType = event.MsgLocation
		content = &event.MessageEventContent{
			MsgType: msgType,
			Body:    evt.Content.Body,
			GeoURI:  evt.Content.GeoURI,
		}
	default:
		msgType = event.MsgText
		content = &event.MessageEventContent{
			MsgType: msgType,
			Body:    evt.Content.Body,
		}
	}

	// Create the remote event
	remoteEvt := &simplevent.Message[*event.MessageEventContent]{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventMessage,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.
					Str("portal_id", evt.PortalKey.ID).
					Str("message_id", evt.ID).
					Str("sender_id", evt.Sender.ID)
			},
			PortalKey: networkid.PortalKey{
				ID:       MakePortalID(evt.PortalKey.ID),
				Receiver: MakeUserLoginID(evt.PortalKey.Receiver),
			},
			Sender:       sender,
			CreatePortal: true,
			Timestamp:    time.Unix(evt.Timestamp, 0),
		},
		ID:   MakeMessageID(evt.ID),
		Data: content,
	}

	// If there's media, we'll need to handle it separately
	// For now, queue the text message
	gc.bridge.QueueRemoteEvent(userLogin, remoteEvt)
}

// handleReactionEvent processes an incoming reaction event.
func (gc *GarminConnector) handleReactionEvent(ctx context.Context, evt *ReactionEvent) {
	gc.log.Info().
		Str("portal_id", evt.PortalKey.ID).
		Str("reaction_id", evt.ID).
		Str("emoji", evt.Emoji).
		Str("operation", evt.Operation).
		Msg("Processing reaction event")

	userLogin := gc.getUserLogin()
	if userLogin == nil {
		gc.log.Warn().Msg("No user login found, cannot process reaction")
		return
	}

	portalKey := networkid.PortalKey{
		ID:       MakePortalID(evt.PortalKey.ID),
		Receiver: MakeUserLoginID(evt.PortalKey.Receiver),
	}

	eventType := bridgev2.RemoteEventReaction
	if evt.Operation == "remove" {
		eventType = bridgev2.RemoteEventReactionRemove
	}

	remoteEvt := &simplevent.Reaction{
		EventMeta: simplevent.EventMeta{
			Type: eventType,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.
					Str("portal_id", evt.PortalKey.ID).
					Str("reaction_id", evt.ID).
					Str("target_id", evt.TargetID)
			},
			PortalKey: portalKey,
			Sender: bridgev2.EventSender{
				IsFromMe: false,
				Sender:   MakeUserID(evt.PortalKey.Receiver),
			},
			Timestamp: time.Unix(evt.Timestamp, 0),
		},
		TargetMessage: MakeMessageID(evt.TargetID),
		Emoji:         evt.Emoji,
	}
	gc.bridge.QueueRemoteEvent(userLogin, remoteEvt)
}

// handleMediaReadyEvent processes a media_ready notification.
func (gc *GarminConnector) handleMediaReadyEvent(ctx context.Context, event *MediaReadyEvent) {
	gc.log.Info().
		Str("delivery_id", event.DeliveryID).
		Str("media_id", event.Media.ID).
		Msg("Media now ready for download")

	// TODO: Trigger media download for any pending messages waiting on this media
}

// FetchMedia downloads media from the backend.
func (gc *GarminConnector) FetchMedia(ctx context.Context, attachmentID string) (io.ReadCloser, string, error) {
	req, err := gc.newRequest(ctx, "GET", "/media/"+attachmentID, nil)
	if err != nil {
		return nil, "", err
	}

	resp, err := gc.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("media fetch failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, "", fmt.Errorf("media fetch returned status %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	return resp.Body, contentType, nil
}

// SendMessage sends a message to the Garmin network via the backend.
func (gc *GarminConnector) SendMessage(ctx context.Context, portalID, text, eventID, roomID string) (string, error) {
	body := map[string]any{
		"portal_key": map[string]string{
			"id":       portalID,
			"receiver": gc.config.UserID,
		},
		"content": map[string]string{
			"msgtype": "m.text",
			"body":    text,
		},
		"event_id": eventID,
		"room_id":  roomID,
	}

	req, err := gc.newRequest(ctx, "POST", "/send", body)
	if err != nil {
		return "", err
	}

	resp, err := gc.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("send request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		OK        bool   `json:"ok"`
		JobID     int    `json:"job_id"`
		MessageID string `json:"message_id,omitempty"`
		PortalID  string `json:"portal_id,omitempty"`
		Error     string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	if !result.OK {
		return "", fmt.Errorf("send failed: %s", result.Error)
	}

	gc.log.Info().Int("job_id", result.JobID).Msg("Message queued")
	return result.MessageID, nil
}

// networkid helpers

// MakeUserID creates a network user ID from a Garmin address (phone/email).
func MakeUserID(address string) networkid.UserID {
	return networkid.UserID(address)
}

// MakeUserLoginID creates a network user login ID from a Garmin address (phone/email).
func MakeUserLoginID(address string) networkid.UserLoginID {
	return networkid.UserLoginID(address)
}

// MakePortalID creates a network portal ID from a conversation ID.
func MakePortalID(conversationID string) networkid.PortalID {
	return networkid.PortalID(conversationID)
}

// MakeMessageID creates a network message ID from an OTA UUID.
func MakeMessageID(otaUUID string) networkid.MessageID {
	return networkid.MessageID(otaUUID)
}
