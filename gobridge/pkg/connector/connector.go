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
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
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

// GetConfig returns the connector configuration.
func (gc *GarminConnector) GetConfig() any {
	return gc.config
}

// GetDBMetaTypes returns database meta types (none needed for this connector).
func (gc *GarminConnector) GetDBMetaTypes() bridgev2.DBMetaTypes {
	return bridgev2.DBMetaTypes{}
}

// LoadUserLogin loads a user login from the database.
func (gc *GarminConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	// Create a GarminClient for this login
	client := &GarminClient{
		connector: gc,
		userLogin: login,
		log:       gc.log.With().Str("user", string(login.ID)).Logger(),
	}
	login.Client = client
	return nil
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
func (gc *GarminConnector) handleMessageEvent(ctx context.Context, event *MessageEvent) {
	gc.log.Info().
		Str("portal_id", event.PortalKey.ID).
		Str("message_id", event.ID).
		Bool("is_from_me", event.Sender.IsFromMe).
		Msg("Processing message event")

	// TODO: Queue this event to the bridge for processing
	// This would involve:
	// 1. Finding or creating the portal (room)
	// 2. Finding or creating the ghost user (sender)
	// 3. Creating the Matrix event
	// 4. If media is present and ready, fetching it via GET /media/{id}
}

// handleReactionEvent processes an incoming reaction event.
func (gc *GarminConnector) handleReactionEvent(ctx context.Context, event *ReactionEvent) {
	gc.log.Info().
		Str("portal_id", event.PortalKey.ID).
		Str("reaction_id", event.ID).
		Str("emoji", event.Emoji).
		Msg("Processing reaction event")

	// TODO: Queue this event to the bridge for processing
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

// MakePortalID creates a network portal ID from a conversation ID.
func MakePortalID(conversationID string) networkid.PortalID {
	return networkid.PortalID(conversationID)
}

// MakeMessageID creates a network message ID from an OTA UUID.
func MakeMessageID(otaUUID string) networkid.MessageID {
	return networkid.MessageID(otaUUID)
}
