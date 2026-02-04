package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// GarminClient implements bridgev2.NetworkAPI for a logged-in Garmin user.
type GarminClient struct {
	connector *GarminConnector
	userLogin *bridgev2.UserLogin
	log       zerolog.Logger
}

// Ensure GarminClient implements NetworkAPI
var _ bridgev2.NetworkAPI = (*GarminClient)(nil)

// Connect establishes the connection (no-op for Garmin as backend handles this).
func (gc *GarminClient) Connect(ctx context.Context) error {
	gc.log.Info().Msg("Client connecting")
	// The Python backend maintains the actual Garmin connection
	// We just verify it's accessible
	return gc.connector.verifyBackend(ctx)
}

// Disconnect closes the connection.
func (gc *GarminClient) Disconnect() {
	gc.log.Info().Msg("Client disconnecting")
	// Nothing to disconnect - backend stays running
}

// IsLoggedIn returns whether the client is logged in.
func (gc *GarminClient) IsLoggedIn() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return gc.connector.verifyBackend(ctx) == nil
}

// LogoutRemote logs out from the remote network.
func (gc *GarminClient) LogoutRemote(ctx context.Context) {
	gc.log.Info().Msg("Remote logout requested (no-op for Garmin)")
	// Garmin doesn't support remote logout - the app stays logged in
}

// IsThisUser checks if the given user ID is this user.
func (gc *GarminClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return string(userID) == gc.connector.config.UserID
}

// GetChatInfo returns information about a chat/conversation.
func (gc *GarminClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	gc.log.Debug().Str("portal_id", string(portal.ID)).Msg("GetChatInfo")

	// TODO: Fetch chat info from backend via /portals endpoint
	// For now, return basic info
	return &bridgev2.ChatInfo{
		Name:   nil,
		Topic:  nil,
		Avatar: nil,
		Members: &bridgev2.ChatMemberList{
			IsFull: false,
		},
	}, nil
}

// GetUserInfo returns information about a user.
func (gc *GarminClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	gc.log.Debug().Str("user_id", string(ghost.ID)).Msg("GetUserInfo")

	// Garmin doesn't provide user profile info, just identifiers
	userID := string(ghost.ID)
	return &bridgev2.UserInfo{
		Name: &userID,
		Identifiers: []string{
			userID,
		},
	}, nil
}

// HandleMatrixMessage handles an outgoing message from Matrix.
func (gc *GarminClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	gc.log.Info().
		Str("room_id", msg.Portal.MXID.String()).
		Str("event_id", msg.Event.ID.String()).
		Msg("Handling Matrix message")

	// Extract text from the event
	content := msg.Content
	text := ""
	if content.Body != "" {
		text = content.Body
	}

	if text == "" {
		return nil, fmt.Errorf("empty message body")
	}

	// Send via backend
	portalID := string(msg.Portal.ID)
	eventID := msg.Event.ID.String()
	roomID := msg.Portal.MXID.String()

	messageID, err := gc.connector.SendMessage(ctx, portalID, text, eventID, roomID)
	if err != nil {
		return nil, err
	}

	return &bridgev2.MatrixMessageResponse{
		DB: &bridgev2.DBMessage{
			ID: MakeMessageID(messageID),
		},
	}, nil
}

// HandleMatrixEdit handles an edit from Matrix.
func (gc *GarminClient) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	gc.log.Warn().Msg("Edit not supported by Garmin")
	return fmt.Errorf("edits not supported")
}

// HandleMatrixMessageRemove handles a message removal from Matrix.
func (gc *GarminClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	gc.log.Warn().Msg("Message removal not supported by Garmin")
	return fmt.Errorf("message removal not supported")
}

// HandleMatrixReaction handles a reaction from Matrix.
func (gc *GarminClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*bridgev2.DBReaction, error) {
	gc.log.Warn().Msg("Reactions from Matrix not yet implemented")
	// TODO: Implement reaction sending via backend
	return nil, fmt.Errorf("reactions not implemented")
}

// HandleMatrixReactionRemove handles a reaction removal from Matrix.
func (gc *GarminClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	gc.log.Warn().Msg("Reaction removal not supported by Garmin")
	return fmt.Errorf("reaction removal not supported")
}

// HandleMatrixReadReceipt handles a read receipt from Matrix.
func (gc *GarminClient) HandleMatrixReadReceipt(ctx context.Context, receipt *bridgev2.MatrixReadReceipt) error {
	// Garmin doesn't support read receipts
	return nil
}

// HandleMatrixTyping handles typing notifications from Matrix.
func (gc *GarminClient) HandleMatrixTyping(ctx context.Context, typing *bridgev2.MatrixTyping) error {
	// Garmin doesn't support typing notifications
	return nil
}
