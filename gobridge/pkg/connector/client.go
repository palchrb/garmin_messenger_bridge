package connector

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
)

// GarminClient implements bridgev2.NetworkAPI for a logged-in Garmin user.
type GarminClient struct {
	connector *GarminConnector
	userLogin *bridgev2.UserLogin
	log       zerolog.Logger
}

// Ensure GarminClient implements NetworkAPI and IdentifierResolvingNetworkAPI
var _ bridgev2.NetworkAPI = (*GarminClient)(nil)
var _ bridgev2.IdentifierResolvingNetworkAPI = (*GarminClient)(nil)

// Connect establishes the connection (no-op for Garmin as backend handles this).
func (gc *GarminClient) Connect(ctx context.Context) {
	gc.log.Info().Msg("Client connecting")
	// The Python backend maintains the actual Garmin connection
	// We just verify it's accessible
	if err := gc.connector.verifyBackend(ctx); err != nil {
		gc.log.Error().Err(err).Msg("Failed to verify backend connection")
		gc.userLogin.BridgeState.Send(status.BridgeState{
			StateEvent: status.StateUnknownError,
			Error:      "garmin-backend-error",
			Message:    err.Error(),
		})
		return
	}
	gc.userLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateConnected,
	})
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
		DB: &database.Message{
			ID: MakeMessageID(messageID),
		},
	}, nil
}


// ResolveIdentifier looks up a Garmin user by their identifier (phone or email).
// This is used to start new DM chats.
func (gc *GarminClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	gc.log.Info().Str("identifier", identifier).Bool("create_chat", createChat).Msg("Resolving identifier")

	// For Garmin, we can create a DM with any valid identifier
	// The actual validation happens when the message is sent
	userID := MakeUserID(identifier)

	resp := &bridgev2.ResolveIdentifierResponse{
		UserID: userID,
		UserInfo: &bridgev2.UserInfo{
			Name:        &identifier,
			Identifiers: []string{identifier},
		},
	}

	if createChat {
		// Create a portal ID based on the remote user's ID
		portalID := MakePortalID(identifier)
		resp.Chat = &bridgev2.CreateChatResponse{
			PortalKey: networkid.PortalKey{
				ID:       portalID,
				Receiver: MakeUserLoginID(gc.connector.config.UserID),
			},
			PortalInfo: &bridgev2.ChatInfo{
				Members: &bridgev2.ChatMemberList{
					IsFull:           true,
					TotalMemberCount: 2,
					Members: []bridgev2.ChatMember{
						{
							EventSender: bridgev2.EventSender{
								IsFromMe: true,
								Sender:   MakeUserID(gc.connector.config.UserID),
							},
						},
						{
							EventSender: bridgev2.EventSender{
								Sender: userID,
							},
						},
					},
				},
			},
		}
	}

	return resp, nil
}

// GetCapabilities returns capabilities specific to this login.
func (gc *GarminClient) GetCapabilities(ctx context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{
		LocationMessage:  event.CapLevelFullySupported,
		Reaction:         event.CapLevelFullySupported,
		ReadReceipts:     false,
	}
}
