package connector

import (
	"context"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// GarminLogin implements bridgev2.LoginProcess for Garmin authentication.
type GarminLogin struct {
	connector *GarminConnector
	user      *bridgev2.User
	log       zerolog.Logger
}

// Ensure GarminLogin implements LoginProcess
var _ bridgev2.LoginProcess = (*GarminLogin)(nil)

// Start begins the login process.
func (gl *GarminLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	gl.log.Info().Msg("Starting login process")

	// The login for Garmin is just configuring the backend connection
	// The actual authentication happens in the Garmin Messenger Android app
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "backend_config",
		Instructions: "Configure the Python backend connection. The backend should already be running and connected to Redroid.",
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type:        bridgev2.LoginInputFieldTypeURL,
					ID:          "backend_url",
					Name:        "Backend URL",
					Description: "URL of the Python backend API (e.g., http://localhost:8808)",
					Pattern:     `^https?://.+`,
				},
				{
					Type:        bridgev2.LoginInputFieldTypeToken,
					ID:          "auth_token",
					Name:        "Auth Token",
					Description: "Bearer token for authenticating with the backend (HTTP_TOKEN env var)",
				},
				{
					Type:        bridgev2.LoginInputFieldTypeUsername,
					ID:          "user_id",
					Name:        "Garmin User ID",
					Description: "Your Garmin identifier (phone number or email)",
				},
			},
		},
	}, nil
}

// SubmitUserInput handles user input during login.
func (gl *GarminLogin) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	backendURL := input["backend_url"]
	authToken := input["auth_token"]
	userID := input["user_id"]

	gl.log.Info().Str("backend_url", backendURL).Str("user_id", userID).Msg("Received login input")

	// Update connector config
	gl.connector.config.BackendURL = backendURL
	gl.connector.config.AuthToken = authToken
	gl.connector.config.UserID = userID

	// Verify the backend connection
	if err := gl.connector.verifyBackend(ctx); err != nil {
		return &bridgev2.LoginStep{
			Type:         bridgev2.LoginStepTypeUserInput,
			StepID:       "backend_config",
			Instructions: "Backend verification failed: " + err.Error() + "\n\nPlease check the configuration and try again.",
			UserInputParams: &bridgev2.LoginUserInputParams{
				Fields: []bridgev2.LoginInputDataField{
					{
						Type:        bridgev2.LoginInputFieldTypeURL,
						ID:          "backend_url",
						Name:        "Backend URL",
						Description: "URL of the Python backend API",
						Pattern:     `^https?://.+`,
					},
					{
						Type:        bridgev2.LoginInputFieldTypeToken,
						ID:          "auth_token",
						Name:        "Auth Token",
						Description: "Bearer token for authenticating with the backend",
					},
					{
						Type:        bridgev2.LoginInputFieldTypeUsername,
						ID:          "user_id",
						Name:        "Garmin User ID",
						Description: "Your Garmin identifier",
					},
				},
			},
		}, nil
	}

	// Success - let the bridge framework create the UserLogin
	// We store the metadata in the connector config for now
	// It will be loaded via LoadUserLogin when the bridge starts up next time

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "complete",
		Instructions: "Successfully connected to Garmin Messenger bridge!",
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: networkid.UserLoginID(userID),
		},
	}, nil
}

// Cancel cancels the login process.
func (gl *GarminLogin) Cancel() {
	gl.log.Info().Msg("Login cancelled")
}
