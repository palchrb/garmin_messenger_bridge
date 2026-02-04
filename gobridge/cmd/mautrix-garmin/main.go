// mautrix-garmin is a Matrix-Garmin Messenger bridge using bridgev2.
//
// This bridge connects Matrix to Garmin Messenger via a Python backend
// that handles ADB UI automation to interact with the Garmin Messenger
// Android app running in Redroid.
package main

import (
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/your-org/mautrix-garmin/pkg/connector"
)

// Build info - set during build
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func main() {
	m := mxmain.BridgeMain{
		Name:        "mautrix-garmin",
		Description: "A Matrix-Garmin Messenger bridge",
		URL:         "https://github.com/your-org/mautrix-garmin",
		Version:     Tag,
		Commit:      Commit,
		BuildTime:   BuildTime,

		Connector: connector.NewGarminConnector(),
	}

	m.InitVersion()
	bridgeconfig.RegisterNetworkFlags(&m.ConfigPath)
	m.Run()
}
