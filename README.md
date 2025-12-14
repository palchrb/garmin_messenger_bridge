# Garmin Messenger Bridge

Bridge that watches Garmin Messenger data from a Redroid container and exposes a simple HTTP API for Maubot to send/receive messages.

## Docker build/run

### Option A: Docker Compose (Redroid + bridge)

The repository ships with a `docker-compose.yml` that starts both Redroid (service name `redroid` by default, container name overridable via `REDROID_SERVICE`) and the bridge on the same network. It binds the Garmin data directory and a writable state directory from the host, and exposes the provision API on port 8788.

1. Copy `garmin.env.example` to `garmin.env` and adjust values (especially `PROVISION_SECRET`).
2. Create the host bind-mounts (they are relative to the directory where you run Compose):
   ```bash
   mkdir -p data state
   ```
3. (Optional) If you rename the Redroid service/image, set `REDROID_SERVICE` (and
   `ADB_TARGET` if you want an explicit host:port) so the bridge can reach the
   right ADB endpoint. The default resolves to `<REDROID_SERVICE>:5555` where
   `REDROID_SERVICE` defaults to `redroid13`.

4. Start everything:
   ```bash
   docker compose up -d --build
   ```

### Option A.1: Using an already running Redroid

If Redroid is already running elsewhere, you can still use the bridge container from this repo:

1. Ensure the Garmin app data directory from Redroid is bind-mounted read-only to `/android-data` in the bridge container.
2. Mount a writable state directory (for example `./state`) to `/var/lib/garmin-bridge`.
3. Point `ADB_TARGET` (in `garmin.env` or `docker run -e`) to the reachable Redroid ADB endpoint, e.g. `ADB_TARGET=redroid13:5555` or `ADB_TARGET=<host>:5555`. Compose now exposes `REDROID_SERVICE` and an `environment` override for convenience; keep only one source of truth (remove the Compose override if you set it in `garmin.env`).
4. Start only the bridge service:
   ```bash
   docker compose up -d --build garmin-bridge
   # or build+run manually as in Option B
   ```

### Option B: Manual docker build/run (bridge only)

1. Adjust your environment variables (for example via `garmin.env`) so paths like `DB_PATH`, `ROOT_DIR`, and `STATE_DIR` match your mounts. By default the state database will live at `/var/lib/garmin-bridge/statestore.db` inside the container, which maps to the repo-local `./state` directory when you use the provided bind mount.
2. Build the bridge image:
   ```bash
   docker build -t garmin-bridge .
   ```
3. Run the container (example with host bind-mounts for Garmin data and bridge state):
   ```bash
   docker run \
     -p 8788:8788 \
     -v ./data:/android-data:ro \
     -v ./state:/var/lib/garmin-bridge \
     -e DB_PATH=/android-data/data/com.garmin.android.apps.messenger/databases/message.db \
     -e ROOT_DIR=/android-data/data/com.garmin.android.apps.messenger/no_backup \
     -e STATE_DIR=/var/lib/garmin-bridge \
     -e ADB_TARGET=redroid13:5555 \
     garmin-bridge

   Configure `ADB_TARGET` once in `garmin.env` (or your shell) instead of also overriding it in Compose; the default `redroid13:5555` works when the bridge runs alongside Redroid in the same Compose project.
   ```

The included `Dockerfile` installs ADB tools, copies the application code, and starts `python -m app.main`.

## State database

`StateStore` creates the SQLite schema automatically on startup if the file does not exist (in `STATE_DIR`/`statestore.db` by default), so no manual initialization is required. Ensure the state directory is mounted as a writable volume so the file persists across container restarts.
