# Rotatarr — Prowlarr Indexer Repair Tool

Rotatarr is a Dockerized Python script to check Prowlarr indexers on a configurable schedule, attempt to fix failing indexers by testing alternative base URLs, and test applying a FlareSolverr tag if desired.

Features
- Periodic checks for failing indexers
- Tests alternate base URLs found in indexer definitions
- Optionally adds a configured Prowlarr tag (like `FlareSolverr`) to the indexer and re-tests
- Saves working combinations and rolls back when all attempts fail

Configuration
- PROWLARR_URL — base URL for Prowlarr (e.g., `http://localhost:9696`)
- PROWLARR_API_KEY — your Prowlarr API key
- CHECK_INTERVAL_MIN — loop interval in minutes (default 30)
- TAG_TO_TRY — optional tag name to try (e.g., `FlareSolverr`) — set empty to skip
 - APPLY_TAG_SAVE_BEFORE_TEST — when `true`, the tool will persist the tag/baseUrl change to Prowlarr before running the API test to better replicate the UI behavior (defaults to `false`).
 - TEST_AS_UI — when `true`, the script will attempt a minimal 'UI-like' payload if the full indexer payload fails the test; this can replicate UI-specific behavior and is useful for indexers blocked by Cloudflare (defaults to `false`).
 - FORCE_TEST_INDEXERS — a comma-separated list of indexer names or ids that should be forced to test even if they're in cooldown; useful to retest specific indexers (defaults to empty).
- TAG_FORCE — boolean `true`|`false` default `false` to add tag when testing
- DRY_RUN — if `true` only prints changes, does not persist updates
 - TEST_RETRIES — number of retries when tests return transient errors (429/timeout), default 2
 - TEST_RETRY_DELAY_SEC — base delay seconds between test retries, default 2
 - INDEXER_MAX_ATTEMPTS — number of consecutive failures before cooling down the indexer, default 3
 - INDEXER_COOLDOWN_MIN — cooldown duration in minutes, default 60
 - INDEXER_STATE_FILE — path for persisted indexer state (JSON); default `/app/data/indexer_state.json`

Usage

### Quick Start with Pre-built Image

1. Download the `docker-compose.yml` file from this repository
2. Edit the environment variables in `docker-compose.yml`:
   - Set your `PROWLARR_URL` (e.g., `http://your-prowlarr-host:9696`)
   - Set your `PROWLARR_API_KEY`
   - Configure other options as needed
3. Run the container:

```bash
docker compose up -d
```

The image is automatically pulled from GitHub Container Registry at `ghcr.io/northernpowerhouse/rotatarr:latest`.

### Test Run

To test the configuration before running continuously:

```bash
docker compose run --rm -e ONE_SHOT=true -e DRY_RUN=true rotatarr
```

### Building from Source

If you want to build from source:

```bash
git clone https://github.com/northernpowerhouse/rotatarr.git
cd rotatarr
docker build -t rotatarr:latest .
# Edit docker-compose.yml to use "build: ." instead of the image
docker compose up -d
```

### Local Python Development

For development, you can run directly with Python:

```bash
python rotatarr.py
```

Note: Set `ONE_SHOT=true` to run once and exit instead of the default loop mode.

Note: This tool relies on the Prowlarr API. The Prowlarr API docs are at https://prowlarr.com/docs/api/ .

Persisting state:
- By default cooldowns and failure counts are persisted to `/app/data/indexer_state.json`. Make sure you add a volume mapping for `./data:/app/data` in `docker-compose.yml` so state is preserved between container runs.
