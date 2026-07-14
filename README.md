# hermes-engram

Backend plugin for **Engram**, the mobile companion app for
[hermes-agent](../hermes-agent) (UI prototype: `Engram Mobile Interface.zip`, Claude Design).

Exposes threads (chat), routines (cron), profiles, models, feedback, and a live
message stream over HTTP + WebSocket, mounted by the hermes dashboard at:

```
/api/plugins/hermes-engram
```

- **API spec:** [API.md](API.md) (narrative — auth flow, profile contract, WS protocol) ·
  `GET /spec/openapi.json` (machine-readable, generated from the live routes) ·
  `GET /spec/docs` (Swagger UI, logged-in browser)
- **Implementation:** [dashboard/plugin_api.py](dashboard/plugin_api.py)

## Install on another instance (from git)

hermes has a built-in git plugin installer; this repo is the package. Push it to a git
host as `hermes-engram` (the install dir is derived from the repo name and must match the
plugin name), then on the target instance:

```sh
hermes plugins install <owner>/hermes-engram --enable   # or a full git/ssh URL
hermes dashboard   # (re)start — the API mounts at /api/plugins/hermes-engram
```

Updates later: `hermes plugins update hermes-engram` + dashboard restart.
Remove: `hermes plugins remove hermes-engram`.

Per-instance setup the installer does NOT cover:

1. **Remote access (the mobile app):** a non-loopback bind requires an auth provider.
   Configure `dashboard.basic_auth` in that instance's `~/.hermes/config.yaml`:

   ```yaml
   dashboard:
     basic_auth:
       username: <user>
       password_hash: "<scrypt hash>"   # python -c "from plugins.dashboard_auth.basic import hash_password; print(hash_password('...'))"
       secret: "<32+ random bytes>"     # stable token signing — sessions survive restarts
       session_ttl_seconds: 2592000
   ```

   Then run `hermes dashboard --host <tailscale-ip> --port 9119 --no-open`.
2. **Routine execution:** the cron scheduler lives in the gateway — `hermes gateway install`
   (otherwise routines only fire via manual `hermes cron tick`).
3. The plugin tolerates hermes version skew (signature fallbacks for older installs), but
   it needs the dashboard plugin system — any reasonably current hermes-agent.

## Install (symlink, for development)

```sh
mkdir -p ~/.hermes/plugins
ln -s "$(pwd)" ~/.hermes/plugins/hermes-engram
```

Enable it (user-source plugins are opt-in — both the agent loader and the
dashboard's route mounter check `plugins.enabled` in `~/.hermes/config.yaml`):

```yaml
plugins:
  enabled:
    - hermes-engram
```

Then start the dashboard; the API mounts automatically:

```sh
hermes dashboard
```

## Smoke test

```sh
TOKEN=...   # session token printed at dashboard startup
BASE=http://127.0.0.1:8000/api/plugins/hermes-engram

curl -s -H "Authorization: Bearer $TOKEN" $BASE/health | jq
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/threads?limit=5" | jq
curl -s -H "Authorization: Bearer $TOKEN" $BASE/routines | jq
```

Live tail (WS): `ws://127.0.0.1:8000/api/plugins/hermes-engram/events?token=$TOKEN`

## Layout

```
plugin.yaml              agent-side manifest (kind: standalone)
__init__.py              agent-side register(ctx) shell
dashboard/manifest.json  dashboard plugin manifest (hidden tab, api -> plugin_api.py)
dashboard/plugin_api.py  the API (FastAPI APIRouter)
dashboard/dist/index.js  no-op frontend entry (API-only plugin)
API.md                   narrative spec (OpenAPI is generated: GET /spec/openapi.json)
```
