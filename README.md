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
