# Engram API — hermes-agent plugin spec

Engram is the mobile companion app for hermes-agent (prototype: `Engram Mobile Interface.zip`,
built in Claude Design). This plugin exposes the HTTP + WebSocket API the app consumes.

- **Plugin name:** `hermes-engram`
- **Mount point:** `/api/plugins/hermes-engram` on the hermes dashboard server (`hermes dashboard`)
- **Version:** 0.1.0

## Why a dashboard plugin

hermes-agent has two plugin surfaces. The agent-side plugin system (`plugin.yaml` +
`__init__.py::register(ctx)`) registers tools/hooks but cannot mount HTTP routes. The
dashboard plugin system (`dashboard/manifest.json` + `dashboard/plugin_api.py` exposing a
FastAPI `APIRouter`) is the supported way to serve HTTP and WebSocket endpoints — the
bundled kanban plugin is the reference implementation. Engram uses both: the agent-side
shell makes the plugin visible to `hermes plugins`, the dashboard side serves the API.

## Authentication & transport

Depends on how the dashboard is bound:

**Loopback bind** (default `127.0.0.1`): `Authorization: Bearer <session-token>` with the
per-process token injected into the SPA as `window.__HERMES_SESSION_TOKEN__`; WS takes it
as `?token=`.

**Non-loopback bind** (Tailscale IP / LAN — how Engram actually connects): hermes requires
an auth provider and disables the loopback token. With `dashboard.basic_auth` configured:

1. **Mint an access token** (public endpoint, no auth):
   `POST /auth/password-login` with `{"provider": "basic", "username": "…", "password": "…"}`.
   The token is returned in the `Set-Cookie: hermes_session_at=<ACCESS_TOKEN>` response
   header (strip surrounding quotes if present; quoted and bare forms are both accepted).
   Lifetime = `dashboard.basic_auth.session_ttl_seconds`. On 401, mint a new one.
2. **HTTP:** `Authorization: Bearer <ACCESS_TOKEN>` — on Engram routes only, via the
   plugin's scoped shim (an outermost ASGI middleware that re-carries the Bearer token as
   the `hermes_session_at` cookie before the core gate verifies it; the shim never does its
   own verification). `Cookie: hermes_session_at=<ACCESS_TOKEN>` works equally on all
   dashboard routes. Plain Bearer on non-Engram core routes is still rejected by design.
   Recommended client design: store username/password in the device keyring, do the
   password-login exchange programmatically, keep the access token in memory, and re-login
   automatically on 401 — never make the user paste tokens.
3. **WebSocket:** `wss?://…/events?token=<ACCESS_TOKEN>` — the plugin's WS gate verifies
   the access token against the registered auth providers, in addition to everything the
   core gate accepts (single-use `?ticket=` from `POST /api/auth/ws-ticket`, etc.).

### Client auth flow (recommended)

The connection screen collects **endpoint + username + password**; the password lives in
the device keyring. The app never shows or stores tokens — it exchanges credentials for a
session automatically:

```
login(endpoint, username, password):
    POST {endpoint}/auth/password-login
         {"provider": "basic", "username": username, "password": password}
    200 → access_token = Set-Cookie[hermes_session_at]   (strip quotes)
          # a refresh token cookie (hermes_session_rt) is also issued; ignoring it
          # and re-logging-in with keyring credentials is simpler and equivalent
    401 → bad credentials (generic — never distinguishes user vs password)
    429 → rate limited, back off (login attempts are budgeted per IP)
    404 → wrong provider name

request(path):   Authorization: Bearer {access_token}     # Engram routes
ws(path):        ?token={access_token}
on 401 mid-session: login() again, retry once
```

The access token is a stateless HMAC-signed value verified by the dashboard's own auth
provider on every request — the plugin performs no credential handling of its own.

**Transport choice:** WebSocket primary — `/events` is a push tail, so polling only wastes
battery and adds latency. Fall back to polling (`GET /threads/{id}?after_message_id=`)
when the WS drops (e.g. tailnet blips), and reconnect the WS with `?since=<last cursor>`.

## Prototype → API mapping

| Prototype screen | Endpoints |
|---|---|
| Connection (health checks, token test) | `GET /health` |
| Feed (thread cards, filters, unread) | `GET /threads`, `WS /events` |
| Thread (chat, tool cards, send, queued sends) | `GET /threads/{id}`, `POST /threads/{id}/messages` |
| New thread sheet (message, model picker) | `POST /threads`, `GET /models` |
| Thread actions (rename, close/archive) | `PATCH /threads/{id}`, `DELETE /threads/{id}` |
| Close-thread feedback sheet | `POST /feedback` |
| Profiles row + profile screen | `GET /profiles`, `GET /profiles/{name}` |
| Profile config (SOUL.md, model) | `GET /profiles/{name}`, `PATCH /profiles/{name}` |
| Routines timeline + routine detail | `GET /routines`, `GET /routines/{id}` |
| Edit routine sheet (times, instructions) | `PATCH /routines/{id}`, `POST /routines/{id}/pause`, `.../resume` |
| Run now | `POST /routines/{id}/run` |
| Run history + run detail | `GET /routines/{id}/runs`, `GET /runs/{session_id}` |
| Run feedback sheet | `POST /feedback` |
| Model picker (grouped by provider) | `GET /models` |

Concepts map onto existing hermes primitives — the plugin adds no parallel state:

| Engram concept | hermes primitive |
|---|---|
| Profile | hermes profile = its own `HERMES_HOME` (`~/.hermes/profiles/<name>/`) with its own `state.db`, cron store, and `SOUL.md` |
| Thread | `SessionDB` session in the owning profile's `state.db` |
| Message / tool card | `messages` rows (role + `tool_calls`/`tool_call_id`) |
| Send message | in-process `tui_gateway` JSON-RPC (`session.create`/`session.resume` + `prompt.submit`, with its `profile` param for non-launch profiles) |
| Routine | cron job in the owning profile's `cron/jobs.json` |
| Routine run | session with id `cron_<job>_<ts>`, `source='cron'`, in the owning profile's `state.db` |
| Model list | `hermes_cli.inventory.build_models_payload` |

**Profile is a first-class dimension, not a display attribute.** Every thread and routine
belongs to exactly one profile. Listings default to `profile=all` (aggregated across
profiles, each item tagged with its `profile`); item endpoints (`/threads/{id}`,
`/runs/{id}`, message send, rename/archive/delete) take `?profile=` (default `default`)
because ids are only meaningful within a profile's store. Routine item endpoints accept
`?profile=` as an optional hint and otherwise locate the job by searching every profile.
Clients must carry each item's `profile` field back on follow-up calls.

---

## Endpoints

All paths below are relative to `/api/plugins/hermes-engram`.

### System

#### `GET /health`
Connection-screen health check.

```json
{
  "ok": true,
  "service": "hermes-engram",
  "version": "0.1.0",
  "server_time": 1752444000.12,
  "server_time_iso": "2026-07-14T12:00:00+08:00",
  "profile": {"active": "default", "count": 3},
  "threads": {"total": 128},
  "routines": {"total": 5, "enabled": 4},
  "gateway": {"chat_rpc": true},
  "scopes": ["chat", "threads", "routines", "profiles", "models", "feedback"]
}
```
`server_time` lets the client compute clock skew. `gateway.chat_rpc=false` means reads
work but message sending is unavailable (tui_gateway not importable).

#### `GET /models`
Model picker, grouped by provider. Normalized from hermes' inventory (only providers
that actually have models appear); pass a model's `id` as `model` when creating threads.

```json
{
  "current": {"provider": "azure-foundry", "model": "gpt-5.6-terra"},
  "groups": [
    {
      "provider": "anthropic", "label": "Anthropic",
      "authenticated": true, "current_provider": false,
      "models": [
        {"id": "claude-fable-5", "current": false},
        {"id": "claude-opus-4-8", "current": false}
      ]
    }
  ]
}
```

### Threads

Thread `status` is derived: `running` (a live gateway session is mid-turn),
`open` (default), `resolved` (archived). `blocked` (pending approval/clarify
question) is planned — it needs the gateway's interactive-block state, exposed in a
later iteration; the feed's "Needs you" filter degrades gracefully without it.

#### `GET /threads`
Query params: `limit` (default 30), `offset`, `status` = `open|resolved|all` (default `open`),
`profile` = a profile name or `all` (default `all` — aggregates every profile's store,
merged by `last_active`), `q` (title/content search — reserved).

```json
{
  "threads": [
    {
      "id": "20260714_090412_ab12cd34",
      "profile": "erudifi",
      "topic": "Vendor NDA",
      "preview": "Needs your call on the liability cap",
      "status": "open",
      "running": false,
      "source": "tui",
      "message_count": 12,
      "started_at": 1752444000.0,
      "last_active": 1752448000.0,
      "model": "claude-sonnet-4-5"
    }
  ],
  "total": 128, "limit": 30, "offset": 0, "profile": "all"
}
```

#### `POST /threads`
Create a thread and (optionally) submit the first message. Body:

```json
{"message": "Review the Meridian NDA", "title": "Vendor NDA", "model": "claude-sonnet-4-5", "profile": null}
```

`message` required. Returns `201`:

```json
{"thread_id": "20260714_120001_cd34ef56", "gateway_sid": "a1b2c3d4", "accepted": true}
```

The turn runs asynchronously; follow it over `WS /events` or by polling `GET /threads/{id}`.

#### `GET /threads/{id}`
Full thread detail. Query params: `profile` (default `default` — use the `profile` the
thread was listed with), `limit`, `offset`, `after_message_id` (poll fallback —
only messages with DB id greater than this). `PATCH`/`DELETE`/`POST …/messages` take the
same `profile` param.

Messages are normalized to the prototype's kinds:

```json
{
  "thread": {"id": "…", "topic": "Vendor NDA", "status": "open", "running": false, "...": "as in list"},
  "messages": [
    {"id": 991, "kind": "event", "text": "session started", "ts": 1752444000.0},
    {"id": 992, "kind": "me",    "text": "Review the Meridian NDA", "ts": 1752444010.0},
    {"id": 993, "kind": "tool",  "tool": {"name": "contract-review", "input": "{…json args…}",
                                           "output": "clauses_ok: 23/24…", "status": "ok"},
                 "ts": 1752444030.0},
    {"id": 995, "kind": "agent", "text": "Redlines are back. Everything's clean except clause 7.2…",
                 "ts": 1752444042.0, "reasoning": null}
  ]
}
```

Mapping: `user` → `me`; `assistant` text → `agent`; each `assistant.tool_calls[i]` → one
`tool` entry whose `output` is joined from the matching `role=tool` row (by `tool_call_id`,
status `ok` when a result exists, `running` otherwise); `system` → `event`.

#### `POST /threads/{id}/messages`
Send a message into an existing thread. Body: `{"text": "…"}`. Returns `202`:

```json
{"accepted": true, "gateway_sid": "a1b2c3d4", "queued": false}
```

`queued: true` when the agent was mid-turn — the gateway queues it as the next turn
(the prototype's "queued — delivers after current run"). Under the hood:
`session.resume {session_id}` then `prompt.submit {session_id: sid, text}` via the
in-process tui_gateway dispatcher. `503` when the chat RPC surface is unavailable.

#### `PATCH /threads/{id}`
Body: `{"title": "New topic"}` and/or `{"archived": true}` (archive = the prototype's
"close thread"; unarchive with `false`). Returns the updated thread summary.

#### `DELETE /threads/{id}`
Permanently deletes the session and its messages. `{"deleted": true}`.

### Routines

A routine is a hermes cron job in its owning profile's store. `instructions` is the job's
prompt; `schedule` accepts what `hermes cron` accepts (`"every 1h"`, `"daily at 9:00"`,
cron expressions, ISO timestamps for one-shots). `GET /routines` defaults to `profile=all`;
`POST /routines` takes `profile` in the body (default `default`); item endpoints accept
`?profile=` as a hint or find the job across profiles. Cross-profile mutations run the cron
library in a short-lived subprocess under that profile's `HERMES_HOME` (the library binds
its store at import time), so they carry ~150ms extra latency.

Routine objects carry `"profile"`, mirroring threads.

#### `GET /routines`
```json
{
  "routines": [
    {
      "id": "job_ab12", "name": "Applicant screening",
      "instructions": "Screen for senior backend roles first…",
      "schedule": {"kind": "cron", "expr": "0 9,12,15,18 * * *", "human": "0 9,12,15,18 * * *"},
      "enabled": true, "state": "scheduled",
      "next_run_at": "2026-07-14T15:00:00+08:00",
      "last_run_at": "2026-07-14T12:00:00+08:00",
      "last_status": "ok", "model": null, "deliver": null
    }
  ]
}
```

#### `POST /routines`
`{"name": "…", "instructions": "…", "schedule": "every 1h", "model": null}` → `201` with the job.

#### `GET /routines/{id}` — job + `runs` (latest 10, same shape as below).
#### `PATCH /routines/{id}` — any of `name`, `instructions`, `schedule`, `model`, `deliver`.
#### `POST /routines/{id}/run` — run on the next scheduler tick (prototype "Run now"). `202`.
#### `POST /routines/{id}/pause` / `POST /routines/{id}/resume`
#### `DELETE /routines/{id}`

#### `GET /routines/{id}/runs`
Run history (newest first, `limit`/`offset`):

```json
{"runs": [{"session_id": "cron_job_ab12_1752444000", "started_at": 1752444000.0,
            "ended_at": 1752444041.0, "status": "ok", "preview": "12 screened, 3 shortlisted",
            "message_count": 9}]}
```

#### `GET /runs/{session_id}`
Run transcript — same message normalization as `GET /threads/{id}` (thinking/tool/agent
cards in the prototype's run detail screen). Works for any session id, but exists so run
detail doesn't have to pretend a cron run is a chat thread.

### Profiles

#### `GET /profiles`
```json
{
  "active": "default",
  "profiles": [
    {"name": "default", "is_default": true, "active": true, "model": "claude-sonnet-4-5",
     "provider": "anthropic", "skill_count": 14, "description": "…", "gateway_running": false}
  ]
}
```

#### `GET /profiles/{name}`
Profile detail + `soul` (SOUL.md content, `null` if absent) + `skills` (names from the
profile's `skills/` dir).

#### `PATCH /profiles/{name}`
Body: `{"soul": "full new SOUL.md content"}`. Writes the profile's SOUL.md. Model/guardrail
edits are deliberately not exposed yet (the prototype routes those through chat with the
profile; config.yaml writes need more validation than v0 should carry).

### Inbox

Routine reports as first-class items — the app's "what happened while I was away" surface.
Items are **derived** from completed cron-run sessions (every run persists as a
`source='cron'` session in its profile's store, regardless of the job's `deliver` target),
with Engram-owned read/unread state layered on top (`~/.hermes/engram/inbox.json`).
`kind` is extensible: `routine_run` today; agent asks / explicit deliveries later.

Note: routines only execute when the hermes gateway (cron scheduler) is running —
`hermes gateway install` — or via a manual `hermes cron tick`.

#### `GET /inbox`
Query params: `limit`, `offset`, `unread=true` (only unread), `profile` (default `all`).

```json
{
  "items": [
    {
      "id": "default:cron_6b9e6045db9f_20260714_032829",
      "kind": "routine_run",
      "profile": "default",
      "routine_id": "6b9e6045db9f",
      "routine_name": "Applicant screening",
      "run_session_id": "cron_6b9e6045db9f_20260714_032829",
      "status": "ok",
      "summary": "12 screened, 3 shortlisted",
      "created_at": 1783970912.88,
      "read": false
    }
  ],
  "total": 14, "unread": 3, "limit": 30, "offset": 0
}
```

`unread` in the envelope is the total unread count (badge), independent of filters.
Open an item's full report via `GET /runs/{run_session_id}?profile={profile}`.

#### `PATCH /inbox/{item_id}`
Body `{"read": true|false}`. Item id is the `{profile}:{run_session_id}` composite.

#### `POST /inbox/read-all`
Marks everything read (optionally scoped with `?profile=`). Returns `{ok, marked}`.

### Feedback

#### `POST /feedback`
The prototype's close-thread and run-feedback sheets. Stored append-only in
`~/.hermes/engram/feedback.jsonl`; a later iteration folds it into SOUL.md / routine
instructions (the "v6 → v7" flow).

```json
{
  "target": {"kind": "thread", "thread_id": "20260714_090412_ab12cd34"},
  "verdict": "good",
  "chips": ["Fast turnaround"],
  "note": "Fewer check-ins next time"
}
```
`target.kind` = `thread` | `routine` | `routine_run` (with `routine_id` / `run_session_id`).
Returns `{"ok": true}`.

### Live updates

#### `WS /events?since=<cursor>&profile=<name|all>`
Kanban-style DB tail: polls every profile's `state.db` (or one, with `profile=`) and
pushes new message rows tagged with their profile. Auth via `?token=` (access token or
loopback token) / `?ticket=`. The cursor is an **opaque JSON object** (`{profile: row_id}`)
— echo the last frame's `cursor` back as `since` (URL-encoded) on reconnect; omit for
"from now".

Server → client frames:

```json
{"type": "hello", "cursor": {"default": 1807, "erudifi": 177}}
{"type": "pulse", "running": false, "count": 0, "ts": 1783971000.1}
{"type": "messages", "cursor": {"default": 1809, "erudifi": 177},
 "items": [{"id": 1809, "profile": "default", "session_id": "20260714_…",
             "kind": "agent", "text": "…", "tool_name": null, "ts": 1752448100.0}]}
{"type": "pulse", "running": true, "count": 1, "ts": 1783971004.2}
```

**Pulse — activity light + keepalive in one frame.** Sent immediately after `hello`, on
every busy/idle flip (and concurrent-turn `count` change), and at least every **25s**
regardless. Two client behaviors bind to it:

1. Header indicator: `running` drives the prototype's pulse dot, `count` the "N running"
   label. Global by design — per-thread state is the `status`/`running` fields on
   `GET /threads`, and new messages announce themselves on this socket anyway.
2. Staleness watchdog: if no frame of any kind arrives for ~40s (1.5× the pulse
   interval), consider the connection dead and reconnect with the last `cursor`.

Every persisted message (from any surface: this API, the TUI, the desktop app, cron runs)
appears here — the feed screen updates no matter where a turn ran. Token-level streaming
deltas are *not* on this socket; for live typing indicators the app can additionally speak
the dashboard's `/api/ws` JSON-RPC protocol (same server, same token), which this plugin
deliberately does not duplicate.

## Errors

Standard FastAPI shape: `{"detail": "…"}` with 400 (validation), 404 (unknown id),
409 (conflict, e.g. duplicate title), 413/timeouts as applicable, 503 (chat RPC surface
unavailable — reads still work).

## Out of scope for v0 (planned)

- `blocked` thread status + decision options (needs gateway interactive-block state).
- Attachments upload on threads (kanban has the pattern; port it).
- Notification preferences & push — client-side or a later `/notifications` resource.
- Feedback auto-folding into SOUL.md / routine instructions.
