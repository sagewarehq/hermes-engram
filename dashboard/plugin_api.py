"""Engram dashboard plugin — backend API routes.

Mounted at /api/plugins/hermes-engram/ by the hermes dashboard plugin system.
Spec: API.md at the plugin root (OpenAPI is generated from the live routes at
GET /spec/openapi.json). The mobile app's chat threads,
routines, profiles, and models all map onto existing hermes primitives:

  threads   -> hermes_state.SessionDB sessions + messages (~/.hermes/state.db)
  sending   -> in-process tui_gateway JSON-RPC (session.create/resume + prompt.submit)
  routines  -> cron/jobs.py cron jobs; run history via SessionDB.list_cron_job_runs
  profiles  -> hermes_cli.profiles (~/.hermes/profiles/<name>/, SOUL.md)
  models    -> hermes_cli.inventory.build_models_payload

Auth: HTTP routes sit behind the dashboard's session-token middleware like every
core /api route. The /events WebSocket delegates to web_server._ws_auth_ok so it
accepts the same credentials as core WS endpoints in every deployment mode
(loopback ?token=, OAuth single-use ?ticket=, server-internal ?internal=).

Turns started through this API stream their deltas to a discarding transport —
clients follow progress via the /events DB tail (message granularity) or by
speaking the dashboard's own /api/ws JSON-RPC protocol for token-level streaming.
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi import status as http_status
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

VERSION = "0.1.0"
SOURCE_LABEL = "engram"
_EVENT_POLL_SECONDS = 1.5
_PULSE_INTERVAL = 25.0  # max seconds between pulse frames (keepalive bound)
_RPC_TIMEOUT_SECONDS = 60.0
_MAX_SOUL_BYTES = 256 * 1024

router = APIRouter()


# ---------------------------------------------------------------------------
# Infrastructure helpers
# ---------------------------------------------------------------------------

def _hermes_home() -> Path:
    from hermes_constants import get_hermes_home

    return Path(get_hermes_home())


# ---------------------------------------------------------------------------
# Profile scoping.
#
# A hermes profile IS a HERMES_HOME directory with its own state.db (threads)
# and cron store (routines) — profile is therefore a first-class dimension of
# this API, not a display attribute. Thread/routine endpoints take a
# ``profile`` param; listings can aggregate across every profile.
# ---------------------------------------------------------------------------

def _profile_names() -> list:
    from hermes_cli import profiles as profiles_mod

    try:
        return [p.name for p in profiles_mod.list_profiles()]
    except Exception as exc:
        log.warning("engram: list_profiles failed: %s", exc)
        return ["default"]


def _profile_home_or_404(name: str) -> Path:
    from hermes_cli import profiles as profiles_mod

    try:
        if name != "default" and not profiles_mod.profile_exists(name):
            raise HTTPException(status_code=404, detail=f"profile {name} not found")
        home = Path(profiles_mod.get_profile_dir(name))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"bad profile {name!r}: {exc}")
    if not home.is_dir():
        raise HTTPException(status_code=404, detail=f"profile {name} not found")
    return home


def _is_launch_profile(name: str) -> bool:
    """True when ``name`` resolves to this dashboard process's own HERMES_HOME."""
    try:
        return _profile_home_or_404(name).resolve() == _hermes_home().resolve()
    except HTTPException:
        return False


def _state_db_path(profile: str = "default") -> Path:
    return _profile_home_or_404(profile) / "state.db"


_db_lock = threading.Lock()
_db_instances: dict = {}


def _db(profile: str = "default"):
    """Per-profile SessionDB handle (SessionDB locks internally)."""
    path = _state_db_path(profile)
    key = str(path.resolve())
    with _db_lock:
        inst = _db_instances.get(key)
        if inst is None:
            from hermes_state import SessionDB

            inst = SessionDB(db_path=path)
            _db_instances[key] = inst
        return inst


def _ws_upgrade_authorized(ws: "WebSocket") -> bool:
    """Authorize a WS upgrade.

    Two accepted credential shapes:
    1. Whatever the dashboard's canonical gate accepts (loopback ?token=,
       OAuth single-use ?ticket=, server-internal ?internal=).
    2. ``?token=<session access token>`` — the value of the
       ``hermes_session_at`` cookie minted by /auth/password-login, verified
       against every registered dashboard auth provider. This lets the mobile
       app reuse its one access token for both HTTP (Cookie header) and WS
       (query param) instead of minting a single-use ticket per connect.

    Imported lazily so the module stays importable in bare test harnesses,
    where we accept (matching the kanban plugin's established behaviour).
    """
    try:
        from hermes_cli import web_server as _ws
    except Exception:
        return True
    if _ws._ws_auth_ok(ws):
        return True
    token = ws.query_params.get("token", "")
    if token:
        try:
            from hermes_cli.dashboard_auth.registry import list_providers

            for provider in list_providers():
                try:
                    if provider.verify_session(access_token=token):
                        return True
                except Exception:
                    continue
        except Exception as exc:
            log.warning("engram ws auth: provider verify failed: %s", exc)
    return False


# ---------------------------------------------------------------------------
# Bearer -> cookie shim, Engram routes only.
#
# In gated (non-loopback) mode the dashboard authenticates via the
# ``hermes_session_at`` session cookie; ``Authorization: Bearer`` is not
# accepted and hermes' token-auth seam only supports exact-path routes, which
# can't cover parameterized paths like /threads/{id}. Mobile clients expect a
# plain bearer token, so for requests under our own prefix that carry a Bearer
# header and no session cookie, rewrite the header into cookie form BEFORE the
# core auth gate sees it. The real gate still performs the actual verification
# — this shim never authenticates anything itself, it only re-shapes where the
# same credential is carried. Installed at import time (plugin mount happens
# during web_server module init, before the middleware stack is built).
# ---------------------------------------------------------------------------

_MOUNT_PREFIX = "/api/plugins/hermes-engram"


def _trace_request(scope) -> None:
    """Append a one-line trace for client debugging (~/.hermes/engram/requests.log).

    Only fires for requests that present an Authorization header, a ?token=
    query, or target the Engram prefix — i.e. the mobile app's traffic, not
    the dashboard SPA's. Records credential *shape*, never values.
    """
    try:
        path = str(scope.get("path", ""))
        headers = dict(scope.get("headers") or [])
        auth = headers.get(b"authorization", b"")
        cookie = headers.get(b"cookie", b"")
        query = scope.get("query_string") or b""
        has_token_qp = b"token=" in query
        if not (auth or has_token_qp or path.startswith(_MOUNT_PREFIX)):
            return
        line = json.dumps({
            "ts": time.time(),
            "type": scope.get("type"),
            "method": scope.get("method"),
            "path": path,
            "auth_header": (
                "bearer" if auth[:7].lower() == b"bearer " else ("other" if auth else None)
            ),
            "session_cookie": b"hermes_session_at=" in cookie,
            "token_query_param": has_token_qp,
            "client": (scope.get("client") or [None])[0],
        })
        dest = _hermes_home() / "engram"
        dest.mkdir(parents=True, exist_ok=True)
        with open(dest / "requests.log", "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


class _EngramBearerShim:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") in ("http", "websocket"):
            _trace_request(scope)
        if (
            scope.get("type") in ("http", "websocket")
            and str(scope.get("path", "")).startswith(_MOUNT_PREFIX)
        ):
            headers = list(scope.get("headers") or [])
            has_session_cookie = any(
                k == b"cookie" and b"hermes_session_at=" in v for k, v in headers
            )
            auth = next((v for k, v in headers if k == b"authorization"), b"")
            if not has_session_cookie and auth[:7].lower() == b"bearer ":
                token = auth[7:].strip().strip(b'"')
                if token:
                    rewritten = []
                    appended = False
                    for k, v in headers:
                        if k == b"cookie":
                            rewritten.append((k, v + b"; hermes_session_at=" + token))
                            appended = True
                        else:
                            rewritten.append((k, v))
                    if not appended:
                        rewritten.append((b"cookie", b"hermes_session_at=" + token))
                    scope = dict(scope)
                    scope["headers"] = rewritten
        await self.app(scope, receive, send)


def _install_bearer_shim() -> None:
    try:
        from hermes_cli.web_server import app as _dashboard_app

        _dashboard_app.add_middleware(_EngramBearerShim)
        log.info("engram: bearer->cookie auth shim installed for %s", _MOUNT_PREFIX)
    except Exception as exc:
        # Non-fatal: clients can still authenticate with the Cookie header.
        log.warning("engram: could not install bearer shim (%s)", exc)


_install_bearer_shim()


# ---------------------------------------------------------------------------
# tui_gateway RPC bridge — drives chat turns in-process
# ---------------------------------------------------------------------------

class _CollectingTransport:
    """Minimal tui_gateway Transport: captures response frames, drops events."""

    def __init__(self) -> None:
        self.frames: "queue.Queue[dict]" = queue.Queue()

    def write(self, obj: dict) -> bool:
        try:
            self.frames.put(obj)
        except Exception:
            pass
        return True

    def close(self) -> None:
        pass


def _chat_rpc_available() -> bool:
    try:
        import tui_gateway.server  # noqa: F401

        return True
    except Exception:
        return False


def _rpc(method: str, params: dict, timeout: float = _RPC_TIMEOUT_SECONDS) -> dict:
    """Call a tui_gateway JSON-RPC method in-process and return its result.

    dispatch() answers short handlers inline; long handlers run on the gateway
    pool and write the response frame to the bound transport, so we wait on our
    collecting transport for the frame carrying our request id.
    """
    try:
        from tui_gateway import server as tg
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"chat RPC surface unavailable (tui_gateway import failed: {exc})",
        )

    transport = _CollectingTransport()
    rid = f"engram-{uuid.uuid4().hex[:10]}"
    req = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
    try:
        resp = tg.dispatch(req, transport=transport)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"{method} dispatch failed: {exc}")

    if resp is None:
        deadline = time.time() + timeout
        resp = None
        while time.time() < deadline:
            try:
                frame = transport.frames.get(timeout=max(0.05, deadline - time.time()))
            except queue.Empty:
                break
            if isinstance(frame, dict) and frame.get("id") == rid:
                resp = frame
                break
        if resp is None:
            raise HTTPException(status_code=504, detail=f"{method} timed out")

    err = resp.get("error")
    if err:
        code = err.get("code")
        msg = err.get("message", "gateway error")
        if code in (4007,):  # session not found
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=502, detail=f"{method}: {msg}")
    result = resp.get("result")
    return result if isinstance(result, dict) else {}


_running_cache: dict = {"ts": 0.0, "map": {}}
_running_cache_lock = threading.Lock()


def _running_map() -> dict:
    """{persistent_session_id: live_status} for sessions live in the gateway.

    Cached for ~1s: the /events typing loop polls this per connected client
    every 1.5s, and one RPC per second total is plenty for an indicator.
    """
    with _running_cache_lock:
        if time.time() - _running_cache["ts"] < 1.0:
            return _running_cache["map"]
    if not _chat_rpc_available():
        return {}
    try:
        result = _rpc("session.active_list", {}, timeout=5.0)
    except Exception:
        return {}
    out: dict = {}
    for row in result.get("sessions") or []:
        key = row.get("session_key")
        if key:
            out[str(key)] = str(row.get("status") or "")
    with _running_cache_lock:
        _running_cache.update(ts=time.time(), map=out)
    return out


# tui_gateway._session_live_status vocabulary: "working" (mid-turn),
# "starting" (agent building), "waiting" (blocked on an interactive
# question/approval), "idle".
_BUSY_STATUSES = frozenset({"working", "starting"})


def _typing_set() -> frozenset:
    """Persistent session ids whose live gateway session is mid-turn."""
    return frozenset(
        sid for sid, status in _running_map().items() if status in _BUSY_STATUSES
    )


# ---------------------------------------------------------------------------
# Serialization — SessionDB rows -> Engram shapes
# ---------------------------------------------------------------------------

def _thread_dict(row: dict, running_map: Optional[dict] = None,
                 profile: str = "default") -> dict:
    sid = str(row.get("id") or "")
    archived = bool(row.get("archived") or 0)
    live_status = (running_map or {}).get(sid, "")
    running = live_status in _BUSY_STATUSES
    if archived:
        status = "resolved"
    elif live_status == "waiting":
        # A live session parked on an interactive question/approval — the
        # prototype's "blocked / needs you" state.
        status = "blocked"
    elif running:
        status = "running"
    else:
        status = "open"
    # A cron session's stored preview is the scheduler's injected
    # "[IMPORTANT: You are running as a scheduled cron job...]" preamble —
    # never what the user should see. _list_threads_for overlays the run's
    # last assistant message instead; this strip is the fallback for any
    # path that skips the overlay.
    preview = row.get("preview") or None
    if preview and preview.lstrip().startswith("[IMPORTANT:"):
        preview = None
    return {
        "id": sid,
        "profile": profile,
        "status": status,
        "topic": row.get("title") or None,
        "preview": preview,
        "running": running,
        "source": row.get("source") or "",
        "message_count": row.get("message_count") or 0,
        "started_at": row.get("started_at"),
        "last_active": row.get("last_active")
        or row.get("effective_last_active")
        or row.get("updated_at")
        or row.get("ended_at"),
        "model": row.get("model") or None,
    }


def _parse_tool_calls(raw: Any) -> list:
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _content_text(content: Any) -> str:
    """Flatten message content (string or structured parts) to display text."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict):
                t = p.get("text") or p.get("content")
                if isinstance(t, str):
                    parts.append(t)
            elif isinstance(p, str):
                parts.append(p)
        return "\n".join(parts)
    return str(content)


def _normalize_messages(rows: list) -> list:
    """Map raw message rows to the prototype's kinds: me/agent/tool/event.

    Each assistant tool_call becomes one `tool` entry; the matching `role=tool`
    result row (by tool_call_id) fills its output and flips status to ok.
    """
    out: list = []
    tool_by_call_id: dict = {}
    for row in rows:
        role = str(row.get("role") or "")
        mid = row.get("id")
        ts = row.get("timestamp")
        text = _content_text(row.get("content"))

        if role == "user":
            out.append({"id": mid, "kind": "me", "text": text, "ts": ts})
        elif role == "assistant":
            for tc in _parse_tool_calls(row.get("tool_calls")):
                fn = tc.get("function") or {}
                entry = {
                    "id": mid,
                    "kind": "tool",
                    "ts": ts,
                    "text": None,
                    "tool": {
                        "name": fn.get("name") or tc.get("name") or "tool",
                        "input": fn.get("arguments") or None,
                        "output": None,
                        "status": "running",
                    },
                }
                call_id = tc.get("id")
                if call_id:
                    tool_by_call_id[call_id] = entry
                out.append(entry)
            if text.strip():
                msg = {"id": mid, "kind": "agent", "text": text, "ts": ts}
                reasoning = row.get("reasoning") or row.get("reasoning_content")
                if reasoning:
                    msg["reasoning"] = _content_text(reasoning)
                out.append(msg)
        elif role == "tool":
            call_id = row.get("tool_call_id")
            entry = tool_by_call_id.get(call_id) if call_id else None
            if entry is not None:
                entry["tool"]["output"] = text
                entry["tool"]["status"] = "ok"
            else:
                out.append({
                    "id": mid,
                    "kind": "tool",
                    "ts": ts,
                    "text": None,
                    "tool": {
                        "name": row.get("tool_name") or "tool",
                        "input": None,
                        "output": text,
                        "status": "ok",
                    },
                })
        elif role == "system":
            out.append({"id": mid, "kind": "event", "text": text, "ts": ts})
        # anything else (developer notes, etc.) is skipped
    return out


def _session_transcript(session_id: str, limit: Optional[int], offset: int,
                        after_message_id: Optional[int],
                        profile: str = "default") -> dict:
    db = _db(profile)
    sess = db.get_session(session_id)
    if not sess:
        raise HTTPException(status_code=404, detail=f"session {session_id} not found")
    try:
        rows = db.get_messages(session_id, limit=limit, offset=offset)
    except TypeError:
        # Older hermes installs: get_messages(session_id) only — slice here.
        rows = db.get_messages(session_id)
        if offset:
            rows = rows[offset:]
        if limit is not None:
            rows = rows[:limit]
    if after_message_id is not None:
        rows = [r for r in rows if (r.get("id") or 0) > after_message_id]
    running_map = _running_map()
    return {
        "thread": _thread_dict(sess, running_map, profile),
        "messages": _normalize_messages(rows),
    }


# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------

_PLUGIN_ROOT = Path(__file__).resolve().parent.parent


@router.get("/spec/openapi.json")
def spec_openapi_json():
    """OpenAPI spec generated from the live route objects — can't drift from code.

    Scoped to the Engram routes only (the dashboard's app-wide /openapi.json
    mixes in hundreds of core hermes routes). The /events WebSocket and the
    auth/profile contracts aren't expressible in OpenAPI — see /spec/api.md.
    """
    from fastapi.openapi.utils import get_openapi
    from fastapi.routing import APIRoute
    from hermes_cli.web_server import app as dashboard_app

    routes = [
        r for r in dashboard_app.routes
        if isinstance(r, APIRoute) and r.path.startswith(_MOUNT_PREFIX)
    ]
    spec = get_openapi(
        title="Engram API (hermes-engram plugin)",
        version=VERSION,
        description=(
            "Generated from the mounted routes. Auth: Bearer access token from "
            "POST /auth/password-login (see /spec/api.md for the full auth flow, "
            "the (profile, id) composite-key contract, and the /events WebSocket "
            "protocol, none of which OpenAPI can express)."
        ),
        routes=routes,
    )
    spec.setdefault("components", {}).setdefault("securitySchemes", {})["bearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "description": "hermes_session_at access token from POST /auth/password-login",
    }
    spec["security"] = [{"bearerAuth": []}]
    return spec


@router.get("/spec/docs")
def spec_docs():
    """Interactive Swagger UI over the generated spec (needs a logged-in
    browser session, since the page itself sits behind the dashboard gate)."""
    from fastapi.openapi.docs import get_swagger_ui_html

    return get_swagger_ui_html(
        openapi_url=f"{_MOUNT_PREFIX}/spec/openapi.json",
        title="Engram API docs",
    )


@router.get("/spec/api.md")
def spec_api_md():
    """The narrative spec (auth flow, profile semantics, WS protocol)."""
    from fastapi.responses import FileResponse

    path = _PLUGIN_ROOT / "API.md"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="API.md not found")
    return FileResponse(path, media_type="text/markdown", filename="API.md")


@router.get("/health")
def health():
    now = datetime.now().astimezone()
    threads_total = None
    routines_total = routines_enabled = None
    try:
        threads_total = 0
        routines_total = routines_enabled = 0
        for name in _profile_names():
            try:
                threads_total += _db(name).session_count()
            except Exception:
                pass
            jobs = _profile_jobs(name)
            routines_total += len(jobs)
            routines_enabled += sum(1 for j in jobs if j.get("enabled", True))
    except Exception as exc:
        log.warning("engram health: counts failed: %s", exc)

    active_profile = None
    profile_count = None
    try:
        from hermes_cli import profiles as profiles_mod

        active_profile = profiles_mod.get_active_profile_name()
        profile_count = len(profiles_mod.list_profiles())
    except Exception as exc:
        log.warning("engram health: profiles failed: %s", exc)

    scopes = ["chat", "threads", "routines", "profiles", "models", "feedback"]
    if _kanban() is not None:
        # Feature-detected by the app: older hermes installs have no kanban.
        scopes.append("tasks")

    return {
        "ok": True,
        "service": "hermes-engram",
        "version": VERSION,
        "server_time": time.time(),
        "server_time_iso": now.isoformat(),
        "profile": {"active": active_profile, "count": profile_count},
        "threads": {"total": threads_total},
        "routines": {"total": routines_total, "enabled": routines_enabled},
        "gateway": {"chat_rpc": _chat_rpc_available()},
        "scopes": scopes,
    }


@router.get("/models")
def models():
    """Model picker, normalized for the app.

    hermes' inventory payload nests bare model-name strings under provider
    records in an internal shape; re-shape it here into a stable contract:
    ``current`` (the active provider+model) and ``groups`` (one per provider
    that actually has models, each model an object so fields can be added
    without breaking clients).
    """
    try:
        from hermes_cli.inventory import build_models_payload, load_picker_context

        ctx = load_picker_context()
        # Kwarg availability varies across hermes versions — retry without the
        # optional ones so the picker still loads on older installs.
        for kwargs in (
            {"picker_hints": True, "probe_custom_providers": False},
            {"picker_hints": True},
            {},
        ):
            try:
                payload = build_models_payload(ctx, **kwargs)
                break
            except TypeError:
                continue
        else:
            raise RuntimeError("no compatible build_models_payload signature")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"model inventory failed: {exc}")

    cur_model = payload.get("model")
    cur_provider = payload.get("provider")
    groups = []
    for p in payload.get("providers") or []:
        names = [m for m in (p.get("models") or []) if isinstance(m, str)]
        if not names:
            continue
        is_current_provider = bool(p.get("is_current"))
        groups.append({
            "provider": p.get("slug"),
            "label": p.get("name") or p.get("slug"),
            "authenticated": bool(p.get("authenticated")),
            "current_provider": is_current_provider,
            "models": [
                {"id": n, "current": is_current_provider and n == cur_model}
                for n in names
            ],
        })
    return {
        "current": {"provider": cur_provider, "model": cur_model},
        "groups": groups,
    }


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------

def _parse_source_filter(source: Optional[str]) -> tuple:
    """``"engram"`` → include set; ``"!engram"`` → exclude set; None → no filter.

    Comma-separated; a leading ``!`` flips the whole list to an exclusion
    (mixing include and exclude labels makes no sense, so the first label
    decides the mode).
    """
    if not source:
        return None, None
    labels = [s.strip().lower() for s in source.split(",") if s.strip()]
    if not labels:
        return None, None
    if labels[0].startswith("!"):
        return None, frozenset(l.lstrip("!") for l in labels if l.lstrip("!"))
    return frozenset(labels), None


def _list_threads_for(profile: str, fetch: int, include_archived: bool,
                      status: str, running_map: dict,
                      include_sources: Optional[frozenset] = None,
                      exclude_sources: Optional[frozenset] = None) -> list:
    try:
        db = _db(profile)
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("engram: opening %s state.db failed: %s", profile, exc)
        return []
    # Push the source filter into SQL where the installed SessionDB supports
    # it (correct LIMIT); the Python filter below stays as the portable path.
    kwargs: dict = dict(
        limit=fetch,
        order_by_last_active=True,
        include_archived=include_archived,
    )
    if include_sources and len(include_sources) == 1:
        kwargs["source"] = next(iter(include_sources))
    elif exclude_sources:
        kwargs["exclude_sources"] = sorted(exclude_sources)
    try:
        rows = db.list_sessions_rich(**kwargs)
    except TypeError:
        try:
            rows = db.list_sessions_rich(
                limit=fetch,
                order_by_last_active=True,
                include_archived=include_archived,
            )
        except TypeError:
            rows = db.list_sessions_rich(limit=fetch)
    if include_sources:
        rows = [r for r in rows
                if (r.get("source") or "").strip().lower() in include_sources]
    elif exclude_sources:
        rows = [r for r in rows
                if (r.get("source") or "").strip().lower() not in exclude_sources]
    if status == "resolved":
        rows = [r for r in rows if r.get("archived")]
    threads = [_thread_dict(r, running_map, profile) for r in rows]
    _overlay_cron_previews(profile, threads)
    return threads


def _overlay_cron_previews(profile: str, threads: list) -> None:
    """Give cron threads a useful preview: the run's last assistant message.

    The stored preview (first user message) is always the scheduler preamble,
    which _thread_dict drops — so without this, cron threads render blank.
    The last assistant message is the run's report, or the agent's question
    when a routine ends by asking for input (the whole point of surfacing
    runs as threads).
    """
    ids = [t["id"] for t in threads
           if (t.get("source") or "").lower() == "cron" and not t.get("preview")]
    if not ids:
        return
    try:
        path = _state_db_path(profile)
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except Exception as exc:
        log.warning("engram: cron preview overlay skipped for %s: %s", profile, exc)
        return
    try:
        by_id = {t["id"]: t for t in threads}
        for sid in ids:
            row = conn.execute(
                "SELECT content FROM messages WHERE session_id = ? "
                "AND role = 'assistant' AND active = 1 ORDER BY id DESC LIMIT 1",
                (sid,),
            ).fetchone()
            if row:
                text = _content_text(row[0]).strip()
                if text:
                    by_id[sid]["preview"] = text[:200]
    except Exception as exc:
        log.warning("engram: cron preview overlay failed for %s: %s", profile, exc)
    finally:
        conn.close()


@router.get("/threads")
def list_threads(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: str = Query("open", pattern="^(open|resolved|all)$"),
    profile: str = Query("all", description='Profile name, or "all" to aggregate'),
    source: Optional[str] = Query(
        None,
        description='Comma-separated source labels to include (e.g. "engram"), '
                    'or "!"-prefixed to exclude (e.g. "!engram"). Every thread '
                    'carries the surface it was created from: "engram" (this '
                    'API), "tui"/"desktop" (hermes), "cron" (routine runs).',
    ),
):
    include_archived = status in ("resolved", "all")
    include_sources, exclude_sources = _parse_source_filter(source)
    running_map = _running_map()
    fetch = limit + offset
    if include_sources or exclude_sources:
        # Over-fetch: on older installs the SQL pushdown falls back and the
        # filter runs in Python over a LIMIT'd page (multi-source always does).
        fetch = max(fetch * 2, 200)
    profiles = _profile_names() if profile == "all" else [profile]
    if profile != "all":
        _profile_home_or_404(profile)  # 404 early on unknown profile

    threads: list = []
    total = 0
    total_known = True
    for name in profiles:
        threads.extend(_list_threads_for(name, fetch, include_archived, status,
                                         running_map, include_sources, exclude_sources))
        try:
            db = _db(name)
            if include_sources:
                # sources are disjoint, so per-source counts sum cleanly
                total += sum(db.session_count(include_archived=include_archived, source=s)
                             for s in include_sources)
            elif exclude_sources:
                total += db.session_count(include_archived=include_archived,
                                          exclude_sources=sorted(exclude_sources))
            else:
                total += db.session_count(include_archived=include_archived)
        except Exception:
            total_known = False
    threads.sort(key=lambda t: t.get("last_active") or t.get("started_at") or 0, reverse=True)
    threads = threads[offset:offset + limit]
    return {
        "threads": threads,
        "total": total if total_known else None,
        "limit": limit,
        "offset": offset,
        "profile": profile,
        "source": source,
    }


class CreateThreadBody(BaseModel):
    message: str = Field(min_length=1)
    title: Optional[str] = None
    model: Optional[str] = None
    profile: Optional[str] = None


_model_ids_cache: dict = {"ts": 0.0, "ids": frozenset(), "providers": frozenset()}


def _validate_model_id(model: str) -> None:
    """400 on a model id the inventory doesn't know.

    Without this, session.create happily stores the bad id and the agent
    build dies AFTER the 201 — the user message persists but no reply ever
    comes, with nothing to tell the client why. The classic mistake is
    passing a provider slug (e.g. "azure-foundry") as the model.
    """
    now = time.time()
    if now - _model_ids_cache["ts"] > 300 or not _model_ids_cache["ids"]:
        try:
            inventory = models()
            _model_ids_cache.update(
                ts=now,
                ids=frozenset(m["id"] for g in inventory["groups"] for m in g["models"]),
                providers=frozenset(g["provider"] for g in inventory["groups"]),
            )
        except Exception as exc:
            log.warning("engram: model validation skipped (inventory failed: %s)", exc)
            return
    if model in _model_ids_cache["ids"]:
        return
    if model in _model_ids_cache["providers"]:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{model!r} is a provider, not a model — pick one of its model "
                "ids from GET /models (groups[].models[].id)"
            ),
        )
    raise HTTPException(status_code=400, detail=f"unknown model id {model!r} — see GET /models")


@router.post("/threads", status_code=201)
def create_thread(payload: CreateThreadBody):
    profile = payload.profile or "default"
    _profile_home_or_404(profile)
    if payload.model:
        _validate_model_id(payload.model)
    params: dict = {"cols": 80, "source": SOURCE_LABEL}
    if payload.title:
        params["title"] = payload.title
    if payload.model:
        params["model"] = payload.model
    if not _is_launch_profile(profile):
        # tui_gateway's app-global remote mode: agent + persistence bind to
        # this profile's HERMES_HOME/state.db instead of the launch profile's.
        params["profile"] = profile
    created = _rpc("session.create", params)
    sid = str(created.get("session_id") or "")
    if not sid:
        raise HTTPException(status_code=502, detail="session.create returned no session id")
    submit_params: dict = {"session_id": sid, "text": payload.message}
    submitted = _rpc("prompt.submit", submit_params)
    # `stored_session_id` is the persistent SessionDB id (state.db row);
    # `session_id` is only the live in-process gateway handle.
    thread_id = created.get("stored_session_id") or created.get("session_key") or sid
    return {
        "thread_id": str(thread_id),
        "profile": profile,
        "gateway_sid": sid,
        "accepted": True,
        "queued": submitted.get("status") == "queued",
    }


@router.get("/threads/{thread_id}")
def get_thread(
    thread_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    after_message_id: Optional[int] = Query(None),
    profile: str = Query("default"),
):
    return _session_transcript(thread_id, limit, offset, after_message_id, profile)


class SendMessageBody(BaseModel):
    text: str = Field(min_length=1)


@router.post("/threads/{thread_id}/messages", status_code=202)
def send_message(thread_id: str, payload: SendMessageBody, profile: str = Query("default")):
    db = _db(profile)
    if not db.get_session(thread_id):
        raise HTTPException(status_code=404, detail=f"thread {thread_id} not found")
    resume_params: dict = {"session_id": thread_id, "cols": 80}
    if not _is_launch_profile(profile):
        resume_params["profile"] = profile
    resumed = _rpc("session.resume", resume_params)
    sid = str(resumed.get("session_id") or "")
    if not sid:
        raise HTTPException(status_code=502, detail="session.resume returned no live session id")
    submitted = _rpc("prompt.submit", {"session_id": sid, "text": payload.text})
    return {
        "accepted": True,
        "gateway_sid": sid,
        "queued": submitted.get("status") == "queued",
    }


class UpdateThreadBody(BaseModel):
    title: Optional[str] = None
    archived: Optional[bool] = None


@router.patch("/threads/{thread_id}")
def update_thread(thread_id: str, payload: UpdateThreadBody, profile: str = Query("default")):
    db = _db(profile)
    if not db.get_session(thread_id):
        raise HTTPException(status_code=404, detail=f"thread {thread_id} not found")
    if payload.title is not None:
        try:
            db.set_session_title(thread_id, payload.title)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
    if payload.archived is not None:
        db.set_session_archived(thread_id, bool(payload.archived))
    return {"thread": _thread_dict(db.get_session(thread_id) or {}, _running_map(), profile)}


@router.delete("/threads/{thread_id}")
def delete_thread(thread_id: str, profile: str = Query("default")):
    db = _db(profile)
    if not db.get_session(thread_id):
        raise HTTPException(status_code=404, detail=f"thread {thread_id} not found")
    db.delete_session(thread_id)
    return {"deleted": True, "thread_id": thread_id, "profile": profile}


# ---------------------------------------------------------------------------
# Routines (cron jobs)
# ---------------------------------------------------------------------------

def _schedule_dict(schedule: Any) -> dict:
    if not isinstance(schedule, dict):
        return {"kind": "unknown", "human": str(schedule or "")}
    kind = schedule.get("kind") or "unknown"
    human = ""
    if kind == "interval":
        human = f"every {schedule.get('minutes')} min"
    elif kind == "cron":
        human = str(schedule.get("expr") or "")
    elif kind == "once":
        human = f"once at {schedule.get('run_at')}"
    out = dict(schedule)
    out["human"] = human
    return out


def _routine_dict(job: dict, profile: str = "default") -> dict:
    # Non-launch profiles' jobs come straight from jobs.json without cron's
    # read normalization, so name can be null there — the app requires one.
    name = (job.get("name") or "").strip() \
        or (job.get("prompt") or "")[:50].strip() \
        or job.get("id") or "routine"
    return {
        "id": job.get("id"),
        "profile": profile,
        "name": name,
        "instructions": job.get("prompt"),
        "schedule": _schedule_dict(job.get("schedule")),
        "enabled": bool(job.get("enabled", True)),
        "state": job.get("state"),
        "next_run_at": job.get("next_run_at"),
        "last_run_at": job.get("last_run_at"),
        "last_status": job.get("last_status") or job.get("last_result"),
        "model": job.get("model"),
        "deliver": job.get("deliver"),
    }


def _profile_jobs(profile: str) -> list:
    """All cron jobs (incl. disabled) for one profile.

    The launch profile goes through the cron library (full normalization).
    Other profiles' stores are read straight from their jobs.json —
    ``cron/jobs.py`` binds its paths to this process's HERMES_HOME at import
    time, so the library can't be pointed elsewhere in-process.
    """
    if _is_launch_profile(profile):
        from cron.jobs import list_jobs

        return list_jobs(include_disabled=True)
    jobs_file = _profile_home_or_404(profile) / "cron" / "jobs.json"
    if not jobs_file.is_file():
        return []
    try:
        data = json.loads(jobs_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        log.warning("engram: reading %s failed: %s", jobs_file, exc)
        return []
    jobs = data.get("jobs") if isinstance(data, dict) else data
    return [j for j in (jobs or []) if isinstance(j, dict)]


def _find_job(job_id: str, profile_hint: Optional[str] = None) -> tuple:
    """Locate a job by id across profiles → (profile, job). 404 if absent."""
    profiles = [profile_hint] if profile_hint and profile_hint != "all" else _profile_names()
    for name in profiles:
        for job in _profile_jobs(name):
            if job.get("id") == job_id or job.get("name") == job_id:
                return name, job
    raise HTTPException(status_code=404, detail=f"routine {job_id} not found")


# Finite one-shot jobs self-destruct when they hit their repeat limit —
# cron/jobs.py mark_job_run pops them from jobs.json — so a completed task
# leaves no trace in the live store. Its run sessions in state.db are the
# durable record (session id ``cron_{job_id}_{ts}``, title "<job-name> · <date>"):
# synthesize state="completed" routines from recent cron sessions whose job id
# is gone, so the app keeps showing finished tasks.
_COMPLETED_SCAN_LIMIT = 300  # newest cron sessions considered per profile


def _epoch_iso(epoch: Any) -> Optional[str]:
    """Session epoch seconds -> tz-aware ISO, matching jobs.json timestamps."""
    try:
        return datetime.fromtimestamp(float(epoch)).astimezone().isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def _strip_cron_preamble(text: str) -> Optional[str]:
    """First user message of a cron run -> the job's prompt.

    The scheduler prefixes every run with an "[IMPORTANT: You are running as
    a scheduled cron job...]" block. It can contain nested brackets
    ("[SILENT]"), so cut at the closing "]" followed by a blank line rather
    than the first "]".
    """
    text = (text or "").strip()
    if text.startswith("[IMPORTANT:"):
        end = text.find("]\n\n")
        text = text[end + 1:].strip() if end != -1 else ""
    return text or None


def _completed_routine_dict(profile: str, job_id: str, row: Any, conn: Any) -> dict:
    """Newest run session row -> synthetic completed routine."""
    title = str(row["title"] or "")
    # Run titles are "<job-name> · <short date>" (rename tests may lack the tail).
    name = title.rsplit(" · ", 1)[0].strip() or job_id
    instructions = None
    try:
        first = conn.execute(
            "SELECT content FROM messages WHERE session_id = ? AND role = 'user' "
            "AND active = 1 ORDER BY id LIMIT 1",
            (row["id"],),
        ).fetchone()
        if first:
            instructions = _strip_cron_preamble(_content_text(first["content"]))
    except sqlite3.Error:
        pass
    ended = row["ended_at"]
    end_reason = str(row["end_reason"] or "")
    if ended:
        status = "error" if "error" in end_reason.lower() else "ok"
    else:
        # The job is gone, so nothing can still be running it — an
        # unfinalized newest run means the scheduler died mid-run.
        status = "stale"
    return {
        "id": job_id,
        "profile": profile,
        "name": name,
        "instructions": instructions,
        "schedule": {"kind": "once", "human": "one-time"},
        "enabled": False,
        "state": "completed",
        "next_run_at": None,
        "last_run_at": _epoch_iso(ended or row["started_at"]),
        "last_status": status,
        "model": None,
        "deliver": None,
    }


def _completed_routines(profile: str, days: int, live_ids: set) -> list:
    """Completed (self-deleted) jobs for one profile, from recent cron runs."""
    try:
        path = _state_db_path(profile)
    except HTTPException:
        return []
    if not path.is_file():
        return []
    cutoff = time.time() - days * 86400.0
    out: list = []
    seen: set = set()
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error as exc:
        log.warning("engram: opening %s failed: %s", path, exc)
        return []
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, title, started_at, ended_at, end_reason FROM sessions "
            "WHERE source = 'cron' AND started_at >= ? "
            "ORDER BY started_at DESC LIMIT ?",
            (cutoff, _COMPLETED_SCAN_LIMIT),
        ).fetchall()
        for r in rows:
            job_id = _parse_cron_session_id(r["id"])
            if not job_id or job_id in live_ids or job_id in seen:
                continue
            seen.add(job_id)
            out.append(_completed_routine_dict(profile, job_id, r, conn))
    except sqlite3.Error as exc:
        log.warning("engram: scanning completed routines failed: %s", exc)
    finally:
        conn.close()
    return out


def _find_completed_routine(job_id: str, profile_hint: Optional[str]) -> tuple:
    """Locate a finished (self-deleted) job by the run sessions it left behind.

    Bounded ``[prefix, hi)`` id-range scan like SessionDB.list_cron_job_runs —
    no time window, so old completed routines stay openable from stale clients.
    Raises 404 if no profile has runs for the id.
    """
    profiles = [profile_hint] if profile_hint and profile_hint != "all" else _profile_names()
    prefix = f"cron_{job_id}_"
    prefix_hi = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    for name in profiles:
        try:
            path = _state_db_path(name)
        except HTTPException:
            continue
        if not path.is_file():
            continue
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
        except sqlite3.Error:
            continue
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT id, title, started_at, ended_at, end_reason FROM sessions "
                "WHERE source = 'cron' AND id >= ? AND id < ? "
                "ORDER BY started_at DESC LIMIT 1",
                (prefix, prefix_hi),
            ).fetchone()
            if row:
                return name, _completed_routine_dict(name, job_id, row, conn)
        except sqlite3.Error:
            pass
        finally:
            conn.close()
    raise HTTPException(status_code=404, detail=f"routine {job_id} not found")


# Mutation ops for non-launch profiles run in a short-lived subprocess with
# HERMES_HOME pointed at that profile, reusing the exact cron library logic
# (locking, schedule parsing, next-run derivation) under the right store.
_CRON_OP_SCRIPT = """
import json, sys
op = json.loads(sys.stdin.read())
from cron import jobs as J
kind = op["op"]
if kind == "create":
    out = J.create_job(prompt=op["prompt"], schedule=op["schedule"],
                       name=op.get("name"), model=op.get("model"),
                       origin=op.get("origin"))
elif kind == "update":
    updates = op.get("updates") or {}
    if "schedule_raw" in op:
        updates["schedule"] = J.parse_schedule(op["schedule_raw"])
    out = J.update_job(op["job_id"], updates)
elif kind == "pause":
    out = J.pause_job(op["job_id"], reason=op.get("reason"))
elif kind == "resume":
    out = J.resume_job(op["job_id"])
elif kind == "trigger":
    out = J.trigger_job(op["job_id"])
elif kind == "remove":
    out = J.remove_job(op["job_id"])
else:
    raise ValueError(f"unknown op {kind!r}")
print(json.dumps(out))
"""


def _cron_exec(profile: str, op: dict) -> Any:
    """Run a cron mutation under ``profile``'s HERMES_HOME and return its result."""
    import os
    import subprocess
    import sys

    import cron.jobs as _cron_jobs_mod

    repo_root = Path(_cron_jobs_mod.__file__).resolve().parents[1]
    env = dict(os.environ)
    env["HERMES_HOME"] = str(_profile_home_or_404(profile))
    env["PYTHONPATH"] = str(repo_root)
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _CRON_OP_SCRIPT],
            input=json.dumps(op),
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
            cwd=str(repo_root),
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="cron operation timed out")
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip().splitlines()[-1:] or ["cron operation failed"]
        raise HTTPException(status_code=400, detail=detail[0])
    try:
        return json.loads(proc.stdout)
    except ValueError:
        raise HTTPException(status_code=502, detail="cron operation returned no result")


def _cron_mutate(profile: str, op: dict) -> Any:
    """Dispatch a cron mutation to the right store for ``profile``."""
    if not _is_launch_profile(profile):
        return _cron_exec(profile, op)
    from cron import jobs as J

    kind = op["op"]
    try:
        if kind == "create":
            return J.create_job(prompt=op["prompt"], schedule=op["schedule"],
                                name=op.get("name"), model=op.get("model"),
                                origin=op.get("origin"))
        if kind == "update":
            updates = op.get("updates") or {}
            if "schedule_raw" in op:
                updates["schedule"] = J.parse_schedule(op["schedule_raw"])
            return J.update_job(op["job_id"], updates)
        if kind == "pause":
            return J.pause_job(op["job_id"], reason=op.get("reason"))
        if kind == "resume":
            return J.resume_job(op["job_id"])
        if kind == "trigger":
            return J.trigger_job(op["job_id"])
        if kind == "remove":
            return J.remove_job(op["job_id"])
    except (ValueError, KeyError) as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    raise HTTPException(status_code=400, detail=f"unknown op {op['op']!r}")


# An unfinished run without a new message for this long is presumed dead.
# ``ended_at`` stays NULL when the scheduler is killed mid-run (gateway or
# dashboard restart), which otherwise reads as "running" forever. Every agent
# turn persists message rows as it goes, so a live run's last_active keeps
# moving; 15 minutes of silence on an unfinalized cron session means orphaned.
_RUN_STALE_AFTER = 900.0


def _run_dict(row: dict) -> dict:
    ended = row.get("ended_at")
    end_reason = str(row.get("end_reason") or "")
    if ended:
        status = "error" if "error" in end_reason.lower() else "ok"
    else:
        last = row.get("last_active") or row.get("started_at") or 0
        try:
            silent = time.time() - float(last)
        except (TypeError, ValueError):
            silent = _RUN_STALE_AFTER
        status = "running" if silent < _RUN_STALE_AFTER else "stale"
    # A cron session's first user message is the scheduler's injected
    # "[IMPORTANT: You are running as a scheduled cron job...]" preamble —
    # useless as a row summary, so drop it and let clients fall back to
    # status + duration.
    preview = row.get("preview") or None
    if preview and preview.lstrip().startswith("[IMPORTANT:"):
        preview = None
    return {
        "session_id": row.get("id"),
        "started_at": row.get("started_at"),
        "ended_at": ended,
        "status": status,
        "preview": preview,
        "message_count": row.get("message_count") or 0,
    }


@router.get("/routines")
def list_routines(
    profile: str = Query("all", description='Profile name, or "all"'),
    completed_days: int = Query(
        7, ge=0, le=90,
        description="Also include one-shot jobs that finished (and self-deleted) "
                    "within this many days; 0 = live jobs only",
    ),
):
    profiles = _profile_names() if profile == "all" else [profile]
    if profile != "all":
        _profile_home_or_404(profile)
    routines: list = []
    completed: list = []
    for name in profiles:
        jobs = _profile_jobs(name)
        routines.extend(_routine_dict(j, name) for j in jobs)
        if completed_days > 0:
            live_ids = {j.get("id") for j in jobs}
            completed.extend(_completed_routines(name, completed_days, live_ids))
    completed.sort(key=lambda r: r.get("last_run_at") or "", reverse=True)
    return {"routines": routines + completed, "profile": profile}


class CreateRoutineBody(BaseModel):
    instructions: str = Field(min_length=1)
    schedule: str = Field(min_length=1)
    name: Optional[str] = None
    model: Optional[str] = None
    profile: Optional[str] = None


@router.post("/routines", status_code=201)
def create_routine(payload: CreateRoutineBody):
    profile = payload.profile or "default"
    _profile_home_or_404(profile)
    job = _cron_mutate(profile, {
        "op": "create",
        "prompt": payload.instructions,
        "schedule": payload.schedule,
        "name": payload.name,
        "model": payload.model,
        "origin": {"source": SOURCE_LABEL},
    })
    return {"routine": _routine_dict(job, profile)}


@router.get("/routines/{job_id}")
def get_routine(job_id: str, profile: Optional[str] = Query(None)):
    try:
        pname, job = _find_job(job_id, profile)
        routine = _routine_dict(job, pname)
        run_job_id = job["id"]
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        # Finished one-shots self-delete from jobs.json; serve them from
        # their surviving run sessions so the detail screen still opens.
        pname, routine = _find_completed_routine(job_id, profile)
        run_job_id = job_id
    runs: list = []
    try:
        runs = [_run_dict(r) for r in _db(pname).list_cron_job_runs(run_job_id, limit=10)]
    except Exception as exc:
        log.warning("engram: list_cron_job_runs failed: %s", exc)
    return {"routine": routine, "runs": runs}


class UpdateRoutineBody(BaseModel):
    name: Optional[str] = None
    instructions: Optional[str] = None
    schedule: Optional[str] = None
    model: Optional[str] = None
    deliver: Optional[str] = None


@router.patch("/routines/{job_id}")
def update_routine(job_id: str, payload: UpdateRoutineBody,
                   profile: Optional[str] = Query(None)):
    pname, job = _find_job(job_id, profile)
    updates: dict = {}
    if payload.name is not None:
        updates["name"] = payload.name
    if payload.instructions is not None:
        updates["prompt"] = payload.instructions
    if payload.model is not None:
        updates["model"] = payload.model or None
    if payload.deliver is not None:
        updates["deliver"] = payload.deliver or None
    op: dict = {"op": "update", "job_id": job["id"], "updates": updates}
    if payload.schedule is not None:
        op["schedule_raw"] = payload.schedule
    if not updates and "schedule_raw" not in op:
        raise HTTPException(status_code=400, detail="no fields to update")
    updated = _cron_mutate(pname, op)
    if updated is None:
        raise HTTPException(status_code=404, detail=f"routine {job_id} not found")
    return {"routine": _routine_dict(updated, pname)}


@router.post("/routines/{job_id}/run", status_code=202)
def run_routine(job_id: str, profile: Optional[str] = Query(None)):
    pname, job = _find_job(job_id, profile)
    updated = _cron_mutate(pname, {"op": "trigger", "job_id": job["id"]})
    if updated is None:
        raise HTTPException(status_code=404, detail=f"routine {job_id} not found")
    return {"routine": _routine_dict(updated, pname), "triggered": True}


@router.post("/routines/{job_id}/pause")
def pause_routine(job_id: str, profile: Optional[str] = Query(None)):
    pname, job = _find_job(job_id, profile)
    updated = _cron_mutate(pname, {"op": "pause", "job_id": job["id"],
                                   "reason": "paused from Engram"})
    if updated is None:
        raise HTTPException(status_code=404, detail=f"routine {job_id} not found")
    return {"routine": _routine_dict(updated, pname)}


@router.post("/routines/{job_id}/resume")
def resume_routine(job_id: str, profile: Optional[str] = Query(None)):
    pname, job = _find_job(job_id, profile)
    updated = _cron_mutate(pname, {"op": "resume", "job_id": job["id"]})
    if updated is None:
        raise HTTPException(status_code=404, detail=f"routine {job_id} not found")
    return {"routine": _routine_dict(updated, pname)}


@router.delete("/routines/{job_id}")
def delete_routine(job_id: str, profile: Optional[str] = Query(None)):
    pname, job = _find_job(job_id, profile)
    if not _cron_mutate(pname, {"op": "remove", "job_id": job["id"]}):
        raise HTTPException(status_code=404, detail=f"routine {job_id} not found")
    return {"deleted": True, "routine_id": job_id, "profile": pname}


@router.get("/routines/{job_id}/runs")
def routine_runs(
    job_id: str,
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    profile: Optional[str] = Query(None),
):
    try:
        pname, job = _find_job(job_id, profile)
        run_job_id = job["id"]
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        pname, _routine = _find_completed_routine(job_id, profile)
        run_job_id = job_id
    rows = _db(pname).list_cron_job_runs(run_job_id, limit=limit, offset=offset)
    return {"runs": [_run_dict(r) for r in rows], "profile": pname}


@router.get("/runs/{session_id}")
def get_run(
    session_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    profile: str = Query("default"),
):
    # The app navigates to runs without knowing their profile. Cron session
    # ids are globally unique (cron_{job_id}_{ts}), so fall through to the
    # other profiles' stores before giving up.
    try:
        return _session_transcript(session_id, limit, offset, None, profile)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
    for name in _profile_names():
        if name == profile:
            continue
        try:
            return _session_transcript(session_id, limit, offset, None, name)
        except HTTPException as exc:
            if exc.status_code != 404:
                raise
    raise HTTPException(status_code=404, detail=f"session {session_id} not found")


# ---------------------------------------------------------------------------
# Inbox — routine reports as first-class items.
#
# Every cron run is already persisted as a ``source='cron'`` session in its
# profile's state.db, whatever the job's ``deliver`` target is. The inbox is
# DERIVED from those rows (nothing new to intercept in hermes) with Engram's
# own read/unread overlay stored at ~/.hermes/engram/inbox.json. Item ids are
# ``{profile}:{run_session_id}`` so the app never needs a separate profile
# param here. ``kind`` is extensible — routine_run today; agent asks /
# deliveries later.
# ---------------------------------------------------------------------------

_INBOX_SCAN_LIMIT = 200  # newest cron runs considered per profile
_inbox_lock = threading.Lock()


def _inbox_store_path() -> Path:
    return _hermes_home() / "engram" / "inbox.json"


def _inbox_read_state() -> dict:
    try:
        data = json.loads(_inbox_store_path().read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _inbox_save_read_state(state: dict) -> None:
    dest = _inbox_store_path()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(state), encoding="utf-8")


_CRON_SESSION_RE = None


def _parse_cron_session_id(session_id: str) -> Optional[str]:
    """cron_<job_id>_<timestamp> -> job_id (None if not a cron session id)."""
    global _CRON_SESSION_RE
    if _CRON_SESSION_RE is None:
        import re

        # timestamp tail is digits possibly with one underscore (YYYYmmdd_HHMMSS)
        _CRON_SESSION_RE = re.compile(r"^cron_(?P<job>.+?)_(?P<ts>\d{8}_\d{6}|\d+)$")
    m = _CRON_SESSION_RE.match(session_id or "")
    return m.group("job") if m else None


def _scan_inbox_profile(profile: str) -> list:
    """Completed cron runs for one profile, newest first (no read overlay)."""
    try:
        path = _state_db_path(profile)
    except HTTPException:
        return []
    if not path.is_file():
        return []
    job_names = {}
    try:
        job_names = {j.get("id"): j.get("name") for j in _profile_jobs(profile)}
    except Exception:
        pass
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT id, started_at, ended_at, end_reason FROM sessions "
            "WHERE source = 'cron' AND ended_at IS NOT NULL "
            "ORDER BY ended_at DESC LIMIT ?",
            (_INBOX_SCAN_LIMIT,),
        ).fetchall()
        items = []
        for r in rows:
            job_id = _parse_cron_session_id(r["id"])
            summary_row = conn.execute(
                "SELECT content FROM messages WHERE session_id = ? AND role = 'assistant' "
                "AND active = 1 ORDER BY id DESC LIMIT 1",
                (r["id"],),
            ).fetchone()
            summary = _content_text(summary_row["content"])[:500] if summary_row else None
            end_reason = str(r["end_reason"] or "")
            items.append({
                "id": f"{profile}:{r['id']}",
                "kind": "routine_run",
                "profile": profile,
                "routine_id": job_id,
                "routine_name": job_names.get(job_id),
                "run_session_id": r["id"],
                "status": "error" if "error" in end_reason.lower() else "ok",
                "summary": summary,
                "created_at": r["ended_at"] or r["started_at"],
            })
        return items
    except sqlite3.Error as exc:
        log.warning("engram inbox scan (%s) failed: %s", profile, exc)
        return []
    finally:
        conn.close()


def _inbox_items(profile: str = "all") -> list:
    profiles = _profile_names() if profile == "all" else [profile]
    if profile != "all":
        _profile_home_or_404(profile)
    items: list = []
    for name in profiles:
        items.extend(_scan_inbox_profile(name))
    items.sort(key=lambda i: i.get("created_at") or 0, reverse=True)
    read = _inbox_read_state()
    for i in items:
        i["read"] = bool(read.get(i["id"]))
    return items


@router.get("/inbox")
def get_inbox(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    unread: bool = Query(False, description="Only unread items"),
    profile: str = Query("all"),
):
    items = _inbox_items(profile)
    unread_count = sum(1 for i in items if not i["read"])
    if unread:
        items = [i for i in items if not i["read"]]
    return {
        "items": items[offset:offset + limit],
        "total": len(items),
        "unread": unread_count,
        "limit": limit,
        "offset": offset,
    }


class InboxMarkBody(BaseModel):
    read: bool = True


@router.patch("/inbox/{item_id}")
def mark_inbox_item(item_id: str, payload: InboxMarkBody):
    if ":" not in item_id:
        raise HTTPException(status_code=400, detail="item id is {profile}:{run_session_id}")
    with _inbox_lock:
        state = _inbox_read_state()
        if payload.read:
            state[item_id] = True
        else:
            state.pop(item_id, None)
        _inbox_save_read_state(state)
    return {"id": item_id, "read": payload.read}


@router.post("/inbox/read-all")
def mark_inbox_all_read(profile: str = Query("all")):
    items = _inbox_items(profile)
    with _inbox_lock:
        state = _inbox_read_state()
        for i in items:
            state[i["id"]] = True
        _inbox_save_read_state(state)
    return {"ok": True, "marked": len(items)}


# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------

def _profile_summary(p: Any, active_name: str) -> dict:
    return {
        "name": p.name,
        "is_default": bool(p.is_default),
        "active": p.name == active_name,
        "model": p.model,
        "provider": p.provider,
        "skill_count": p.skill_count,
        "description": getattr(p, "description", None),
        "gateway_running": bool(p.gateway_running),
    }


@router.get("/profiles")
def list_profiles_endpoint():
    from hermes_cli import profiles as profiles_mod

    active = profiles_mod.get_active_profile_name()
    out = []
    for p in profiles_mod.list_profiles():
        summary = _profile_summary(p, active)
        try:
            summary["thread_count"] = _db(p.name).session_count()
        except Exception:
            summary["thread_count"] = None
        try:
            summary["routine_count"] = len(_profile_jobs(p.name))
        except Exception:
            summary["routine_count"] = None
        out.append(summary)
    return {"active": active, "profiles": out}


def _profile_dir_or_404(name: str) -> Path:
    from hermes_cli import profiles as profiles_mod

    try:
        if name != "default" and not profiles_mod.profile_exists(name):
            raise HTTPException(status_code=404, detail=f"profile {name} not found")
        pdir = Path(profiles_mod.get_profile_dir(name))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not pdir.is_dir():
        raise HTTPException(status_code=404, detail=f"profile {name} not found")
    return pdir


@router.get("/profiles/{name}")
def get_profile(name: str):
    from hermes_cli import profiles as profiles_mod

    pdir = _profile_dir_or_404(name)
    active = profiles_mod.get_active_profile_name()
    summary = None
    for p in profiles_mod.list_profiles():
        if p.name == name:
            summary = _profile_summary(p, active)
            break
    soul = None
    soul_path = pdir / "SOUL.md"
    if soul_path.is_file():
        try:
            soul = soul_path.read_text(encoding="utf-8")
        except OSError as exc:
            log.warning("engram: reading SOUL.md failed: %s", exc)
    skills: list = []
    skills_dir = pdir / "skills"
    if skills_dir.is_dir():
        skills = sorted(p.name for p in skills_dir.iterdir() if not p.name.startswith("."))
    return {"profile": summary or {"name": name}, "soul": soul, "skills": skills}


class UpdateProfileBody(BaseModel):
    soul: str


@router.patch("/profiles/{name}")
def update_profile(name: str, payload: UpdateProfileBody):
    if len(payload.soul.encode("utf-8")) > _MAX_SOUL_BYTES:
        raise HTTPException(status_code=413, detail="SOUL.md too large")
    pdir = _profile_dir_or_404(name)
    try:
        (pdir / "SOUL.md").write_text(payload.soul, encoding="utf-8")
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"failed to write SOUL.md: {exc}")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------

class FeedbackTarget(BaseModel):
    kind: str = Field(pattern="^(thread|routine|routine_run)$")
    thread_id: Optional[str] = None
    routine_id: Optional[str] = None
    run_session_id: Optional[str] = None


class FeedbackBody(BaseModel):
    target: FeedbackTarget
    verdict: str = Field(pattern="^(good|needs_work)$")
    chips: list[str] = Field(default_factory=list)
    note: Optional[str] = None


@router.post("/feedback")
def post_feedback(payload: FeedbackBody):
    record = {
        "ts": time.time(),
        "iso": datetime.now().astimezone().isoformat(),
        "target": payload.target.model_dump(exclude_none=True),
        "verdict": payload.verdict,
        "chips": payload.chips,
        "note": payload.note,
    }
    dest = _hermes_home() / "engram"
    try:
        dest.mkdir(parents=True, exist_ok=True)
        with open(dest / "feedback.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"failed to store feedback: {exc}")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Tasks — the app's "Needs you" surface over the kanban board.
#
# A task is a kanban board row (hermes_cli.kanban_db, its own per-board
# SQLite store), not a session; every dispatch attempt runs as an ordinary
# hermes session, recorded in task_runs with the worker's session id in the
# run metadata. This surface is deliberately narrow: list + detail + the two
# human verbs (comment, reply-and-resume). Board management, drag-drop and
# free status editing stay in the kanban dashboard.
#
# Statuses collapse into four app-facing groups; the dispatcher's state
# machine is otherwise opaque to the app:
#   needs_you : blocked, review, and triage with block_recurrences > 0
#               (a task the block-loop guard routed to triage — without this
#               clause it would silently vanish from the app's blocked view)
#   running   : running
#   queued    : triage, todo, scheduled, ready
#   done      : done
# ---------------------------------------------------------------------------

_TASK_GROUPS = ("needs_you", "running", "queued", "done", "all")

# Comment authors that render as the human side of the conversation. Worker
# comments carry the assignee profile name; `hermes kanban comment` uses the
# active profile or "user".
_TASK_HUMAN_AUTHORS = frozenset({"engram", "dashboard", "user", "human"})

# Event kinds whose payload["reason"] is the worker's question to the human.
_TASK_QUESTION_KINDS = ("blocked", "block_loop_detected", "spawn_auto_blocked")


def _kanban():
    """kanban_db module, or None on hermes builds that predate kanban."""
    try:
        from hermes_cli import kanban_db
        return kanban_db
    except Exception:
        return None


def _kanban_or_503():
    kb = _kanban()
    if kb is None:
        raise HTTPException(
            status_code=503,
            detail="kanban is not available on this hermes install",
        )
    return kb


def _task_board(kb, board: Optional[str]) -> Optional[str]:
    """Validate a ?board= slug (mirrors the kanban plugin's _resolve_board)."""
    if not board:
        return None
    try:
        normed = kb._normalize_board_slug(board)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if normed and normed != kb.DEFAULT_BOARD and not kb.board_exists(normed):
        raise HTTPException(status_code=404, detail=f"board {normed!r} does not exist")
    return normed


def _task_conn(kb, board: Optional[str] = None):
    try:
        kb.init_db(board=board)
    except Exception as exc:
        log.warning("engram: kanban init_db failed: %s", exc)
    return kb.connect(board=board)


def _task_group(task: Any) -> str:
    s = task.status
    if s in ("blocked", "review"):
        return "needs_you"
    if s == "triage" and (task.block_recurrences or 0) > 0:
        return "needs_you"
    if s == "running":
        return "running"
    if s == "done":
        return "done"
    return "queued"


def _task_maps(conn, task_ids: list) -> tuple[dict, dict, dict]:
    """(question, comment_count, last_activity) per task id, in three queries."""
    if not task_ids:
        return {}, {}, {}
    marks = ",".join("?" * len(task_ids))
    questions: dict = {}
    rows = conn.execute(
        f"SELECT task_id, payload FROM task_events "
        f"WHERE task_id IN ({marks}) AND kind IN "
        f"({','.join('?' * len(_TASK_QUESTION_KINDS))}) "
        f"ORDER BY created_at ASC, id ASC",
        [*task_ids, *_TASK_QUESTION_KINDS],
    ).fetchall()
    for r in rows:  # ascending — the newest reason wins
        try:
            payload = json.loads(r["payload"]) if r["payload"] else {}
        except Exception:
            payload = {}
        if payload.get("reason"):
            questions[r["task_id"]] = payload["reason"]
    counts = {
        r["task_id"]: r["n"]
        for r in conn.execute(
            f"SELECT task_id, COUNT(*) AS n FROM task_comments "
            f"WHERE task_id IN ({marks}) GROUP BY task_id",
            task_ids,
        ).fetchall()
    }
    activity = {
        r["task_id"]: r["ts"]
        for r in conn.execute(
            f"SELECT task_id, MAX(created_at) AS ts FROM task_events "
            f"WHERE task_id IN ({marks}) GROUP BY task_id",
            task_ids,
        ).fetchall()
    }
    return questions, counts, activity


def _task_card(task: Any, question: Optional[str], comment_count: int,
               last_activity: Optional[int]) -> dict:
    return {
        "id": task.id,
        "title": task.title,
        "group": _task_group(task),
        "status": task.status,
        "block_kind": task.block_kind,
        "block_recurrences": task.block_recurrences,
        "assignee": task.assignee,
        "priority": task.priority,
        "created_by": task.created_by,
        "question": question,
        "origin_session_id": task.session_id,
        "comment_count": comment_count,
        "created_at": task.created_at,
        "started_at": task.started_at,
        "completed_at": task.completed_at,
        "last_activity": last_activity or task.completed_at
                         or task.started_at or task.created_at,
    }


def _task_messages(comments: list, events: list) -> list:
    """Merge the comment thread and event log into me/agent/event messages.

    Worker questions (the reason on blocked-style events) render as agent
    bubbles — they are the worker speaking, not board bookkeeping.
    `commented` events are skipped (the comment rows carry the content).
    """
    out: list = []
    comment_bodies = []
    for c in comments:
        side = "me" if (c.author or "").lower() in _TASK_HUMAN_AUTHORS else "agent"
        comment_bodies.append(c.body or "")
        out.append({
            "id": f"c{c.id}", "kind": side, "text": c.body,
            "author": c.author, "ts": c.created_at,
        })
    for e in events:
        payload = e.payload or {}
        if e.kind == "commented":
            continue
        if e.kind in _TASK_QUESTION_KINDS and payload.get("reason"):
            # `hermes kanban block` (CLI and worker tool) also appends the
            # reason as a comment — don't show the question twice.
            if any(payload["reason"] in body for body in comment_bodies):
                continue
            out.append({
                "id": f"e{e.id}", "kind": "agent",
                "text": payload["reason"], "author": "worker",
                "ts": e.created_at, "event_kind": e.kind,
            })
            continue
        text = e.kind.replace("_", " ")
        detail = payload.get("reason") or payload.get("summary") \
            or payload.get("status") or payload.get("error")
        if detail:
            text = f"{text}: {detail}"
        out.append({
            "id": f"e{e.id}", "kind": "event", "text": text,
            "ts": e.created_at, "event_kind": e.kind,
        })
    out.sort(key=lambda m: (m["ts"] or 0, m["id"]))
    return out


def _task_run_dict(run: Any) -> dict:
    meta = run.metadata
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except Exception:
            meta = {}
    meta = meta or {}
    return {
        "id": run.id,
        "profile": run.profile,
        "status": run.status,
        "outcome": run.outcome,
        "summary": run.summary,
        "error": run.error,
        "started_at": run.started_at,
        "ended_at": run.ended_at,
        # The attempt's transcript: open with GET /runs/{id}?profile={profile}.
        "worker_session_id": meta.get("worker_session_id"),
    }


@router.get("/tasks")
def list_tasks_endpoint(
    group: Optional[str] = Query(None, description="needs_you|running|queued|done|all"),
    origin_session: Optional[str] = Query(
        None, description="Only tasks created from this session/thread id"),
    board: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    if group is not None and group not in _TASK_GROUPS:
        raise HTTPException(
            status_code=400,
            detail=f"group must be one of {', '.join(_TASK_GROUPS)}",
        )
    kb = _kanban_or_503()
    conn = _task_conn(kb, board=_task_board(kb, board))
    try:
        tasks = kb.list_tasks(conn, session_id=origin_session)
        if group and group != "all":
            tasks = [t for t in tasks if _task_group(t) == group]
        total = len(tasks)
        _, _, activity = _task_maps(conn, [t.id for t in tasks])
        tasks.sort(
            key=lambda t: activity.get(t.id) or t.completed_at
            or t.started_at or t.created_at or 0,
            reverse=True,
        )
        page = tasks[offset:offset + limit]
        questions, counts, _ = _task_maps(conn, [t.id for t in page])
        return {
            "tasks": [
                _task_card(t, questions.get(t.id), counts.get(t.id, 0),
                           activity.get(t.id))
                for t in page
            ],
            "total": total, "limit": limit, "offset": offset,
            "board": kb.get_current_board() if not board else board,
        }
    finally:
        conn.close()


@router.get("/tasks/{task_id}")
def get_task_endpoint(task_id: str, board: Optional[str] = Query(None)):
    kb = _kanban_or_503()
    conn = _task_conn(kb, board=_task_board(kb, board))
    try:
        task = kb.get_task(conn, task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")
        comments = kb.list_comments(conn, task_id)
        events = kb.list_events(conn, task_id)
        questions, counts, activity = _task_maps(conn, [task_id])
        card = _task_card(task, questions.get(task_id), counts.get(task_id, 0),
                          activity.get(task_id))
        card["body"] = task.body
        card["result"] = task.result
        links = {
            "parents": [r["parent_id"] for r in conn.execute(
                "SELECT parent_id FROM task_links WHERE child_id = ?",
                (task_id,)).fetchall()],
            "children": [r["child_id"] for r in conn.execute(
                "SELECT child_id FROM task_links WHERE parent_id = ?",
                (task_id,)).fetchall()],
        }
        return {
            "task": card,
            "messages": _task_messages(comments, events),
            "runs": [_task_run_dict(r) for r in kb.list_runs(conn, task_id)],
            "links": links,
        }
    finally:
        conn.close()


class TaskReplyBody(BaseModel):
    text: str
    resume: bool = True
    author: Optional[str] = "engram"


@router.post("/tasks/{task_id}/reply")
def reply_task(task_id: str, payload: TaskReplyBody,
               board: Optional[str] = Query(None)):
    """Comment on a task and (by default) send it back to the dispatcher.

    The two human verbs in one call: the answer lands in the comment thread
    (the next worker spawn reads it via build_worker_context) and the task
    flips blocked/scheduled -> ready. `resume` on a task that isn't blocked
    just queues the comment for the next attempt — not an error.
    """
    text = (payload.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    kb = _kanban_or_503()
    conn = _task_conn(kb, board=_task_board(kb, board))
    try:
        task = kb.get_task(conn, task_id)
        if task is None:
            raise HTTPException(status_code=404, detail=f"task {task_id} not found")
        kb.add_comment(conn, task_id, author=payload.author or "engram", body=text)
        resumed = False
        if payload.resume:
            if task.status in ("blocked", "scheduled"):
                resumed = kb.unblock_task(conn, task_id)
            elif task.status == "triage" and (task.block_recurrences or 0) > 0:
                # Loop-guard-routed task: a human answer is the triage
                # decision, so send it back to the pool — parent-gated the
                # same way unblock_task gates ready vs todo.
                with kb.write_txn(conn):
                    undone = conn.execute(
                        "SELECT 1 FROM task_links l "
                        "JOIN tasks p ON p.id = l.parent_id "
                        "WHERE l.child_id = ? AND p.status != 'done' LIMIT 1",
                        (task_id,),
                    ).fetchone()
                    new_status = "todo" if undone else "ready"
                    cur = conn.execute(
                        "UPDATE tasks SET status = ?, consecutive_failures = 0, "
                        "last_failure_error = NULL "
                        "WHERE id = ? AND status = 'triage'",
                        (new_status, task_id),
                    )
                    if cur.rowcount == 1:
                        kb._append_event(conn, task_id, "unblocked",
                                         {"status": new_status, "from": "triage"})
                        resumed = True
        updated = kb.get_task(conn, task_id)
        questions, counts, activity = _task_maps(conn, [task_id])
        return {
            "ok": True,
            "resumed": resumed,
            "task": _task_card(updated, questions.get(task_id),
                               counts.get(task_id, 0), activity.get(task_id))
            if updated else None,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# WS /events — DB tail of new messages (kanban-style poll loop)
# ---------------------------------------------------------------------------

_KIND_BY_ROLE = {"user": "me", "assistant": "agent", "tool": "tool", "system": "event"}


def _events_fetch_one(profile: str, cursor: int) -> tuple[int, list]:
    """Read messages with id > cursor from one profile's state.db (RO conn)."""
    try:
        path = _state_db_path(profile)
    except HTTPException:
        return cursor, []
    if not path.is_file():
        return cursor, []
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT m.id, m.session_id, m.role, m.content, m.tool_name, m.timestamp, "
            "s.source AS source "
            "FROM messages m LEFT JOIN sessions s ON s.id = m.session_id "
            "WHERE m.id > ? AND m.active = 1 ORDER BY m.id ASC LIMIT 200",
            (cursor,),
        ).fetchall()
        items = []
        new_cursor = cursor
        for r in rows:
            new_cursor = r["id"]
            role = r["role"] or ""
            items.append({
                "id": r["id"],
                "profile": profile,
                "session_id": r["session_id"],
                "source": r["source"] or "",
                "kind": _KIND_BY_ROLE.get(role, "event"),
                "text": _content_text(r["content"])[:2000],
                "tool_name": r["tool_name"],
                "ts": r["timestamp"],
            })
        return new_cursor, items
    finally:
        conn.close()


def _events_fetch(cursors: dict) -> tuple[dict, list]:
    """Tail every profile's messages table. Cursors: {profile: last_row_id}."""
    items: list = []
    out = dict(cursors)
    for profile, cur in cursors.items():
        try:
            out[profile], batch = _events_fetch_one(profile, cur)
        except sqlite3.Error as exc:
            log.warning("engram events tail (%s) failed: %s", profile, exc)
            continue
        items.extend(batch)
    items.sort(key=lambda m: m.get("ts") or 0)
    return out, items


# The messages cursor only surfaces row INSERTs, so a session's
# running -> ok/error flip (sessions.ended_at being stamped, often with no
# accompanying message row) never produces a frame on its own. Clients used
# to paper over this with HTTP polls; instead the events socket tracks each
# profile's unfinished-session set per tick and announces transitions as
# "session" frames.

def _session_lifecycle_snapshot(profile: str) -> Optional[dict]:
    """{session_id: source} of unfinished sessions, or None on read failure.

    None (rather than {}) matters: treating a transient failure as "no
    unfinished sessions" would fabricate an ended-transition for every live
    session, then a started-transition when the read recovers.
    """
    try:
        path = _state_db_path(profile)
    except HTTPException:
        return {}
    if not path.is_file():
        return {}
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error:
        return None
    try:
        rows = conn.execute(
            "SELECT id, source FROM sessions WHERE ended_at IS NULL"
        ).fetchall()
        return {r[0]: r[1] or "" for r in rows}
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def _session_ended_rows(profile: str, ids: list) -> list:
    if not ids:
        return []
    try:
        path = _state_db_path(profile)
    except HTTPException:
        return []
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error:
        return []
    conn.row_factory = sqlite3.Row
    try:
        marks = ",".join("?" * len(ids))
        return conn.execute(
            f"SELECT id, source, ended_at, end_reason FROM sessions WHERE id IN ({marks})",
            ids,
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def _session_transitions(profiles: list, prev: Optional[dict]) -> tuple[dict, list]:
    """Diff unfinished-session sets against the previous tick.

    Returns (state, items). The first call (prev=None) only establishes the
    baseline — currently-running sessions are not replayed as transitions.
    ``status`` mirrors the run vocabulary: "running" on start, "ok"/"error"
    on finish (from end_reason, like _run_dict).
    """
    state: dict = {}
    for p in profiles:
        snap = _session_lifecycle_snapshot(p)
        # Keep the previous view of a profile whose store couldn't be read.
        state[p] = snap if snap is not None else (prev or {}).get(p, {})
    if prev is None:
        return state, []
    items: list = []
    now = time.time()
    for p in profiles:
        before = prev.get(p, {})
        after = state.get(p, {})
        for sid, source in after.items():
            if sid not in before:
                items.append({
                    "profile": p, "session_id": sid, "source": source,
                    "status": "running", "ts": now,
                })
        ended_ids = [sid for sid in before if sid not in after]
        sources = {sid: before[sid] for sid in ended_ids}
        rows = {r["id"]: r for r in _session_ended_rows(p, ended_ids)}
        for sid in ended_ids:
            row = rows.get(sid)
            # A vanished row (deleted session) still ends the run for clients.
            end_reason = str(row["end_reason"] or "") if row else ""
            items.append({
                "profile": p, "session_id": sid,
                "source": (row["source"] if row else None) or sources.get(sid) or "",
                "status": "error" if "error" in end_reason.lower() else "ok",
                "ended_at": row["ended_at"] if row else None,
                "end_reason": end_reason or None,
                "ts": now,
            })
    return state, items


def _events_max_id(profile: str) -> int:
    try:
        path = _state_db_path(profile)
    except HTTPException:
        return 0
    if not path.is_file():
        return 0
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    try:
        row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM messages").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def _initial_cursors(profiles: list, since_raw: Optional[str]) -> dict:
    """Build {profile: cursor}. ``since`` is the JSON cursor object from a
    previous frame (opaque to clients); a bare int is tolerated and applied
    to the default profile only. Profiles missing from ``since`` start at
    their current tail (no replay)."""
    parsed: dict = {}
    if since_raw:
        try:
            decoded = json.loads(since_raw)
            if isinstance(decoded, dict):
                parsed = {str(k): int(v) for k, v in decoded.items()}
            elif isinstance(decoded, int):
                parsed = {"default": decoded}
        except (ValueError, TypeError):
            parsed = {}
    cursors: dict = {}
    for name in profiles:
        cursors[name] = parsed[name] if name in parsed else _events_max_id(name)
    return cursors


@router.websocket("/events")
async def events_ws(ws: WebSocket):
    if not _ws_upgrade_authorized(ws):
        await ws.close(code=http_status.WS_1008_POLICY_VIOLATION)
        return
    await ws.accept()
    try:
        profile = ws.query_params.get("profile") or "all"
        profiles = _profile_names() if profile == "all" else [profile]
        since_raw = ws.query_params.get("since")
        cursors = await asyncio.to_thread(_initial_cursors, profiles, since_raw)

        await ws.send_json({"type": "hello", "cursor": cursors})
        # Pulse = global agent activity light + keepalive in one frame.
        # Sent immediately after hello, then on every busy/idle flip, and at
        # least every _PULSE_INTERVAL seconds regardless — a client watchdog
        # that hasn't seen ANY frame for ~1.5x the interval should reconnect.
        # Deliberately global, not per-thread: per-thread state lives on
        # GET /threads, and new messages announce themselves here anyway.
        status: dict = {}
        last_pulse = 0.0
        lifecycle: Optional[dict] = None  # unfinished-session baseline per profile
        while True:
            busy = await asyncio.to_thread(_typing_set)
            now = {"running": bool(busy), "count": len(busy)}
            if now != status or time.time() - last_pulse >= _PULSE_INTERVAL:
                status = now
                last_pulse = time.time()
                await ws.send_json({"type": "pulse", **status, "ts": last_pulse})
            cursors, items = await asyncio.to_thread(_events_fetch, cursors)
            if items:
                await ws.send_json({"type": "messages", "cursor": cursors, "items": items})
            lifecycle, transitions = await asyncio.to_thread(
                _session_transitions, profiles, lifecycle
            )
            if transitions:
                await ws.send_json({"type": "session", "sessions": transitions,
                                    "ts": time.time()})
            await asyncio.sleep(_EVENT_POLL_SECONDS)
    except WebSocketDisconnect:
        return
    except asyncio.CancelledError:
        return
    except Exception as exc:  # defensive: never crash the dashboard worker
        log.warning("engram event stream error: %s", exc)
        try:
            await ws.close()
        except Exception:
            pass
