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
    return {
        "id": sid,
        "profile": profile,
        "status": status,
        "topic": row.get("title") or None,
        "preview": row.get("preview") or None,
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
        "scopes": ["chat", "threads", "routines", "profiles", "models", "feedback"],
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

def _list_threads_for(profile: str, fetch: int, include_archived: bool,
                      status: str, running_map: dict) -> list:
    try:
        db = _db(profile)
    except HTTPException:
        raise
    except Exception as exc:
        log.warning("engram: opening %s state.db failed: %s", profile, exc)
        return []
    try:
        rows = db.list_sessions_rich(
            limit=fetch,
            order_by_last_active=True,
            include_archived=include_archived,
        )
    except TypeError:
        rows = db.list_sessions_rich(limit=fetch)
    if status == "resolved":
        rows = [r for r in rows if r.get("archived")]
    return [_thread_dict(r, running_map, profile) for r in rows]


@router.get("/threads")
def list_threads(
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
    status: str = Query("open", pattern="^(open|resolved|all)$"),
    profile: str = Query("all", description='Profile name, or "all" to aggregate'),
):
    include_archived = status in ("resolved", "all")
    running_map = _running_map()
    fetch = limit + offset
    profiles = _profile_names() if profile == "all" else [profile]
    if profile != "all":
        _profile_home_or_404(profile)  # 404 early on unknown profile

    threads: list = []
    total = 0
    total_known = True
    for name in profiles:
        threads.extend(_list_threads_for(name, fetch, include_archived, status, running_map))
        try:
            total += _db(name).session_count(include_archived=include_archived)
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
    return {
        "id": job.get("id"),
        "profile": profile,
        "name": job.get("name"),
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


def _run_dict(row: dict) -> dict:
    ended = row.get("ended_at")
    end_reason = str(row.get("end_reason") or "")
    status = "running" if not ended else ("error" if "error" in end_reason.lower() else "ok")
    return {
        "session_id": row.get("id"),
        "started_at": row.get("started_at"),
        "ended_at": ended,
        "status": status,
        "preview": row.get("preview") or None,
        "message_count": row.get("message_count") or 0,
    }


@router.get("/routines")
def list_routines(profile: str = Query("all", description='Profile name, or "all"')):
    profiles = _profile_names() if profile == "all" else [profile]
    if profile != "all":
        _profile_home_or_404(profile)
    routines: list = []
    for name in profiles:
        routines.extend(_routine_dict(j, name) for j in _profile_jobs(name))
    return {"routines": routines, "profile": profile}


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
    pname, job = _find_job(job_id, profile)
    runs: list = []
    try:
        runs = [_run_dict(r) for r in _db(pname).list_cron_job_runs(job["id"], limit=10)]
    except Exception as exc:
        log.warning("engram: list_cron_job_runs failed: %s", exc)
    return {"routine": _routine_dict(job, pname), "runs": runs}


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
    pname, job = _find_job(job_id, profile)
    rows = _db(pname).list_cron_job_runs(job["id"], limit=limit, offset=offset)
    return {"runs": [_run_dict(r) for r in rows], "profile": pname}


@router.get("/runs/{session_id}")
def get_run(
    session_id: str,
    limit: Optional[int] = Query(None, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    profile: str = Query("default"),
):
    return _session_transcript(session_id, limit, offset, None, profile)


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
            "SELECT id, session_id, role, content, tool_name, timestamp "
            "FROM messages WHERE id > ? AND active = 1 ORDER BY id ASC LIMIT 200",
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

        busy: frozenset = await asyncio.to_thread(_typing_set)
        status = {"running": bool(busy), "count": len(busy)}
        await ws.send_json({"type": "hello", "cursor": cursors, "agent": status})
        while True:
            cursors, items = await asyncio.to_thread(_events_fetch, cursors)
            if items:
                await ws.send_json({"type": "messages", "cursor": cursors, "items": items})
            # Global agent activity light — fires only when the busy/idle
            # state (or concurrent-turn count) changes. Deliberately NOT
            # per-thread: per-thread state lives on GET /threads (status/
            # running), and new messages announce themselves on this socket
            # anyway. One turn = two frames (start, finish).
            busy = await asyncio.to_thread(_typing_set)
            now = {"running": bool(busy), "count": len(busy)}
            if now != status:
                status = now
                await ws.send_json({"type": "agent_status", **status})
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
