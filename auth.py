"""
Two-level access:

- Agent: ?token=… (or form field token) matching Key Vault GCTools--BearerToken.
  Only /, /eletricidade, /gas. Admin routes → 403. Admin UI hidden.

- Admin: Flask session after Microsoft MSAL sign-in and allow-list row in
  GoConnection.dbo.vw_GCTools_Admins. Full access.

- /login, /auth/callback, and /logout are unauthenticated entry points for the
  OAuth flow. Anything else without valid agent token or admin session → 403.
"""

from __future__ import annotations

import hmac
import os
import re
from threading import Lock
from typing import Any

from cachetools import TTLCache
from flask import Flask, Response, g, redirect, request, session
from db_config import DatabaseConfigError, get_allowed_agent_name

_APP_ENDESA = "endesacalc"
_APP_EDP = "edpsimulator"
_VALID_APPS = frozenset({_APP_ENDESA, _APP_EDP})
_CALC_PATHS = frozenset({"/", "/endesa-calculator", "/eletricidade", "/gas", "/edp-simulator", "/edp-simulator/calcular"})
_ADMIN_PATHS = frozenset({"/config_ele", "/config_gas", "/download_template"})
_AUTH_EXEMPT_PATHS = frozenset({"/login", "/logout", "/auth/callback"})
_ADMIN_SELECTOR_PATHS = frozenset({"/app-selector", "/select-app"})
_KNOWN_EXACT_PATHS = _CALC_PATHS | _ADMIN_PATHS | _AUTH_EXEMPT_PATHS
_HANDSHAKE_REGISTER_PATH = "/internal/register-handshake"
_KNOWN_EXACT_PATHS = _KNOWN_EXACT_PATHS | _ADMIN_SELECTOR_PATHS
_GUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
_PENDING_HANDSHAKES = TTLCache(maxsize=500, ttl=60)
_PENDING_HANDSHAKES_LOCK = Lock()
_AGENT_API_PREFIXES = ("/api/notes", "/api/chat")
_AGENT_API_EXACT_PATHS = frozenset({"/api/sniper/ele"})
_SHARED_APP_CONTEXT_PATHS = frozenset(
    {
        "/api/notes",
        "/api/chat",
        "/api/broadcast",
        "/api/admin/manage_note",
        "/admin/notas",
        "/admin/export_leads",
    }
)


def _normalize_app(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if raw in _VALID_APPS:
        return raw
    return None


def _scope_to_app(scope: str) -> str | None:
    s = str(scope or "").strip().lower()
    if s == "edp":
        return _APP_EDP
    if s == "endesa":
        return _APP_ENDESA
    return None


def _query_scope_app() -> str | None:
    from_qs = _scope_to_app(request.args.get("scope") or request.form.get("scope") or "")
    if from_qs:
        return from_qs
    if request.is_json:
        body = request.get_json(silent=True) or {}
        return _scope_to_app(body.get("scope", ""))
    return None


def _path_app(path: str) -> str | None:
    if path in {"/endesa-calculator", "/eletricidade", "/gas", "/config_ele", "/config_gas", "/download_template", "/api/sniper/ele"}:
        return _APP_ENDESA
    if path == "/edp-simulator" or path.startswith("/edp-simulator/") or path == "/admin/edp-simulator" or path.startswith("/admin/edp-simulator/"):
        return _APP_EDP
    if (
        path in _SHARED_APP_CONTEXT_PATHS
        or path.startswith("/api/notes/")
        or path.startswith("/api/chat/")
    ):
        return _query_scope_app()
    return None


def _app_home_path(app_key: str) -> str:
    return "/edp-simulator" if app_key == _APP_EDP else "/endesa-calculator"


def _admin_app_permissions() -> dict[str, bool]:
    return {
        _APP_ENDESA: bool(session.get("admin_app_endesa", False)),
        _APP_EDP: bool(session.get("admin_app_edp", False)),
    }


def _effective_route_path() -> str:
    """
    Path as seen by Flask route rules.

    Under IIS with a virtual directory (e.g. /GC_Tools), some setups pass PATH_INFO
    including that prefix (/GC_Tools/login). Exempt paths and calculators are
    registered as /login, /, etc., so we strip GCTOOLS_PATH_PREFIX when set
    (e.g. /GC_Tools) to match.
    """
    path = request.path or "/"
    configured_prefix = (os.environ.get("GCTOOLS_PATH_PREFIX") or "").strip().rstrip("/")
    script_root = (request.script_root or "").strip().rstrip("/")

    # Prefer explicit config; also support IIS-provided script_root automatically.
    for prefix in (configured_prefix, script_root):
        if not prefix:
            continue
        if path == prefix or path == prefix + "/":
            path = "/"
            break
        if path.startswith(prefix + "/"):
            path = path[len(prefix) :] or "/"
            break

    # Normalize trailing slashes to avoid false negatives (/gas/ vs /gas).
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/") or "/"

    # IIS virtual-directory fallback: if PATH_INFO still includes one leading
    # segment (e.g. /GC_Tools/gas), reduce to /gas when it matches known routes.
    parts = path.split("/", 2)
    if len(parts) >= 3 and parts[0] == "":
        candidate = "/" + parts[2] if parts[2] else "/"
        if candidate in _KNOWN_EXACT_PATHS:
            return candidate
    if len(parts) == 2 and parts[0] == "" and parts[1]:
        # Handle "<prefix>" with no trailing slash as app root, but only when
        # it matches the configured prefix or current script_root.
        seg = "/" + parts[1]
        if configured_prefix and seg == configured_prefix:
            return "/"
        if script_root and seg == script_root:
            return "/"

    return path


def _agent_token_config(app: Flask) -> str | None:
    t = app.config.get("GCTOOLS_BEARER_TOKEN")
    return str(t).strip() if t else None


def _token_match(expected: str, provided: str) -> bool:
    if len(expected) != len(provided):
        return False
    return hmac.compare_digest(
        expected.encode("utf-8"), provided.encode("utf-8")
    )


def _query_token() -> str:
    return (request.args.get("token") or request.form.get("token") or "").strip()


def _bearer_token() -> str:
    authz = (request.headers.get("Authorization") or "").strip()
    if not authz:
        return ""
    scheme, _, token = authz.partition(" ")
    if scheme.lower() != "bearer":
        return ""
    return token.strip()


def _query_handshake_token() -> str:
    return (request.args.get("ht") or "").strip()


def _consume_handshake(token: str) -> dict[str, Any] | None:
    with _PENDING_HANDSHAKES_LOCK:
        payload = _PENDING_HANDSHAKES.pop(token, None)
    if isinstance(payload, dict):
        agent_id = payload.get("agent_id")
        if isinstance(agent_id, int):
            return payload
    return None


def _store_handshake(token: str, agent_id: int, agent_name: str, app_key: str) -> None:
    with _PENDING_HANDSHAKES_LOCK:
        _PENDING_HANDSHAKES[token] = {"agent_id": agent_id, "agent_name": agent_name, "app": app_key}


def _is_agent_allowed_path(path: str) -> bool:
    if path in _CALC_PATHS:
        return True
    if path in _AGENT_API_EXACT_PATHS:
        return True
    return path.startswith(_AGENT_API_PREFIXES)


def register_access_control(app: Flask) -> None:
    @app.post(_HANDSHAKE_REGISTER_PATH)
    def register_handshake() -> Response:
        data = request.get_json(silent=True) or {}
        token = str(data.get("token", "")).strip()
        raw_agent_id = data.get("agentId")
        app_key = _normalize_app(data.get("app"))
        if not token or not _GUID_RE.fullmatch(token):
            return Response(
                "Bad request. JSON body must include a valid GUID token.",
                status=400,
                mimetype="text/plain",
            )
        if isinstance(raw_agent_id, bool) or not isinstance(raw_agent_id, int):
            return Response(
                "Bad request. JSON body must include integer agentId.",
                status=400,
                mimetype="text/plain",
            )
        if not app_key:
            return Response(
                "Bad request. JSON body must include app (endesacalc|edpsimulator).",
                status=400,
                mimetype="text/plain",
            )
        agent_id = int(raw_agent_id)
        try:
            agent_name = get_allowed_agent_name(agent_id)
        except DatabaseConfigError:
            return Response(
                "Service unavailable while validating agentId.",
                status=503,
                mimetype="text/plain",
            )
        if agent_name is None:
            return Response(
                "Forbidden. agentId is not allowed.",
                status=403,
                mimetype="text/plain",
            )
        _store_handshake(token, agent_id, agent_name, app_key)
        return Response(status=200)

    @app.before_request
    def _access() -> Response | None:
        if request.method == "OPTIONS":
            return None

        path = _effective_route_path()
        expected = _agent_token_config(app)

        if path.startswith("/static/"):
            return None
        if path == "/favicon.ico":
            return None

        if path == _HANDSHAKE_REGISTER_PATH:
            if request.method != "POST":
                return Response("Method not allowed.", status=405, mimetype="text/plain")
            if not expected:
                return Response(
                    "Server misconfiguration: GCTOOLS_BEARER_TOKEN is not set.",
                    status=503,
                    mimetype="text/plain",
                )
            got_bearer = _bearer_token()
            if not got_bearer or not _token_match(expected, got_bearer):
                return Response(
                    "Unauthorized. Provide a valid static Bearer token.",
                    status=401,
                    mimetype="text/plain",
                )
            return None

        if path in _AUTH_EXEMPT_PATHS:
            return None

        if session.get("admin_logged_in"):
            requested_app = _path_app(path)
            perms = _admin_app_permissions()
            if path in _ADMIN_SELECTOR_PATHS:
                g.gctools_role = "admin"
                return None
            if requested_app and not perms.get(requested_app, False):
                return Response("Forbidden: no permission for this app.", status=403, mimetype="text/plain")
            if path in _SHARED_APP_CONTEXT_PATHS and requested_app is None:
                selected = _normalize_app(session.get("selected_app"))
                if not selected or not perms.get(selected, False):
                    return Response("Forbidden: select an app first.", status=403, mimetype="text/plain")
            g.gctools_role = "admin"
            return None

        ht = _query_handshake_token()
        if ht:
            handshake_payload = _consume_handshake(ht)
            if not handshake_payload:
                return Response(
                    "Unauthorized. Handshake token is invalid or expired.",
                    status=401,
                    mimetype="text/plain",
                )
            session["authenticated"] = True
            session["agent_id"] = int(handshake_payload["agent_id"])
            session["agent_name"] = str(handshake_payload.get("agent_name") or "")
            agent_app = _normalize_app(handshake_payload.get("app")) or _APP_ENDESA
            session["agent_app"] = agent_app
            root = (request.script_root or "").rstrip("/")
            return redirect(root + _app_home_path(agent_app))

        if session.get("authenticated"):
            g.gctools_role = "agent"
            requested_app = _path_app(path)
            session_app = _normalize_app(session.get("agent_app")) or _APP_ENDESA
            if (
                requested_app is None
                and (path in _SHARED_APP_CONTEXT_PATHS or path.startswith("/api/notes/") or path.startswith("/api/chat/"))
            ):
                requested_app = session_app
            if requested_app and requested_app != session_app:
                return Response(
                    "Forbidden: agent session does not allow this app.",
                    status=403,
                    mimetype="text/plain",
                )
            if not _is_agent_allowed_path(path):
                return Response(
                    "Forbidden: agent session does not allow this route.",
                    status=403,
                    mimetype="text/plain",
                )
            return None

        if not expected:
            return Response(
                "Server misconfiguration: GCTOOLS_BEARER_TOKEN is not set.",
                status=503,
                mimetype="text/plain",
            )

        got = _query_token()
        if not got or not _token_match(expected, got):
            return Response(
                "Forbidden. Provide a valid ?token= on calculator URLs or sign in at /login.",
                status=403,
                mimetype="text/plain",
            )

        g.gctools_role = "agent"
        requested_app = _path_app(path)
        request_app = _normalize_app(request.args.get("app") or request.form.get("app"))
        if requested_app and request_app and requested_app != request_app:
            return Response(
                "Forbidden: token app does not match route.",
                status=403,
                mimetype="text/plain",
            )
        if not _is_agent_allowed_path(path):
            return Response(
                "Forbidden: agent token does not allow this route.",
                status=403,
                mimetype="text/plain",
            )
        return None

    @app.context_processor
    def _inject():
        role = getattr(g, "gctools_role", None)
        is_admin = bool(session.get("admin_logged_in"))
        agent_tok = ""
        if role == "agent":
            agent_tok = _query_token()
        active_app = _normalize_app(session.get("selected_app")) if is_admin else _normalize_app(session.get("agent_app"))
        perms = _admin_app_permissions() if is_admin else {_APP_ENDESA: False, _APP_EDP: False}
        return {
            "is_admin": is_admin,
            "agent_token": agent_tok,
            "operator_id": str(session.get("agent_id") or ""),
            "operator_name": str(session.get("agent_name") or ""),
            "active_app": active_app or "",
            "admin_can_endesa": bool(perms.get(_APP_ENDESA, False)),
            "admin_can_edp": bool(perms.get(_APP_EDP, False)),
        }
