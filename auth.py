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

from flask import Flask, Response, g, request, session

_CALC_PATHS = frozenset({"/", "/eletricidade", "/gas"})
_ADMIN_PATHS = frozenset({"/config_ele", "/config_gas", "/download_template"})
_AUTH_EXEMPT_PATHS = frozenset({"/login", "/logout", "/auth/callback"})
_KNOWN_EXACT_PATHS = _CALC_PATHS | _ADMIN_PATHS | _AUTH_EXEMPT_PATHS


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


def register_access_control(app: Flask) -> None:
    @app.before_request
    def _access() -> Response | None:
        if request.method == "OPTIONS":
            return None

        path = _effective_route_path()

        if path.startswith("/static/"):
            return None
        if path == "/favicon.ico":
            return None

        if path in _AUTH_EXEMPT_PATHS:
            return None

        if session.get("admin_logged_in"):
            g.gctools_role = "admin"
            return None

        expected = _agent_token_config(app)
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
        if path not in _CALC_PATHS:
            return Response(
                "Forbidden: agent token only allows access to calculators.",
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
        return {
            "is_admin": is_admin,
            "agent_token": agent_tok,
        }
