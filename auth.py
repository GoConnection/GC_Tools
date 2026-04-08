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

from flask import Flask, Response, g, request, session

_CALC_PATHS = frozenset({"/", "/eletricidade", "/gas"})
_ADMIN_PATHS = frozenset({"/config_ele", "/config_gas", "/download_template"})
_AUTH_EXEMPT_PATHS = frozenset({"/login", "/logout", "/auth/callback"})


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

        path = request.path

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
