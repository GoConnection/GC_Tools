"""
Microsoft Entra ID (MSAL) authorization code flow for GC Tools admin sign-in.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import msal
import pyodbc
from flask import Flask, url_for

from db_config import open_sql_connection

# MSAL injects reserved OIDC scopes (openid, profile, …) itself; do not pass them.
_SCOPES = ["email"]

# View: GoConnection.dbo.vw_GCTools_Admins — column holding Entra sign-in email
_DEFAULT_EMAIL_COL = "Email"
_ADMIN_VIEW = "GoConnection.dbo.vw_GCTools_Admins"
_COL_ENDESA_APP = "EndesaB2B_Calc_App"
_COL_EDP_APP = "EDP_Simulator_App"


def _authority(tenant_id: str) -> str:
    return f"https://login.microsoftonline.com/{tenant_id}"


def build_msal_app(flask_app: Flask) -> msal.ConfidentialClientApplication:
    return msal.ConfidentialClientApplication(
        flask_app.config["MSAL_CLIENT_ID"],
        authority=_authority(flask_app.config["MSAL_TENANT_ID"]),
        client_credential=flask_app.config["MSAL_CLIENT_SECRET"],
    )


def get_authorization_url(
    flask_app: Flask, redirect_uri: str, state: str
) -> str:
    app = build_msal_app(flask_app)
    return app.get_authorization_request_url(
        _SCOPES,
        state=state,
        redirect_uri=redirect_uri,
        prompt="select_account",
    )


def acquire_token_by_auth_code(
    flask_app: Flask, code: str, redirect_uri: str
) -> Dict[str, Any]:
    app = build_msal_app(flask_app)
    return app.acquire_token_by_authorization_code(
        code,
        scopes=_SCOPES,
        redirect_uri=redirect_uri,
    )


def email_from_id_token_claims(result: Dict[str, Any]) -> Optional[str]:
    if not result or "error" in result:
        return None
    claims = result.get("id_token_claims") or {}
    for key in ("email", "preferred_username", "upn"):
        v = claims.get(key)
        if v and str(v).strip():
            return str(v).strip().lower()
    return None


def _email_column_sql() -> str:
    col = (os.environ.get("GC_TOOLS_ADMIN_EMAIL_COLUMN") or _DEFAULT_EMAIL_COL).strip()
    if not col.replace("_", "").isalnum():
        return _DEFAULT_EMAIL_COL
    return col


def is_email_in_gctools_admins(email: str) -> bool:
    """True if normalized email exists in vw_GCTools_Admins."""
    em = (email or "").strip().lower()
    if not em or "@" not in em:
        return False

    col = _email_column_sql()
    sql = (
        f"SELECT 1 FROM {_ADMIN_VIEW} "
        f"WHERE LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(512))))) = ?"
    )

    try:
        conn = open_sql_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql, (em,))
            return cur.fetchone() is not None
        finally:
            conn.close()
    except pyodbc.Error:
        return False


def get_admin_app_permissions(email: str) -> Optional[Dict[str, bool]]:
    """Return app permissions for an admin email from vw_GCTools_Admins."""
    em = (email or "").strip().lower()
    if not em or "@" not in em:
        return None

    col = _email_column_sql()
    sql = (
        f"SELECT TOP 1 [{_COL_ENDESA_APP}], [{_COL_EDP_APP}] "
        f"FROM {_ADMIN_VIEW} "
        f"WHERE LOWER(LTRIM(RTRIM(CAST([{col}] AS NVARCHAR(512))))) = ?"
    )

    try:
        conn = open_sql_connection()
        try:
            cur = conn.cursor()
            cur.execute(sql, (em,))
            row = cur.fetchone()
            if not row:
                return None
            endesa = bool(int(row[0])) if row[0] is not None else False
            edp = bool(int(row[1])) if row[1] is not None else False
            return {"endesacalc": endesa, "edpsimulator": edp}
        finally:
            conn.close()
    except (pyodbc.Error, ValueError, TypeError):
        return None


def get_msal_redirect_uri() -> str:
    """Must run inside a Flask request context unless GCTOOLS_MSAL_REDIRECT_URI is set."""
    override = (os.environ.get("GCTOOLS_MSAL_REDIRECT_URI") or "").strip()
    if override:
        return override.rstrip("/")
    return url_for("auth_callback", _external=True)
