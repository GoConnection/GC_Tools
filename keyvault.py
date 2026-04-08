"""
Load application secrets from Azure Key Vault using DefaultAzureCredential.

Configure the vault via environment variable:
  AZURE_KEY_VAULT_URL — full URI, e.g. https://goconnection-keyvault.vault.azure.net/
  or
  AZURE_KEY_VAULT_NAME — vault name only (URL is built as https://{name}.vault.azure.net/)

Secrets are merged into Flask app.config. Env overrides after load (local dev):
GCTOOLS_BEARER_TOKEN, MSAL_CLIENT_ID, MSAL_CLIENT_SECRET, MSAL_TENANT_ID.
"""

from __future__ import annotations

import os
from typing import Any, Dict

from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceNotFoundError,
    ServiceRequestError,
)
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

SECRET_SQL_CONNECTION_STRING = "ConnectionStrings--GoConnectionClientes"
SECRET_BEARER_TOKEN = "GCTools--BearerToken"
SECRET_MSAL_CLIENT_ID = "GCTools--MsalClientId"
SECRET_MSAL_CLIENT_SECRET = "GCTools--MsalClientSecret"
SECRET_MSAL_TENANT_ID = "GCTools--MsalTenantId"

CONFIG_SQL_CONNECTION_STRING = "SQLSERVER_CONNECTION_STRING"
CONFIG_BEARER_TOKEN = "GCTOOLS_BEARER_TOKEN"
CONFIG_MSAL_CLIENT_ID = "MSAL_CLIENT_ID"
CONFIG_MSAL_CLIENT_SECRET = "MSAL_CLIENT_SECRET"
CONFIG_MSAL_TENANT_ID = "MSAL_TENANT_ID"


class KeyVaultConfigurationError(RuntimeError):
    """Raised when Key Vault cannot be reached or required secrets are missing."""


def _resolve_vault_url() -> str:
    url = (os.environ.get("AZURE_KEY_VAULT_URL") or "").strip().rstrip("/")
    if url:
        return url
    name = (os.environ.get("AZURE_KEY_VAULT_NAME") or "").strip()
    if name:
        return f"https://{name}.vault.azure.net"
    raise KeyVaultConfigurationError(
        "Azure Key Vault is not configured. Set AZURE_KEY_VAULT_URL "
        "(e.g. https://goconnection-keyvault.vault.azure.net/) or AZURE_KEY_VAULT_NAME "
        "(e.g. goconnection-keyvault)."
    )


def _require_non_empty_secret(name: str, value: str | None) -> str:
    if value is None or not str(value).strip():
        raise KeyVaultConfigurationError(
            f"Key Vault secret {name!r} is missing, empty, or disabled."
        )
    return str(value).strip()


def load_key_vault_config() -> Dict[str, Any]:
    """
    Fetch required secrets from Key Vault.

    Returns a dict suitable for Flask app.config.update().
    """
    vault_url = _resolve_vault_url()
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)

    try:
        sql_secret = client.get_secret(SECRET_SQL_CONNECTION_STRING)
        bearer_secret = client.get_secret(SECRET_BEARER_TOKEN)
        msal_cid = client.get_secret(SECRET_MSAL_CLIENT_ID)
        msal_sec = client.get_secret(SECRET_MSAL_CLIENT_SECRET)
        msal_ten = client.get_secret(SECRET_MSAL_TENANT_ID)
    except ResourceNotFoundError as e:
        raise KeyVaultConfigurationError(
            f"Key Vault secret not found. Ensure {SECRET_SQL_CONNECTION_STRING!r}, "
            f"{SECRET_BEARER_TOKEN!r}, {SECRET_MSAL_CLIENT_ID!r}, "
            f"{SECRET_MSAL_CLIENT_SECRET!r}, and {SECRET_MSAL_TENANT_ID!r} exist in vault "
            f"{vault_url}. Underlying error: {e}"
        ) from e
    except ClientAuthenticationError as e:
        raise KeyVaultConfigurationError(
            "Could not authenticate to Azure Key Vault with DefaultAzureCredential. "
            "For local development, sign in with Azure CLI (`az login`) or configure "
            "another credential chain. In Azure, grant this app’s managed identity "
            f"access to the vault. Underlying error: {e}"
        ) from e
    except HttpResponseError as e:
        raise KeyVaultConfigurationError(
            f"Key Vault request failed (HTTP {e.status_code}): {e.message}. Vault: {vault_url}"
        ) from e
    except ServiceRequestError as e:
        raise KeyVaultConfigurationError(
            f"Key Vault is unreachable (network error). Vault: {vault_url}. Underlying error: {e}"
        ) from e

    return {
        CONFIG_SQL_CONNECTION_STRING: _require_non_empty_secret(
            SECRET_SQL_CONNECTION_STRING, sql_secret.value
        ),
        CONFIG_BEARER_TOKEN: _require_non_empty_secret(
            SECRET_BEARER_TOKEN, bearer_secret.value
        ),
        CONFIG_MSAL_CLIENT_ID: _require_non_empty_secret(
            SECRET_MSAL_CLIENT_ID, msal_cid.value
        ),
        CONFIG_MSAL_CLIENT_SECRET: _require_non_empty_secret(
            SECRET_MSAL_CLIENT_SECRET, msal_sec.value
        ),
        CONFIG_MSAL_TENANT_ID: _require_non_empty_secret(
            SECRET_MSAL_TENANT_ID, msal_ten.value
        ),
    }


def apply_key_vault_secrets_to_app(app) -> None:
    """Load Key Vault secrets and merge into Flask app.config (fail fast on error)."""
    app.config.update(load_key_vault_config())
