"""
Load and persist tariff configuration from SQL Server (schema endesa by default).

Uses Flask current_app.config['SQLSERVER_CONNECTION_STRING'].
Schema overridable via SQL_SERVER_SCHEMA (default: endesa).

pyodbc requires an ODBC-style connection string with a Driver= clause. Secrets from
.NET / SqlClient often omit Driver=; we prepend ODBC Driver 18 if missing.

SqlClient uses different keywords and values than ODBC (e.g. Data Source vs Server,
TrustServerCertificate=True vs yes). We translate in memory only so the Key Vault
secret stays unchanged for other apps. Install the driver on the host:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
Override the driver name with env SQLSERVER_ODBC_DRIVER if you use 17 or another edition.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Tuple

import pyodbc
from flask import current_app


def _schema() -> str:
    return (os.environ.get("SQL_SERVER_SCHEMA") or "endesa").strip()


def _q(table: str) -> str:
    return f"[{_schema()}].[{table}]"


# SqlClient-only keys ODBC Driver 18 rejects as "Invalid connection string attribute"
_ODBC_DROP_KEYS = frozenset(
    {
        "multipleactiveresultsets",
        "persist security info",
        "pooling",
        "min pool size",
        "max pool size",
        "load balance timeout",
        "replication",
        "attachdbfilename",
        "context connection",
        "transaction binding",
        "enlist",
        "user instance",
        "connectretrycount",
        "connectretryinterval",
        "column encryption setting",
    }
)


def _parse_connection_pairs(conn_str: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for raw_seg in conn_str.split(";"):
        seg = raw_seg.strip()
        if not seg or "=" not in seg:
            continue
        key, _, val = seg.partition("=")
        pairs.append((key.strip(), val.strip()))
    return pairs


def _odbc_key_alias(kl: str) -> str:
    """Map ADO.NET / SqlClient key names to ODBC Driver keywords (lowercase internal)."""
    if kl == "data source":
        return "server"
    if kl == "initial catalog":
        return "database"
    if kl in ("user id", "userid", "user"):
        return "uid"
    if kl == "password":
        return "pwd"
    if kl in ("connect timeout", "connection timeout"):
        return "logintimeout"
    if kl == "integrated security":
        return "trusted_connection"
    if kl == "application name":
        return "app"
    return kl


def _normalize_auth_value(v: str) -> str:
    """e.g. 'Active Directory Default' -> ActiveDirectoryDefault"""
    s = v.strip()
    if re.match(r"(?i)^active\s+directory\s+", s):
        return re.sub(r"\s+", "", s)
    return s


def _bool_to_yes_no(v: str) -> str | None:
    vl = v.strip().lower()
    if vl in ("true", "1", "yes", "sspi"):
        return "yes"
    if vl in ("false", "0", "no"):
        return "no"
    return None


def _odbc_compat_from_dotnet(conn_str: str) -> str:
    """
    Map SqlClient connection strings to ODBC Driver 17/18 (keywords + values).
    Does not alter the secret in Key Vault — only the string used by pyodbc.
    """
    _canon = {
        "server": "Server",
        "database": "Database",
        "uid": "UID",
        "pwd": "PWD",
        "encrypt": "Encrypt",
        "trustservercertificate": "TrustServerCertificate",
        "authentication": "Authentication",
        "trusted_connection": "Trusted_Connection",
        "logintimeout": "LoginTimeout",
        "driver": "Driver",
        "dsn": "DSN",
        "app": "App",
    }
    _known = frozenset(_canon.keys())

    out: List[str] = []
    for k, v in _parse_connection_pairs(conn_str):
        kl = k.lower()
        if kl in _ODBC_DROP_KEYS:
            continue
        odbc_l = _odbc_key_alias(kl)

        if odbc_l == "trusted_connection":
            yn = _bool_to_yes_no(v)
            if yn is not None:
                v = yn
        elif odbc_l == "authentication":
            v = _normalize_auth_value(v)
        elif odbc_l == "trustservercertificate":
            yn = _bool_to_yes_no(v)
            if yn is not None:
                v = yn
        elif odbc_l == "encrypt":
            vl = v.lower()
            if vl in ("true", "1"):
                v = "yes"
            elif vl in ("false", "0"):
                v = "no"
            elif vl in ("mandatory", "optional", "strict"):
                v = vl

        if odbc_l in _known:
            canon = _canon[odbc_l]
        else:
            canon = k

        out.append(f"{canon}={v}")
    return ";".join(out)


def _normalize_pyodbc_connection_string(conn_str: str) -> str:
    """Ensure Driver= or DSN= is present; adapt .NET keywords for ODBC."""
    s = (conn_str or "").strip()
    if not s:
        return s
    s = _odbc_compat_from_dotnet(s)
    if re.search(r"(?i)\b(Driver|DSN)\s*=", s):
        return s
    driver = (os.environ.get("SQLSERVER_ODBC_DRIVER") or "ODBC Driver 18 for SQL Server").strip()
    return f"Driver={{{driver}}};{s}"


def _conn() -> pyodbc.Connection:
    raw = current_app.config["SQLSERVER_CONNECTION_STRING"]
    return pyodbc.connect(_normalize_pyodbc_connection_string(raw))


def open_sql_connection() -> pyodbc.Connection:
    """ODBC connection using the same Key Vault string as tariff config (Flask app context)."""
    return _conn()


def _float(v: Any) -> float:
    if v is None:
        return 0.0
    return float(v)


class DatabaseConfigError(RuntimeError):
    """Raised when the database cannot be read or written."""


def fetch_full_config() -> Dict[str, Any]:
    """Return the top-level dict shape expected by the Flask app (GN_CONFIG + EE_CONFIG)."""
    try:
        with _conn() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(
                f"""
                SELECT DisplayName, PriceB1, PriceB2, SortOrder
                FROM {_q("GasFidelizationOption")}
                WHERE FidelityCode = ?
                ORDER BY SortOrder
                """,
                ("12M",),
            )
            fixo_12m = [
                {"nome": r[0], "b1": _float(r[1]), "b2": _float(r[2])}
                for r in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT DisplayName, PriceB1, PriceB2, SortOrder
                FROM {_q("GasFidelizationOption")}
                WHERE FidelityCode = ?
                ORDER BY SortOrder
                """,
                ("24M",),
            )
            fixo_24m = [
                {"nome": r[0], "b1": _float(r[1]), "b2": _float(r[2])}
                for r in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT Escalao, TermoFixoDiario, EnergiaUnit
                FROM {_q("GasEscalaoTariff")}
                ORDER BY Escalao
                """
            )
            tar: Dict[str, Dict[str, float]] = {}
            for r in cur.fetchall():
                tar[str(int(r[0]))] = {"fixo": _float(r[1]), "en": _float(r[2])}

            gn_config = {"fixo_12m": fixo_12m, "fixo_24m": fixo_24m, "tar": tar}

            cur.execute(
                f"""
                SELECT KvaLabel, UnitPrice
                FROM {_q("ElecBtnPowerPrice")}
                ORDER BY SortOrder
                """
            )
            potencias = {str(r[0]).strip(): _float(r[1]) for r in cur.fetchall()}

            cur.execute(
                f"SELECT PotPonta, PotContratada FROM {_q('ElecBtePowerTerm')} WHERE Id = 1"
            )
            bte_row = cur.fetchone()
            if not bte_row:
                raise DatabaseConfigError(
                    "Missing row in ElecBtePowerTerm with Id = 1."
                )
            pot_ponta, pot_contratada = _float(bte_row[0]), _float(bte_row[1])

            cur.execute(
                f"""
                SELECT Segment, EnergyType, TariffProfile, P1, P2, P3, P4
                FROM {_q("ElecTariffMatrix")}
                """
            )
            tar_rows: Dict[Tuple[str, str, str], Dict[str, float]] = {}
            for r in cur.fetchall():
                seg, ee, prof = str(r[0]).strip(), str(r[1]).strip(), str(r[2]).strip()
                tar_rows[(seg, ee, prof)] = {
                    "p1": _float(r[3]),
                    "p2": _float(r[4]),
                    "p3": _float(r[5]),
                    "p4": _float(r[6]),
                }

            def ee_block(seg: str, ee: str) -> Dict[str, Any]:
                produtos: List[Dict[str, Any]] = []
                cur.execute(
                    f"""
                    SELECT SortOrder, ProductName, TariffProfile, P1, P2, P3, P4
                    FROM {_q("ElecProduct")}
                    WHERE Segment = ? AND EnergyType = ?
                    ORDER BY SortOrder
                    """,
                    (seg, ee),
                )
                for r in cur.fetchall():
                    produtos.append(
                        {
                            "ordem": int(r[0]),
                            "nome": r[1],
                            "tipo": str(r[2]).strip(),
                            "p1": _float(r[3]),
                            "p2": _float(r[4]),
                            "p3": _float(r[5]),
                            "p4": _float(r[6]),
                        }
                    )
                tmap: Dict[str, Dict[str, float]] = {}
                for prof in (
                    ["SIM", "BIH", "TRI"]
                    if seg == "BTN"
                    else ["TETRA"]
                ):
                    key = (seg, ee, prof)
                    if key in tar_rows:
                        tmap[prof] = dict(tar_rows[key])
                    else:
                        tmap[prof] = {"p1": 0.0, "p2": 0.0, "p3": 0.0, "p4": 0.0}
                return {"produtos": produtos, "tar": tmap}

            ee_config = {
                "BTN": {
                    "potencias": potencias,
                    "EN": ee_block("BTN", "EN"),
                    "EV": ee_block("BTN", "EV"),
                },
                "BTE": {
                    "pot_ponta": pot_ponta,
                    "pot_contratada": pot_contratada,
                    "EN": ee_block("BTE", "EN"),
                    "EV": ee_block("BTE", "EV"),
                },
            }

            return {"GN_CONFIG": gn_config, "EE_CONFIG": ee_config}
    except DatabaseConfigError:
        raise
    except pyodbc.Error as e:
        raise DatabaseConfigError(f"Database error while loading config: {e}") from e


def save_gas_config(gn: Dict[str, Any]) -> None:
    try:
        with _conn() as conn:
            cur = conn.cursor()

            cur.execute(
                f"""
                SELECT Id FROM {_q("GasFidelizationOption")}
                WHERE FidelityCode = ? ORDER BY SortOrder
                """,
                ("12M",),
            )
            ids_12 = [r[0] for r in cur.fetchall()]
            for i, p in enumerate(gn.get("fixo_12m", [])):
                if i >= len(ids_12):
                    break
                cur.execute(
                    f"""
                    UPDATE {_q("GasFidelizationOption")}
                    SET PriceB1 = ?, PriceB2 = ?
                    WHERE Id = ?
                    """,
                    (_float(p.get("b1")), _float(p.get("b2")), ids_12[i]),
                )

            cur.execute(
                f"""
                SELECT Id FROM {_q("GasFidelizationOption")}
                WHERE FidelityCode = ? ORDER BY SortOrder
                """,
                ("24M",),
            )
            ids_24 = [r[0] for r in cur.fetchall()]
            f24 = gn.get("fixo_24m") or []
            if f24 and ids_24:
                p0 = f24[0]
                cur.execute(
                    f"""
                    UPDATE {_q("GasFidelizationOption")}
                    SET PriceB1 = ?, PriceB2 = ?
                    WHERE Id = ?
                    """,
                    (_float(p0.get("b1")), _float(p0.get("b2")), ids_24[0]),
                )

            for k, v in gn.get("tar", {}).items():
                cur.execute(
                    f"""
                    UPDATE {_q("GasEscalaoTariff")}
                    SET TermoFixoDiario = ?, EnergiaUnit = ?
                    WHERE Escalao = ?
                    """,
                    (_float(v.get("fixo")), _float(v.get("en")), int(k)),
                )

            conn.commit()
    except pyodbc.Error as e:
        raise DatabaseConfigError(f"Database error while saving gas config: {e}") from e


def clear_all_electricity_products(cur: pyodbc.Cursor) -> None:
    cur.execute(f"DELETE FROM {_q('ElecProduct')}")


def insert_product_row(
    cur: pyodbc.Cursor,
    segment: str,
    energy_type: str,
    sort_order: int,
    name: str,
    profile: str,
    p1: float,
    p2: float,
    p3: float,
    p4: float,
) -> None:
    cur.execute(
        f"""
        INSERT INTO {_q("ElecProduct")}
        (Segment, EnergyType, SortOrder, ProductName, TariffProfile, P1, P2, P3, P4)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (segment, energy_type, sort_order, name, profile, p1, p2, p3, p4),
    )


def save_electricity_config(ee: Dict[str, Any]) -> None:
    """Update power, TAR matrix, and full product catalog from EE_CONFIG dict."""
    try:
        with _conn() as conn:
            cur = conn.cursor()

            for k, v in ee["BTN"]["potencias"].items():
                cur.execute(
                    f"""
                    UPDATE {_q("ElecBtnPowerPrice")}
                    SET UnitPrice = ?
                    WHERE KvaLabel = ?
                    """,
                    (_float(v), str(k).strip()),
                )

            cur.execute(
                f"""
                UPDATE {_q("ElecBtePowerTerm")}
                SET PotPonta = ?, PotContratada = ?
                WHERE Id = 1
                """,
                (_float(ee["BTE"]["pot_ponta"]), _float(ee["BTE"]["pot_contratada"])),
            )

            def upsert_tar(seg: str, energy: str, prof: str, d: Dict[str, float]) -> None:
                cur.execute(
                    f"""
                    UPDATE {_q("ElecTariffMatrix")}
                    SET P1 = ?, P2 = ?, P3 = ?, P4 = ?
                    WHERE Segment = ? AND EnergyType = ? AND TariffProfile = ?
                    """,
                    (
                        _float(d.get("p1")),
                        _float(d.get("p2")),
                        _float(d.get("p3")),
                        _float(d.get("p4")),
                        seg,
                        energy,
                        prof,
                    ),
                )

            ten = ee["BTN"]["EN"]["tar"]
            for prof in ("SIM", "BIH", "TRI"):
                upsert_tar("BTN", "EN", prof, ten[prof])
            tev = ee["BTN"]["EV"]["tar"]
            for prof in ("SIM", "BIH", "TRI"):
                upsert_tar("BTN", "EV", prof, tev[prof])
            ben = ee["BTE"]["EN"]["tar"]
            upsert_tar("BTE", "EN", "TETRA", ben["TETRA"])
            bev = ee["BTE"]["EV"]["tar"]
            upsert_tar("BTE", "EV", "TETRA", bev["TETRA"])

            clear_all_electricity_products(cur)
            for seg in ("BTN", "BTE"):
                for energy in ("EN", "EV"):
                    for p in ee[seg][energy]["produtos"]:
                        insert_product_row(
                            cur,
                            seg,
                            energy,
                            int(p["ordem"]),
                            str(p["nome"]),
                            str(p["tipo"]),
                            _float(p.get("p1")),
                            _float(p.get("p2")),
                            _float(p.get("p3")),
                            _float(p.get("p4")),
                        )

            conn.commit()
    except pyodbc.Error as e:
        raise DatabaseConfigError(
            f"Database error while saving electricity config: {e}"
        ) from e


def persist_imported_products(
    products_by_segment_ee: List[Tuple[str, str, Dict[str, Any]]],
) -> None:
    """
    products_by_segment_ee: list of (segment, energy_type, product_dict)
    Clears all products then inserts the given list in order.
    """
    try:
        with _conn() as conn:
            cur = conn.cursor()
            clear_all_electricity_products(cur)
            for seg, energy, p in products_by_segment_ee:
                insert_product_row(
                    cur,
                    seg,
                    energy,
                    int(p["ordem"]),
                    str(p["nome"]),
                    str(p["tipo"]),
                    _float(p.get("p1")),
                    _float(p.get("p2")),
                    _float(p.get("p3")),
                    _float(p.get("p4")),
                )
            conn.commit()
    except pyodbc.Error as e:
        raise DatabaseConfigError(
            f"Database error while importing electricity products: {e}"
        ) from e


def clear_electricity_products_only() -> None:
    """Delete all rows from ElecProduct (TARs and power settings unchanged)."""
    conn = _conn()
    try:
        cur = conn.cursor()
        clear_all_electricity_products(cur)
        conn.commit()
    except pyodbc.Error as e:
        conn.rollback()
        raise DatabaseConfigError(
            f"Database error while clearing electricity products: {e}"
        ) from e
    finally:
        conn.close()
