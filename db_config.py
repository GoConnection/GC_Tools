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
import json
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


def _conn_by_config_key(config_key: str) -> pyodbc.Connection:
    raw = str(current_app.config.get(config_key) or "").strip()
    if not raw:
        raise DatabaseConfigError(
            f"Missing required database configuration: {config_key}"
        )
    return pyodbc.connect(_normalize_pyodbc_connection_string(raw))


def open_sql_connection() -> pyodbc.Connection:
    """ODBC connection using the same Key Vault string as tariff config (Flask app context)."""
    return _conn()


def get_allowed_agent_name(agent_id: int) -> str | None:
    """
    Return agent display name from OCGoConnection.dbo.[User] when allowed.

    Constraints:
      Active = 1
      Deleted = 0
      DisabledAcc = 0
      Locked = 0
      UserTypeID = 200
    """
    try:
        with _conn_by_config_key("OCGO_SQLSERVER_CONNECTION_STRING") as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT TOP 1 HumanName
                FROM [dbo].[User]
                WHERE UserID = ?
                  AND Active = 1
                  AND Deleted = 0
                  AND DisabledAcc = 0
                  AND Locked = 0
                  AND UserTypeID = 200
                """,
                (int(agent_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            return str(row[0]).strip() if row[0] is not None else ""
    except pyodbc.Error as e:
        raise DatabaseConfigError(
            f"Database error while loading allowed agent IDs: {e}"
        ) from e


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

            # ==========================================
            # NOVO: LEITURA DAS POTÊNCIAS BTN COM CICLO / EV
            # ==========================================
            cur.execute(
                f"""
                SELECT KvaLabel, UnitPrice, EnergiaVerde, Ciclo
                FROM {_q("ElecBtnPowerPrice")}
                ORDER BY SortOrder
                """
            )
            
            pot_en = {}
            pot_ev = {'SIM': {}, 'BIH': {}, 'TRI': {}, 'TRI+': {}}
            
            for r in cur.fetchall():
                kva = str(r[0]).strip()
                price = _float(r[1])
                is_verde = int(r[2]) if r[2] is not None else 0
                ciclo = str(r[3]).strip().upper() if r[3] else None
                
                if is_verde == 1 and ciclo:
                    if ciclo not in pot_ev:
                        pot_ev[ciclo] = {}
                    pot_ev[ciclo][kva] = price
                elif is_verde == 0 or is_verde is None:
                    pot_en[kva] = price

            # ==========================================
            # NOVO: LEITURA DAS POTÊNCIAS BTE (NORMAL vs VERDE)
            # ==========================================
            cur.execute(
                f"SELECT PotPonta, PotContratada, EnergiaVerde FROM {_q('ElecBtePowerTerm')}"
            )
            bte_rows = cur.fetchall()
            pot_ponta_en, pot_contratada_en = 0.0, 0.0
            pot_ponta_ev, pot_contratada_ev = 0.0, 0.0
            
            for r in bte_rows:
                is_verde = int(r[2]) if r[2] is not None else 0
                if is_verde == 1:
                    pot_ponta_ev, pot_contratada_ev = _float(r[0]), _float(r[1])
                else:
                    pot_ponta_en, pot_contratada_en = _float(r[0]), _float(r[1])

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
                    ["SIM", "BIH", "TRI", "TRI+"]
                    if seg == "BTN"
                    else ["TETRA"]
                ):
                    key = (seg, ee, prof)
                    if key in tar_rows:
                        tmap[prof] = dict(tar_rows[key])
                    else:
                        tmap[prof] = {"p1": 0.0, "p2": 0.0, "p3": 0.0, "p4": 0.0}
                return {"produtos": produtos, "tar": tmap}

            # ==========================================
            # NOVO: DISTRIBUIÇÃO DAS POTÊNCIAS NO EE_CONFIG (BTN e BTE)
            # ==========================================
            ee_en_block = ee_block("BTN", "EN")
            ee_en_block["potencias"] = pot_en
            
            ee_ev_block = ee_block("BTN", "EV")
            ee_ev_block["potencias"] = pot_ev

            ee_bte_en_block = ee_block("BTE", "EN")
            ee_bte_en_block["pot_ponta"] = pot_ponta_en
            ee_bte_en_block["pot_contratada"] = pot_contratada_en

            ee_bte_ev_block = ee_block("BTE", "EV")
            ee_bte_ev_block["pot_ponta"] = pot_ponta_ev
            ee_bte_ev_block["pot_contratada"] = pot_contratada_ev

            ee_config = {
                "BTN": {
                    "potencias": pot_en, # Mantido para garantir que nada para trás se quebra
                    "EN": ee_en_block,
                    "EV": ee_ev_block,
                },
                "BTE": {
                    "pot_ponta": pot_ponta_en,
                    "pot_contratada": pot_contratada_en,
                    "EN": ee_bte_en_block,
                    "EV": ee_bte_ev_block,
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

            # ==========================================
            # NOVO: LÓGICA DE GRAVAÇÃO (INSERT/UPDATE) DOS CICLOS BTN
            # ==========================================
            cur.execute(f"SELECT KvaLabel, SortOrder FROM {_q('ElecBtnPowerPrice')} WHERE EnergiaVerde = 0 OR EnergiaVerde IS NULL")
            sort_orders = {str(r[0]).strip(): int(r[1]) for r in cur.fetchall()}

            # GRAVA POTÊNCIAS DA ENERGIA NORMAL BTN
            if "EN" in ee["BTN"] and "potencias" in ee["BTN"]["EN"]:
                for k, v in ee["BTN"]["EN"]["potencias"].items():
                    k_str = str(k).strip()
                    v_float = _float(v)
                    cur.execute(
                        f"""
                        UPDATE {_q("ElecBtnPowerPrice")}
                        SET UnitPrice = ?
                        WHERE KvaLabel = ? AND (EnergiaVerde = 0 OR EnergiaVerde IS NULL)
                        """,
                        (v_float, k_str),
                    )
                    if cur.rowcount == 0:
                        so = sort_orders.get(k_str, 99)
                        cur.execute(
                            f"""
                            INSERT INTO {_q("ElecBtnPowerPrice")}
                            (KvaLabel, UnitPrice, SortOrder, EnergiaVerde, Ciclo)
                            VALUES (?, ?, ?, 0, NULL)
                            """,
                            (k_str, v_float, so)
                        )

            # GRAVA POTÊNCIAS DA ENERGIA VERDE (DIVIDIDAS POR CICLO) BTN
            if "EV" in ee["BTN"] and "potencias" in ee["BTN"]["EV"]:
                for ciclo, pot_dict in ee["BTN"]["EV"]["potencias"].items():
                    for k, v in pot_dict.items():
                        k_str = str(k).strip()
                        v_float = _float(v)
                        cur.execute(
                            f"""
                            UPDATE {_q("ElecBtnPowerPrice")}
                            SET UnitPrice = ?
                            WHERE KvaLabel = ? AND EnergiaVerde = 1 AND Ciclo = ?
                            """,
                            (v_float, k_str, ciclo),
                        )
                        if cur.rowcount == 0:
                            so = sort_orders.get(k_str, 99)
                            cur.execute(
                                f"""
                                INSERT INTO {_q("ElecBtnPowerPrice")}
                                (KvaLabel, UnitPrice, SortOrder, EnergiaVerde, Ciclo)
                                VALUES (?, ?, ?, 1, ?)
                                """,
                                (k_str, v_float, so, ciclo)
                            )
            
            # RETROCOMPATIBILIDADE BTN
            elif "potencias" in ee["BTN"]:
                for k, v in ee["BTN"]["potencias"].items():
                    cur.execute(
                        f"""
                        UPDATE {_q("ElecBtnPowerPrice")}
                        SET UnitPrice = ?
                        WHERE KvaLabel = ? AND (EnergiaVerde = 0 OR EnergiaVerde IS NULL)
                        """,
                        (_float(v), str(k).strip()),
                    )


            # ==========================================
            # NOVO: LÓGICA DE GRAVAÇÃO (INSERT/UPDATE) BTE
            # ==========================================
            # GRAVA POTÊNCIAS BTE DA ENERGIA NORMAL
            if "EN" in ee["BTE"] and "pot_ponta" in ee["BTE"]["EN"]:
                cur.execute(
                    f"""
                    UPDATE {_q("ElecBtePowerTerm")}
                    SET PotPonta = ?, PotContratada = ?
                    WHERE EnergiaVerde = 0 OR EnergiaVerde IS NULL
                    """,
                    (_float(ee["BTE"]["EN"]["pot_ponta"]), _float(ee["BTE"]["EN"]["pot_contratada"])),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        f"""
                        INSERT INTO {_q("ElecBtePowerTerm")}
                        (PotPonta, PotContratada, EnergiaVerde)
                        VALUES (?, ?, 0)
                        """,
                        (_float(ee["BTE"]["EN"]["pot_ponta"]), _float(ee["BTE"]["EN"]["pot_contratada"]))
                    )

            # GRAVA POTÊNCIAS BTE DA ENERGIA VERDE
            if "EV" in ee["BTE"] and "pot_ponta" in ee["BTE"]["EV"]:
                cur.execute(
                    f"""
                    UPDATE {_q("ElecBtePowerTerm")}
                    SET PotPonta = ?, PotContratada = ?
                    WHERE EnergiaVerde = 1
                    """,
                    (_float(ee["BTE"]["EV"]["pot_ponta"]), _float(ee["BTE"]["EV"]["pot_contratada"])),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        f"""
                        INSERT INTO {_q("ElecBtePowerTerm")}
                        (PotPonta, PotContratada, EnergiaVerde)
                        VALUES (?, ?, 1)
                        """,
                        (_float(ee["BTE"]["EV"]["pot_ponta"]), _float(ee["BTE"]["EV"]["pot_contratada"]))
                    )
            
            # RETROCOMPATIBILIDADE BTE
            elif "pot_ponta" in ee["BTE"]:
                cur.execute(
                    f"""
                    UPDATE {_q("ElecBtePowerTerm")}
                    SET PotPonta = ?, PotContratada = ?
                    WHERE EnergiaVerde = 0 OR EnergiaVerde IS NULL
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

# ==========================================
# NOTAS CRM (SQL SERVER INTEGRATION)
# ==========================================

def load_notes_sql() -> Dict[str, List[Dict[str, Any]]]:
    """Lê todas as leads da base de dados SQL e converte de volta para o formato JSON legado para o app.py."""
    try:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT Id, OperadorToken, Title, Subtitle, NIPC, Phone, Descricao, Status, FollowupDate, FollowupTime, Scenarios, LockedFields, History, Archived, LastUpdated, CreatedAt FROM {_q('CRM_Leads')}")
            
            data = {}
            for row in cur.fetchall():
                token = str(row.OperadorToken).strip()
                if token not in data:
                    data[token] = []
                
                lead = {
                    "id": str(row.Id),
                    "title": str(row.Title) if row.Title else "",
                    "subtitle": str(row.Subtitle) if row.Subtitle else "",
                    "nipc": str(row.NIPC) if row.NIPC else "",
                    "phone": str(row.Phone) if row.Phone else "",
                    "desc": str(row.Descricao) if row.Descricao else "",
                    "status": str(row.Status) if row.Status else "lead",
                    "followup_date": str(row.FollowupDate) if row.FollowupDate else "",
                    "followup_time": str(row.FollowupTime) if row.FollowupTime else "",
                    "archived": bool(row.Archived),
                    "last_updated": str(row.LastUpdated) if row.LastUpdated else "",
                    "created_at": str(row.CreatedAt) if row.CreatedAt else ""
                }
                
                # Descodificar JSON string de volta para as estruturas originais
                try: lead["scenarios"] = json.loads(row.Scenarios) if row.Scenarios else []
                except: lead["scenarios"] = []
                
                try: lead["locked_fields"] = json.loads(row.LockedFields) if row.LockedFields else {}
                except: lead["locked_fields"] = {}
                
                try: lead["history"] = json.loads(row.History) if row.History else []
                except: lead["history"] = []
                
                data[token].append(lead)
            
            return data
    except Exception as e:
        print(f"Erro a carregar as notas do SQL: {e}")
        return {}


def save_notes_sql(data: Dict[str, List[Dict[str, Any]]]) -> None:
    """Grava o dicionário de notas (formato JSON legado) diretamente na tabela SQL através de UPSERT."""
    try:
        with _conn() as conn:
            cur = conn.cursor()
            
            for token, leads in data.items():
                for lead in leads:
                    lead_id = lead.get("id")
                    if not lead_id: continue
                    
                    scenarios_json = json.dumps(lead.get("scenarios", []), ensure_ascii=False)
                    locked_json = json.dumps(lead.get("locked_fields", {}), ensure_ascii=False)
                    history_json = json.dumps(lead.get("history", []), ensure_ascii=False)
                    archived_bit = 1 if lead.get("archived") else 0
                    
                    cur.execute(f"SELECT Id FROM {_q('CRM_Leads')} WHERE Id = ?", (lead_id,))
                    exists = cur.fetchone()
                    
                    if exists:
                        cur.execute(f"""
                            UPDATE {_q('CRM_Leads')}
                            SET OperadorToken = ?, Title = ?, Subtitle = ?, NIPC = ?, Phone = ?, 
                                Descricao = ?, Status = ?, FollowupDate = ?, FollowupTime = ?, 
                                Scenarios = ?, LockedFields = ?, History = ?, Archived = ?, 
                                LastUpdated = ?, CreatedAt = ?
                            WHERE Id = ?
                        """, (
                            token, lead.get("title", ""), lead.get("subtitle", ""), lead.get("nipc", ""), 
                            lead.get("phone", ""), lead.get("desc", ""), lead.get("status", "lead"), 
                            lead.get("followup_date", ""), lead.get("followup_time", ""),
                            scenarios_json, locked_json, history_json, archived_bit, 
                            lead.get("last_updated", ""), lead.get("created_at", ""),
                            lead_id
                        ))
                    else:
                        cur.execute(f"""
                            INSERT INTO {_q('CRM_Leads')} 
                            (Id, OperadorToken, Title, Subtitle, NIPC, Phone, Descricao, Status, 
                            FollowupDate, FollowupTime, Scenarios, LockedFields, History, Archived, 
                            LastUpdated, CreatedAt)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            lead_id, token, lead.get("title", ""), lead.get("subtitle", ""), lead.get("nipc", ""), 
                            lead.get("phone", ""), lead.get("desc", ""), lead.get("status", "lead"), 
                            lead.get("followup_date", ""), lead.get("followup_time", ""),
                            scenarios_json, locked_json, history_json, archived_bit, 
                            lead.get("last_updated", ""), lead.get("created_at", "")
                        ))
            
            conn.commit()
    except pyodbc.Error as e:
        raise DatabaseConfigError(f"Erro de base de dados a guardar as notas CRM: {e}") from e

# ==========================================
# CHAT & BROADCAST (SQL SERVER INTEGRATION)
# ==========================================

def load_chat_sql(scope: str = "endesa") -> Dict[str, Any]:
    """Lê as mensagens da tabela unificada simulando a estrutura original do JSON."""
    try:
        with _conn() as conn:
            cur = conn.cursor()
            scope_norm = str(scope or "endesa").strip().lower()
            scope_norm = "edp" if scope_norm == "edp" else "endesa"
            
            # Carregar o último Broadcast (Top 1 mais recente)
            if scope_norm == "edp":
                cur.execute(
                    f"""
                    SELECT TOP 1 Id, MessageText, Timestamp
                    FROM {_q('calculadora_mensagens')}
                    WHERE TipoMensagem = 'broadcast'
                      AND TokenId = ?
                    ORDER BY CreatedAt DESC
                    """,
                    ("edp",),
                )
            else:
                cur.execute(
                    f"""
                    SELECT TOP 1 Id, MessageText, Timestamp
                    FROM {_q('calculadora_mensagens')}
                    WHERE TipoMensagem = 'broadcast'
                      AND (TokenId IS NULL OR TokenId = '' OR TokenId = ?)
                    ORDER BY CreatedAt DESC
                    """,
                    ("endesa",),
                )
            b_row = cur.fetchone()
            broadcast_data = {}
            if b_row:
                broadcast_data = {
                    "id": str(b_row.Id),
                    "text": str(b_row.MessageText),
                    "timestamp": str(b_row.Timestamp)
                }
            
            # Carregar as mensagens privadas (limitado às últimas 1000 para não estourar a memória)
            # Carregamos em DESC para ter as mais novas, e depois ordenamos ASC no app
            if scope_norm == "edp":
                cur.execute(
                    f"""
                    SELECT Id, TokenId, Sender, MessageText, Timestamp, IsReadAdmin, IsReadOp
                    FROM (
                        SELECT TOP 1000 Id, TokenId, Sender, MessageText, Timestamp, IsReadAdmin, IsReadOp, CreatedAt
                        FROM {_q('calculadora_mensagens')}
                        WHERE TipoMensagem = 'privado'
                          AND TokenId LIKE ?
                        ORDER BY CreatedAt DESC
                    ) sub
                    ORDER BY sub.CreatedAt ASC
                    """,
                    ("edp::%",),
                )
            else:
                cur.execute(
                    f"""
                    SELECT Id, TokenId, Sender, MessageText, Timestamp, IsReadAdmin, IsReadOp
                    FROM (
                        SELECT TOP 1000 Id, TokenId, Sender, MessageText, Timestamp, IsReadAdmin, IsReadOp, CreatedAt
                        FROM {_q('calculadora_mensagens')}
                        WHERE TipoMensagem = 'privado'
                          AND (TokenId IS NULL OR TokenId NOT LIKE ?)
                        ORDER BY CreatedAt DESC
                    ) sub
                    ORDER BY sub.CreatedAt ASC
                    """,
                    ("edp::%",),
                )
            
            messages = []
            for row in cur.fetchall():
                messages.append({
                    "id": str(row.Id),
                    "token_id": str(row.TokenId) if row.TokenId else "",
                    "sender": str(row.Sender),
                    "text": str(row.MessageText),
                    "timestamp": str(row.Timestamp),
                    "is_read_admin": bool(row.IsReadAdmin),
                    "is_read_op": bool(row.IsReadOp)
                })
                
            return {"messages": messages, "broadcast": broadcast_data}
    except Exception as e:
        print(f"Erro a carregar o chat do SQL: {e}")
        return {"messages": [], "broadcast": {}}


def save_chat_sql(data: Dict[str, Any], scope: str = "endesa") -> None:
    """Grava as mensagens novas e atualiza os estados lidos via UPSERT."""
    try:
        with _conn() as conn:
            cur = conn.cursor()
            scope_norm = str(scope or "endesa").strip().lower()
            scope_norm = "edp" if scope_norm == "edp" else "endesa"
            
            # 1. Guardar Mensagens Privadas
            messages = data.get("messages", [])
            for msg in messages:
                msg_id = msg.get("id")
                if not msg_id: continue
                
                # Upsert: Verifica se existe
                cur.execute(f"SELECT Id FROM {_q('calculadora_mensagens')} WHERE Id = ?", (msg_id,))
                exists = cur.fetchone()
                
                if exists:
                    # Se existe, atualizamos apenas se foi lido (o texto nunca muda no chat)
                    cur.execute(f"""
                        UPDATE {_q('calculadora_mensagens')}
                        SET IsReadAdmin = ?, IsReadOp = ?
                        WHERE Id = ?
                    """, (
                        1 if msg.get("is_read_admin") else 0,
                        1 if msg.get("is_read_op") else 0,
                        msg_id
                    ))
                else:
                    # Inserir nova mensagem
                    cur.execute(f"""
                        INSERT INTO {_q('calculadora_mensagens')} 
                        (Id, TipoMensagem, TokenId, Sender, MessageText, Timestamp, IsReadAdmin, IsReadOp)
                        VALUES (?, 'privado', ?, ?, ?, ?, ?, ?)
                    """, (
                        msg_id,
                        msg.get("token_id", ""),
                        msg.get("sender", ""),
                        msg.get("text", ""),
                        msg.get("timestamp", ""),
                        1 if msg.get("is_read_admin") else 0,
                        1 if msg.get("is_read_op") else 0
                    ))
                    
            # 2. Guardar novo Broadcast (se houver)
            broadcast = data.get("broadcast", {})
            b_id = broadcast.get("id")
            if b_id:
                cur.execute(f"SELECT Id FROM {_q('calculadora_mensagens')} WHERE Id = ?", (b_id,))
                if not cur.fetchone():
                    # Os Broadcasts inserem-se sempre como registos novos (para manter histórico)
                    cur.execute(f"""
                        INSERT INTO {_q('calculadora_mensagens')} 
                        (Id, TipoMensagem, TokenId, Sender, MessageText, Timestamp, IsReadAdmin, IsReadOp)
                        VALUES (?, 'broadcast', ?, 'Admin', ?, ?, 1, 1)
                    """, (
                        b_id,
                        scope_norm,
                        broadcast.get("text", ""),
                        broadcast.get("timestamp", "")
                    ))
                    
            conn.commit()
    except pyodbc.Error as e:
        raise DatabaseConfigError(f"Erro de base de dados a guardar o chat: {e}") from e