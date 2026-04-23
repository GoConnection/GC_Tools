from __future__ import annotations

from typing import Any

from db_config import open_sql_connection


def get_db_connection():
    return open_sql_connection()


def _row_to_dict(columns, row) -> dict[str, Any]:
    return {columns[idx]: row[idx] for idx in range(len(columns))}


def get_tarifas_eletricidade() -> list[dict[str, Any]]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                oferta,
                tarifa,
                periodo,
                preco_kwh,
                preco_potencia_dia,
                tem_debito_direto,
                tem_fatura_eletronica,
                ativo,
                updated_at
            FROM [edp].[edp_simulator_tarifas_eletricidade]
            WHERE ativo = 1
            ORDER BY oferta, tarifa, periodo
            """
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [_row_to_dict(cols, row) for row in rows]


def get_tarifas_gas() -> list[dict[str, Any]]:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id,
                escalao,
                fidelizacao_anos,
                preco_kwh,
                preco_fixo_dia,
                ativo,
                updated_at
            FROM [edp].[edp_simulator_tarifas_gas]
            WHERE ativo = 1
            ORDER BY escalao, fidelizacao_anos
            """
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return [_row_to_dict(cols, row) for row in rows]


def upsert_tarifa_eletricidade(data: dict) -> None:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM [edp].[edp_simulator_tarifas_eletricidade]
            WHERE oferta = ?
              AND tarifa = ?
              AND periodo = ?
              AND tem_debito_direto = ?
              AND tem_fatura_eletronica = ?
            """,
            (
                data["oferta"],
                data["tarifa"],
                data["periodo"],
                data["tem_debito_direto"],
                data["tem_fatura_eletronica"],
            ),
        )
        row = cur.fetchone()

        if row:
            cur.execute(
                """
                UPDATE [edp].[edp_simulator_tarifas_eletricidade]
                SET
                    preco_kwh = ?,
                    preco_potencia_dia = ?,
                    ativo = ?,
                    updated_at = GETDATE()
                WHERE id = ?
                """,
                (
                    data["preco_kwh"],
                    data["preco_potencia_dia"],
                    data.get("ativo", 1),
                    int(row[0]),
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO [edp].[edp_simulator_tarifas_eletricidade] (
                    oferta,
                    tarifa,
                    periodo,
                    preco_kwh,
                    preco_potencia_dia,
                    tem_debito_direto,
                    tem_fatura_eletronica,
                    ativo,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
                """,
                (
                    data["oferta"],
                    data["tarifa"],
                    data["periodo"],
                    data["preco_kwh"],
                    data["preco_potencia_dia"],
                    data["tem_debito_direto"],
                    data["tem_fatura_eletronica"],
                    data.get("ativo", 1),
                ),
            )
        conn.commit()


def upsert_tarifa_gas(data: dict) -> None:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM [edp].[edp_simulator_tarifas_gas]
            WHERE escalao = ?
              AND fidelizacao_anos = ?
            """,
            (data["escalao"], data["fidelizacao_anos"]),
        )
        row = cur.fetchone()

        if row:
            cur.execute(
                """
                UPDATE [edp].[edp_simulator_tarifas_gas]
                SET
                    preco_kwh = ?,
                    preco_fixo_dia = ?,
                    ativo = ?,
                    updated_at = GETDATE()
                WHERE id = ?
                """,
                (
                    data["preco_kwh"],
                    data["preco_fixo_dia"],
                    data.get("ativo", 1),
                    int(row[0]),
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO [edp].[edp_simulator_tarifas_gas] (
                    escalao,
                    fidelizacao_anos,
                    preco_kwh,
                    preco_fixo_dia,
                    ativo,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, GETDATE())
                """,
                (
                    data["escalao"],
                    data["fidelizacao_anos"],
                    data["preco_kwh"],
                    data["preco_fixo_dia"],
                    data.get("ativo", 1),
                ),
            )
        conn.commit()


def delete_tarifa_eletricidade(id: int) -> None:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM [edp].[edp_simulator_tarifas_eletricidade]
            WHERE id = ?
            """,
            (int(id),),
        )
        conn.commit()


def delete_tarifa_gas(id: int) -> None:
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM [edp].[edp_simulator_tarifas_gas]
            WHERE id = ?
            """,
            (int(id),),
        )
        conn.commit()
