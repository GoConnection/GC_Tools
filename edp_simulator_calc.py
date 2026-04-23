from __future__ import annotations

from typing import Any

IMPOSTOS_MULTIPLIER = 1.2484


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _flags_from_atributos(atributos: str) -> tuple[int, int]:
    normalized = (atributos or "").strip().lower()
    if normalized == "debito direto":
        return 1, 0
    if normalized == "fatura eletronica":
        return 0, 1
    if normalized == "debito direto + fatura eletronica":
        return 1, 1
    return 0, 0


def calcular_simulacao(inputs: dict, tarifas_ele: list, tarifas_gas: list) -> dict:
    atributos = inputs.get("atributos", "Nenhum atributo")
    oferta = inputs.get("oferta")
    tarifa = inputs.get("tarifa")
    tem_tarifa_social = bool(inputs.get("tem_tarifa_social"))
    potencia_kva = _to_float(inputs.get("potencia_kva"))
    num_dias = _to_float(inputs.get("num_dias"))
    consumo = inputs.get("consumo") or {}

    result = {
        "fatura_edp_eletricidade": None,
        "fatura_edp_gas": None,
        "fatura_edp_total": None,
        "poupanca_valor": None,
        "poupanca_percentagem": None,
        "poupanca_anual": None,
        "erro": None,
    }

    if tem_tarifa_social and potencia_kva > 6.9:
        result["erro"] = "Cliente nao elegivel para TS"
        return result

    tem_debito_direto, tem_fatura_eletronica = _flags_from_atributos(atributos)

    ele_rows = [
        row
        for row in tarifas_ele
        if str(row.get("oferta", "")).strip() == str(oferta).strip()
        and str(row.get("tarifa", "")).strip() == str(tarifa).strip()
        and int(row.get("tem_debito_direto", 0)) == tem_debito_direto
        and int(row.get("tem_fatura_eletronica", 0)) == tem_fatura_eletronica
        and int(row.get("ativo", 1)) == 1
    ]

    if not ele_rows:
        result["erro"] = "Nao foram encontradas tarifas de eletricidade para os parametros selecionados."
        return result

    map_periodo = {str(row.get("periodo", "")).strip(): row for row in ele_rows}

    total_ele_base = 0.0
    for periodo, consumo_kwh in consumo.items():
        periodo_nome = str(periodo).strip()
        if periodo_nome not in map_periodo:
            result["erro"] = f"Tarifa de eletricidade em falta para o periodo: {periodo_nome}"
            return result
        total_ele_base += _to_float(consumo_kwh) * _to_float(map_periodo[periodo_nome].get("preco_kwh"))

    preco_potencia_dia = _to_float(ele_rows[0].get("preco_potencia_dia"))
    total_ele_base += potencia_kva * preco_potencia_dia * num_dias

    fatura_edp_eletricidade = total_ele_base * IMPOSTOS_MULTIPLIER
    result["fatura_edp_eletricidade"] = round(fatura_edp_eletricidade, 2)

    fatura_edp_gas = None
    if bool(inputs.get("tem_gas")):
        gas_escalao = inputs.get("gas_escalao")
        gas_fidelizacao_anos = _to_int(inputs.get("gas_fidelizacao_anos"))
        gas_num_dias = _to_float(inputs.get("gas_num_dias"))
        gas_consumo_kwh = _to_float(inputs.get("gas_consumo_kwh"))

        gas_rows = [
            row
            for row in tarifas_gas
            if str(row.get("escalao", "")).strip() == str(gas_escalao).strip()
            and _to_int(row.get("fidelizacao_anos")) == gas_fidelizacao_anos
            and int(row.get("ativo", 1)) == 1
        ]
        if not gas_rows:
            result["erro"] = "Nao foram encontradas tarifas de gas para os parametros selecionados."
            return result

        gas_row = gas_rows[0]
        total_gas_base = (gas_consumo_kwh * _to_float(gas_row.get("preco_kwh"))) + (
            gas_num_dias * _to_float(gas_row.get("preco_fixo_dia"))
        )
        fatura_edp_gas = total_gas_base * IMPOSTOS_MULTIPLIER
        result["fatura_edp_gas"] = round(fatura_edp_gas, 2)

    fatura_edp_total = fatura_edp_eletricidade + (fatura_edp_gas or 0.0)
    result["fatura_edp_total"] = round(fatura_edp_total, 2)

    fatura_concorrencia = _to_float(inputs.get("fatura_concorrencia"), default=0.0)
    poupanca_valor = fatura_concorrencia - fatura_edp_total
    result["poupanca_valor"] = round(poupanca_valor, 2)

    if fatura_concorrencia > 0:
        result["poupanca_percentagem"] = round((poupanca_valor / fatura_concorrencia) * 100, 2)
    else:
        result["poupanca_percentagem"] = None

    result["poupanca_anual"] = round(poupanca_valor * 12, 2)
    return result
