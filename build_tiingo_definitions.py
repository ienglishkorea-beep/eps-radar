from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import requests

from config import TIINGO_API_KEY, TIINGO_BASE_URL, TIINGO_ENDPOINTS, DEFAULT_TIMEOUT_SECONDS


OUTPUT_JSON_PATH = "data/tiingo_definitions.json"
OUTPUT_GROUPED_JSON_PATH = "data/tiingo_definitions_grouped.json"
OUTPUT_CORE_MAP_JSON_PATH = "data/tiingo_core_field_map.json"


CORE_FIELD_CANDIDATES: Dict[str, List[str]] = {
    "market_cap": ["marketCap"],
    "enterprise_value": ["enterpriseVal"],
    "pe": ["peRatio"],
    "shares_outstanding": ["sharesOutstanding"],
    "gross_margin": ["grossMargin"],
    "net_margin": ["netMargin", "profitMargin"],
    "profit_margin": ["profitMargin", "netMargin"],
    "revenue": ["revenue"],
    "gross_profit": ["grossProfit"],
    "ebit": ["ebit"],
    "operating_income": ["opInc"],
    "net_income": ["netInc", "netIncome"],
    "cfo": ["ncfo"],
    "capex": ["capEx"],
    "free_cash_flow": ["freeCashFlow"],
    "debt": ["debt"],
    "cash": ["cashAndCashEquivalents"],
    "current_liabilities": ["liabilitiesCurrent"],
}


def validate() -> None:
    if not TIINGO_API_KEY:
        raise RuntimeError("TIINGO_API_KEY 환경변수가 비어 있다.")


def request_json(url: str) -> Any:
    response = requests.get(
        url,
        params={"token": TIINGO_API_KEY},
        headers={"Content-Type": "application/json"},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def fetch_definitions() -> List[Dict[str, Any]]:
    url = f"{TIINGO_BASE_URL}{TIINGO_ENDPOINTS['fundamental_definitions']}"
    raw = request_json(url)
    if not isinstance(raw, list):
        raise RuntimeError("definitions 응답이 list가 아니다.")
    return raw


def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "dataCode": row.get("dataCode"),
                "name": row.get("name"),
                "description": row.get("description"),
                "statementType": row.get("statementType"),
                "units": row.get("units"),
            }
        )
    out = [r for r in out if r.get("dataCode")]
    out.sort(key=lambda x: ((x.get("statementType") or ""), (x.get("dataCode") or "")))
    return out


def group_by_statement_type(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        st = row.get("statementType") or "unknown"
        grouped.setdefault(st, []).append(row)
    for st in grouped:
        grouped[st] = sorted(grouped[st], key=lambda x: x["dataCode"])
    return grouped


def build_core_map(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    by_code: Dict[str, Dict[str, Any]] = {r["dataCode"]: r for r in rows}
    core_map: Dict[str, Dict[str, Any]] = {}

    for logical_name, candidates in CORE_FIELD_CANDIDATES.items():
        matched = None
        for code in candidates:
            if code in by_code:
                matched = by_code[code]
                break

        if matched is None:
            core_map[logical_name] = {
                "matched": False,
                "dataCode": None,
                "statementType": None,
                "units": None,
                "name": None,
            }
        else:
            core_map[logical_name] = {
                "matched": True,
                "dataCode": matched.get("dataCode"),
                "statementType": matched.get("statementType"),
                "units": matched.get("units"),
                "name": matched.get("name"),
            }

    return core_map


def save_json(path: str, obj: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def print_summary(rows: List[Dict[str, Any]], grouped: Dict[str, List[Dict[str, Any]]], core_map: Dict[str, Dict[str, Any]]) -> None:
    print(f"definitions total: {len(rows)}")
    print("")

    print("[statementType counts]")
    for st, items in grouped.items():
        print(f"- {st}: {len(items)}")
    print("")

    print("[core field map]")
    for logical_name, info in core_map.items():
        if info["matched"]:
            print(
                f"- {logical_name}: "
                f"{info['dataCode']} | {info['statementType']} | {info['units']} | {info['name']}"
            )
        else:
            print(f"- {logical_name}: NOT FOUND")


def main() -> None:
    validate()

    raw = fetch_definitions()
    rows = normalize_rows(raw)
    grouped = group_by_statement_type(rows)
    core_map = build_core_map(rows)

    save_json(OUTPUT_JSON_PATH, rows)
    save_json(OUTPUT_GROUPED_JSON_PATH, grouped)
    save_json(OUTPUT_CORE_MAP_JSON_PATH, core_map)

    print_summary(rows, grouped, core_map)
    print("")
    print(f"saved: {OUTPUT_JSON_PATH}")
    print(f"saved: {OUTPUT_GROUPED_JSON_PATH}")
    print(f"saved: {OUTPUT_CORE_MAP_JSON_PATH}")


if __name__ == "__main__":
    main()
