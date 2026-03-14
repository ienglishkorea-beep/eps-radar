import json
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests

# =========================================================
# Tiingo deep probe inside EPS Radar
# ---------------------------------------------------------
# 목적
# 1) definitions에서 dataCode 확인
# 2) daily / statements 응답을 재귀적으로 깊게 탐색
# 3) 약자형 + 풀네임 + 붙여쓴 형태를 전부 후보로 넣어 최대한 회수
# =========================================================

PROBE_TICKERS = ["MSFT", "AVGO"]
LOOKBACK_DAYS = 1200
OUT_DIR = "tiingo_probe_output"
REQUEST_TIMEOUT = 30
MAX_SAMPLES_PER_CODE = 8

FIELD_CANDIDATES: Dict[str, List[str]] = {
    "gross_margin": [
        "grossMargin",
        "grossprofitmargin",
        "grossProfitMargin",
    ],
    "operating_margin": [
        "opMargin",
        "operatingMargin",
        "operatingincomeMargin",
        "ebitMargin",
        "operatingmargin",
        "operatingincomemargin",
        "ebitmargin",
    ],
    "net_margin": [
        "netMargin",
        "netIncomeMargin",
        "profitMargin",
        "netmargin",
        "netincomemargin",
        "profitmargin",
    ],
    "pe": [
        "peRatio",
        "priceEarningsRatio",
        "priceToEarningsRatio",
        "peratio",
        "priceearningsratio",
        "pricetoearningsratio",
    ],
    "enterprise_value": [
        "enterpriseVal",
        "enterpriseValue",
        "enterprisevalue",
    ],
    "market_cap": [
        "marketCap",
        "marketCapitalization",
        "marketcap",
        "marketcapitalization",
    ],

    "revenue": [
        "revenue",
        "totalRevenue",
        "salesRevenue",
        "sales",
        "totalrevenue",
        "salesrevenue",
    ],
    "revenue_growth": [
        "revenueGrowth",
        "salesGrowth",
        "revenueYoYGrowth",
        "totalRevenueGrowth",
        "revenuegrowth",
        "salesgrowth",
        "revenueyoygrowth",
        "totalrevenuegrowth",
        "revenuegrowthrate",
        "salesgrowthrate",
    ],

    "roic": [
        "roic",
        "returnOnInvestedCapital",
        "returnOnCapital",
        "returnoninvestedcapital",
        "returnoncapital",
    ],

    "ev_sales": [
        "evToSales",
        "enterpriseToRevenue",
        "enterpriseValueToRevenue",
        "enterpriseValueToSales",
        "evSales",
        "evtosales",
        "enterprisetorevenue",
        "enterprisevaluetorevenue",
        "enterprisevaluetosales",
        "evsales",
    ],
    "ev_ebitda": [
        "evToEbitda",
        "enterpriseValueToEbitda",
        "evEbitda",
        "evtoebitda",
        "enterprisevaluetoebitda",
        "evebitda",
    ],
    "ev_ebit": [
        "evToEbit",
        "enterpriseValueToEbit",
        "evEbit",
        "evtoebit",
        "enterprisevaluetoebit",
        "evebit",
    ],

    "cfo": [
        "ncfo",
        "cashFromOperations",
        "cashFlowFromOperations",
        "netCashFlowFromOperations",
        "operatingCashFlow",
        "operatingcashflow",
        "cashfromoperations",
        "cashflowfromoperations",
        "netcashflowfromoperations",
    ],
    "cfo_growth": [
        "cashFromOperationsGrowth",
        "cashFlowFromOperationsGrowth",
        "operatingCashFlowGrowth",
        "netCashFlowFromOperationsGrowth",
        "cashfromoperationsgrowth",
        "cashflowfromoperationsgrowth",
        "operatingcashflowgrowth",
        "netcashflowfromoperationsgrowth",
    ],

    "capex": [
        "capex",
        "capitalExpenditure",
        "capitalexpenditure",
    ],
    "capex_growth": [
        "capexGrowth",
        "capitalExpenditureGrowth",
        "capexgrowth",
        "capitalexpendituregrowth",
    ],

    "debt": [
        "debt",
        "totalDebt",
        "debtNonCurrent",
        "debtCurrent",
        "longTermDebt",
        "longTermDebtTotal",
        "currentDebt",
        "nonCurrentDebt",
        "liabilitiesCurrent",
        "liabilitiesNonCurrent",
        "totaldebt",
        "debtnoncurrent",
        "debtcurrent",
        "longtermdebt",
        "longtermdebttotal",
        "currentdebt",
        "noncurrentdebt",
        "liabilitiescurrent",
        "liabilitiesnoncurrent",
    ],

    "gross_profit": [
        "grossProfit",
        "grossprofit",
    ],
    "operating_income": [
        "opinc",
        "operatingIncome",
        "operatingProfit",
        "ebit",
        "operatingincome",
        "operatingprofit",
    ],
    "net_income": [
        "netIncm",
        "netIncome",
        "netIncmComStock",
        "netIncomeCommonStock",
        "netincome",
        "netincmcomstock",
        "netincomecommonstock",
    ],

    "free_cash_flow": [
        "freeCashFlow",
        "freecashflow",
    ],
}

POSSIBLE_VALUE_KEYS = [
    "value",
    "rawValue",
    "reportedValue",
    "amount",
    "metricValue",
    "dataValue",
]

POSSIBLE_DATE_KEYS = [
    "date",
    "reportDate",
    "fiscalDate",
    "asOfDate",
    "calendarDate",
    "endDate",
    "startDate",
]

POSSIBLE_PERIOD_KEYS = [
    "year",
    "quarter",
    "period",
    "frequency",
    "statementType",
]


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def norm(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    for ch in ["/", "-", "_", "(", ")", "%", ":", ",", ".", "[", "]"]:
        s = s.replace(ch, " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s.strip()


def get_tiingo_token() -> str:
    for key_name in ["TIINGO_API_KEY", "TIINGO_TOKEN", "TIINGO_KEY"]:
        value = os.getenv(key_name, "").strip()
        if value:
            print(f"[INFO] using token from env: {key_name}")
            return value
    raise RuntimeError("Tiingo token env not found. Tried: TIINGO_API_KEY, TIINGO_TOKEN, TIINGO_KEY")


def get_json(url: str, token: str, params: Optional[Dict[str, Any]] = None) -> Any:
    final_params = dict(params or {})
    final_params["token"] = token
    headers = {"Content-Type": "application/json"}

    resp = requests.get(
        url,
        headers=headers,
        params=final_params,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_definitions(token: str) -> List[Dict[str, Any]]:
    url = "https://api.tiingo.com/tiingo/fundamentals/definitions"
    data = get_json(url, token)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def fetch_daily(token: str, ticker: str) -> Any:
    end_date = date.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    url = f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/daily"
    return get_json(
        url,
        token,
        {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
    )


def fetch_statements(token: str, ticker: str) -> Any:
    end_date = date.today()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    url = f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/statements"
    return get_json(
        url,
        token,
        {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "asReported": "true",
        },
    )


def find_definitions_by_code(definitions: List[Dict[str, Any]], codes: List[str]) -> List[Dict[str, Any]]:
    code_set = {str(c).lower() for c in codes}
    out: List[Dict[str, Any]] = []

    for row in definitions:
        data_code = str(row.get("dataCode") or "").lower()
        if data_code in code_set:
            out.append(
                {
                    "dataCode": row.get("dataCode"),
                    "name": row.get("name"),
                    "statementType": row.get("statementType"),
                    "units": row.get("units"),
                    "description": row.get("description"),
                }
            )
    return out


def compact_context(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in POSSIBLE_DATE_KEYS + POSSIBLE_PERIOD_KEYS + ["ticker", "name", "dataCode", "label"]:
        if key in d and d.get(key) is not None:
            out[key] = d.get(key)
    return out


def looks_scalar(x: Any) -> bool:
    return isinstance(x, (str, int, float, bool)) or x is None


def extract_samples_deep(obj: Any, codes: List[str], max_samples: int = MAX_SAMPLES_PER_CODE) -> Dict[str, List[Dict[str, Any]]]:
    wanted = {str(c).lower(): c for c in codes}
    result: Dict[str, List[Dict[str, Any]]] = {c: [] for c in codes}

    def push(code: str, sample: Dict[str, Any]) -> None:
        arr = result[code]
        if len(arr) >= max_samples:
            return

        key = json.dumps(sample, ensure_ascii=False, sort_keys=True, default=str)
        existing = {json.dumps(x, ensure_ascii=False, sort_keys=True, default=str) for x in arr}
        if key not in existing:
            arr.append(sample)

    def recurse(node: Any, path: str = "") -> None:
        if isinstance(node, dict):
            node_keys_lower = {str(k).lower(): k for k in node.keys()}

            # Case 1: key 직접 일치
            for code_lower, original_code in wanted.items():
                if code_lower in node_keys_lower:
                    real_key = node_keys_lower[code_lower]
                    value = node.get(real_key)
                    if value is not None and looks_scalar(value):
                        push(
                            original_code,
                            {
                                "path": f"{path}.{real_key}" if path else str(real_key),
                                "value": value,
                                "context": compact_context(node),
                            },
                        )

            # Case 2: dataCode / label / name / metric / key 값이 code와 일치
            descriptor_keys = ["dataCode", "label", "name", "metric", "key"]
            desc_values = [str(node.get(k)).lower() for k in descriptor_keys if node.get(k) is not None]

            for code_lower, original_code in wanted.items():
                if code_lower in desc_values:
                    found_value = None
                    found_value_key = None

                    for vk in POSSIBLE_VALUE_KEYS:
                        if vk in node and node.get(vk) is not None:
                            found_value = node.get(vk)
                            found_value_key = vk
                            break

                    if found_value is None:
                        for k, v in node.items():
                            if k in descriptor_keys:
                                continue
                            if looks_scalar(v) and isinstance(v, (int, float, str)) and v not in ["", None]:
                                found_value = v
                                found_value_key = k
                                break

                    if found_value is not None:
                        push(
                            original_code,
                            {
                                "path": path or "$",
                                "value_key": found_value_key,
                                "value": found_value,
                                "context": compact_context(node),
                            },
                        )

            for k, v in node.items():
                next_path = f"{path}.{k}" if path else str(k)
                recurse(v, next_path)

        elif isinstance(node, list):
            for i, item in enumerate(node):
                next_path = f"{path}[{i}]"
                recurse(item, next_path)

    recurse(obj)

    return {k: v for k, v in result.items() if v}


def print_definition_section(field_name: str, rows: List[Dict[str, Any]]) -> None:
    print(f"\n[{field_name}] definitions")
    if not rows:
        print("  - none")
        return

    for row in rows:
        print(
            f"  - dataCode={row.get('dataCode')} | "
            f"name={row.get('name')} | "
            f"statementType={row.get('statementType')} | "
            f"units={row.get('units')}"
        )


def print_value_section(title: str, samples_by_code: Dict[str, List[Dict[str, Any]]]) -> None:
    print(f"  {title}:")
    if not samples_by_code:
        print("    - none")
        return

    for code, samples in samples_by_code.items():
        print(f"    - {code}")
        for s in samples[:MAX_SAMPLES_PER_CODE]:
            print(f"      {s}")


def build_final_report(
    definitions: List[Dict[str, Any]],
    daily_raw: Any,
    statements_raw: Any,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {}

    for field_name, codes in FIELD_CANDIDATES.items():
        defs = find_definitions_by_code(definitions, codes)
        daily_samples = extract_samples_deep(daily_raw, codes)
        statement_samples = extract_samples_deep(statements_raw, codes)

        report[field_name] = {
            "candidate_codes": codes,
            "definitions": defs,
            "daily_samples": daily_samples,
            "statement_samples": statement_samples,
            "daily_present": bool(daily_samples),
            "statements_present": bool(statement_samples),
        }

    return report


def print_final_report(ticker: str, report: Dict[str, Any]) -> None:
    print("\n" + "=" * 100)
    print(f"[FINAL DEEP DIRECT REPORT] {ticker}")
    print("=" * 100)

    for field_name, info in report.items():
        status = []
        if info["daily_present"]:
            status.append("daily")
        if info["statements_present"]:
            status.append("statements")
        status_text = ",".join(status) if status else "missing"

        print(f"\n[{field_name}] -> {status_text}")
        print(f"  candidate_codes: {info['candidate_codes']}")
        print_definition_section(field_name, info["definitions"])
        print_value_section("daily_samples", info["daily_samples"])
        print_value_section("statement_samples", info["statement_samples"])


def main() -> None:
    ensure_dir(OUT_DIR)
    token = get_tiingo_token()

    definitions = fetch_definitions(token)
    save_json(os.path.join(OUT_DIR, "definitions_raw.json"), definitions)

    for ticker in PROBE_TICKERS:
        try:
            daily_raw = fetch_daily(token, ticker)
        except Exception as e:
            daily_raw = {"error": str(e)}

        try:
            statements_raw = fetch_statements(token, ticker)
        except Exception as e:
            statements_raw = {"error": str(e)}

        save_json(os.path.join(OUT_DIR, f"{ticker.lower()}_daily_raw.json"), daily_raw)
        save_json(os.path.join(OUT_DIR, f"{ticker.lower()}_statements_raw.json"), statements_raw)

        report = build_final_report(definitions, daily_raw, statements_raw)
        save_json(os.path.join(OUT_DIR, f"{ticker.lower()}_final_deep_report.json"), report)
        print_final_report(ticker, report)

    print("\n완료")
    print(f"출력 폴더: {OUT_DIR}")
    print("다음에 볼 파일:")
    print(" - msft_final_deep_report.json")
    print(" - avgo_final_deep_report.json")


if __name__ == "__main__":
    main()
