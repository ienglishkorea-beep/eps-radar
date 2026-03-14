import json
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests

# =========================================================
# Tiingo direct code test inside EPS Radar
# ---------------------------------------------------------
# 목적:
# 1) definitions에서 dataCode 후보 확인
# 2) daily 응답에서 해당 code를 직접 조회
# 3) statements 응답에서 해당 code를 직접 조회
# 4) 실제 값 샘플이 나오는지 최종 확인
# =========================================================

PROBE_TICKERS = ["MSFT", "AVGO"]
LOOKBACK_DAYS = 730
OUT_DIR = "tiingo_probe_output"
REQUEST_TIMEOUT = 30

# 최종 확인용 code 후보
FIELD_CANDIDATES: Dict[str, List[str]] = {
    "gross_margin": ["grossMargin"],
    "operating_margin": ["opMargin"],
    "net_margin": ["netMargin"],
    "pe": ["peRatio"],
    "enterprise_value": ["enterpriseVal"],
    "market_cap": ["marketCap"],

    # statements / daily 후보
    "revenue": ["revenue", "totalRevenue"],
    "revenue_growth": ["revenueGrowth", "salesGrowth", "revenueYoYGrowth", "totalRevenueGrowth"],
    "roic": ["roic", "returnOnInvestedCapital"],
    "ev_sales": ["evToSales", "enterpriseToRevenue", "evSales"],
    "ev_ebitda": ["evToEbitda", "evEbitda"],
    "ev_ebit": ["evToEbit", "evEbit"],

    "cfo": ["cashFromOperations", "operatingCashFlow", "ncfo"],
    "cfo_growth": ["cashFromOperationsGrowth", "operatingCashFlowGrowth"],

    "capex": ["capex", "capitalExpenditure"],
    "capex_growth": ["capexGrowth", "capitalExpenditureGrowth"],

    "debt": [
        "debt",
        "totalDebt",
        "debtNonCurrent",
        "debtCurrent",
        "longTermDebt",
        "longTermDebtTotal",
    ],

    "gross_profit": ["grossProfit"],
    "operating_income": ["opinc", "operatingIncome", "ebit"],
    "net_income": ["netIncm", "netIncome", "netIncmComStock"],
    "free_cash_flow": ["freeCashFlow"],
}


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def save_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


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
    code_set = {c.lower() for c in codes}
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


def extract_daily_samples(daily_raw: Any, codes: List[str], max_samples: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}

    if not isinstance(daily_raw, list):
        return result

    for code in codes:
        samples: List[Dict[str, Any]] = []
        for row in daily_raw:
            if not isinstance(row, dict):
                continue
            if code in row and row.get(code) is not None:
                samples.append(
                    {
                        "date": row.get("date"),
                        "value": row.get(code),
                    }
                )
            if len(samples) >= max_samples:
                break

        if samples:
            result[code] = samples

    return result


def collect_statement_objects(obj: Any, out: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    if out is None:
        out = []

    if isinstance(obj, dict):
        out.append(obj)
        for v in obj.values():
            collect_statement_objects(v, out)
    elif isinstance(obj, list):
        for item in obj:
            collect_statement_objects(item, out)

    return out


def extract_statement_samples(statements_raw: Any, codes: List[str], max_samples: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    objects = collect_statement_objects(statements_raw)

    for code in codes:
        samples: List[Dict[str, Any]] = []

        for obj in objects:
            if code in obj and obj.get(code) is not None:
                sample = {
                    "value": obj.get(code),
                }
                # 기간/날짜 계열 있으면 같이 기록
                for key in ["date", "reportDate", "fiscalDate", "year", "quarter"]:
                    if key in obj:
                        sample[key] = obj.get(key)
                samples.append(sample)

            if len(samples) >= max_samples:
                break

        if samples:
            result[code] = samples

    return result


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
        for s in samples:
            print(f"      {s}")


def build_final_report(
    definitions: List[Dict[str, Any]],
    daily_raw: Any,
    statements_raw: Any,
) -> Dict[str, Any]:
    report: Dict[str, Any] = {}

    for field_name, codes in FIELD_CANDIDATES.items():
        defs = find_definitions_by_code(definitions, codes)
        daily_samples = extract_daily_samples(daily_raw, codes)
        statement_samples = extract_statement_samples(statements_raw, codes)

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
    print(f"[FINAL DIRECT CODE REPORT] {ticker}")
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
        save_json(os.path.join(OUT_DIR, f"{ticker.lower()}_final_direct_report.json"), report)
        print_final_report(ticker, report)

    print("\n완료")
    print(f"출력 폴더: {OUT_DIR}")
    print("다음에 볼 파일:")
    print(" - msft_final_direct_report.json")
    print(" - avgo_final_direct_report.json")


if __name__ == "__main__":
    main()
