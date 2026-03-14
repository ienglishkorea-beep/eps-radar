import json
import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import requests


# =========================================================
# Tiingo Fundamental Probe Only
# ---------------------------------------------------------
# 목적:
# - EPS 레이더 repo 안에서 바로 Tiingo fundamentals field 존재 확인
# - requirements.txt 수정 없이 requests로 직접 호출
# - data_client.py 수정 없음
# - 스캐너 본체 재구축 전 "필드가 실제로 오는지"만 확인
# =========================================================

PROBE_TICKERS = ["MSFT", "AVGO"]
LOOKBACK_DAYS = 730
OUT_DIR = "tiingo_probe_output"
REQUEST_TIMEOUT = 30

TARGETS: Dict[str, List[str]] = {
    "revenue_growth": ["revenue growth", "sales growth", "yoy revenue", "yoy sales"],
    "gross_margin": ["gross margin", "gross profit margin"],
    "operating_margin": ["operating margin", "ebit margin", "ebita margin"],
    "net_margin": ["net income margin", "net margin"],
    "roic": ["roic", "return on invested capital"],
    "pe": ["pe ratio", "price earnings", "price / earnings"],
    "ev_sales": ["ev/sales", "enterprise value sales"],
    "ev_ebitda": ["ev/ebitda", "enterprise value ebitda"],
    "ev_ebit": ["ev/ebit", "enterprise value ebit"],
    "cfo": ["cash from operations", "operating cash flow"],
    "cfo_growth": ["cash from operations growth", "operating cash flow growth"],
    "capex": ["capital expenditure", "capex"],
    "capex_growth": ["capital expenditure growth", "capex growth"],
    "debt": ["total debt", "debt"],
    "enterprise_value": ["enterprise value"],
    "market_cap": ["market cap", "market capitalization"],
    "gross_profit": ["gross profit"],
    "operating_income": ["operating income", "ebit"],
    "net_income": ["net income"],
}


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


def any_match(texts: List[str], keywords: List[str]) -> bool:
    joined = " | ".join([norm(t) for t in texts if t is not None])
    return any(norm(k) in joined for k in keywords)


def flatten_keys(obj: Any, prefix: str = "", out: Optional[List[str]] = None) -> List[str]:
    if out is None:
        out = []

    if isinstance(obj, dict):
        for k, v in obj.items():
            key_path = f"{prefix}.{k}" if prefix else str(k)
            out.append(key_path)
            flatten_keys(v, key_path, out)
    elif isinstance(obj, list):
        for i, item in enumerate(obj[:100]):
            key_path = f"{prefix}[{i}]"
            out.append(key_path)
            flatten_keys(item, key_path, out)

    return out


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

    headers = {
        "Content-Type": "application/json",
    }

    resp = requests.get(
        url,
        headers=headers,
        params=final_params,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_definitions(token: str) -> List[Dict[str, Any]]:
    # Tiingo fundamentals definitions
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

    # daily fundamentals
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

    # statements fundamentals
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


def find_definition_matches(definitions: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    matches: Dict[str, List[Dict[str, Any]]] = {k: [] for k in TARGETS.keys()}

    for row in definitions:
        texts = [
            row.get("dataCode"),
            row.get("name"),
            row.get("description"),
            row.get("statementType"),
            row.get("units"),
        ]

        for target_name, keywords in TARGETS.items():
            if any_match(texts, keywords):
                matches[target_name].append(
                    {
                        "dataCode": row.get("dataCode"),
                        "name": row.get("name"),
                        "statementType": row.get("statementType"),
                        "units": row.get("units"),
                        "description": row.get("description"),
                    }
                )

    for key, rows in matches.items():
        seen: Set[Tuple[Any, Any]] = set()
        deduped = []
        for row in rows:
            row_key = (row.get("dataCode"), row.get("name"))
            if row_key in seen:
                continue
            seen.add(row_key)
            deduped.append(row)
        matches[key] = deduped

    return matches


def summarize_presence(
    daily_raw: Any,
    statements_raw: Any,
    matched_definitions: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    report: Dict[str, Dict[str, Any]] = {}

    daily_paths = flatten_keys(daily_raw)
    statements_paths = flatten_keys(statements_raw)

    daily_paths_norm = [norm(x) for x in daily_paths]
    statements_paths_norm = [norm(x) for x in statements_paths]

    for target_name, defs in matched_definitions.items():
        candidate_codes = [d.get("dataCode") for d in defs if d.get("dataCode")]
        candidate_names = [d.get("name") for d in defs if d.get("name")]
        search_terms = candidate_codes + candidate_names + TARGETS[target_name]

        daily_hits = []
        statements_hits = []

        for path, pnorm in zip(daily_paths, daily_paths_norm):
            if any(norm(term) in pnorm for term in search_terms if term):
                daily_hits.append(path)

        for path, pnorm in zip(statements_paths, statements_paths_norm):
            if any(norm(term) in pnorm for term in search_terms if term):
                statements_hits.append(path)

        report[target_name] = {
            "candidate_dataCodes": candidate_codes,
            "candidate_names": candidate_names,
            "daily_hits": daily_hits[:50],
            "statements_hits": statements_hits[:50],
            "daily_present": len(daily_hits) > 0,
            "statements_present": len(statements_hits) > 0,
        }

    return report


def print_definition_report(matches: Dict[str, List[Dict[str, Any]]]) -> None:
    print("\n" + "=" * 100)
    print("[TIINGO DEFINITIONS MATCH REPORT]")
    print("=" * 100)

    for target_name, rows in matches.items():
        print(f"\n[{target_name}]")
        if not rows:
            print("  - no definition match")
            continue
        for row in rows[:10]:
            print(
                f"  - dataCode={row.get('dataCode')} | "
                f"name={row.get('name')} | "
                f"statementType={row.get('statementType')} | "
                f"units={row.get('units')}"
            )


def print_presence_report(ticker: str, report: Dict[str, Dict[str, Any]]) -> None:
    print("\n" + "=" * 100)
    print(f"[TIINGO PRESENCE REPORT] {ticker}")
    print("=" * 100)

    for target_name, info in report.items():
        status = []
        if info["daily_present"]:
            status.append("daily")
        if info["statements_present"]:
            status.append("statements")
        status_text = ",".join(status) if status else "missing"

        print(f"\n[{target_name}] -> {status_text}")
        if info["candidate_dataCodes"]:
            print("  candidate_dataCodes:")
            for x in info["candidate_dataCodes"][:10]:
                print(f"    - {x}")
        if info["daily_hits"]:
            print("  daily_hits:")
            for x in info["daily_hits"][:10]:
                print(f"    - {x}")
        if info["statements_hits"]:
            print("  statements_hits:")
            for x in info["statements_hits"][:10]:
                print(f"    - {x}")


def main() -> None:
    ensure_dir(OUT_DIR)
    token = get_tiingo_token()

    definitions = fetch_definitions(token)
    definition_matches = find_definition_matches(definitions)

    save_json(os.path.join(OUT_DIR, "definitions_raw.json"), definitions)
    save_json(os.path.join(OUT_DIR, "definitions_matches.json"), definition_matches)
    print_definition_report(definition_matches)

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

        presence = summarize_presence(daily_raw, statements_raw, definition_matches)
        save_json(os.path.join(OUT_DIR, f"{ticker.lower()}_presence_report.json"), presence)
        print_presence_report(ticker, presence)

    print("\n완료")
    print(f"출력 폴더: {OUT_DIR}")
    print("다음에 볼 파일:")
    print(" - definitions_matches.json")
    print(" - msft_presence_report.json")
    print(" - avgo_presence_report.json")


if __name__ == "__main__":
    main()
