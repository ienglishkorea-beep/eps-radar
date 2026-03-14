import datetime as dt
import json
import os
import time
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

USER_AGENT = {"User-Agent": "Mozilla/5.0"}
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "").strip()

REQUEST_SLEEP = 0.05
REQUEST_RETRY = 3

BASE_DIR = Path(".")
CACHE_DIR = BASE_DIR / "cache"
PRICE_CACHE_DIR = CACHE_DIR / "prices"
META_CACHE_DIR = CACHE_DIR / "meta"
FUND_CACHE_DIR = CACHE_DIR / "fundamentals"
UNIVERSE_CACHE_DIR = CACHE_DIR / "universe"

for d in [CACHE_DIR, PRICE_CACHE_DIR, META_CACHE_DIR, FUND_CACHE_DIR, UNIVERSE_CACHE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

_PRICE_CACHE: Dict[str, pd.DataFrame] = {}
_DAILY_META_CACHE: Dict[str, Optional[dict]] = {}
_FUND_META_CACHE: Dict[str, Optional[dict]] = {}
_FUND_METRICS_CACHE: Dict[str, Optional[dict]] = {}
_FUND_STMT_CACHE: Dict[str, Optional[dict]] = {}
_FUND_DEF_CACHE: Dict[str, Optional[dict]] = {}
_QUOTE_INFO_CACHE: Dict[str, Optional[dict]] = {}
_SUMMARY_CACHE: Dict[str, Optional[dict]] = {}
_SP1500_CACHE: Optional[list] = None

UNIVERSE_TTL_HOURS = 24
DAILY_META_TTL_HOURS = 24
FUND_META_TTL_HOURS = 24 * 7
FUND_METRICS_TTL_HOURS = 24 * 2
FUND_STMT_TTL_HOURS = 24 * 7
FUND_DEF_TTL_HOURS = 24 * 30


# =========================================================
# basic utils
# =========================================================


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or pd.isna(x):
            return None
        return int(float(x))
    except Exception:
        return None


def _now_utc() -> dt.datetime:
    return dt.datetime.utcnow()


def _hours_since_mtime(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        mtime = dt.datetime.utcfromtimestamp(path.stat().st_mtime)
        return (_now_utc() - mtime).total_seconds() / 3600.0
    except Exception:
        return None


def _is_fresh(path: Path, ttl_hours: int) -> bool:
    age = _hours_since_mtime(path)
    if age is None:
        return False
    return age <= ttl_hours


def _fetch_json_with_retry(
    url: str,
    headers: Optional[dict] = None,
    timeout: int = 20,
) -> Optional[Any]:
    for _ in range(REQUEST_RETRY):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(REQUEST_SLEEP)
    return None


def _tiingo_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Token {TIINGO_API_KEY}",
    }


def _json_path(directory: Path, ticker: str, suffix: str = "") -> Path:
    if suffix:
        return directory / f"{ticker.upper()}{suffix}.json"
    return directory / f"{ticker.upper()}.json"


def _price_path(ticker: str) -> Path:
    return PRICE_CACHE_DIR / f"{ticker.upper()}.csv"


def _load_json(path: Path) -> Optional[Any]:
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_json(path: Path, payload: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass


def _load_or_refresh_json_cache(
    ticker: str,
    directory: Path,
    ttl_hours: int,
    memory_cache: Dict[str, Optional[dict]],
    fetcher,
    file_suffix: str = "",
) -> Optional[dict]:
    ticker = ticker.upper()
    cache_key = f"{ticker}{file_suffix}"
    if cache_key in memory_cache:
        return memory_cache[cache_key]

    path = _json_path(directory, ticker, suffix=file_suffix)

    if _is_fresh(path, ttl_hours):
        payload = _load_json(path)
        memory_cache[cache_key] = payload if isinstance(payload, dict) else None
        return memory_cache[cache_key]

    payload = fetcher(ticker)
    if isinstance(payload, dict):
        _save_json(path, payload)
        memory_cache[cache_key] = payload
        return payload

    stale = _load_json(path)
    memory_cache[cache_key] = stale if isinstance(stale, dict) else None
    return memory_cache[cache_key]


def _normalize_obj(obj: Any) -> Any:
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")
    if isinstance(obj, pd.Series):
        return obj.to_dict()
    return obj


def _to_records(obj: Any) -> List[dict]:
    obj = _normalize_obj(obj)

    if obj is None:
        return []

    if isinstance(obj, list):
        out = []
        for item in obj:
            item = _normalize_obj(item)
            if isinstance(item, dict):
                out.append(item)
        return out

    if isinstance(obj, dict):
        if "rows" in obj and isinstance(obj["rows"], list):
            return [x for x in obj["rows"] if isinstance(x, dict)]

        # dict of row objects
        row_like = []
        for _, v in obj.items():
            if isinstance(v, dict):
                row_like.append(v)
        if row_like:
            return row_like

        return [obj]

    return []


def _deep_get(payload: Any, keys: List[str]) -> Any:
    current = payload
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current


def _pick_first_number(payload: Any, key_candidates: List[List[str]]) -> Optional[float]:
    for path in key_candidates:
        val = _deep_get(payload, path)
        f = safe_float(val)
        if f is not None:
            return f
    return None


def _pick_first_str(payload: Any, key_candidates: List[List[str]]) -> Optional[str]:
    for path in key_candidates:
        val = _deep_get(payload, path)
        if val is None:
            continue
        try:
            s = str(val).strip()
            if s:
                return s
        except Exception:
            continue
    return None


def _coerce_date(value: Any) -> Optional[pd.Timestamp]:
    try:
        if value is None:
            return None
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_localize(None)
        return pd.Timestamp(ts)
    except Exception:
        return None


def _latest_dated_record(records: List[dict]) -> Optional[dict]:
    if not records:
        return None

    dated: List[Tuple[pd.Timestamp, dict]] = []

    date_keys = [
        "date",
        "asOfDate",
        "reportDate",
        "calendarDate",
        "statementDate",
        "filingDate",
    ]

    for r in records:
        found = None
        for k in date_keys:
            ts = _coerce_date(r.get(k))
            if ts is not None:
                found = ts
                break
        if found is not None:
            dated.append((found, r))

    if dated:
        dated.sort(key=lambda x: x[0], reverse=True)
        return dated[0][1]

    return records[0]


def _search_number_in_records(
    records: List[dict],
    keys: List[str],
    prefer_latest: bool = True,
) -> Optional[float]:
    if not records:
        return None

    scan = records
    if prefer_latest:
        latest = _latest_dated_record(records)
        if latest is not None:
            scan = [latest] + [r for r in records if r is not latest]

    for r in scan:
        for k in keys:
            val = r.get(k)
            f = safe_float(val)
            if f is not None:
                return f
    return None


def _search_str_in_records(
    records: List[dict],
    keys: List[str],
) -> Optional[str]:
    if not records:
        return None

    latest = _latest_dated_record(records)
    scan = [latest] + [r for r in records if r is not latest] if latest is not None else records

    for r in scan:
        for k in keys:
            val = r.get(k)
            if val is None:
                continue
            s = str(val).strip()
            if s:
                return s
    return None


def _extract_statement_value(
    records: List[dict],
    line_keys: List[str],
    period_priority: List[str],
) -> Optional[float]:
    if not records:
        return None

    # 1) exact period preference
    for period in period_priority:
        period_rows = [
            r for r in records
            if str(r.get("frequency", "")).lower() == period.lower()
            or str(r.get("periodType", "")).lower() == period.lower()
            or str(r.get("statementType", "")).lower() == period.lower()
            or str(r.get("dimension", "")).lower() == period.lower()
        ]
        if period_rows:
            latest = _latest_dated_record(period_rows)
            if latest is not None:
                for k in line_keys:
                    f = safe_float(latest.get(k))
                    if f is not None:
                        return f

    # 2) latest any-period
    latest = _latest_dated_record(records)
    if latest is not None:
        for k in line_keys:
            f = safe_float(latest.get(k))
            if f is not None:
                return f

    # 3) brute force
    for r in records:
        for k in line_keys:
            f = safe_float(r.get(k))
            if f is not None:
                return f

    return None


def _latest_and_prior_year_same_freq(
    records: List[dict],
    line_keys: List[str],
) -> Tuple[Optional[float], Optional[float]]:
    if not records:
        return None, None

    quarterly_like = []
    for r in records:
        freq = str(r.get("frequency", "")).lower()
        ptype = str(r.get("periodType", "")).lower()
        dim = str(r.get("dimension", "")).lower()
        if any(x in [freq, ptype, dim] for x in ["quarter", "q", "quarterly"]):
            quarterly_like.append(r)

    if not quarterly_like:
        quarterly_like = records

    dated_rows = []
    for r in quarterly_like:
        ts = (
            _coerce_date(r.get("date"))
            or _coerce_date(r.get("asOfDate"))
            or _coerce_date(r.get("reportDate"))
            or _coerce_date(r.get("calendarDate"))
            or _coerce_date(r.get("statementDate"))
        )
        if ts is not None:
            dated_rows.append((ts, r))

    if len(dated_rows) < 5:
        latest = _latest_dated_record(quarterly_like)
        if latest is None:
            return None, None
        latest_val = None
        for k in line_keys:
            latest_val = safe_float(latest.get(k))
            if latest_val is not None:
                break
        return latest_val, None

    dated_rows.sort(key=lambda x: x[0], reverse=True)

    latest_row = dated_rows[0][1]
    prior_row = dated_rows[4][1]

    latest_val = None
    prior_val = None

    for k in line_keys:
        latest_val = safe_float(latest_row.get(k))
        if latest_val is not None:
            break

    for k in line_keys:
        prior_val = safe_float(prior_row.get(k))
        if prior_val is not None:
            break

    return latest_val, prior_val


def _calc_yoy_growth(current: Optional[float], prior: Optional[float]) -> Optional[float]:
    if current is None or prior in [None, 0]:
        return None
    try:
        return (current / prior) - 1.0
    except Exception:
        return None


# =========================================================
# Tiingo price layer
# =========================================================


def _fetch_tiingo_prices_range(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    if not TIINGO_API_KEY:
        return pd.DataFrame()

    url = (
        f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
        f"?startDate={start_date}&endDate={end_date}"
    )
    data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
    if not data:
        return pd.DataFrame()

    try:
        df = pd.DataFrame(data)
        if df.empty:
            return pd.DataFrame()

        keep_map = {
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }

        cols = [c for c in keep_map if c in df.columns]
        if len(cols) < 6:
            return pd.DataFrame()

        df = df[cols].rename(columns=keep_map).copy()

        need = ["date", "open", "high", "low", "close", "volume"]
        if any(c not in df.columns for c in need):
            return pd.DataFrame()

        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=need).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def _read_price_csv(path: Path) -> pd.DataFrame:
    try:
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_csv(path)
        if df.empty:
            return pd.DataFrame()
        need = ["date", "open", "high", "low", "close", "volume"]
        if any(c not in df.columns for c in need):
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=need).sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def _write_price_csv(path: Path, df: pd.DataFrame) -> None:
    try:
        df.to_csv(path, index=False)
    except Exception:
        pass


def download_price_history(ticker: str, period: str = "1y") -> pd.DataFrame:
    cache_key = f"{ticker.upper()}|{period}"
    cached = _PRICE_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()

    path = _price_path(ticker)
    existing = _read_price_csv(path)

    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=400)

    if existing.empty:
        fetched = _fetch_tiingo_prices_range(
            ticker=ticker,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        if not fetched.empty:
            _write_price_csv(path, fetched)
            _PRICE_CACHE[cache_key] = fetched.copy()
            return fetched.copy()
        _PRICE_CACHE[cache_key] = pd.DataFrame()
        return pd.DataFrame()

    existing = existing[existing["date"] >= pd.Timestamp(start_date)].copy()
    existing = existing.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)

    last_date = existing["date"].max().date() if not existing.empty else None
    if last_date is None:
        fetched = _fetch_tiingo_prices_range(
            ticker=ticker,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )
        if not fetched.empty:
            _write_price_csv(path, fetched)
            _PRICE_CACHE[cache_key] = fetched.copy()
            return fetched.copy()
        _PRICE_CACHE[cache_key] = existing.copy()
        return existing.copy()

    if last_date < end_date:
        inc_start = last_date + dt.timedelta(days=1)
        fetched_new = _fetch_tiingo_prices_range(
            ticker=ticker,
            start_date=inc_start.isoformat(),
            end_date=end_date.isoformat(),
        )
        if not fetched_new.empty:
            merged = pd.concat([existing, fetched_new], ignore_index=True)
            merged = merged.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
            _write_price_csv(path, merged)
            _PRICE_CACHE[cache_key] = merged.copy()
            return merged.copy()

    _PRICE_CACHE[cache_key] = existing.copy()
    return existing.copy()


def build_price_metrics(hist: pd.DataFrame) -> Dict[str, Optional[float]]:
    if hist is None or hist.empty:
        return {
            "price": None,
            "ma50": None,
            "ma200": None,
            "high_52w": None,
            "volume": None,
            "avg_volume_3m": None,
            "high_20d": None,
        }

    close = hist["close"].copy()
    high = hist["high"].copy()
    volume = hist["volume"].copy()

    ma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else None
    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else None
    high_52w = high.tail(252).max() if len(high) >= 252 else high.max()
    avg_volume_3m = volume.tail(63).mean() if len(volume) >= 20 else None
    high_20d = high.tail(20).max() if len(high) >= 20 else None

    return {
        "price": safe_float(close.iloc[-1]),
        "ma50": safe_float(ma50),
        "ma200": safe_float(ma200),
        "high_52w": safe_float(high_52w),
        "volume": safe_float(volume.iloc[-1]),
        "avg_volume_3m": safe_float(avg_volume_3m),
        "high_20d": safe_float(high_20d),
    }


# =========================================================
# Tiingo fundamentals layer
# =========================================================


def _fetch_tiingo_daily_meta_remote(ticker: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}"
    data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
    return data if isinstance(data, dict) else None


def _fetch_tiingo_fund_meta_remote(ticker: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None

    candidates = [
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/meta",
        f"https://api.tiingo.com/tiingo/fundamentals/meta?tickers={ticker}",
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}",
    ]

    for url in candidates:
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                return {"rows": data}
    return None


def _fetch_tiingo_fund_metrics_remote(ticker: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None

    candidates = [
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/daily",
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/metrics",
        f"https://api.tiingo.com/tiingo/fundamentals/metrics?tickers={ticker}",
    ]

    for url in candidates:
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                return {"rows": data}
    return None


def _fetch_tiingo_fund_stmt_remote(ticker: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None

    candidates = [
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/statements",
        f"https://api.tiingo.com/tiingo/fundamentals/statements?tickers={ticker}",
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/fundamentals",
    ]

    for url in candidates:
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                return {"rows": data}
    return None


def _fetch_tiingo_fund_def_remote(_: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None

    candidates = [
        "https://api.tiingo.com/tiingo/fundamentals/definitions",
        "https://api.tiingo.com/tiingo/fundamentals/fields",
    ]

    for url in candidates:
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and len(data) > 0:
            if isinstance(data[0], dict):
                return {"rows": data}
    return None


def _fetch_tiingo_daily_meta(ticker: str) -> Optional[dict]:
    return _load_or_refresh_json_cache(
        ticker=ticker,
        directory=META_CACHE_DIR,
        ttl_hours=DAILY_META_TTL_HOURS,
        memory_cache=_DAILY_META_CACHE,
        fetcher=_fetch_tiingo_daily_meta_remote,
        file_suffix="_daily_meta",
    )


def _fetch_tiingo_fundamentals_meta(ticker: str) -> Optional[dict]:
    return _load_or_refresh_json_cache(
        ticker=ticker,
        directory=FUND_CACHE_DIR,
        ttl_hours=FUND_META_TTL_HOURS,
        memory_cache=_FUND_META_CACHE,
        fetcher=_fetch_tiingo_fund_meta_remote,
        file_suffix="_fund_meta",
    )


def _fetch_tiingo_fundamentals_metrics(ticker: str) -> Optional[dict]:
    return _load_or_refresh_json_cache(
        ticker=ticker,
        directory=FUND_CACHE_DIR,
        ttl_hours=FUND_METRICS_TTL_HOURS,
        memory_cache=_FUND_METRICS_CACHE,
        fetcher=_fetch_tiingo_fund_metrics_remote,
        file_suffix="_fund_metrics",
    )


def _fetch_tiingo_fundamentals_statements(ticker: str) -> Optional[dict]:
    return _load_or_refresh_json_cache(
        ticker=ticker,
        directory=FUND_CACHE_DIR,
        ttl_hours=FUND_STMT_TTL_HOURS,
        memory_cache=_FUND_STMT_CACHE,
        fetcher=_fetch_tiingo_fund_stmt_remote,
        file_suffix="_fund_stmt",
    )


def _fetch_tiingo_fundamentals_definitions() -> Optional[dict]:
    # single shared file
    key = "__definitions__"
    if key in _FUND_DEF_CACHE:
        return _FUND_DEF_CACHE[key]

    path = FUND_CACHE_DIR / "definitions.json"

    if _is_fresh(path, FUND_DEF_TTL_HOURS):
        payload = _load_json(path)
        _FUND_DEF_CACHE[key] = payload if isinstance(payload, dict) else None
        return _FUND_DEF_CACHE[key]

    payload = _fetch_tiingo_fund_def_remote("ALL")
    if isinstance(payload, dict):
        _save_json(path, payload)
        _FUND_DEF_CACHE[key] = payload
        return payload

    stale = _load_json(path)
    _FUND_DEF_CACHE[key] = stale if isinstance(stale, dict) else None
    return _FUND_DEF_CACHE[key]


def _extract_sector_industry(
    daily_meta: dict,
    fund_meta: dict,
) -> Tuple[Optional[str], Optional[str]]:
    payload = {
        "daily_meta": daily_meta or {},
        "fund_meta": fund_meta or {},
    }

    sector = _pick_first_str(
        payload,
        [
            ["fund_meta", "sector"],
            ["fund_meta", "meta", "sector"],
            ["daily_meta", "sector"],
            ["fund_meta", "rows", "sector"],
        ],
    )

    industry = _pick_first_str(
        payload,
        [
            ["fund_meta", "industry"],
            ["fund_meta", "meta", "industry"],
            ["daily_meta", "industry"],
            ["fund_meta", "rows", "industry"],
        ],
    )

    if sector is None or industry is None:
        meta_rows = _to_records(fund_meta)
        if sector is None:
            sector = _search_str_in_records(meta_rows, ["sector", "sectorName", "gicsSector"])
        if industry is None:
            industry = _search_str_in_records(meta_rows, ["industry", "industryName", "gicsIndustry"])

    return sector, industry


def _extract_market_cap(
    daily_meta: dict,
    fund_meta: dict,
    fund_metrics: dict,
) -> Optional[float]:
    payload = {
        "daily_meta": daily_meta or {},
        "fund_meta": fund_meta or {},
        "fund_metrics": fund_metrics or {},
    }

    direct = _pick_first_number(
        payload,
        [
            ["fund_metrics", "marketCap"],
            ["fund_metrics", "market_cap"],
            ["fund_metrics", "marketcap"],
            ["fund_meta", "marketCap"],
            ["fund_meta", "market_cap"],
            ["fund_meta", "marketcap"],
            ["daily_meta", "marketCap"],
            ["daily_meta", "market_cap"],
            ["daily_meta", "marketcap"],
        ],
    )
    if direct is not None:
        return direct

    metric_rows = _to_records(fund_metrics)
    found = _search_number_in_records(
        metric_rows,
        [
            "marketCap",
            "market_cap",
            "marketcap",
            "marketCapitalization",
            "market_capitalization",
        ],
    )
    if found is not None:
        return found

    meta_rows = _to_records(fund_meta)
    found = _search_number_in_records(
        meta_rows,
        [
            "marketCap",
            "market_cap",
            "marketcap",
            "marketCapitalization",
            "market_capitalization",
        ],
    )
    if found is not None:
        return found

    return None


def _statement_line_keys() -> Dict[str, List[str]]:
    return {
        "revenue": [
            "revenue",
            "totalRevenue",
            "total_revenue",
            "revenues",
            "salesRevenueNet",
            "sales_revenue_net",
        ],
        "gross_profit": [
            "grossProfit",
            "gross_profit",
            "grossIncome",
            "gross_income",
        ],
        "operating_income": [
            "operatingIncome",
            "operating_income",
            "incomeFromOperations",
            "operatingProfit",
            "ebit",
        ],
    }


def _extract_financial_base_fields(
    fund_stmt: dict,
) -> Dict[str, Optional[float]]:
    stmt_rows = _to_records(fund_stmt)
    keys = _statement_line_keys()

    revenue_latest, revenue_prior = _latest_and_prior_year_same_freq(stmt_rows, keys["revenue"])
    gross_profit_latest = _extract_statement_value(stmt_rows, keys["gross_profit"], ["quarter", "q", "quarterly", "annual", "fy"])
    operating_income_latest = _extract_statement_value(stmt_rows, keys["operating_income"], ["quarter", "q", "quarterly", "annual", "fy"])

    revenue_growth = _calc_yoy_growth(revenue_latest, revenue_prior)

    gross_margin = None
    if gross_profit_latest is not None and revenue_latest not in [None, 0]:
        try:
            gross_margin = gross_profit_latest / revenue_latest
        except Exception:
            gross_margin = None

    operating_margin = None
    if operating_income_latest is not None and revenue_latest not in [None, 0]:
        try:
            operating_margin = operating_income_latest / revenue_latest
        except Exception:
            operating_margin = None

    return {
        "revenue_latest": revenue_latest,
        "revenue_prior_yoy_base": revenue_prior,
        "revenue_growth": revenue_growth,
        "gross_profit_latest": gross_profit_latest,
        "operating_income_latest": operating_income_latest,
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
    }


# =========================================================
# public outputs used by radar
# =========================================================


def get_quote_info(ticker: str) -> Optional[dict]:
    ticker = ticker.upper()
    if ticker in _QUOTE_INFO_CACHE:
        return _QUOTE_INFO_CACHE[ticker]

    daily_meta = _fetch_tiingo_daily_meta(ticker) or {}
    fund_meta = _fetch_tiingo_fundamentals_meta(ticker) or {}
    fund_metrics = _fetch_tiingo_fundamentals_metrics(ticker) or {}

    market_cap = _extract_market_cap(daily_meta, fund_meta, fund_metrics)
    name = _pick_first_str(
        {"daily_meta": daily_meta, "fund_meta": fund_meta},
        [
            ["daily_meta", "name"],
            ["daily_meta", "description"],
            ["daily_meta", "ticker"],
            ["fund_meta", "name"],
            ["fund_meta", "companyName"],
            ["fund_meta", "company_name"],
        ],
    )

    if market_cap is None and name is None:
        _QUOTE_INFO_CACHE[ticker] = None
        return None

    out = {
        "market_cap": market_cap,
        "name": name,
    }
    _QUOTE_INFO_CACHE[ticker] = out
    return out


def get_quote_summary_modules(ticker: str) -> Optional[dict]:
    ticker = ticker.upper()
    if ticker in _SUMMARY_CACHE:
        return _SUMMARY_CACHE[ticker]

    daily_meta = _fetch_tiingo_daily_meta(ticker) or {}
    fund_meta = _fetch_tiingo_fundamentals_meta(ticker) or {}
    fund_metrics = _fetch_tiingo_fundamentals_metrics(ticker) or {}
    fund_stmt = _fetch_tiingo_fundamentals_statements(ticker) or {}
    fund_defs = _fetch_tiingo_fundamentals_definitions() or {}

    if not daily_meta and not fund_meta and not fund_metrics and not fund_stmt:
        _SUMMARY_CACHE[ticker] = None
        return None

    derived = _extract_financial_base_fields(fund_stmt)

    out = {
        "_source": "tiingo",
        "ticker": ticker,
        "daily_meta": daily_meta,
        "fund_meta": fund_meta,
        "fund_metrics": fund_metrics,
        "fund_stmt": fund_stmt,
        "fund_defs": fund_defs,
        "derived": derived,
    }
    _SUMMARY_CACHE[ticker] = out
    return out


def get_eps_estimate_from_summary(summary: dict) -> Optional[float]:
    return None


def get_profile_from_summary(summary: dict) -> Dict[str, Optional[str]]:
    if not isinstance(summary, dict):
        return {"sector": None, "industry": None}

    sector, industry = _extract_sector_industry(
        daily_meta=summary.get("daily_meta") or {},
        fund_meta=summary.get("fund_meta") or {},
    )
    return {
        "sector": sector,
        "industry": industry,
    }


def get_revenue_growth_from_summary(summary: dict) -> Optional[float]:
    if not isinstance(summary, dict):
        return None

    derived = summary.get("derived") or {}
    direct = safe_float(derived.get("revenue_growth"))
    if direct is not None:
        return direct

    payload = summary
    return _pick_first_number(
        payload,
        [
            ["fund_metrics", "revenueGrowth"],
            ["fund_metrics", "revenue_growth"],
            ["fund_meta", "revenueGrowth"],
            ["fund_meta", "revenue_growth"],
        ],
    )


def get_margin_data_from_summary(summary: dict) -> Dict[str, Optional[float]]:
    if not isinstance(summary, dict):
        return {"gross_margin": None, "operating_margin": None}

    derived = summary.get("derived") or {}

    gross_margin = safe_float(derived.get("gross_margin"))
    operating_margin = safe_float(derived.get("operating_margin"))

    if gross_margin is None:
        gross_margin = _pick_first_number(
            summary,
            [
                ["fund_metrics", "grossMargin"],
                ["fund_metrics", "gross_margin"],
                ["fund_meta", "grossMargin"],
                ["fund_meta", "gross_margin"],
            ],
        )

    if operating_margin is None:
        operating_margin = _pick_first_number(
            summary,
            [
                ["fund_metrics", "operatingMargin"],
                ["fund_metrics", "operating_margin"],
                ["fund_meta", "operatingMargin"],
                ["fund_meta", "operating_margin"],
            ],
        )

    return {
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
    }


def get_earnings_date_note_from_summary(summary: dict) -> Optional[str]:
    if not isinstance(summary, dict):
        return None

    return _pick_first_str(
        summary,
        [
            ["fund_meta", "nextEarningsDate"],
            ["fund_meta", "next_earnings_date"],
            ["daily_meta", "nextEarningsDate"],
            ["daily_meta", "next_earnings_date"],
        ],
    )


# =========================================================
# universe
# =========================================================


def get_sp1500_tickers() -> list:
    global _SP1500_CACHE
    if _SP1500_CACHE is not None:
        return list(_SP1500_CACHE)

    universe_path = UNIVERSE_CACHE_DIR / "sp1500.csv"

    if _is_fresh(universe_path, UNIVERSE_TTL_HOURS):
        try:
            df = pd.read_csv(universe_path)
            tickers = [str(t).replace(".", "-").strip().upper() for t in df["ticker"].tolist()]
            _SP1500_CACHE = sorted(list(set(tickers)))
            return list(_SP1500_CACHE)
        except Exception:
            pass

    urls = [
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
    ]

    tickers = []
    for url in urls:
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        tables = pd.read_html(StringIO(r.text))
        symbols = tables[0]["Symbol"].tolist()
        tickers.extend(symbols)

    tickers = [str(t).replace(".", "-").strip().upper() for t in tickers]
    tickers = sorted(list(set(tickers)))

    try:
        pd.DataFrame({"ticker": tickers}).to_csv(universe_path, index=False)
    except Exception:
        pass

    _SP1500_CACHE = tickers
    return list(_SP1500_CACHE)
