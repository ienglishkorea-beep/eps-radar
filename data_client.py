import datetime as dt
import os
import time
from io import StringIO
from typing import Dict, Optional, List, Any

import pandas as pd
import requests

USER_AGENT = {"User-Agent": "Mozilla/5.0"}
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")

REQUEST_SLEEP = 0.05
REQUEST_RETRY = 3

_PRICE_CACHE: Dict[str, pd.DataFrame] = {}
_DAILY_META_CACHE: Dict[str, Optional[dict]] = {}
_FUND_META_CACHE: Dict[str, Optional[dict]] = {}
_FUND_STMT_CACHE: Dict[str, Optional[dict]] = {}
_QUOTE_INFO_CACHE: Dict[str, Optional[dict]] = {}
_SUMMARY_CACHE: Dict[str, Optional[dict]] = {}
_SP1500_CACHE: Optional[list] = None


def safe_float(x) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


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


# =========================================================
# Tiingo price layer
# =========================================================

def _fetch_tiingo_prices(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
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

        df = df.dropna(subset=need).sort_values("date").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def download_price_history(ticker: str, period: str = "1y") -> pd.DataFrame:
    cache_key = f"{ticker}|{period}"
    cached = _PRICE_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()

    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=400)

    df = _fetch_tiingo_prices(
        ticker=ticker,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    _PRICE_CACHE[cache_key] = df.copy()
    return df.copy()


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
# Tiingo metadata / fundamentals layer
# =========================================================

def _fetch_tiingo_daily_meta(ticker: str) -> Optional[dict]:
    if ticker in _DAILY_META_CACHE:
        return _DAILY_META_CACHE[ticker]

    if not TIINGO_API_KEY:
        _DAILY_META_CACHE[ticker] = None
        return None

    url = f"https://api.tiingo.com/tiingo/daily/{ticker}"
    data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
    if not isinstance(data, dict):
        _DAILY_META_CACHE[ticker] = None
        return None

    _DAILY_META_CACHE[ticker] = data
    return data


def _fetch_tiingo_fundamentals_meta(ticker: str) -> Optional[dict]:
    if ticker in _FUND_META_CACHE:
        return _FUND_META_CACHE[ticker]

    if not TIINGO_API_KEY:
        _FUND_META_CACHE[ticker] = None
        return None

    candidates = [
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/meta",
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}",
    ]

    out = None
    for url in candidates:
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if isinstance(data, dict):
            out = data
            break
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            out = data[0]
            break

    _FUND_META_CACHE[ticker] = out
    return out


def _fetch_tiingo_fundamentals_statements(ticker: str) -> Optional[dict]:
    if ticker in _FUND_STMT_CACHE:
        return _FUND_STMT_CACHE[ticker]

    if not TIINGO_API_KEY:
        _FUND_STMT_CACHE[ticker] = None
        return None

    candidates = [
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/statements",
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/fundamentals",
    ]

    out = None
    for url in candidates:
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if isinstance(data, dict):
            out = data
            break
        if isinstance(data, list) and len(data) > 0:
            out = {"rows": data}
            break

    _FUND_STMT_CACHE[ticker] = out
    return out


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


def get_quote_info(ticker: str) -> Optional[dict]:
    if ticker in _QUOTE_INFO_CACHE:
        return _QUOTE_INFO_CACHE[ticker]

    daily_meta = _fetch_tiingo_daily_meta(ticker) or {}
    fund_meta = _fetch_tiingo_fundamentals_meta(ticker) or {}

    market_cap = _pick_first_number(
        {"daily_meta": daily_meta, "fund_meta": fund_meta},
        [
            ["fund_meta", "marketCap"],
            ["fund_meta", "market_cap"],
            ["fund_meta", "meta", "marketCap"],
            ["fund_meta", "meta", "market_cap"],
            ["daily_meta", "marketCap"],
            ["daily_meta", "market_cap"],
        ],
    )

    name = _pick_first_str(
        {"daily_meta": daily_meta, "fund_meta": fund_meta},
        [
            ["daily_meta", "name"],
            ["daily_meta", "description"],
            ["fund_meta", "name"],
            ["fund_meta", "companyName"],
            ["fund_meta", "company_name"],
            ["fund_meta", "meta", "name"],
            ["fund_meta", "meta", "companyName"],
            ["fund_meta", "meta", "company_name"],
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
    if ticker in _SUMMARY_CACHE:
        return _SUMMARY_CACHE[ticker]

    daily_meta = _fetch_tiingo_daily_meta(ticker) or {}
    fund_meta = _fetch_tiingo_fundamentals_meta(ticker) or {}
    fund_stmt = _fetch_tiingo_fundamentals_statements(ticker) or {}

    if not daily_meta and not fund_meta and not fund_stmt:
        _SUMMARY_CACHE[ticker] = None
        return None

    out = {
        "_source": "tiingo",
        "daily_meta": daily_meta,
        "fund_meta": fund_meta,
        "fund_stmt": fund_stmt,
    }
    _SUMMARY_CACHE[ticker] = out
    return out


def get_eps_estimate_from_summary(summary: dict) -> Optional[float]:
    return None


def get_revenue_growth_from_summary(summary: dict) -> Optional[float]:
    if not isinstance(summary, dict):
        return None

    return _pick_first_number(
        summary,
        [
            ["fund_meta", "revenueGrowth"],
            ["fund_meta", "revenue_growth"],
            ["fund_meta", "revenueGrowthYoY"],
            ["fund_meta", "revenue_growth_yoy"],
            ["fund_meta", "metrics", "revenueGrowth"],
            ["fund_meta", "metrics", "revenue_growth"],
            ["fund_stmt", "revenueGrowth"],
            ["fund_stmt", "revenue_growth"],
        ],
    )


def get_margin_data_from_summary(summary: dict) -> Dict[str, Optional[float]]:
    if not isinstance(summary, dict):
        return {"gross_margin": None, "operating_margin": None}

    gross_margin = _pick_first_number(
        summary,
        [
            ["fund_meta", "grossMargins"],
            ["fund_meta", "gross_margin"],
            ["fund_meta", "grossMarginsRatio"],
            ["fund_meta", "metrics", "grossMargins"],
            ["fund_meta", "metrics", "gross_margin"],
            ["fund_stmt", "grossMargins"],
            ["fund_stmt", "gross_margin"],
        ],
    )

    operating_margin = _pick_first_number(
        summary,
        [
            ["fund_meta", "operatingMargins"],
            ["fund_meta", "operating_margin"],
            ["fund_meta", "metrics", "operatingMargins"],
            ["fund_meta", "metrics", "operating_margin"],
            ["fund_stmt", "operatingMargins"],
            ["fund_stmt", "operating_margin"],
        ],
    )

    return {
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
    }


def get_profile_from_summary(summary: dict) -> Dict[str, Optional[str]]:
    if not isinstance(summary, dict):
        return {"sector": None, "industry": None}

    sector = _pick_first_str(
        summary,
        [
            ["fund_meta", "sector"],
            ["fund_meta", "meta", "sector"],
            ["daily_meta", "sector"],
        ],
    )

    industry = _pick_first_str(
        summary,
        [
            ["fund_meta", "industry"],
            ["fund_meta", "meta", "industry"],
            ["daily_meta", "industry"],
        ],
    )

    return {
        "sector": sector,
        "industry": industry,
    }


def get_earnings_date_note_from_summary(summary: dict) -> Optional[str]:
    if not isinstance(summary, dict):
        return None

    raw = _pick_first_str(
        summary,
        [
            ["fund_meta", "nextEarningsDate"],
            ["fund_meta", "next_earnings_date"],
            ["fund_meta", "meta", "nextEarningsDate"],
            ["fund_meta", "meta", "next_earnings_date"],
        ],
    )

    if raw:
        return raw

    return None


# =========================================================
# Universe
# =========================================================

def get_sp1500_tickers() -> list:
    global _SP1500_CACHE

    if _SP1500_CACHE is not None:
        return list(_SP1500_CACHE)

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
    _SP1500_CACHE = sorted(list(set(tickers)))
    return list(_SP1500_CACHE)
