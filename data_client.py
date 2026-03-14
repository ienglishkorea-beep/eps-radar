import datetime as dt
import json
import os
import time
from io import StringIO
from pathlib import Path
from typing import Dict, Optional, List, Any

import pandas as pd
import requests

USER_AGENT = {"User-Agent": "Mozilla/5.0"}
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")

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
_FUND_STMT_CACHE: Dict[str, Optional[dict]] = {}
_QUOTE_INFO_CACHE: Dict[str, Optional[dict]] = {}
_SUMMARY_CACHE: Dict[str, Optional[dict]] = {}
_SP1500_CACHE: Optional[list] = None

UNIVERSE_TTL_HOURS = 24
DAILY_META_TTL_HOURS = 24
FUND_META_TTL_HOURS = 24 * 7
FUND_STMT_TTL_HOURS = 24 * 7

DEBUG_TIINGO = os.getenv("DEBUG_TIINGO", "0").strip() == "1"
DEBUG_TIINGO_SAMPLE_TICKERS = {
    x.strip().upper()
    for x in os.getenv("DEBUG_TIINGO_SAMPLE_TICKERS", "MSFT,NVDA,AMZN,GOOGL,META").split(",")
    if x.strip()
}


def safe_float(x) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
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


def _debug_enabled_for_ticker(ticker: str) -> bool:
    if not DEBUG_TIINGO:
        return False
    return ticker.upper() in DEBUG_TIINGO_SAMPLE_TICKERS


def _print_debug_block(title: str, ticker: str, payload: Any) -> None:
    if not _debug_enabled_for_ticker(ticker):
        return

    print(f"DEBUG {title} {ticker}")

    if payload is None:
        print("  payload: None")
        return

    if isinstance(payload, dict):
        top_keys = list(payload.keys())[:50]
        print(f"  type: dict")
        print(f"  top_keys: {top_keys}")

        for k, v in list(payload.items())[:15]:
            if isinstance(v, dict):
                print(f"  {k}: dict keys={list(v.keys())[:25]}")
            elif isinstance(v, list):
                preview = v[:2]
                print(f"  {k}: list len={len(v)} preview={preview}")
            else:
                print(f"  {k}: {v}")
        return

    if isinstance(payload, list):
        print(f"  type: list len={len(payload)}")
        preview = payload[:2]
        print(f"  preview: {preview}")
        return

    print(f"  type: {type(payload).__name__}")
    print(f"  value: {payload}")


def _json_path(directory: Path, ticker: str, suffix: str = "") -> Path:
    name = f"{ticker.upper()}{suffix}.json" if suffix else f"{ticker.upper()}.json"
    return directory / name


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

    if _debug_enabled_for_ticker(ticker):
        if isinstance(data, list):
            preview = data[:2]
            print(f"DEBUG PRICE {ticker} rows={len(data)} preview={preview}")
        else:
            print(f"DEBUG PRICE {ticker} data_type={type(data).__name__}")

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
            if _debug_enabled_for_ticker(ticker):
                print(f"DEBUG PRICE {ticker} missing columns: {list(df.columns)}")
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
    except Exception as exc:
        if _debug_enabled_for_ticker(ticker):
            print(f"DEBUG PRICE {ticker} exception: {exc}")
        return pd.DataFrame()


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
# Tiingo metadata / fundamentals layer
# =========================================================

def _fetch_tiingo_daily_meta_remote(ticker: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}"
    data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
    _print_debug_block("DAILY_META_REMOTE", ticker, data)
    return data if isinstance(data, dict) else None


def _fetch_tiingo_fund_meta_remote(ticker: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None

    candidates = [
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/meta",
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}",
    ]

    for idx, url in enumerate(candidates, start=1):
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if _debug_enabled_for_ticker(ticker):
            print(f"DEBUG FUND_META_REMOTE {ticker} candidate={idx} url={url}")
            _print_debug_block("FUND_META_REMOTE_PAYLOAD", ticker, data)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            return data[0]
    return None


def _fetch_tiingo_fund_stmt_remote(ticker: str) -> Optional[dict]:
    if not TIINGO_API_KEY:
        return None

    candidates = [
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/statements",
        f"https://api.tiingo.com/tiingo/fundamentals/{ticker}/fundamentals",
    ]

    for idx, url in enumerate(candidates, start=1):
        data = _fetch_json_with_retry(url, headers=_tiingo_headers(), timeout=20)
        if _debug_enabled_for_ticker(ticker):
            print(f"DEBUG FUND_STMT_REMOTE {ticker} candidate={idx} url={url}")
            _print_debug_block("FUND_STMT_REMOTE_PAYLOAD", ticker, data)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and len(data) > 0:
            return {"rows": data}
    return None


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


def _fetch_tiingo_fundamentals_statements(ticker: str) -> Optional[dict]:
    return _load_or_refresh_json_cache(
        ticker=ticker,
        directory=FUND_CACHE_DIR,
        ttl_hours=FUND_STMT_TTL_HOURS,
        memory_cache=_FUND_STMT_CACHE,
        fetcher=_fetch_tiingo_fund_stmt_remote,
        file_suffix="_fund_stmt",
    )


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
    ticker = ticker.upper()
    if ticker in _QUOTE_INFO_CACHE:
        return _QUOTE_INFO_CACHE[ticker]

    daily_meta = _fetch_tiingo_daily_meta(ticker) or {}
    fund_meta = _fetch_tiingo_fundamentals_meta(ticker) or {}

    if _debug_enabled_for_ticker(ticker):
        _print_debug_block("DAILY_META_USED_FOR_QUOTE", ticker, daily_meta)
        _print_debug_block("FUND_META_USED_FOR_QUOTE", ticker, fund_meta)

    market_cap = _pick_first_number(
        {"daily_meta": daily_meta, "fund_meta": fund_meta},
        [
            ["fund_meta", "marketCap"],
            ["fund_meta", "market_cap"],
            ["fund_meta", "meta", "marketCap"],
            ["fund_meta", "meta", "market_cap"],
            ["daily_meta", "marketCap"],
            ["daily_meta", "market_cap"],
            ["daily_meta", "marketcap"],
            ["fund_meta", "marketcap"],
        ],
    )

    name = _pick_first_str(
        {"daily_meta": daily_meta, "fund_meta": fund_meta},
        [
            ["daily_meta", "name"],
            ["daily_meta", "ticker"],
            ["daily_meta", "description"],
            ["fund_meta", "name"],
            ["fund_meta", "companyName"],
            ["fund_meta", "company_name"],
            ["fund_meta", "meta", "name"],
            ["fund_meta", "meta", "companyName"],
            ["fund_meta", "meta", "company_name"],
        ],
    )

    if _debug_enabled_for_ticker(ticker):
        print(f"DEBUG QUOTE_INFO_RESULT {ticker} market_cap={market_cap} name={name}")

    if market_cap is None and name is None:
        _QUOTE_INFO_CACHE[ticker] = None
        return None

    out = {"market_cap": market_cap, "name": name}
    _QUOTE_INFO_CACHE[ticker] = out
    return out


def get_quote_summary_modules(ticker: str) -> Optional[dict]:
    ticker = ticker.upper()
    if ticker in _SUMMARY_CACHE:
        return _SUMMARY_CACHE[ticker]

    daily_meta = _fetch_tiingo_daily_meta(ticker) or {}
    fund_meta = _fetch_tiingo_fundamentals_meta(ticker) or {}
    fund_stmt = _fetch_tiingo_fundamentals_statements(ticker) or {}

    if _debug_enabled_for_ticker(ticker):
        _print_debug_block("SUMMARY_DAILY_META", ticker, daily_meta)
        _print_debug_block("SUMMARY_FUND_META", ticker, fund_meta)
        _print_debug_block("SUMMARY_FUND_STMT", ticker, fund_stmt)

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

    value = _pick_first_number(
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
            ["fund_stmt", "metrics", "revenueGrowth"],
            ["fund_stmt", "metrics", "revenue_growth"],
        ],
    )

    if DEBUG_TIINGO and isinstance(summary, dict):
        ticker = _pick_first_str(summary, [["daily_meta", "ticker"], ["fund_meta", "ticker"]]) or "UNKNOWN"
        if _debug_enabled_for_ticker(ticker):
            print(f"DEBUG REVENUE_GROWTH_RESULT {ticker} value={value}")

    return value


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
            ["fund_stmt", "metrics", "grossMargins"],
            ["fund_stmt", "metrics", "gross_margin"],
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
            ["fund_stmt", "metrics", "operatingMargins"],
            ["fund_stmt", "metrics", "operating_margin"],
        ],
    )

    if DEBUG_TIINGO and isinstance(summary, dict):
        ticker = _pick_first_str(summary, [["daily_meta", "ticker"], ["fund_meta", "ticker"]]) or "UNKNOWN"
        if _debug_enabled_for_ticker(ticker):
            print(
                f"DEBUG MARGIN_RESULT {ticker} gross_margin={gross_margin} "
                f"operating_margin={operating_margin}"
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
            ["fund_stmt", "sector"],
        ],
    )

    industry = _pick_first_str(
        summary,
        [
            ["fund_meta", "industry"],
            ["fund_meta", "meta", "industry"],
            ["daily_meta", "industry"],
            ["fund_stmt", "industry"],
        ],
    )

    if DEBUG_TIINGO and isinstance(summary, dict):
        ticker = _pick_first_str(summary, [["daily_meta", "ticker"], ["fund_meta", "ticker"]]) or "UNKNOWN"
        if _debug_enabled_for_ticker(ticker):
            print(f"DEBUG PROFILE_RESULT {ticker} sector={sector} industry={industry}")

    return {"sector": sector, "industry": industry}


def get_earnings_date_note_from_summary(summary: dict) -> Optional[str]:
    if not isinstance(summary, dict):
        return None
    return _pick_first_str(
        summary,
        [
            ["fund_meta", "nextEarningsDate"],
            ["fund_meta", "next_earnings_date"],
            ["fund_meta", "meta", "nextEarningsDate"],
            ["fund_meta", "meta", "next_earnings_date"],
        ],
    )


# =========================================================
# Universe
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
