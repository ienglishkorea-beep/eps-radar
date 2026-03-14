import datetime as dt
import os
import time
from io import StringIO
from typing import Dict, Optional

import pandas as pd
import requests
import yfinance as yf

USER_AGENT = {"User-Agent": "Mozilla/5.0"}
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")

REQUEST_SLEEP = 0.10
REQUEST_RETRY = 3


def safe_float(x) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def _fetch_json_with_retry(url: str, headers: Optional[dict] = None, timeout: int = 20) -> Optional[dict]:
    for _ in range(REQUEST_RETRY):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(REQUEST_SLEEP)
    return None


# =========================================================
# Tiingo 1차 / Yahoo 2차
# =========================================================

def _fetch_tiingo_prices(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    if not TIINGO_API_KEY:
        return pd.DataFrame()

    url = (
        f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
        f"?startDate={start_date}&endDate={end_date}"
    )
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {TIINGO_API_KEY}",
    }

    for _ in range(REQUEST_RETRY):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            if r.status_code != 200:
                time.sleep(REQUEST_SLEEP)
                continue

            data = r.json()
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame(data)
            if df.empty:
                return pd.DataFrame()

            rename_map = {
                "date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            }
            keep = [c for c in rename_map if c in df.columns]
            if not keep:
                return pd.DataFrame()

            df = df[keep].rename(columns=rename_map).copy()

            need = ["date", "open", "high", "low", "close", "volume"]
            if any(c not in df.columns for c in need):
                return pd.DataFrame()

            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
            for c in ["open", "high", "low", "close", "volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            df = df.dropna(subset=need).sort_values("date").reset_index(drop=True)
            if df.empty:
                return pd.DataFrame()

            return df

        except Exception:
            time.sleep(REQUEST_SLEEP)

    return pd.DataFrame()


def _fetch_yfinance_prices(ticker: str, period: str = "1y") -> pd.DataFrame:
    try:
        df = yf.download(
            ticker,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        if df is None or df.empty:
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0] for c in df.columns]

        df = df.reset_index()
        df = df.rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )

        need = ["date", "open", "high", "low", "close", "volume"]
        if any(col not in df.columns for col in need):
            return pd.DataFrame()

        df = df[need].copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.dropna(subset=need).sort_values("date").reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def download_price_history(ticker: str, period: str = "1y") -> pd.DataFrame:
    """
    강제 우선순위:
    1) Tiingo
    2) Yahoo fallback
    """
    end_date = dt.date.today()

    if period == "1y":
        start_date = end_date - dt.timedelta(days=400)
    else:
        start_date = end_date - dt.timedelta(days=400)

    # 1차: Tiingo
    tiingo_df = _fetch_tiingo_prices(
        ticker=ticker,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    if not tiingo_df.empty:
        return tiingo_df

    # 2차: Yahoo fallback
    return _fetch_yfinance_prices(ticker=ticker, period=period)


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
# Yahoo summary / quote fallback layer
# =========================================================

def get_quote_summary_modules(ticker: str) -> Optional[dict]:
    modules = "earningsTrend,financialData,assetProfile,calendarEvents"
    url = (
        f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
        f"{ticker}?modules={modules}"
    )
    data = _fetch_json_with_retry(url, headers=USER_AGENT, timeout=20)
    if not data:
        return None

    try:
        result = data.get("quoteSummary", {}).get("result")
        if not result:
            return None
        return result[0]
    except Exception:
        return None


def get_quote_info(ticker: str) -> Optional[dict]:
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
    data = _fetch_json_with_retry(url, headers=USER_AGENT, timeout=20)
    if not data:
        return None

    try:
        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            return None

        q = results[0]
        return {
            "market_cap": q.get("marketCap"),
            "name": q.get("shortName"),
        }
    except Exception:
        return None


def get_eps_estimate_from_summary(summary: dict) -> Optional[float]:
    try:
        trends = summary.get("earningsTrend", {}).get("trend", [])
        for target_period in ["+1y", "0y"]:
            for trend in trends:
                if trend.get("period") == target_period:
                    current = trend.get("epsTrend", {}).get("current", {}).get("raw")
                    if current is not None:
                        return float(current)
        return None
    except Exception:
        return None


def get_revenue_growth_from_summary(summary: dict) -> Optional[float]:
    try:
        fd = summary.get("financialData", {})
        growth = fd.get("revenueGrowth")
        if isinstance(growth, dict):
            growth = growth.get("raw")
        return float(growth) if growth is not None else None
    except Exception:
        return None


def get_margin_data_from_summary(summary: dict) -> Dict[str, Optional[float]]:
    try:
        fd = summary.get("financialData", {})

        gross_margin = fd.get("grossMargins", {})
        operating_margin = fd.get("operatingMargins", {})

        if isinstance(gross_margin, dict):
            gross_margin = gross_margin.get("raw")
        if isinstance(operating_margin, dict):
            operating_margin = operating_margin.get("raw")

        return {
            "gross_margin": float(gross_margin) if gross_margin is not None else None,
            "operating_margin": float(operating_margin) if operating_margin is not None else None,
        }
    except Exception:
        return {"gross_margin": None, "operating_margin": None}


def get_profile_from_summary(summary: dict) -> Dict[str, Optional[str]]:
    try:
        ap = summary.get("assetProfile", {})
        return {
            "sector": ap.get("sector"),
            "industry": ap.get("industry"),
        }
    except Exception:
        return {"sector": None, "industry": None}


def get_earnings_date_note_from_summary(summary: dict) -> Optional[str]:
    try:
        ce = summary.get("calendarEvents", {})
        earnings = ce.get("earnings", {})
        dates = earnings.get("earningsDate", [])
        if not dates:
            return None
        first = dates[0]
        raw = first.get("raw") if isinstance(first, dict) else None
        if raw is None:
            return None
        return dt.datetime.utcfromtimestamp(raw).strftime("%Y-%m-%d")
    except Exception:
        return None


def get_sp1500_tickers() -> list:
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
    return sorted(list(set(tickers)))
