import datetime as dt
import time
from io import StringIO
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf

USER_AGENT = {"User-Agent": "Mozilla/5.0"}

# =========================================================
# EPS RADAR v2.1
# ---------------------------------------------------------
# 철학
# - EPS revision 기반 기업 변화 탐지
# - guidance / earnings call / conference call 제거
# - 완전 정량 시스템
#
# 핵심 축
# 1) EPS revision count
# 2) EPS revision speed
# 3) EPS revision magnitude
# 4) EPS revision consistency
# 5) RS acceleration (3M / 6M)
# 6) Supply dry-up
# 7) Industry leadership
# 8) Entry quality
# 9) RS line high
# 10) Tight structure / VCP readiness
#
# 출력 등급
# - ULTRA
# - STRONG
# - WATCH
#
# 별도 태그
# - multibagger_potential
# =========================================================

# -----------------------------
# Universe / liquidity
# -----------------------------
MIN_MARKET_CAP = 2_000_000_000
MIN_PRICE = 10.0
MIN_AVG_DOLLAR_VOLUME = 10_000_000

# -----------------------------
# EPS revision
# -----------------------------
LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 2
REV_SPEED_LOOKBACK_DAYS = 30
REV_SPEED_THRESHOLD = 2

# revision magnitude
REV_MAG_WEAK = 0.01
REV_MAG_MEANINGFUL = 0.03
REV_MAG_STRONG = 0.05
REV_MAG_VERY_STRONG = 0.10

# revision consistency
REV_CONSISTENCY_STRONG = 0.70
REV_CONSISTENCY_VERY_STRONG = 0.85

# -----------------------------
# Growth / trend
# -----------------------------
MIN_REVENUE_GROWTH = 0.15
MIN_HIGH_PROXIMITY = 0.80
RELAXED_HIGH_PROXIMITY = 0.72
AUTO_RELAX_IF_FINAL_LT = 5
MIN_VOLUME_RATIO = 1.0
MIN_RS_OVER_SPY = 0.00

# -----------------------------
# Quality
# -----------------------------
QUALITY_REVENUE_GROWTH = 0.20
QUALITY_GROSS_MARGIN = 0.45
QUALITY_OPERATING_MARGIN = 0.15

# -----------------------------
# Structure / supply
# -----------------------------
SUPPLY_DRYUP_VOL_RATIO_MAX = 0.85
SUPPLY_DRYUP_RANGE_RATIO_MAX = 0.85
TIGHT_RANGE_10D_MAX = 0.08
STRICT_TIGHT_RANGE_10D_MAX = 0.06
MAX_EXTENSION_FROM_MA50 = 0.25
VCP_BASE_LOOKBACK = 60
VCP_MAX_BASE_DEPTH = 0.30

# -----------------------------
# Industry leadership
# -----------------------------
MIN_INDUSTRY_SIZE = 3
TOP_INDUSTRY_RANK_RATIO = 0.20

# -----------------------------
# Runtime
# -----------------------------
REQUEST_SLEEP = 0.10

# -----------------------------
# Sector ETF mapping
# -----------------------------
SECTOR_MAP = {
    "Technology": "XLK",
    "Semiconductors": "SMH",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Healthcare": "XLV",
    "Financial Services": "XLF",
    "Financial": "XLF",
    "Industrials": "XLI",
    "Energy": "XLE",
    "Basic Materials": "XLB",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Communication Services": "XLC",
}

OUTPUT_COLS = [
    "ticker",
    "grade",
    "multibagger_potential",
    "signal_stage",
    "action",
    "revision_count",
    "rev_speed_count_30d",
    "rev_speed_tag",
    "rev_magnitude_sum_90d",
    "rev_magnitude_max_90d",
    "rev_magnitude_tag",
    "revision_consistency",
    "revision_consistency_tag",
    "growth_accel_tag",
    "growth_accel_proxy",
    "quality_proxy_tag",
    "score",
    "revenue_growth",
    "gross_margin",
    "operating_margin",
    "price",
    "entry_price",
    "stop_price",
    "high_20d",
    "market_cap",
    "sector",
    "industry",
    "sector_etf",
    "ret_6m",
    "sector_ret_6m",
    "spy_ret_6m",
    "ret_3m",
    "sector_ret_3m",
    "spy_ret_3m",
    "ma50",
    "ma200",
    "high_52w",
    "high_proximity",
    "volume",
    "avg_volume_3m",
    "volume_ratio",
    "avg_dollar_volume",
    "industry_avg_ret_6m",
    "industry_avg_ret_3m",
    "industry_rank",
    "industry_rank_ratio",
    "industry_size",
    "industry_breadth_pct",
    "rs_line_high",
    "supply_dryup",
    "supply_dryup_vol_ratio",
    "supply_dryup_range_ratio",
    "tight_structure",
    "tight_range_10d",
    "entry_quality_tag",
    "vcp_ready",
    "vcp_score",
    "base_depth",
    "earnings_date_note",
]

# =========================================================
# Utility
# =========================================================


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def safe_float(x) -> Optional[float]:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def pct_change(a: float, b: float) -> Optional[float]:
    if a in [None, 0] or b is None:
        return None
    try:
        return (b / a) - 1.0
    except Exception:
        return None


def now_ts() -> pd.Timestamp:
    return pd.Timestamp(dt.date.today())


# =========================================================
# Universe
# =========================================================


def get_sp1500_tickers() -> List[str]:
    urls = [
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
    ]

    tickers: List[str] = []

    for url in urls:
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        tables = pd.read_html(StringIO(r.text))
        symbols = tables[0]["Symbol"].tolist()
        tickers.extend(symbols)

    tickers = [str(t).replace(".", "-").strip().upper() for t in tickers]
    return sorted(list(set(tickers)))


# =========================================================
# Yahoo raw fetch
# =========================================================


def get_quote_summary_modules(ticker: str) -> Optional[dict]:
    try:
        modules = "earningsTrend,financialData,assetProfile,calendarEvents"
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
            f"{ticker}?modules={modules}"
        )
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        data = r.json()
        result = data.get("quoteSummary", {}).get("result")
        if not result:
            return None
        return result[0]
    except Exception:
        return None


def get_quote_info(ticker: str) -> Optional[dict]:
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        data = r.json()

        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            return None

        q = results[0]
        return {
            "price": q.get("regularMarketPrice"),
            "market_cap": q.get("marketCap"),
            "ma50": q.get("fiftyDayAverage"),
            "ma200": q.get("twoHundredDayAverage"),
            "high_52w": q.get("fiftyTwoWeekHigh"),
            "volume": q.get("regularMarketVolume"),
            "avg_volume_3m": q.get("averageDailyVolume3Month"),
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


# =========================================================
# Price history / structure
# =========================================================


def download_price_history(ticker: str, period: str = "1y") -> pd.DataFrame:
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
        df["date"] = pd.to_datetime(df["date"])
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna().reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def compute_return_from_history(df: pd.DataFrame, lookback_days: int) -> Optional[float]:
    if df.empty or len(df) < lookback_days + 1:
        return None
    start_price = safe_float(df["close"].iloc[-(lookback_days + 1)])
    end_price = safe_float(df["close"].iloc[-1])
    return pct_change(start_price, end_price)


def compute_high_20d(df: pd.DataFrame) -> Optional[float]:
    if df.empty or len(df) < 20:
        return None
    return safe_float(df["high"].tail(20).max())


def compute_range_ratio(df: pd.DataFrame, window: int) -> Optional[float]:
    if df.empty or len(df) < window:
        return None
    high_n = safe_float(df["high"].tail(window).max())
    low_n = safe_float(df["low"].tail(window).min())
    if high_n in [None, 0] or low_n is None:
        return None
    return (high_n - low_n) / high_n


def compute_supply_dryup(df: pd.DataFrame) -> Tuple[bool, Optional[float], Optional[float]]:
    if df.empty or len(df) < 30:
        return False, None, None

    vol_5d = safe_float(df["volume"].tail(5).mean())
    vol_20d = safe_float(df["volume"].tail(20).mean())
    if vol_20d in [None, 0]:
        vol_ratio = None
    else:
        vol_ratio = vol_5d / vol_20d

    range_10d = compute_range_ratio(df, 10)
    range_30d = compute_range_ratio(df, 30)
    if range_10d is None or range_30d in [None, 0]:
        range_ratio = None
    else:
        range_ratio = range_10d / range_30d

    is_ok = (
        vol_ratio is not None
        and range_ratio is not None
        and vol_ratio <= SUPPLY_DRYUP_VOL_RATIO_MAX
        and range_ratio <= SUPPLY_DRYUP_RANGE_RATIO_MAX
    )
    return bool(is_ok), vol_ratio, range_ratio


def compute_rs_line_high(stock_df: pd.DataFrame, spy_df: pd.DataFrame, lookback: int = 126) -> bool:
    if stock_df.empty or spy_df.empty:
        return False

    merged = stock_df[["date", "close"]].merge(
        spy_df[["date", "close"]].rename(columns={"close": "spy_close"}),
        on="date",
        how="inner",
    )
    if merged.empty or len(merged) < lookback:
        return False

    merged["rs_line"] = merged["close"] / merged["spy_close"]
    current = safe_float(merged["rs_line"].iloc[-1])
    prev_high = safe_float(merged["rs_line"].tail(lookback).iloc[:-1].max())
    if current is None or prev_high is None:
        return False
    return current >= prev_high


def compute_entry_quality_tag(
    price: float,
    ma50: float,
    ma200: float,
    high_52w: float,
    high_20d: float,
    tight_range_10d: Optional[float],
) -> str:
    if (
        price is not None
        and ma50 is not None
        and ma200 is not None
        and high_52w not in [None, 0]
        and high_20d not in [None, 0]
        and price > ma50 > ma200
    ):
        high_proximity = price / high_52w
        dist_20d = price / high_20d
        extension_ma50 = (price / ma50) - 1.0

        if (
            high_proximity >= 0.90
            and dist_20d >= 0.95
            and extension_ma50 <= MAX_EXTENSION_FROM_MA50
            and tight_range_10d is not None
            and tight_range_10d <= TIGHT_RANGE_10D_MAX
        ):
            return "READY"
        return "SETUP"
    return "LOOSE"


def compute_vcp_features(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if df.empty or len(df) < VCP_BASE_LOOKBACK:
        return {
            "vcp_ready": False,
            "vcp_score": 0.0,
            "tight_range_10d": None,
            "base_depth": None,
        }

    base = df.tail(VCP_BASE_LOOKBACK).copy()
    high_base = safe_float(base["high"].max())
    low_base = safe_float(base["low"].min())
    if high_base in [None, 0] or low_base is None:
        return {
            "vcp_ready": False,
            "vcp_score": 0.0,
            "tight_range_10d": None,
            "base_depth": None,
        }

    base_depth = (high_base - low_base) / high_base
    tight_range_10d = compute_range_ratio(df, 10)
    supply_dryup, _, _ = compute_supply_dryup(df)

    price = safe_float(df["close"].iloc[-1])
    ma50 = safe_float(df["close"].rolling(50).mean().iloc[-1])
    ma200 = safe_float(df["close"].rolling(200).mean().iloc[-1]) if len(df) >= 200 else None
    high_52w = safe_float(df["high"].tail(252).max()) if len(df) >= 252 else safe_float(df["high"].max())

    near_high = False
    if price is not None and high_52w not in [None, 0]:
        near_high = (price / high_52w) >= 0.85

    price_stack_ok = (
        price is not None
        and ma50 is not None
        and ma200 is not None
        and price > ma50 > ma200
    )

    tight_ok = tight_range_10d is not None and tight_range_10d <= TIGHT_RANGE_10D_MAX
    base_ok = base_depth <= VCP_MAX_BASE_DEPTH

    vcp_ready = bool(price_stack_ok and near_high and tight_ok and supply_dryup and base_ok)

    score = 0.0
    if price_stack_ok:
        score += 1.0
    if near_high:
        score += 1.0
    if tight_ok:
        score += 1.0
    if supply_dryup:
        score += 1.0
    if base_depth <= 0.20:
        score += 1.0
    elif base_ok:
        score += 0.5

    return {
        "vcp_ready": vcp_ready,
        "vcp_score": round(score, 2),
        "tight_range_10d": round(tight_range_10d, 4) if tight_range_10d is not None else None,
        "base_depth": round(base_depth, 4),
    }


# =========================================================
# EPS history
# =========================================================


def load_history() -> pd.DataFrame:
    try:
        df = pd.read_csv("eps_history.csv")
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(
            columns=[
                "date",
                "ticker",
                "eps",
                "prev_eps",
                "revision_pct",
                "up_revision",
                "down_revision",
            ]
        )


# =========================================================
# Revision scoring / tags
# =========================================================


def get_signal_stage(revision_count: int) -> str:
    if revision_count == 2:
        return "INITIAL"
    elif revision_count == 3:
        return "EARLY"
    elif revision_count == 4:
        return "MID"
    return "LATE"


def get_action(revision_count: int) -> str:
    if revision_count in [2, 3]:
        return "BUY"
    elif revision_count == 4:
        return "WATCH"
    return "NO_ENTRY"


def get_rev_speed_tag(rev_speed_count_30d: int) -> str:
    if rev_speed_count_30d >= REV_SPEED_THRESHOLD:
        return "REV_ACCEL"
    return "NORMAL"


def get_revision_magnitude_tag(
    rev_magnitude_sum_90d: float,
    rev_magnitude_max_90d: float,
) -> str:
    if rev_magnitude_sum_90d >= REV_MAG_VERY_STRONG or rev_magnitude_max_90d >= REV_MAG_VERY_STRONG:
        return "VERY_STRONG"
    if rev_magnitude_sum_90d >= REV_MAG_STRONG or rev_magnitude_max_90d >= REV_MAG_STRONG:
        return "STRONG"
    if rev_magnitude_sum_90d >= REV_MAG_MEANINGFUL or rev_magnitude_max_90d >= REV_MAG_MEANINGFUL:
        return "MEANINGFUL"
    if rev_magnitude_sum_90d >= REV_MAG_WEAK or rev_magnitude_max_90d >= REV_MAG_WEAK:
        return "WEAK"
    return "NONE"


def get_revision_consistency_tag(revision_consistency: float) -> str:
    if revision_consistency >= REV_CONSISTENCY_VERY_STRONG:
        return "VERY_STRONG"
    if revision_consistency >= REV_CONSISTENCY_STRONG:
        return "STRONG"
    if revision_consistency >= 0.50:
        return "MIXED"
    return "WEAK"


def get_revision_magnitude_score(
    rev_magnitude_sum_90d: float,
    rev_magnitude_max_90d: float,
) -> float:
    if rev_magnitude_sum_90d >= REV_MAG_VERY_STRONG or rev_magnitude_max_90d >= REV_MAG_VERY_STRONG:
        return 1.00
    if rev_magnitude_sum_90d >= REV_MAG_STRONG or rev_magnitude_max_90d >= REV_MAG_STRONG:
        return 0.75
    if rev_magnitude_sum_90d >= REV_MAG_MEANINGFUL or rev_magnitude_max_90d >= REV_MAG_MEANINGFUL:
        return 0.50
    if rev_magnitude_sum_90d >= REV_MAG_WEAK or rev_magnitude_max_90d >= REV_MAG_WEAK:
        return 0.25
    return 0.00


def get_revision_consistency_score(revision_consistency: float) -> float:
    if revision_consistency >= REV_CONSISTENCY_VERY_STRONG:
        return 1.00
    if revision_consistency >= REV_CONSISTENCY_STRONG:
        return 0.60
    if revision_consistency >= 0.50:
        return 0.25
    return 0.00


# =========================================================
# Growth / quality / score
# =========================================================


def get_growth_accel_proxy(
    revision_count: int,
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
    stock_ret_3m: float,
    sector_ret_3m: float,
    spy_ret_3m: float,
) -> float:
    eps_part = 1.0 if revision_count == 2 else 0.9 if revision_count == 3 else 0.7 if revision_count == 4 else 0.5
    rev_part = 1.0 if revenue_growth >= 0.30 else 0.8 if revenue_growth >= 0.20 else 0.6 if revenue_growth >= 0.15 else 0.0
    breakout_part = 1.0 if high_proximity >= 0.95 else 0.7 if high_proximity >= 0.90 else 0.4 if high_proximity >= 0.85 else 0.0

    rs_6m_part = 1.0 if stock_ret_6m > sector_ret_6m > spy_ret_6m else 0.5 if stock_ret_6m > spy_ret_6m else 0.0
    rs_3m_part = 1.0 if stock_ret_3m > sector_ret_3m > spy_ret_3m else 0.5 if stock_ret_3m > spy_ret_3m else 0.0

    proxy = (
        0.30 * eps_part
        + 0.25 * rev_part
        + 0.15 * breakout_part
        + 0.15 * rs_6m_part
        + 0.15 * rs_3m_part
    )
    return round(proxy, 2)


def get_growth_accel_tag(
    revision_count: int,
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
    stock_ret_3m: float,
    sector_ret_3m: float,
    spy_ret_3m: float,
) -> str:
    rs_6m_strong = stock_ret_6m > sector_ret_6m > spy_ret_6m
    rs_3m_strong = stock_ret_3m > sector_ret_3m > spy_ret_3m

    if (
        revision_count in [2, 3]
        and revenue_growth >= 0.20
        and high_proximity >= 0.90
        and rs_6m_strong
        and rs_3m_strong
    ):
        return "ACCEL"
    elif revision_count in [2, 3] and revenue_growth >= 0.15:
        return "EARLY"
    return "NORMAL"


def get_quality_proxy_tag(
    revenue_growth: Optional[float],
    gross_margin: Optional[float],
    operating_margin: Optional[float],
) -> str:
    if (
        revenue_growth is not None
        and gross_margin is not None
        and operating_margin is not None
        and revenue_growth >= QUALITY_REVENUE_GROWTH
        and gross_margin >= QUALITY_GROSS_MARGIN
        and operating_margin >= QUALITY_OPERATING_MARGIN
    ):
        return "QUALITY"
    return "NORMAL"


def classify_grade(
    revision_count: int,
    rev_speed_count_30d: int,
    rev_magnitude_sum_90d: float,
    rev_magnitude_max_90d: float,
    revision_consistency: float,
    growth_accel_tag: str,
    quality_proxy_tag: str,
    high_proximity: float,
    volume_ratio: float,
    revenue_growth: float,
    rs_line_high: bool,
    supply_dryup: bool,
    tight_structure: bool,
    vcp_ready: bool,
    entry_quality_tag: str,
    industry_top: bool,
) -> str:
    mag_tag = get_revision_magnitude_tag(rev_magnitude_sum_90d, rev_magnitude_max_90d)
    consistency_tag = get_revision_consistency_tag(revision_consistency)

    if (
        revision_count in [2, 3]
        and rev_speed_count_30d >= 1
        and mag_tag in ["STRONG", "VERY_STRONG"]
        and consistency_tag in ["STRONG", "VERY_STRONG"]
        and growth_accel_tag == "ACCEL"
        and quality_proxy_tag == "QUALITY"
        and high_proximity >= 0.85
        and volume_ratio >= 1.2
        and revenue_growth >= 0.15
        and rs_line_high
        and supply_dryup
        and tight_structure
        and vcp_ready
        and entry_quality_tag == "READY"
        and industry_top
    ):
        return "ULTRA"

    if (
        revision_count >= 2
        and mag_tag in ["MEANINGFUL", "STRONG", "VERY_STRONG"]
        and consistency_tag in ["MIXED", "STRONG", "VERY_STRONG"]
        and growth_accel_tag in ["ACCEL", "EARLY"]
        and revenue_growth >= 0.15
        and entry_quality_tag in ["READY", "SETUP"]
    ):
        return "STRONG"

    return "WATCH"


def compute_score(
    revision_count: int,
    rev_speed_tag: str,
    rev_magnitude_sum_90d: float,
    rev_magnitude_max_90d: float,
    revision_consistency: float,
    revenue_growth: float,
    ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
    ret_3m: float,
    sector_ret_3m: float,
    spy_ret_3m: float,
    high_proximity: float,
    volume_ratio: float,
    price: float,
    ma50: float,
    ma200: float,
    rs_line_high: bool,
    supply_dryup: bool,
    tight_structure: bool,
    vcp_ready: bool,
    industry_top: bool,
) -> float:
    eps_score = 1.0 if revision_count == 2 else 0.9 if revision_count == 3 else 0.7 if revision_count == 4 else 0.5
    rev_score = 1.0 if revenue_growth >= 0.30 else 0.8 if revenue_growth >= 0.20 else 0.6 if revenue_growth >= 0.15 else 0.0

    trend_parts = 0.0
    if price > ma50:
        trend_parts += 0.20
    if price > ma200:
        trend_parts += 0.20

    rs_6m_strong = ret_6m > sector_ret_6m > spy_ret_6m
    rs_3m_strong = ret_3m > sector_ret_3m > spy_ret_3m
    if rs_6m_strong:
        trend_parts += 0.25
    elif ret_6m > spy_ret_6m:
        trend_parts += 0.10

    if rs_3m_strong:
        trend_parts += 0.25
    elif ret_3m > spy_ret_3m:
        trend_parts += 0.10

    if rs_line_high:
        trend_parts += 0.10

    trend_score = clamp01(trend_parts)

    prox_score = clamp01((high_proximity - 0.80) / 0.20)
    vol_score = clamp01((volume_ratio - 1.0) / 1.5)
    rev_speed_bonus = 1.0 if rev_speed_tag == "REV_ACCEL" else 0.0
    magnitude_score = get_revision_magnitude_score(rev_magnitude_sum_90d, rev_magnitude_max_90d)
    consistency_score = get_revision_consistency_score(revision_consistency)

    structure_score = 0.0
    if supply_dryup:
        structure_score += 0.30
    if tight_structure:
        structure_score += 0.25
    if vcp_ready:
        structure_score += 0.25
    if industry_top:
        structure_score += 0.20
    structure_score = clamp01(structure_score)

    score = (
        20 * eps_score
        + 16 * rev_score
        + 18 * trend_score
        + 10 * magnitude_score
        + 6 * consistency_score
        + 6 * rev_speed_bonus
        + 8 * prox_score
        + 4 * vol_score
        + 12 * structure_score
    )
    return round(score, 2)


# =========================================================
# Candidate build
# =========================================================


def compute_sector_returns_cached(etfs: List[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for etf in etfs:
        hist = download_price_history(etf, period="1y")
        out[etf] = {
            "ret_6m": compute_return_from_history(hist, 126),
            "ret_3m": compute_return_from_history(hist, 63),
        }
        time.sleep(REQUEST_SLEEP)
    return out


def build_candidate_row(
    ticker: str,
    revision_count: int,
    rev_speed_count_30d: int,
    rev_magnitude_sum_90d: float,
    rev_magnitude_max_90d: float,
    revision_consistency: float,
    spy_df: pd.DataFrame,
    spy_ret_6m: float,
    spy_ret_3m: float,
    sector_returns: Dict[str, dict],
    min_high_proximity: float,
) -> Optional[dict]:
    summary = get_quote_summary_modules(ticker)
    time.sleep(REQUEST_SLEEP)

    quote = get_quote_info(ticker)
    time.sleep(REQUEST_SLEEP)

    if summary is None or quote is None:
        return None

    profile = get_profile_from_summary(summary)
    sector = profile.get("sector")
    industry = profile.get("industry")
    sector_etf = SECTOR_MAP.get(sector) if sector else None
    if sector_etf is None:
        return None

    sector_ret_6m = sector_returns.get(sector_etf, {}).get("ret_6m")
    sector_ret_3m = sector_returns.get(sector_etf, {}).get("ret_3m")
    if sector_ret_6m is None or sector_ret_3m is None:
        return None

    price = safe_float(quote.get("price"))
    market_cap = safe_float(quote.get("market_cap"))
    ma50 = safe_float(quote.get("ma50"))
    ma200 = safe_float(quote.get("ma200"))
    high_52w = safe_float(quote.get("high_52w"))
    volume = safe_float(quote.get("volume"))
    avg_volume_3m = safe_float(quote.get("avg_volume_3m"))

    if (
        price is None
        or market_cap is None
        or ma50 is None
        or ma200 is None
        or high_52w is None
        or high_52w == 0
        or volume is None
        or avg_volume_3m in [None, 0]
    ):
        return None

    avg_dollar_volume = price * avg_volume_3m
    if market_cap < MIN_MARKET_CAP:
        return None
    if price < MIN_PRICE:
        return None
    if avg_dollar_volume < MIN_AVG_DOLLAR_VOLUME:
        return None

    revenue_growth = get_revenue_growth_from_summary(summary)
    if revenue_growth is None or revenue_growth < MIN_REVENUE_GROWTH:
        return None

    margin_data = get_margin_data_from_summary(summary)
    gross_margin = margin_data.get("gross_margin")
    operating_margin = margin_data.get("operating_margin")
    earnings_date_note = get_earnings_date_note_from_summary(summary)

    hist = download_price_history(ticker, period="1y")
    time.sleep(REQUEST_SLEEP)
    if hist.empty or len(hist) < 200:
        return None

    ret_6m = compute_return_from_history(hist, 126)
    ret_3m = compute_return_from_history(hist, 63)
    high_20d = compute_high_20d(hist)

    if ret_6m is None or ret_3m is None or high_20d is None:
        return None

    if price <= ma50:
        return None
    if price <= ma200:
        return None
    if (ret_6m - spy_ret_6m) <= MIN_RS_OVER_SPY:
        return None
    if sector_ret_6m <= spy_ret_6m:
        return None
    if ret_6m <= sector_ret_6m:
        return None

    high_proximity = price / high_52w
    if high_proximity < min_high_proximity:
        return None

    volume_ratio = volume / avg_volume_3m
    if volume_ratio < MIN_VOLUME_RATIO:
        return None

    supply_dryup, supply_dryup_vol_ratio, supply_dryup_range_ratio = compute_supply_dryup(hist)
    rs_line_high = compute_rs_line_high(hist, spy_df, lookback=126)
    vcp = compute_vcp_features(hist)
    tight_range_10d = vcp.get("tight_range_10d")
    tight_structure = bool(tight_range_10d is not None and tight_range_10d <= TIGHT_RANGE_10D_MAX)
    entry_quality_tag = compute_entry_quality_tag(
        price=price,
        ma50=ma50,
        ma200=ma200,
        high_52w=high_52w,
        high_20d=high_20d,
        tight_range_10d=tight_range_10d,
    )

    entry_price = max(high_20d * 1.01, price * 1.02)
    stop_price = entry_price * 0.85

    signal_stage = get_signal_stage(revision_count)
    action = get_action(revision_count)
    rev_speed_tag = get_rev_speed_tag(rev_speed_count_30d)
    rev_magnitude_tag = get_revision_magnitude_tag(rev_magnitude_sum_90d, rev_magnitude_max_90d)
    revision_consistency_tag = get_revision_consistency_tag(revision_consistency)

    growth_accel_proxy = get_growth_accel_proxy(
        revision_count=revision_count,
        revenue_growth=revenue_growth,
        high_proximity=high_proximity,
        stock_ret_6m=ret_6m,
        sector_ret_6m=sector_ret_6m,
        spy_ret_6m=spy_ret_6m,
        stock_ret_3m=ret_3m,
        sector_ret_3m=sector_ret_3m,
        spy_ret_3m=spy_ret_3m,
    )

    growth_accel_tag = get_growth_accel_tag(
        revision_count=revision_count,
        revenue_growth=revenue_growth,
        high_proximity=high_proximity,
        stock_ret_6m=ret_6m,
        sector_ret_6m=sector_ret_6m,
        spy_ret_6m=spy_ret_6m,
        stock_ret_3m=ret_3m,
        sector_ret_3m=sector_ret_3m,
        spy_ret_3m=spy_ret_3m,
    )

    quality_proxy_tag = get_quality_proxy_tag(
        revenue_growth=revenue_growth,
        gross_margin=gross_margin,
        operating_margin=operating_margin,
    )

    multibagger_potential = bool(
        revenue_growth >= 0.20
        and gross_margin is not None
        and operating_margin is not None
        and gross_margin >= 0.45
        and operating_margin >= 0.15
    )

    row = {
        "ticker": ticker,
        "grade": "WATCH",
        "multibagger_potential": multibagger_potential,
        "signal_stage": signal_stage,
        "action": action,
        "revision_count": revision_count,
        "rev_speed_count_30d": rev_speed_count_30d,
        "rev_speed_tag": rev_speed_tag,
        "rev_magnitude_sum_90d": rev_magnitude_sum_90d,
        "rev_magnitude_max_90d": rev_magnitude_max_90d,
        "rev_magnitude_tag": rev_magnitude_tag,
        "revision_consistency": revision_consistency,
        "revision_consistency_tag": revision_consistency_tag,
        "growth_accel_tag": growth_accel_tag,
        "growth_accel_proxy": growth_accel_proxy,
        "quality_proxy_tag": quality_proxy_tag,
        "score": None,
        "revenue_growth": revenue_growth,
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
        "price": price,
        "entry_price": round(entry_price, 2),
        "stop_price": round(stop_price, 2),
        "high_20d": round(high_20d, 2),
        "market_cap": market_cap,
        "sector": sector,
        "industry": industry,
        "sector_etf": sector_etf,
        "ret_6m": ret_6m,
        "sector_ret_6m": sector_ret_6m,
        "spy_ret_6m": spy_ret_6m,
        "ret_3m": ret_3m,
        "sector_ret_3m": sector_ret_3m,
        "spy_ret_3m": spy_ret_3m,
        "ma50": ma50,
        "ma200": ma200,
        "high_52w": high_52w,
        "high_proximity": high_proximity,
        "volume": volume,
        "avg_volume_3m": avg_volume_3m,
        "volume_ratio": volume_ratio,
        "avg_dollar_volume": avg_dollar_volume,
        "industry_avg_ret_6m": None,
        "industry_avg_ret_3m": None,
        "industry_rank": None,
        "industry_rank_ratio": None,
        "industry_size": None,
        "industry_breadth_pct": None,
        "rs_line_high": rs_line_high,
        "supply_dryup": supply_dryup,
        "supply_dryup_vol_ratio": supply_dryup_vol_ratio,
        "supply_dryup_range_ratio": supply_dryup_range_ratio,
        "tight_structure": tight_structure,
        "tight_range_10d": tight_range_10d,
        "entry_quality_tag": entry_quality_tag,
        "vcp_ready": bool(vcp.get("vcp_ready", False)),
        "vcp_score": float(vcp.get("vcp_score", 0.0)),
        "base_depth": vcp.get("base_depth"),
        "earnings_date_note": earnings_date_note,
    }
    return row


def enrich_with_industry_and_grade(final_df: pd.DataFrame) -> pd.DataFrame:
    if final_df.empty:
        return final_df

    industry_stats = (
        final_df.groupby("industry", dropna=False)
        .agg(
            industry_avg_ret_6m=("ret_6m", "mean"),
            industry_avg_ret_3m=("ret_3m", "mean"),
            industry_size=("ticker", "count"),
            industry_breadth_pct=("price", lambda s: 0.0),
        )
        .reset_index()
    )

    breadth_rows = []
    for industry_name, sub in final_df.groupby("industry", dropna=False):
        up = (sub["price"] > sub["ma50"]).sum()
        total = len(sub)
        pct = up / total if total > 0 else None
        breadth_rows.append(
            {
                "industry": industry_name,
                "industry_breadth_pct": pct,
            }
        )
    breadth_df = pd.DataFrame(breadth_rows)
    industry_stats = industry_stats.drop(columns=["industry_breadth_pct"]).merge(
        breadth_df,
        on="industry",
        how="left",
    )

    industry_stats = industry_stats.sort_values(
        ["industry_avg_ret_6m", "industry_avg_ret_3m"],
        ascending=[False, False],
    ).reset_index(drop=True)
    industry_stats["industry_rank"] = range(1, len(industry_stats) + 1)
    industry_stats["industry_rank_ratio"] = industry_stats["industry_rank"] / len(industry_stats)

    final_df = final_df.merge(industry_stats, on="industry", how="left", suffixes=("", "_y"))

    grades = []
    scores = []
    for _, row in final_df.iterrows():
        industry_top = bool(
            row["industry_size"] is not None
            and row["industry_size"] >= MIN_INDUSTRY_SIZE
            and row["industry_rank_ratio"] is not None
            and row["industry_rank_ratio"] <= TOP_INDUSTRY_RANK_RATIO
        )

        grade = classify_grade(
            revision_count=int(row["revision_count"]),
            rev_speed_count_30d=int(row["rev_speed_count_30d"]),
            rev_magnitude_sum_90d=float(row["rev_magnitude_sum_90d"]),
            rev_magnitude_max_90d=float(row["rev_magnitude_max_90d"]),
            revision_consistency=float(row["revision_consistency"]),
            growth_accel_tag=str(row["growth_accel_tag"]),
            quality_proxy_tag=str(row["quality_proxy_tag"]),
            high_proximity=float(row["high_proximity"]),
            volume_ratio=float(row["volume_ratio"]),
            revenue_growth=float(row["revenue_growth"]),
            rs_line_high=bool(row["rs_line_high"]),
            supply_dryup=bool(row["supply_dryup"]),
            tight_structure=bool(row["tight_structure"]),
            vcp_ready=bool(row["vcp_ready"]),
            entry_quality_tag=str(row["entry_quality_tag"]),
            industry_top=industry_top,
        )
        grades.append(grade)

        score = compute_score(
            revision_count=int(row["revision_count"]),
            rev_speed_tag=str(row["rev_speed_tag"]),
            rev_magnitude_sum_90d=float(row["rev_magnitude_sum_90d"]),
            rev_magnitude_max_90d=float(row["rev_magnitude_max_90d"]),
            revision_consistency=float(row["revision_consistency"]),
            revenue_growth=float(row["revenue_growth"]),
            ret_6m=float(row["ret_6m"]),
            sector_ret_6m=float(row["sector_ret_6m"]),
            spy_ret_6m=float(row["spy_ret_6m"]),
            ret_3m=float(row["ret_3m"]),
            sector_ret_3m=float(row["sector_ret_3m"]),
            spy_ret_3m=float(row["spy_ret_3m"]),
            high_proximity=float(row["high_proximity"]),
            volume_ratio=float(row["volume_ratio"]),
            price=float(row["price"]),
            ma50=float(row["ma50"]),
            ma200=float(row["ma200"]),
            rs_line_high=bool(row["rs_line_high"]),
            supply_dryup=bool(row["supply_dryup"]),
            tight_structure=bool(row["tight_structure"]),
            vcp_ready=bool(row["vcp_ready"]),
            industry_top=industry_top,
        )
        scores.append(score)

    final_df["grade"] = grades
    final_df["score"] = scores

    grade_rank = {"ULTRA": 0, "STRONG": 1, "WATCH": 2}
    growth_rank = {"ACCEL": 0, "EARLY": 1, "NORMAL": 2}
    action_rank = {"BUY": 0, "WATCH": 1, "NO_ENTRY": 2}

    final_df["grade_sort"] = final_df["grade"].map(grade_rank).fillna(9)
    final_df["growth_sort"] = final_df["growth_accel_tag"].map(growth_rank).fillna(9)
    final_df["action_sort"] = final_df["action"].map(action_rank).fillna(9)

    final_df = final_df.sort_values(
        [
            "grade_sort",
            "growth_sort",
            "action_sort",
            "score",
            "revision_count",
            "ret_6m",
            "high_proximity",
        ],
        ascending=[True, True, True, False, True, False, False],
    ).reset_index(drop=True)

    final_df = final_df.drop(columns=["grade_sort", "growth_sort", "action_sort"])
    return final_df


# =========================================================
# Output
# =========================================================


def save_empty_outputs() -> None:
    empty = pd.DataFrame(columns=OUTPUT_COLS)
    empty.to_csv("eps_candidates.csv", index=False)
    empty.to_csv("top_candidates.csv", index=False)
    empty.to_csv("ultra_candidates.csv", index=False)


def print_preview(df: pd.DataFrame) -> None:
    if df.empty:
        print("Preview: none")
        return

    preview_cols = [
        "ticker",
        "grade",
        "multibagger_potential",
        "revision_count",
        "rev_speed_count_30d",
        "rev_magnitude_sum_90d",
        "rev_magnitude_max_90d",
        "rev_magnitude_tag",
        "revision_consistency",
        "revision_consistency_tag",
        "growth_accel_tag",
        "quality_proxy_tag",
        "score",
        "revenue_growth",
        "ret_6m",
        "ret_3m",
        "high_proximity",
        "rs_line_high",
        "supply_dryup",
        "tight_structure",
        "vcp_ready",
        "entry_quality_tag",
        "industry_rank",
        "industry_size",
        "industry_breadth_pct",
        "earnings_date_note",
    ]
    show_cols = [c for c in preview_cols if c in df.columns]
    print(df[show_cols].head(10).to_string(index=False))


# =========================================================
# Main
# =========================================================


def main() -> None:
    today = now_ts()

    tickers = get_sp1500_tickers()

    rows = []
    for ticker in tickers:
        summary = get_quote_summary_modules(ticker)
        eps = get_eps_estimate_from_summary(summary) if summary else None
        rows.append(
            {
                "date": today,
                "ticker": ticker,
                "eps": eps,
            }
        )
        time.sleep(REQUEST_SLEEP)

    today_df = pd.DataFrame(rows)

    history = load_history()
    history = pd.concat([history[["date", "ticker", "eps"]], today_df], ignore_index=True)
    history = history.drop_duplicates(subset=["date", "ticker"], keep="last")
    history = history.sort_values(["ticker", "date"]).reset_index(drop=True)

    history["prev_eps"] = history.groupby("ticker")["eps"].shift(1)
    history["revision_pct"] = history.apply(
        lambda row: pct_change(row["prev_eps"], row["eps"])
        if pd.notna(row["prev_eps"]) and pd.notna(row["eps"]) and row["prev_eps"] not in [0, None]
        else None,
        axis=1,
    )
    history["up_revision"] = (
        history["revision_pct"].notna()
        & (history["revision_pct"] > 0)
    ).astype(int)
    history["down_revision"] = (
        history["revision_pct"].notna()
        & (history["revision_pct"] < 0)
    ).astype(int)

    save_history = history[["date", "ticker", "eps", "prev_eps", "revision_pct", "up_revision", "down_revision"]].copy()
    save_history.to_csv("eps_history.csv", index=False)

    cutoff_90 = pd.Timestamp.today().normalize() - pd.Timedelta(days=LOOKBACK_DAYS)
    recent_90 = save_history[save_history["date"] >= cutoff_90].copy()

    revision_summary_90 = (
        recent_90.groupby("ticker", as_index=False)
        .agg(
            revision_count=("up_revision", "sum"),
            total_revision_events=("revision_pct", lambda s: int(pd.Series(s).notna().sum())),
            down_revision_count=("down_revision", "sum"),
            rev_magnitude_sum_90d=("revision_pct", lambda s: float(pd.Series(s)[pd.Series(s) > 0].sum()) if len(pd.Series(s)[pd.Series(s) > 0]) > 0 else 0.0),
            rev_magnitude_max_90d=("revision_pct", lambda s: float(pd.Series(s)[pd.Series(s) > 0].max()) if len(pd.Series(s)[pd.Series(s) > 0]) > 0 else 0.0),
        )
    )

    revision_summary_90["revision_consistency"] = revision_summary_90.apply(
        lambda row: float(row["revision_count"] / row["total_revision_events"])
        if row["total_revision_events"] and row["total_revision_events"] > 0
        else 0.0,
        axis=1,
    )

    stage1 = revision_summary_90[revision_summary_90["revision_count"] >= REVISION_THRESHOLD].copy()
    stage1.to_csv("eps_stage1_raw.csv", index=False)

    cutoff_30 = pd.Timestamp.today().normalize() - pd.Timedelta(days=REV_SPEED_LOOKBACK_DAYS)
    recent_30 = save_history[save_history["date"] >= cutoff_30]

    rev_speed_df = (
        recent_30.groupby("ticker", as_index=False)["up_revision"]
        .sum()
        .rename(columns={"up_revision": "rev_speed_count_30d"})
    )
    rev_speed_map = {}
    if not rev_speed_df.empty:
        rev_speed_map = dict(zip(rev_speed_df["ticker"], rev_speed_df["rev_speed_count_30d"]))

    if stage1.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print("Stage1 raw: 0")
        print("Final candidates: 0")
        print("Ultra candidates: 0")
        print("No EPS revision candidates today.")
        return

    spy_df = download_price_history("SPY", period="1y")
    spy_ret_6m = compute_return_from_history(spy_df, 126)
    spy_ret_3m = compute_return_from_history(spy_df, 63)

    if spy_df.empty or spy_ret_6m is None or spy_ret_3m is None:
        save_empty_outputs()
        print("Done.")
        print("SPY fetch failed.")
        return

    sector_etfs = sorted(set(SECTOR_MAP.values()))
    sector_returns = compute_sector_returns_cached(sector_etfs)

    candidate_rows: List[dict] = []
    for _, row in stage1.iterrows():
        ticker = str(row["ticker"]).upper().strip()
        revision_count = int(row["revision_count"])
        rev_speed_count_30d = int(rev_speed_map.get(ticker, 0))
        rev_magnitude_sum_90d = float(row.get("rev_magnitude_sum_90d", 0.0))
        rev_magnitude_max_90d = float(row.get("rev_magnitude_max_90d", 0.0))
        revision_consistency = float(row.get("revision_consistency", 0.0))

        built = build_candidate_row(
            ticker=ticker,
            revision_count=revision_count,
            rev_speed_count_30d=rev_speed_count_30d,
            rev_magnitude_sum_90d=rev_magnitude_sum_90d,
            rev_magnitude_max_90d=rev_magnitude_max_90d,
            revision_consistency=revision_consistency,
            spy_df=spy_df,
            spy_ret_6m=spy_ret_6m,
            spy_ret_3m=spy_ret_3m,
            sector_returns=sector_returns,
            min_high_proximity=MIN_HIGH_PROXIMITY,
        )
        if built is not None:
            candidate_rows.append(built)

    final_df = pd.DataFrame(candidate_rows)

    if len(final_df) < AUTO_RELAX_IF_FINAL_LT:
        candidate_rows_relaxed: List[dict] = []
        for _, row in stage1.iterrows():
            ticker = str(row["ticker"]).upper().strip()
            revision_count = int(row["revision_count"])
            rev_speed_count_30d = int(rev_speed_map.get(ticker, 0))
            rev_magnitude_sum_90d = float(row.get("rev_magnitude_sum_90d", 0.0))
            rev_magnitude_max_90d = float(row.get("rev_magnitude_max_90d", 0.0))
            revision_consistency = float(row.get("revision_consistency", 0.0))

            built = build_candidate_row(
                ticker=ticker,
                revision_count=revision_count,
                rev_speed_count_30d=rev_speed_count_30d,
                rev_magnitude_sum_90d=rev_magnitude_sum_90d,
                rev_magnitude_max_90d=rev_magnitude_max_90d,
                revision_consistency=revision_consistency,
                spy_df=spy_df,
                spy_ret_6m=spy_ret_6m,
                spy_ret_3m=spy_ret_3m,
                sector_returns=sector_returns,
                min_high_proximity=RELAXED_HIGH_PROXIMITY,
            )
            if built is not None:
                candidate_rows_relaxed.append(built)
        final_df = pd.DataFrame(candidate_rows_relaxed)

    if final_df.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print(f"Stage1 raw: {len(stage1)}")
        print("Final candidates: 0")
        print("Ultra candidates: 0")
        print("No final candidates after quality/trend/structure filters.")
        return

    final_df = enrich_with_industry_and_grade(final_df)

    final_df = final_df[OUTPUT_COLS]
    top_df = final_df.head(10).copy()
    ultra_df = final_df[final_df["grade"] == "ULTRA"].copy()

    final_df.to_csv("eps_candidates.csv", index=False)
    top_df.to_csv("top_candidates.csv", index=False)
    ultra_df.to_csv("ultra_candidates.csv", index=False)

    print("Done.")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Stage1 raw: {len(stage1)}")
    print(f"Final candidates: {len(final_df)}")
    print(f"Ultra candidates: {len(ultra_df)}")
    print(f"SPY 6M return: {round(spy_ret_6m, 4)}")
    print(f"SPY 3M return: {round(spy_ret_3m, 4)}")
    print("")
    print("Preview")
    print_preview(final_df)


if __name__ == "__main__":
    main()
