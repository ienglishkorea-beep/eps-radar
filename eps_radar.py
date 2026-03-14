import math
from typing import Dict, List, Optional, Tuple

import pandas as pd

from data_client import (
    build_price_metrics,
    download_price_history,
    get_earnings_date_note_from_summary,
    get_margin_data_from_summary,
    get_profile_from_summary,
    get_quote_info,
    get_quote_summary_modules,
    get_revenue_growth_from_summary,
    get_sp1500_tickers,
    safe_float,
)

# =========================================================
# EPS RADAR v2 FAST + DIRECT DEBUG
# =========================================================

MIN_MARKET_CAP = 2_000_000_000
MIN_PRICE = 10.0
MIN_AVG_DOLLAR_VOLUME = 10_000_000

STAGE0_HIGH_PROXIMITY = 0.72
STAGE0_RELAXED_HIGH_PROXIMITY = 0.68
STAGE0_MAX_COUNT = 240
STAGE0_RELAXED_MAX_COUNT = 360

STAGE1_PROXY_THRESHOLD = 60.0
RELAXED_STAGE1_PROXY_THRESHOLD = 56.0
AUTO_RELAX_IF_FINAL_LT = 5

MIN_REVENUE_GROWTH = 0.15
MIN_HIGH_PROXIMITY = 0.80
RELAXED_HIGH_PROXIMITY = 0.72
MIN_VOLUME_RATIO = 1.0
MIN_RS_OVER_SPY = 0.00

QUALITY_REVENUE_GROWTH = 0.20
QUALITY_GROSS_MARGIN = 0.45
QUALITY_OPERATING_MARGIN = 0.15

SUPPLY_DRYUP_VOL_RATIO_MAX = 0.85
SUPPLY_DRYUP_RANGE_RATIO_MAX = 0.85
TIGHT_RANGE_10D_MAX = 0.08
MAX_EXTENSION_FROM_MA50 = 0.25
VCP_BASE_LOOKBACK = 60
VCP_MAX_BASE_DEPTH = 0.30

MIN_INDUSTRY_SIZE = 3
TOP_INDUSTRY_RANK_RATIO = 0.20

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
    "is_ultra",
    "is_multibagger",
    "signal_stage",
    "action",
    "expectation_upgrade_score",
    "expectation_band",
    "guidance_tone_proxy",
    "guidance_signal",
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
    "revision_count",
    "rev_speed_count_30d",
    "rev_speed_tag",
    "rev_magnitude_sum_90d",
    "rev_magnitude_max_90d",
    "rev_magnitude_tag",
    "revision_consistency",
    "revision_consistency_tag",
]


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def init_diag() -> Dict[str, int]:
    return {
        "price_history_missing_count": 0,
        "quote_missing_count": 0,
        "summary_missing_count": 0,
        "market_cap_present_count": 0,
        "sector_present_count": 0,
        "industry_present_count": 0,
        "revenue_growth_present_count": 0,
        "gross_margin_present_count": 0,
        "operating_margin_present_count": 0,
        "stage0_price_below_min": 0,
        "stage0_avg_dollar_volume_below_min": 0,
        "stage0_trend_filter_fail": 0,
        "stage0_rs_filter_fail": 0,
        "stage0_high_proximity_fail": 0,
        "market_cap_missing": 0,
        "market_cap_below_min": 0,
        "sector_missing": 0,
        "industry_missing": 0,
        "sector_etf_missing": 0,
        "sector_return_missing": 0,
        "revenue_growth_missing": 0,
        "revenue_growth_below_min": 0,
        "high_proximity_below_min": 0,
        "volume_ratio_below_min": 0,
        "expectation_proxy_below_threshold": 0,
    }


def merge_diag(base: Dict[str, int], extra: Dict[str, int]) -> Dict[str, int]:
    out = dict(base)
    for k, v in extra.items():
        out[k] = out.get(k, 0) + int(v)
    return out


def compute_return_from_history(df: pd.DataFrame, lookback_days: int) -> Optional[float]:
    if df.empty or len(df) < lookback_days + 1:
        return None
    start_price = safe_float(df["close"].iloc[-(lookback_days + 1)])
    end_price = safe_float(df["close"].iloc[-1])
    if start_price in [None, 0] or end_price is None:
        return None
    try:
        return (end_price / start_price) - 1.0
    except Exception:
        return None


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


def expectation_band(score: float) -> str:
    if score >= 80:
        return "VERY_STRONG"
    if score >= 70:
        return "STRONG"
    if score >= 60:
        return "VALID"
    if score >= 50:
        return "WATCH"
    return "WEAK"


def get_signal_stage(proxy_score: float) -> str:
    if proxy_score >= 80:
        return "ACCELERATING"
    if proxy_score >= 70:
        return "EARLY"
    if proxy_score >= 60:
        return "INITIAL"
    return "WATCH"


def get_action(proxy_score: float) -> str:
    if proxy_score >= 70:
        return "BUY"
    if proxy_score >= 60:
        return "WATCH"
    return "NO_ENTRY"


def get_growth_accel_proxy(
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
    stock_ret_3m: float,
    sector_ret_3m: float,
    spy_ret_3m: float,
    rs_line_high: bool,
) -> float:
    rev_part = 1.0 if revenue_growth >= 0.30 else 0.8 if revenue_growth >= 0.20 else 0.6 if revenue_growth >= 0.15 else 0.0
    breakout_part = 1.0 if high_proximity >= 0.95 else 0.7 if high_proximity >= 0.90 else 0.4 if high_proximity >= 0.85 else 0.0
    rs_6m_part = 1.0 if stock_ret_6m > sector_ret_6m > spy_ret_6m else 0.5 if stock_ret_6m > spy_ret_6m else 0.0
    rs_3m_part = 1.0 if stock_ret_3m > sector_ret_3m > spy_ret_3m else 0.5 if stock_ret_3m > spy_ret_3m else 0.0
    rs_line_part = 1.0 if rs_line_high else 0.0

    proxy = (
        0.30 * rev_part
        + 0.20 * breakout_part
        + 0.20 * rs_6m_part
        + 0.20 * rs_3m_part
        + 0.10 * rs_line_part
    )
    return round(proxy, 2)


def get_growth_accel_tag(
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
    stock_ret_3m: float,
    sector_ret_3m: float,
    spy_ret_3m: float,
    rs_line_high: bool,
) -> str:
    rs_6m_strong = stock_ret_6m > sector_ret_6m > spy_ret_6m
    rs_3m_strong = stock_ret_3m > sector_ret_3m > spy_ret_3m

    if revenue_growth >= 0.20 and high_proximity >= 0.90 and rs_6m_strong and rs_3m_strong and rs_line_high:
        return "ACCEL"
    if revenue_growth >= 0.15 and (rs_6m_strong or rs_3m_strong):
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


def get_multibagger_flag(
    revenue_growth: Optional[float],
    gross_margin: Optional[float],
    operating_margin: Optional[float],
    growth_accel_tag: str,
    high_proximity: float,
    rs_line_high: bool,
    tight_structure: bool,
    vcp_ready: bool,
) -> bool:
    if revenue_growth is None or gross_margin is None or operating_margin is None:
        return False

    base_quality = (
        revenue_growth >= QUALITY_REVENUE_GROWTH
        and gross_margin >= QUALITY_GROSS_MARGIN
        and operating_margin >= QUALITY_OPERATING_MARGIN
    )

    extra_checks = 0
    if growth_accel_tag == "ACCEL":
        extra_checks += 1
    if high_proximity >= 0.90:
        extra_checks += 1
    if rs_line_high:
        extra_checks += 1
    if tight_structure or vcp_ready:
        extra_checks += 1

    return bool(base_quality and extra_checks >= 2)


def compute_expectation_upgrade_proxy(
    revenue_growth: Optional[float],
    gross_margin: Optional[float],
    operating_margin: Optional[float],
    ret_3m: Optional[float],
    ret_6m: Optional[float],
    sector_ret_3m: Optional[float],
    sector_ret_6m: Optional[float],
    spy_ret_3m: Optional[float],
    spy_ret_6m: Optional[float],
    high_proximity: Optional[float],
    rs_line_high: bool,
    tight_structure: bool,
    vcp_ready: bool,
    supply_dryup: bool,
) -> float:
    if revenue_growth is None:
        growth_score = 0.0
    elif revenue_growth >= 0.35:
        growth_score = 1.0
    elif revenue_growth >= 0.25:
        growth_score = 0.8
    elif revenue_growth >= 0.15:
        growth_score = 0.6
    elif revenue_growth >= 0.08:
        growth_score = 0.3
    else:
        growth_score = 0.0

    quality_score = 0.0
    quality_checks = 0
    if gross_margin is not None:
        quality_checks += 1
        quality_score += 1.0 if gross_margin >= QUALITY_GROSS_MARGIN else 0.5 if gross_margin >= 0.35 else 0.0
    if operating_margin is not None:
        quality_checks += 1
        quality_score += 1.0 if operating_margin >= QUALITY_OPERATING_MARGIN else 0.5 if operating_margin >= 0.08 else 0.0
    if quality_checks > 0:
        quality_score = quality_score / quality_checks
    else:
        quality_score = 0.0

    trend_score = 0.0
    trend_parts = 0
    if ret_6m is not None and sector_ret_6m is not None and spy_ret_6m is not None:
        trend_parts += 1
        if ret_6m > sector_ret_6m > spy_ret_6m:
            trend_score += 1.0
        elif ret_6m > spy_ret_6m:
            trend_score += 0.5
    if ret_3m is not None and sector_ret_3m is not None and spy_ret_3m is not None:
        trend_parts += 1
        if ret_3m > sector_ret_3m > spy_ret_3m:
            trend_score += 1.0
        elif ret_3m > spy_ret_3m:
            trend_score += 0.5
    if trend_parts > 0:
        trend_score = trend_score / trend_parts
    else:
        trend_score = 0.0

    structure_score = 0.0
    if high_proximity is not None:
        if high_proximity >= 0.95:
            structure_score += 0.35
        elif high_proximity >= 0.90:
            structure_score += 0.25
        elif high_proximity >= 0.85:
            structure_score += 0.15

    if rs_line_high:
        structure_score += 0.20
    if tight_structure:
        structure_score += 0.15
    if vcp_ready:
        structure_score += 0.15
    if supply_dryup:
        structure_score += 0.15

    structure_score = clamp01(structure_score)

    total_score = (
        25.0 * growth_score
        + 15.0 * quality_score
        + 30.0 * trend_score
        + 30.0 * structure_score
    )
    return round(total_score, 2)


def compute_guidance_tone_proxy(
    revenue_growth: Optional[float],
    gross_margin: Optional[float],
    operating_margin: Optional[float],
    ret_3m: Optional[float],
    ret_6m: Optional[float],
    sector_ret_3m: Optional[float],
    sector_ret_6m: Optional[float],
    spy_ret_3m: Optional[float],
    spy_ret_6m: Optional[float],
    high_proximity: Optional[float],
    rs_line_high: bool,
) -> str:
    good_signals = 0

    if revenue_growth is not None and revenue_growth >= 0.20:
        good_signals += 1
    if gross_margin is not None and gross_margin >= QUALITY_GROSS_MARGIN:
        good_signals += 1
    if operating_margin is not None and operating_margin >= QUALITY_OPERATING_MARGIN:
        good_signals += 1
    if (
        ret_6m is not None and sector_ret_6m is not None and spy_ret_6m is not None
        and ret_6m > sector_ret_6m > spy_ret_6m
    ):
        good_signals += 1
    if (
        ret_3m is not None and sector_ret_3m is not None and spy_ret_3m is not None
        and ret_3m > sector_ret_3m > spy_ret_3m
    ):
        good_signals += 1
    if high_proximity is not None and high_proximity >= 0.90:
        good_signals += 1
    if rs_line_high:
        good_signals += 1

    if good_signals >= 6:
        return "STRONG"
    if good_signals >= 4:
        return "NORMAL"
    return "WEAK"


def classify_grade(
    expectation_upgrade_score: float,
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
    if (
        expectation_upgrade_score >= 80
        and growth_accel_tag == "ACCEL"
        and quality_proxy_tag == "QUALITY"
        and high_proximity >= 0.85
        and volume_ratio >= 1.0
        and revenue_growth >= 0.15
        and rs_line_high
        and (supply_dryup or tight_structure or vcp_ready)
        and entry_quality_tag in ["READY", "SETUP"]
        and industry_top
    ):
        return "ULTRA"

    if (
        expectation_upgrade_score >= 65
        and growth_accel_tag in ["ACCEL", "EARLY"]
        and revenue_growth >= 0.15
        and entry_quality_tag in ["READY", "SETUP"]
    ):
        return "STRONG"

    return "WATCH"


def compute_score(
    expectation_upgrade_score: float,
    growth_accel_proxy: float,
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

    proxy_score_01 = clamp01(expectation_upgrade_score / 100.0)

    score = (
        28 * proxy_score_01
        + 14 * growth_accel_proxy
        + 14 * rev_score
        + 16 * trend_score
        + 8 * prox_score
        + 4 * vol_score
        + 16 * structure_score
    )
    return round(score, 2)


def compute_sector_returns_cached(etfs: List[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for etf in etfs:
        hist = download_price_history(etf, period="1y")
        out[etf] = {
            "ret_6m": compute_return_from_history(hist, 126),
            "ret_3m": compute_return_from_history(hist, 63),
        }
    return out


def build_stage0_shortlist(
    tickers: List[str],
    spy_ret_6m: float,
    min_high_proximity: float,
    max_count: int,
    diag: Dict[str, int],
) -> List[dict]:
    rows = []

    for ticker in tickers:
        hist = download_price_history(ticker, period="1y")
        if hist.empty or len(hist) < 200:
            continue

        metrics = build_price_metrics(hist)

        price = safe_float(metrics.get("price"))
        ma50 = safe_float(metrics.get("ma50"))
        ma200 = safe_float(metrics.get("ma200"))
        high_52w = safe_float(metrics.get("high_52w"))
        volume = safe_float(metrics.get("volume"))
        avg_volume_3m = safe_float(metrics.get("avg_volume_3m"))

        if (
            price is None
            or ma50 is None
            or ma200 is None
            or high_52w in [None, 0]
            or volume is None
            or avg_volume_3m in [None, 0]
        ):
            continue

        avg_dollar_volume = price * avg_volume_3m
        if price < MIN_PRICE:
            diag["stage0_price_below_min"] += 1
            continue
        if avg_dollar_volume < MIN_AVG_DOLLAR_VOLUME:
            diag["stage0_avg_dollar_volume_below_min"] += 1
            continue
        if price <= ma50 or price <= ma200:
            diag["stage0_trend_filter_fail"] += 1
            continue

        ret_6m = compute_return_from_history(hist, 126)
        ret_3m = compute_return_from_history(hist, 63)
        if ret_6m is None or ret_3m is None:
            continue

        if (ret_6m - spy_ret_6m) <= MIN_RS_OVER_SPY:
            diag["stage0_rs_filter_fail"] += 1
            continue

        high_proximity = price / high_52w
        if high_proximity < min_high_proximity:
            diag["stage0_high_proximity_fail"] += 1
            continue

        stage0_score = (
            40.0 * clamp01((high_proximity - 0.70) / 0.30)
            + 35.0 * clamp01((ret_6m + 0.10) / 0.80)
            + 25.0 * clamp01((ret_3m + 0.05) / 0.40)
        )

        rows.append(
            {
                "ticker": ticker,
                "stage0_score": round(stage0_score, 2),
                "price": price,
                "ret_6m": ret_6m,
                "ret_3m": ret_3m,
                "high_proximity": high_proximity,
                "avg_dollar_volume": avg_dollar_volume,
            }
        )

    rows.sort(
        key=lambda x: (
            x["stage0_score"],
            x["ret_6m"],
            x["high_proximity"],
            x["avg_dollar_volume"],
        ),
        reverse=True,
    )
    return rows[:max_count]


def print_stage0_debug(stage0_rows: List[dict], sample_n: int = 5) -> None:
    print("Stage0 sample tickers:")
    for row in stage0_rows[:sample_n]:
        print(
            f"{row['ticker']} | stage0 {row['stage0_score']} | "
            f"6m {round(row['ret_6m'],4)} | 3m {round(row['ret_3m'],4)} | "
            f"high {round(row['high_proximity'],4)}"
        )

    print("Direct field debug:")
    for row in stage0_rows[:sample_n]:
        ticker = row["ticker"]
        quote = get_quote_info(ticker)
        summary = get_quote_summary_modules(ticker)
        profile = get_profile_from_summary(summary) if summary else {"sector": None, "industry": None}
        revenue_growth = get_revenue_growth_from_summary(summary) if summary else None
        margin_data = get_margin_data_from_summary(summary) if summary else {"gross_margin": None, "operating_margin": None}

        print(
            f"DEBUG DIRECT {ticker} | "
            f"quote={quote} | "
            f"sector={profile.get('sector')} | "
            f"industry={profile.get('industry')} | "
            f"revenue_growth={revenue_growth} | "
            f"gross_margin={margin_data.get('gross_margin')} | "
            f"operating_margin={margin_data.get('operating_margin')}"
        )


def build_candidate_row(
    ticker: str,
    spy_df: pd.DataFrame,
    spy_ret_6m: float,
    spy_ret_3m: float,
    sector_returns: Dict[str, dict],
    diag: dict,
    min_high_proximity: float,
    min_expectation_proxy: float,
) -> Optional[dict]:
    hist = download_price_history(ticker, period="1y")
    if hist.empty or len(hist) < 200:
        diag["price_history_missing_count"] += 1
        return None

    price_metrics = build_price_metrics(hist)

    price = safe_float(price_metrics.get("price"))
    ma50 = safe_float(price_metrics.get("ma50"))
    ma200 = safe_float(price_metrics.get("ma200"))
    high_52w = safe_float(price_metrics.get("high_52w"))
    volume = safe_float(price_metrics.get("volume"))
    avg_volume_3m = safe_float(price_metrics.get("avg_volume_3m"))
    high_20d = safe_float(price_metrics.get("high_20d"))

    if (
        price is None
        or ma50 is None
        or ma200 is None
        or high_52w in [None, 0]
        or volume is None
        or avg_volume_3m in [None, 0]
        or high_20d is None
    ):
        diag["price_history_missing_count"] += 1
        return None

    avg_dollar_volume = price * avg_volume_3m
    if price < MIN_PRICE:
        return None
    if avg_dollar_volume < MIN_AVG_DOLLAR_VOLUME:
        return None
    if price <= ma50 or price <= ma200:
        return None

    quote = get_quote_info(ticker)
    if quote is None:
        diag["quote_missing_count"] += 1
        return None

    market_cap = safe_float(quote.get("market_cap"))
    if market_cap is None:
        diag["market_cap_missing"] += 1
        return None
    diag["market_cap_present_count"] += 1

    if market_cap < MIN_MARKET_CAP:
        diag["market_cap_below_min"] += 1
        return None

    summary = get_quote_summary_modules(ticker)
    if summary is None:
        diag["summary_missing_count"] += 1
        return None

    profile = get_profile_from_summary(summary)
    sector = profile.get("sector")
    industry = profile.get("industry")

    if sector:
        diag["sector_present_count"] += 1
    else:
        diag["sector_missing"] += 1

    if industry:
        diag["industry_present_count"] += 1
    else:
        diag["industry_missing"] += 1

    sector_etf = SECTOR_MAP.get(sector) if sector else None
    if sector_etf is None:
        diag["sector_etf_missing"] += 1
        return None

    sector_ret_6m = sector_returns.get(sector_etf, {}).get("ret_6m")
    sector_ret_3m = sector_returns.get(sector_etf, {}).get("ret_3m")
    if sector_ret_6m is None or sector_ret_3m is None:
        diag["sector_return_missing"] += 1
        return None

    revenue_growth = get_revenue_growth_from_summary(summary)
    if revenue_growth is None:
        diag["revenue_growth_missing"] += 1
        return None
    diag["revenue_growth_present_count"] += 1

    if revenue_growth < MIN_REVENUE_GROWTH:
        diag["revenue_growth_below_min"] += 1
        return None

    margin_data = get_margin_data_from_summary(summary)
    gross_margin = margin_data.get("gross_margin")
    operating_margin = margin_data.get("operating_margin")

    if gross_margin is not None:
        diag["gross_margin_present_count"] += 1
    if operating_margin is not None:
        diag["operating_margin_present_count"] += 1

    earnings_date_note = get_earnings_date_note_from_summary(summary)

    ret_6m = compute_return_from_history(hist, 126)
    ret_3m = compute_return_from_history(hist, 63)
    if ret_6m is None or ret_3m is None:
        diag["price_history_missing_count"] += 1
        return None

    if (ret_6m - spy_ret_6m) <= MIN_RS_OVER_SPY:
        return None

    high_proximity = price / high_52w
    if high_proximity < min_high_proximity:
        diag["high_proximity_below_min"] += 1
        return None

    volume_ratio = volume / avg_volume_3m
    if volume_ratio < MIN_VOLUME_RATIO:
        diag["volume_ratio_below_min"] += 1
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

    expectation_upgrade_score = compute_expectation_upgrade_proxy(
        revenue_growth=revenue_growth,
        gross_margin=gross_margin,
        operating_margin=operating_margin,
        ret_3m=ret_3m,
        ret_6m=ret_6m,
        sector_ret_3m=sector_ret_3m,
        sector_ret_6m=sector_ret_6m,
        spy_ret_3m=spy_ret_3m,
        spy_ret_6m=spy_ret_6m,
        high_proximity=high_proximity,
        rs_line_high=rs_line_high,
        tight_structure=tight_structure,
        vcp_ready=bool(vcp.get("vcp_ready", False)),
        supply_dryup=supply_dryup,
    )
    if expectation_upgrade_score < min_expectation_proxy:
        diag["expectation_proxy_below_threshold"] += 1
        return None

    expectation_band_label = expectation_band(expectation_upgrade_score)
    guidance_tone = compute_guidance_tone_proxy(
        revenue_growth=revenue_growth,
        gross_margin=gross_margin,
        operating_margin=operating_margin,
        ret_3m=ret_3m,
        ret_6m=ret_6m,
        sector_ret_3m=sector_ret_3m,
        sector_ret_6m=sector_ret_6m,
        spy_ret_3m=spy_ret_3m,
        spy_ret_6m=spy_ret_6m,
        high_proximity=high_proximity,
        rs_line_high=rs_line_high,
    )

    growth_accel_proxy = get_growth_accel_proxy(
        revenue_growth=revenue_growth,
        high_proximity=high_proximity,
        stock_ret_6m=ret_6m,
        sector_ret_6m=sector_ret_6m,
        spy_ret_6m=spy_ret_6m,
        stock_ret_3m=ret_3m,
        sector_ret_3m=sector_ret_3m,
        spy_ret_3m=spy_ret_3m,
        rs_line_high=rs_line_high,
    )

    growth_accel_tag = get_growth_accel_tag(
        revenue_growth=revenue_growth,
        high_proximity=high_proximity,
        stock_ret_6m=ret_6m,
        sector_ret_6m=sector_ret_6m,
        spy_ret_6m=spy_ret_6m,
        stock_ret_3m=ret_3m,
        sector_ret_3m=sector_ret_3m,
        spy_ret_3m=spy_ret_3m,
        rs_line_high=rs_line_high,
    )

    quality_proxy_tag = get_quality_proxy_tag(
        revenue_growth=revenue_growth,
        gross_margin=gross_margin,
        operating_margin=operating_margin,
    )

    multibagger_potential = get_multibagger_flag(
        revenue_growth=revenue_growth,
        gross_margin=gross_margin,
        operating_margin=operating_margin,
        growth_accel_tag=growth_accel_tag,
        high_proximity=high_proximity,
        rs_line_high=rs_line_high,
        tight_structure=tight_structure,
        vcp_ready=bool(vcp.get("vcp_ready", False)),
    )

    entry_price = max(high_20d * 1.01, price * 1.02)
    stop_price = entry_price * 0.85
    signal_stage = get_signal_stage(expectation_upgrade_score)
    action = get_action(expectation_upgrade_score)

    return {
        "ticker": ticker,
        "grade": "WATCH",
        "multibagger_potential": multibagger_potential,
        "is_ultra": False,
        "is_multibagger": multibagger_potential,
        "signal_stage": signal_stage,
        "action": action,
        "expectation_upgrade_score": expectation_upgrade_score,
        "expectation_band": expectation_band_label,
        "guidance_tone_proxy": guidance_tone,
        "guidance_signal": guidance_tone,
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
        "revision_count": 0,
        "rev_speed_count_30d": 0,
        "rev_speed_tag": "PROXY",
        "rev_magnitude_sum_90d": 0.0,
        "rev_magnitude_max_90d": 0.0,
        "rev_magnitude_tag": "PROXY",
        "revision_consistency": 0.0,
        "revision_consistency_tag": "PROXY",
    }


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
        breadth_rows.append({"industry": industry_name, "industry_breadth_pct": pct})

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
    ultra_flags = []
    multibagger_flags = []

    for _, row in final_df.iterrows():
        industry_top = bool(
            row["industry_size"] is not None
            and row["industry_size"] >= MIN_INDUSTRY_SIZE
            and row["industry_rank_ratio"] is not None
            and row["industry_rank_ratio"] <= TOP_INDUSTRY_RANK_RATIO
        )

        grade = classify_grade(
            expectation_upgrade_score=float(row["expectation_upgrade_score"]),
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
        ultra_flags.append(grade == "ULTRA")
        multibagger_flags.append(bool(row["multibagger_potential"]))

        score = compute_score(
            expectation_upgrade_score=float(row["expectation_upgrade_score"]),
            growth_accel_proxy=float(row["growth_accel_proxy"]),
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
    final_df["is_ultra"] = ultra_flags
    final_df["is_multibagger"] = multibagger_flags

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
            "ret_6m",
            "high_proximity",
            "expectation_upgrade_score",
        ],
        ascending=[True, True, True, False, False, False, False],
    ).reset_index(drop=True)

    final_df = final_df.drop(columns=["grade_sort", "growth_sort", "action_sort"])
    return final_df


def save_empty_outputs() -> None:
    empty = pd.DataFrame(columns=OUTPUT_COLS)
    empty.to_csv("eps_candidates.csv", index=False)
    empty.to_csv("top_candidates.csv", index=False)
    empty.to_csv("ultra_candidates.csv", index=False)
    empty.to_csv("multibagger_candidates.csv", index=False)


def print_preview(df: pd.DataFrame) -> None:
    if df.empty:
        print("Preview: none")
        return

    preview_cols = [
        "ticker",
        "grade",
        "multibagger_potential",
        "expectation_upgrade_score",
        "expectation_band",
        "guidance_tone_proxy",
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


def print_diag(title: str, diag: Dict[str, int]) -> None:
    print("")
    print(title)
    for k, v in diag.items():
        print(f"{k}: {v}")


def main() -> None:
    tickers = get_sp1500_tickers()
    print(f"Universe tickers: {len(tickers)}")

    spy_df = download_price_history("SPY", period="1y")
    spy_ret_6m = compute_return_from_history(spy_df, 126)
    spy_ret_3m = compute_return_from_history(spy_df, 63)

    if spy_df.empty or spy_ret_6m is None or spy_ret_3m is None:
        save_empty_outputs()
        print("Done.")
        print("SPY fetch failed.")
        return

    print(f"SPY 6M return: {round(spy_ret_6m, 4)}")
    print(f"SPY 3M return: {round(spy_ret_3m, 4)}")

    sector_etfs = sorted(set(SECTOR_MAP.values()))
    sector_returns = compute_sector_returns_cached(sector_etfs)

    diag_stage0 = init_diag()

    stage0_rows = build_stage0_shortlist(
        tickers=tickers,
        spy_ret_6m=spy_ret_6m,
        min_high_proximity=STAGE0_HIGH_PROXIMITY,
        max_count=STAGE0_MAX_COUNT,
        diag=diag_stage0,
    )
    stage0_tickers = [r["ticker"] for r in stage0_rows]
    print(f"Stage0 shortlist: {len(stage0_tickers)}")
    print_stage0_debug(stage0_rows, sample_n=5)

    diag_stage1 = init_diag()
    candidate_rows: List[dict] = []
    for ticker in stage0_tickers:
        built = build_candidate_row(
            ticker=ticker,
            spy_df=spy_df,
            spy_ret_6m=spy_ret_6m,
            spy_ret_3m=spy_ret_3m,
            sector_returns=sector_returns,
            diag=diag_stage1,
            min_high_proximity=MIN_HIGH_PROXIMITY,
            min_expectation_proxy=STAGE1_PROXY_THRESHOLD,
        )
        if built is not None:
            candidate_rows.append(built)

    final_df = pd.DataFrame(candidate_rows)

    diag_stage0_relaxed = init_diag()
    diag_stage1_relaxed = init_diag()

    if len(final_df) < AUTO_RELAX_IF_FINAL_LT:
        stage0_rows_relaxed = build_stage0_shortlist(
            tickers=tickers,
            spy_ret_6m=spy_ret_6m,
            min_high_proximity=STAGE0_RELAXED_HIGH_PROXIMITY,
            max_count=STAGE0_RELAXED_MAX_COUNT,
            diag=diag_stage0_relaxed,
        )
        relaxed_tickers = [r["ticker"] for r in stage0_rows_relaxed]
        print(f"Stage0 shortlist relaxed: {len(relaxed_tickers)}")

        candidate_rows_relaxed: List[dict] = []
        for ticker in relaxed_tickers:
            built = build_candidate_row(
                ticker=ticker,
                spy_df=spy_df,
                spy_ret_6m=spy_ret_6m,
                spy_ret_3m=spy_ret_3m,
                sector_returns=sector_returns,
                diag=diag_stage1_relaxed,
                min_high_proximity=RELAXED_HIGH_PROXIMITY,
                min_expectation_proxy=RELAXED_STAGE1_PROXY_THRESHOLD,
            )
            if built is not None:
                candidate_rows_relaxed.append(built)

        final_df = pd.DataFrame(candidate_rows_relaxed)

    diag_total = merge_diag(diag_stage0, diag_stage1)
    diag_total = merge_diag(diag_total, diag_stage0_relaxed)
    diag_total = merge_diag(diag_total, diag_stage1_relaxed)

    if final_df.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print(f"Stage0 shortlist: {len(stage0_tickers)}")
        print("Stage1 proxy passed: 0")
        print("Final candidates: 0")
        print("Ultra candidates: 0")
        print("Multibagger candidates: 0")
        print("No final candidates after expectation proxy / quality / trend / structure filters.")
        print_diag("Diagnostics", diag_total)
        return

    stage1_passed = len(final_df)
    final_df = enrich_with_industry_and_grade(final_df)

    final_df = final_df[OUTPUT_COLS]
    top_df = final_df.head(10).copy()
    ultra_df = final_df[final_df["is_ultra"] == True].copy()
    multibagger_df = final_df[final_df["is_multibagger"] == True].copy()

    final_df.to_csv("eps_candidates.csv", index=False)
    top_df.to_csv("top_candidates.csv", index=False)
    ultra_df.to_csv("ultra_candidates.csv", index=False)
    multibagger_df.to_csv("multibagger_candidates.csv", index=False)

    print("Done.")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Stage0 shortlist: {len(stage0_tickers)}")
    print(f"Stage1 proxy passed: {stage1_passed}")
    print(f"Final candidates: {len(final_df)}")
    print(f"Ultra candidates: {len(ultra_df)}")
    print(f"Multibagger candidates: {len(multibagger_df)}")
    print_diag("Diagnostics", diag_total)
    print("")
    print("Preview")
    print_preview(final_df)


if __name__ == "__main__":
    main()
