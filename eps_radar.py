import datetime as dt
import time
from io import StringIO

import pandas as pd
import requests

USER_AGENT = {"User-Agent": "Mozilla/5.0"}

# =========================================
# FINAL EPS + MOMENTUM + SECTOR RADAR
# DETECTION-FIRST + MULTIBAGGER STAR VERSION
# =========================================

# Universe / liquidity
MIN_MARKET_CAP = 2_000_000_000          # $2B
MIN_PRICE = 10.0
MIN_AVG_DOLLAR_VOLUME = 10_000_000      # $10M

# EPS revision
LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 2
REQUEST_SLEEP = 0.10

# Growth / trend / breakout
MIN_REVENUE_GROWTH = 0.15               # 15%
MIN_HIGH_PROXIMITY = 0.80               # within 20% of 52W high
RELAXED_HIGH_PROXIMITY = 0.72
AUTO_RELAX_IF_FINAL_LT = 5

MIN_VOLUME_RATIO = 1.0
MIN_RS_OVER_SPY = 0.00

# Multibagger subset
MIN_MULTIBAGGER_CAP = 2_000_000_000
MAX_MULTIBAGGER_CAP = 25_000_000_000
MIN_MULTIBAGGER_REVENUE_GROWTH = 0.20
MIN_MULTIBAGGER_HIGH_PROXIMITY = 0.90
MIN_MULTIBAGGER_VOLUME_RATIO = 1.2

# Sector ETF mapping
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
    "is_multibagger",
    "detection_number",
    "signal_stage",
    "action",
    "growth_accel_tag",
    "growth_accel_proxy",
    "quality_proxy_tag",
    "score",
    "revision_count",
    "revenue_growth",
    "gross_margin",
    "operating_margin",
    "price",
    "entry_price",
    "stop_price",
    "high_20d",
    "market_cap",
    "sector",
    "sector_etf",
    "ret_6m",
    "sector_ret_6m",
    "spy_ret_6m",
    "ma50",
    "ma200",
    "high_52w",
    "high_proximity",
    "volume",
    "avg_volume_3m",
    "volume_ratio",
    "avg_dollar_volume",
]


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def get_sp1500_tickers():
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

    tickers = [t.replace(".", "-") for t in tickers]
    return sorted(list(set(tickers)))


def get_eps_estimate(ticker: str):
    try:
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
            f"{ticker}?modules=earningsTrend"
        )
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        data = r.json()

        result = data.get("quoteSummary", {}).get("result")
        if not result:
            return None

        trends = result[0].get("earningsTrend", {}).get("trend", [])
        for trend in trends:
            if trend.get("period") == "+1y":
                current = trend.get("epsTrend", {}).get("current", {}).get("raw")
                if current is not None:
                    return float(current)

        return None
    except Exception:
        return None


def load_history():
    try:
        df = pd.read_csv("eps_history.csv")
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date", "ticker", "eps", "up_revision"])


def get_quote_info(ticker: str):
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
            "sector": q.get("sector"),
        }
    except Exception:
        return None


def get_6m_return(ticker: str):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        data = r.json()

        result = data.get("chart", {}).get("result")
        if not result:
            return None

        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [c for c in closes if c is not None]
        if len(closes) < 2:
            return None

        start_price = closes[0]
        end_price = closes[-1]
        if start_price in [0, None]:
            return None

        return (end_price / start_price) - 1
    except Exception:
        return None


def get_20d_high(ticker: str):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1mo&interval=1d"
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        data = r.json()

        result = data.get("chart", {}).get("result")
        if not result:
            return None

        highs = result[0].get("indicators", {}).get("quote", [{}])[0].get("high", [])
        highs = [h for h in highs if h is not None]
        if not highs:
            return None

        return max(highs)
    except Exception:
        return None


def get_revenue_growth(ticker: str):
    try:
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
            f"{ticker}?modules=financialData"
        )
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        data = r.json()

        result = data.get("quoteSummary", {}).get("result")
        if not result:
            return None

        financial_data = result[0].get("financialData", {})
        growth = financial_data.get("revenueGrowth")
        if growth is None:
            return None

        raw = growth.get("raw")
        if raw is None:
            return None

        return float(raw)
    except Exception:
        return None


def get_margin_data(ticker: str):
    """
    ROIC 직접값 대신 멀티백어 전용 quality proxy로 사용.
    Yahoo financialData에서 grossMargins / operatingMargins 읽음.
    """
    try:
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
            f"{ticker}?modules=financialData"
        )
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        data = r.json()

        result = data.get("quoteSummary", {}).get("result")
        if not result:
            return None

        fd = result[0].get("financialData", {})

        gross_margin = fd.get("grossMargins", {})
        operating_margin = fd.get("operatingMargins", {})

        gross_margin = gross_margin.get("raw") if isinstance(gross_margin, dict) else None
        operating_margin = operating_margin.get("raw") if isinstance(operating_margin, dict) else None

        return {
            "gross_margin": float(gross_margin) if gross_margin is not None else None,
            "operating_margin": float(operating_margin) if operating_margin is not None else None,
        }
    except Exception:
        return None


def get_sector_etf(sector_name):
    if not sector_name:
        return None
    return SECTOR_MAP.get(sector_name)


# =========================================
# Detection / stage / proxy logic
# =========================================
def get_signal_stage(revision_count: int) -> str:
    if revision_count == 2:
        return "INITIAL"
    elif revision_count == 3:
        return "EARLY"
    elif revision_count == 4:
        return "MID"
    else:
        return "LATE"


def get_action(revision_count: int) -> str:
    if revision_count in [2, 3]:
        return "BUY"
    elif revision_count == 4:
        return "WATCH"
    else:
        return "NO_ENTRY"


def get_eps_score(revision_count: int) -> float:
    if revision_count == 2:
        return 1.00
    elif revision_count == 3:
        return 0.90
    elif revision_count == 4:
        return 0.70
    else:
        return 0.50


def get_revenue_score(revenue_growth: float) -> float:
    if revenue_growth >= 0.30:
        return 1.00
    elif revenue_growth >= 0.20:
        return 0.80
    elif revenue_growth >= 0.15:
        return 0.60
    elif revenue_growth >= 0.10:
        return 0.30
    else:
        return 0.0


def get_breakout_proxy_score(high_proximity: float) -> float:
    if high_proximity >= 0.95:
        return 1.0
    elif high_proximity >= 0.90:
        return 0.7
    elif high_proximity >= 0.85:
        return 0.4
    else:
        return 0.0


def get_sector_proxy_score(stock_ret_6m, sector_ret_6m, spy_ret_6m) -> float:
    if sector_ret_6m > spy_ret_6m and stock_ret_6m > sector_ret_6m:
        return 1.0
    elif sector_ret_6m > spy_ret_6m:
        return 0.5
    else:
        return 0.0


def get_growth_accel_proxy(
    revision_count: int,
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
) -> float:
    eps_part = get_eps_score(revision_count)
    rev_part = get_revenue_score(revenue_growth)
    breakout_part = get_breakout_proxy_score(high_proximity)
    sector_part = get_sector_proxy_score(stock_ret_6m, sector_ret_6m, spy_ret_6m)

    proxy = (
        0.35 * eps_part +
        0.35 * rev_part +
        0.15 * breakout_part +
        0.15 * sector_part
    )
    return round(proxy, 2)


def get_growth_accel_tag(
    revision_count: int,
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
) -> str:
    strong_sector = sector_ret_6m > spy_ret_6m
    strong_stock = stock_ret_6m > sector_ret_6m

    if revision_count in [2, 3] and revenue_growth >= 0.20 and high_proximity >= 0.90 and strong_sector and strong_stock:
        return "ACCEL"
    elif revision_count in [2, 3] and revenue_growth >= 0.15:
        return "EARLY"
    else:
        return "NORMAL"


def get_quality_proxy_tag(revenue_growth, gross_margin, operating_margin):
    """
    멀티백어 전용 품질 대체 태그.
    정확한 ROIC 상승 대신:
    고성장 + 높은 총마진 + 높은 영업마진
    """
    if (
        revenue_growth is not None
        and gross_margin is not None
        and operating_margin is not None
        and revenue_growth >= 0.20
        and gross_margin >= 0.45
        and operating_margin >= 0.15
    ):
        return "QUALITY"
    return "NORMAL"


def compute_score(
    revision_count,
    revenue_growth,
    ret_6m,
    sector_ret_6m,
    spy_ret_6m,
    high_proximity,
    volume_ratio,
    price,
    ma50,
    ma200,
):
    eps_score = get_eps_score(revision_count)
    rev_score = get_revenue_score(revenue_growth)

    trend_parts = 0.0
    if price > ma50:
        trend_parts += 0.35
    if price > ma200:
        trend_parts += 0.35

    rs_vs_spy = ret_6m - spy_ret_6m
    trend_parts += 0.30 * clamp01(rs_vs_spy / 0.30)
    trend_score = clamp01(trend_parts)

    prox_score = clamp01((high_proximity - 0.80) / 0.20)
    vol_score = clamp01((volume_ratio - 1.0) / 1.5)
    sector_score = 1.0 if sector_ret_6m > spy_ret_6m else 0.0
    stock_sector_score = 1.0 if ret_6m > sector_ret_6m else 0.0

    score = (
        35 * eps_score +
        25 * rev_score +
        15 * trend_score +
        10 * prox_score +
        5 * vol_score +
        5 * sector_score +
        5 * stock_sector_score
    )
    return round(score, 2)


def save_empty_outputs():
    empty = pd.DataFrame(columns=OUTPUT_COLS)
    empty.to_csv("eps_candidates.csv", index=False)
    empty.to_csv("top_candidates.csv", index=False)
    empty.to_csv("multibagger_candidates.csv", index=False)


def build_candidates(stage1, spy_ret_6m, sector_returns, min_high_proximity):
    rows = []

    for _, row in stage1.iterrows():
        ticker = row["ticker"]
        revision_count = int(row["revision_count"])

        quote = get_quote_info(ticker)
        time.sleep(REQUEST_SLEEP)

        ret_6m = get_6m_return(ticker)
        time.sleep(REQUEST_SLEEP)

        revenue_growth = get_revenue_growth(ticker)
        time.sleep(REQUEST_SLEEP)

        margin_data = get_margin_data(ticker)
        time.sleep(REQUEST_SLEEP)

        high_20d = get_20d_high(ticker)
        time.sleep(REQUEST_SLEEP)

        if quote is None or ret_6m is None or revenue_growth is None or margin_data is None or high_20d is None:
            continue

        price = quote.get("price")
        market_cap = quote.get("market_cap")
        ma50 = quote.get("ma50")
        ma200 = quote.get("ma200")
        high_52w = quote.get("high_52w")
        volume = quote.get("volume")
        avg_volume_3m = quote.get("avg_volume_3m")
        sector = quote.get("sector")

        gross_margin = margin_data.get("gross_margin")
        operating_margin = margin_data.get("operating_margin")

        if (
            price is None
            or market_cap is None
            or ma50 is None
            or ma200 is None
            or high_52w is None
            or volume is None
            or avg_volume_3m is None
            or high_52w == 0
            or avg_volume_3m == 0
        ):
            continue

        sector_etf = get_sector_etf(sector)
        if sector_etf is None:
            continue

        sector_ret_6m = sector_returns.get(sector_etf)
        if sector_ret_6m is None:
            continue

        # Universe
        if market_cap < MIN_MARKET_CAP:
            continue
        if price < MIN_PRICE:
            continue

        avg_dollar_volume = price * avg_volume_3m
        if avg_dollar_volume < MIN_AVG_DOLLAR_VOLUME:
            continue

        # Growth
        if revenue_growth < MIN_REVENUE_GROWTH:
            continue

        # Trend
        if price <= ma50:
            continue
        if price <= ma200:
            continue
        if (ret_6m - spy_ret_6m) <= MIN_RS_OVER_SPY:
            continue

        # Strong sector / strong stock
        if sector_ret_6m <= spy_ret_6m:
            continue
        if ret_6m <= sector_ret_6m:
            continue

        # Breakout proximity
        high_proximity = price / high_52w
        if high_proximity < min_high_proximity:
            continue

        # Volume
        volume_ratio = volume / avg_volume_3m
        if volume_ratio < MIN_VOLUME_RATIO:
            continue

        # Entry / Stop
        entry_price = max(high_20d * 1.01, price * 1.02)
        stop_price = entry_price * 0.85

        signal_stage = get_signal_stage(revision_count)
        action = get_action(revision_count)

        growth_accel_proxy = get_growth_accel_proxy(
            revision_count=revision_count,
            revenue_growth=revenue_growth,
            high_proximity=high_proximity,
            stock_ret_6m=ret_6m,
            sector_ret_6m=sector_ret_6m,
            spy_ret_6m=spy_ret_6m,
        )

        growth_accel_tag = get_growth_accel_tag(
            revision_count=revision_count,
            revenue_growth=revenue_growth,
            high_proximity=high_proximity,
            stock_ret_6m=ret_6m,
            sector_ret_6m=sector_ret_6m,
            spy_ret_6m=spy_ret_6m,
        )

        quality_proxy_tag = get_quality_proxy_tag(
            revenue_growth=revenue_growth,
            gross_margin=gross_margin,
            operating_margin=operating_margin,
        )

        score = compute_score(
            revision_count=revision_count,
            revenue_growth=revenue_growth,
            ret_6m=ret_6m,
            sector_ret_6m=sector_ret_6m,
            spy_ret_6m=spy_ret_6m,
            high_proximity=high_proximity,
            volume_ratio=volume_ratio,
            price=price,
            ma50=ma50,
            ma200=ma200,
        )

        # Multibagger classification
        is_multibagger = (
            (market_cap >= MIN_MULTIBAGGER_CAP)
            and (market_cap <= MAX_MULTIBAGGER_CAP)
            and (revenue_growth >= MIN_MULTIBAGGER_REVENUE_GROWTH)
            and (high_proximity >= MIN_MULTIBAGGER_HIGH_PROXIMITY)
            and (volume_ratio >= MIN_MULTIBAGGER_VOLUME_RATIO)
            and (action != "NO_ENTRY")
            and (growth_accel_tag == "ACCEL")
            and (quality_proxy_tag == "QUALITY")
        )

        rows.append({
            "ticker": ticker,
            "is_multibagger": bool(is_multibagger),
            "detection_number": revision_count,
            "signal_stage": signal_stage,
            "action": action,
            "growth_accel_tag": growth_accel_tag,
            "growth_accel_proxy": growth_accel_proxy,
            "quality_proxy_tag": quality_proxy_tag,
            "score": score,
            "revision_count": revision_count,
            "revenue_growth": revenue_growth,
            "gross_margin": gross_margin,
            "operating_margin": operating_margin,
            "price": price,
            "entry_price": round(entry_price, 2),
            "stop_price": round(stop_price, 2),
            "high_20d": round(high_20d, 2),
            "market_cap": market_cap,
            "sector": sector,
            "sector_etf": sector_etf,
            "ret_6m": ret_6m,
            "sector_ret_6m": sector_ret_6m,
            "spy_ret_6m": spy_ret_6m,
            "ma50": ma50,
            "ma200": ma200,
            "high_52w": high_52w,
            "high_proximity": high_proximity,
            "volume": volume,
            "avg_volume_3m": avg_volume_3m,
            "volume_ratio": volume_ratio,
            "avg_dollar_volume": avg_dollar_volume,
        })

    return pd.DataFrame(rows)


def main():
    today = pd.Timestamp(dt.date.today())
    tickers = get_sp1500_tickers()

    # 1) EPS snapshot
    rows = []
    for ticker in tickers:
        eps = get_eps_estimate(ticker)
        rows.append({
            "date": today,
            "ticker": ticker,
            "eps": eps,
        })
        time.sleep(REQUEST_SLEEP)

    today_df = pd.DataFrame(rows)

    # 2) History
    history = load_history()
    history = pd.concat([history[["date", "ticker", "eps"]], today_df], ignore_index=True)
    history = history.drop_duplicates(subset=["date", "ticker"], keep="last")
    history = history.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 3) Revisions
    history["prev_eps"] = history.groupby("ticker")["eps"].shift(1)
    history["up_revision"] = (
        history["eps"].notna()
        & history["prev_eps"].notna()
        & (history["eps"] > history["prev_eps"])
    ).astype(int)

    save_history = history[["date", "ticker", "eps", "up_revision"]].copy()
    save_history.to_csv("eps_history.csv", index=False)

    # 4) Stage1 raw
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=LOOKBACK_DAYS)
    recent = save_history[save_history["date"] >= cutoff]

    summary = (
        recent.groupby("ticker", as_index=False)["up_revision"]
        .sum()
        .rename(columns={"up_revision": "revision_count"})
    )

    stage1 = summary[summary["revision_count"] >= REVISION_THRESHOLD].copy()
    stage1.to_csv("eps_stage1_raw.csv", index=False)

    if stage1.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print("Stage1 raw: 0")
        print("Final candidates: 0")
        print("Multibagger candidates: 0")
        return

    # 5) Benchmark returns
    spy_ret_6m = get_6m_return("SPY")
    time.sleep(REQUEST_SLEEP)

    if spy_ret_6m is None:
        save_empty_outputs()
        print("Done.")
        print("SPY return fetch failed")
        return

    sector_etfs = sorted(set(SECTOR_MAP.values()))
    sector_returns = {}
    for etf in sector_etfs:
        sector_returns[etf] = get_6m_return(etf)
        time.sleep(REQUEST_SLEEP)

    # 6) Main pass
    final_df = build_candidates(stage1, spy_ret_6m, sector_returns, MIN_HIGH_PROXIMITY)

    # 7) Auto relax
    if len(final_df) < AUTO_RELAX_IF_FINAL_LT:
        final_df = build_candidates(stage1, spy_ret_6m, sector_returns, RELAXED_HIGH_PROXIMITY)

    if final_df.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print(f"Stage1 raw: {len(stage1)}")
        print("Final candidates: 0")
        print("Multibagger candidates: 0")
        return

    # 8) Sort
    growth_rank = {"ACCEL": 0, "EARLY": 1, "NORMAL": 2}
    action_rank = {"BUY": 0, "WATCH": 1, "NO_ENTRY": 2}

    final_df["growth_sort"] = final_df["growth_accel_tag"].map(growth_rank).fillna(9)
    final_df["action_sort"] = final_df["action"].map(action_rank).fillna(9)

    final_df = final_df.sort_values(
        ["growth_sort", "action_sort", "score", "revision_count", "ret_6m", "high_proximity"],
        ascending=[True, True, False, True, False, False],
    ).reset_index(drop=True)

    final_df = final_df.drop(columns=["growth_sort", "action_sort"])
    final_df = final_df[OUTPUT_COLS]
    final_df.to_csv("eps_candidates.csv", index=False)

    top_df = final_df.head(10).copy()
    top_df.to_csv("top_candidates.csv", index=False)

    multi_df = final_df[final_df["is_multibagger"] == True].copy()
    multi_df.to_csv("multibagger_candidates.csv", index=False)

    print("Done.")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Stage1 raw: {len(stage1)}")
    print(f"Final candidates: {len(final_df)}")
    print(f"Multibagger candidates: {len(multi_df)}")
    print(f"SPY 6M return: {round(spy_ret_6m, 4)}")


if __name__ == "__main__":
    main()
