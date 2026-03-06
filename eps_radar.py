import datetime as dt
import time
from io import StringIO

import pandas as pd
import requests

# =========================================
# FINAL RADAR SETTINGS
# =========================================
LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 3
REQUEST_SLEEP = 0.12

# 기본 후보 필터
MIN_MARKET_CAP = 300_000_000          # $300M
MIN_REVENUE_GROWTH = 0.15             # 15%
MIN_HIGH_PROXIMITY = 0.80             # 52주 고점의 80% 이상
MIN_RS_OVER_SPY = 0.00                # SPY보다 6개월 수익률 높아야 함
MIN_AVG_DOLLAR_VOLUME = 5_000_000     # 평균 일거래대금 $5M 이상

# 자동 완화
AUTO_RELAX_IF_FINAL_LT = 5
RELAXED_HIGH_PROXIMITY = 0.75

# 멀티배거 후보 필터
MIN_MULTIBAGGER_CAP = 2_000_000_000   # $2B
MAX_MULTIBAGGER_CAP = 20_000_000_000  # $20B
MIN_MULTIBAGGER_REVENUE_GROWTH = 0.20 # 20%
MIN_MULTIBAGGER_HIGH_PROXIMITY = 0.90 # 52주 고점의 90% 이상

USER_AGENT = {"User-Agent": "Mozilla/5.0"}


# =========================================
# UNIVERSE
# =========================================
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


# =========================================
# EPS ESTIMATE
# =========================================
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


# =========================================
# HISTORY
# =========================================
def load_history():
    try:
        df = pd.read_csv("eps_history.csv")
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date", "ticker", "eps", "up_revision"])


# =========================================
# QUOTE / PRICE
# =========================================
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
            "ma200": q.get("twoHundredDayAverage"),
            "high_52w": q.get("fiftyTwoWeekHigh"),
            "avg_volume_3m": q.get("averageDailyVolume3Month"),
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


# =========================================
# FUNDAMENTALS
# =========================================
def get_revenue_growth(ticker: str):
    """
    Yahoo financialData.revenueGrowth 사용
    0.15 = YoY 매출 성장률 15%
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


# =========================================
# OUTPUT HELPERS
# =========================================
def save_empty_outputs():
    empty_cols = [
        "ticker",
        "revision_count",
        "market_cap",
        "price",
        "ma200",
        "ret_6m",
        "spy_ret_6m",
        "high_52w",
        "high_proximity",
        "revenue_growth",
        "avg_dollar_volume",
    ]

    pd.DataFrame(columns=empty_cols).to_csv("eps_candidates.csv", index=False)
    pd.DataFrame(columns=empty_cols).to_csv("top_candidates.csv", index=False)
    pd.DataFrame(columns=empty_cols).to_csv("multibagger_candidates.csv", index=False)


# =========================================
# MAIN
# =========================================
def main():
    today = pd.Timestamp(dt.date.today())
    tickers = get_sp1500_tickers()

    # 1) 오늘 EPS 스냅샷 수집
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

    # 2) 히스토리 누적
    history = load_history()
    history = pd.concat([history[["date", "ticker", "eps"]], today_df], ignore_index=True)
    history = history.drop_duplicates(subset=["date", "ticker"], keep="last")
    history = history.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 3) 전일 대비 EPS 상향 여부
    history["prev_eps"] = history.groupby("ticker")["eps"].shift(1)
    history["up_revision"] = (
        history["eps"].notna()
        & history["prev_eps"].notna()
        & (history["eps"] > history["prev_eps"])
    ).astype(int)

    save_history = history[["date", "ticker", "eps", "up_revision"]].copy()
    save_history.to_csv("eps_history.csv", index=False)

    # 4) 최근 90일 EPS 상향 횟수
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=LOOKBACK_DAYS)
    recent = save_history[save_history["date"] >= cutoff]

    summary = (
        recent.groupby("ticker", as_index=False)["up_revision"]
        .sum()
        .rename(columns={"up_revision": "revision_count"})
    )

    stage1 = summary[summary["revision_count"] >= REVISION_THRESHOLD].copy()

    if stage1.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print("Stage1 candidates: 0")
        print("Final candidates: 0")
        print("Multibagger candidates: 0")
        return

    # 5) SPY 상대강도 기준
    spy_ret_6m = get_6m_return("SPY")
    time.sleep(REQUEST_SLEEP)

    if spy_ret_6m is None:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print("SPY return fetch failed")
        return

    # 6) 기본 필터 적용
    final_rows = []

    for _, row in stage1.iterrows():
        ticker = row["ticker"]
        revision_count = row["revision_count"]

        quote = get_quote_info(ticker)
        time.sleep(REQUEST_SLEEP)

        ret_6m = get_6m_return(ticker)
        time.sleep(REQUEST_SLEEP)

        revenue_growth = get_revenue_growth(ticker)
        time.sleep(REQUEST_SLEEP)

        if quote is None or ret_6m is None or revenue_growth is None:
            continue

        price = quote.get("price")
        market_cap = quote.get("market_cap")
        ma200 = quote.get("ma200")
        high_52w = quote.get("high_52w")
        avg_volume_3m = quote.get("avg_volume_3m")

        if (
            price is None
            or market_cap is None
            or ma200 is None
            or high_52w is None
            or avg_volume_3m is None
            or high_52w == 0
        ):
            continue

        # 시총
        if market_cap < MIN_MARKET_CAP:
            continue

        # MA200 위
        if price <= ma200:
            continue

        # SPY 대비 상대강도 우위
        if (ret_6m - spy_ret_6m) <= MIN_RS_OVER_SPY:
            continue

        # 매출 성장
        if revenue_growth < MIN_REVENUE_GROWTH:
            continue

        # 유동성(평균 일거래대금)
        avg_dollar_volume = price * avg_volume_3m
        if avg_dollar_volume < MIN_AVG_DOLLAR_VOLUME:
            continue

        # 52주 고점 근접
        high_proximity = price / high_52w
        if high_proximity < MIN_HIGH_PROXIMITY:
            continue

        final_rows.append({
            "ticker": ticker,
            "revision_count": revision_count,
            "market_cap": market_cap,
            "price": price,
            "ma200": ma200,
            "ret_6m": ret_6m,
            "spy_ret_6m": spy_ret_6m,
            "high_52w": high_52w,
            "high_proximity": high_proximity,
            "revenue_growth": revenue_growth,
            "avg_dollar_volume": avg_dollar_volume,
        })

    final_df = pd.DataFrame(final_rows)

    # 7) 후보 너무 적으면 52주 고점 조건 자동 완화
    if len(final_df) < AUTO_RELAX_IF_FINAL_LT:
        relaxed_rows = []

        for _, row in stage1.iterrows():
            ticker = row["ticker"]
            revision_count = row["revision_count"]

            quote = get_quote_info(ticker)
            time.sleep(REQUEST_SLEEP)

            ret_6m = get_6m_return(ticker)
            time.sleep(REQUEST_SLEEP)

            revenue_growth = get_revenue_growth(ticker)
            time.sleep(REQUEST_SLEEP)

            if quote is None or ret_6m is None or revenue_growth is None:
                continue

            price = quote.get("price")
            market_cap = quote.get("market_cap")
            ma200 = quote.get("ma200")
            high_52w = quote.get("high_52w")
            avg_volume_3m = quote.get("avg_volume_3m")

            if (
                price is None
                or market_cap is None
                or ma200 is None
                or high_52w is None
                or avg_volume_3m is None
                or high_52w == 0
            ):
                continue

            if market_cap < MIN_MARKET_CAP:
                continue

            if price <= ma200:
                continue

            if (ret_6m - spy_ret_6m) <= MIN_RS_OVER_SPY:
                continue

            if revenue_growth < MIN_REVENUE_GROWTH:
                continue

            avg_dollar_volume = price * avg_volume_3m
            if avg_dollar_volume < MIN_AVG_DOLLAR_VOLUME:
                continue

            high_proximity = price / high_52w
            if high_proximity < RELAXED_HIGH_PROXIMITY:
                continue

            relaxed_rows.append({
                "ticker": ticker,
                "revision_count": revision_count,
                "market_cap": market_cap,
                "price": price,
                "ma200": ma200,
                "ret_6m": ret_6m,
                "spy_ret_6m": spy_ret_6m,
                "high_52w": high_52w,
                "high_proximity": high_proximity,
                "revenue_growth": revenue_growth,
                "avg_dollar_volume": avg_dollar_volume,
            })

        final_df = pd.DataFrame(relaxed_rows)

    if final_df.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print(f"Stage1 candidates: {len(stage1)}")
        print("Final candidates: 0")
        print("Multibagger candidates: 0")
        return

    # 8) 정렬
    final_df = final_df.sort_values(
        ["revision_count", "ret_6m", "high_proximity", "revenue_growth"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    # 9) 전체 후보
    final_df.to_csv("eps_candidates.csv", index=False)

    # 10) 상위 10개
    top_df = final_df.head(10).copy()
    top_df.to_csv("top_candidates.csv", index=False)

    # 11) 멀티배거 후보
    multi_df = final_df[
        (final_df["market_cap"] >= MIN_MULTIBAGGER_CAP)
        & (final_df["market_cap"] <= MAX_MULTIBAGGER_CAP)
        & (final_df["revenue_growth"] >= MIN_MULTIBAGGER_REVENUE_GROWTH)
        & (final_df["high_proximity"] >= MIN_MULTIBAGGER_HIGH_PROXIMITY)
    ].copy()

    multi_df.to_csv("multibagger_candidates.csv", index=False)

    print("Done.")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Stage1 candidates: {len(stage1)}")
    print(f"Final candidates: {len(final_df)}")
    print(f"Multibagger candidates: {len(multi_df)}")


if __name__ == "__main__":
    main()
