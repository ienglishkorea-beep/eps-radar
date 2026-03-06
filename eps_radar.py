import requests
import pandas as pd
import datetime
from io import StringIO
import time

LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 3
REQUEST_SLEEP = 0.15

MIN_MULTIBAGGER_CAP = 2_000_000_000
MAX_MULTIBAGGER_CAP = 20_000_000_000
MIN_52W_HIGH_PROXIMITY = 0.85


def get_sp1500_tickers():
    urls = [
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
    ]

    headers = {"User-Agent": "Mozilla/5.0"}
    tickers = []

    for url in urls:
        response = requests.get(url, headers=headers, timeout=20)
        tables = pd.read_html(StringIO(response.text))
        symbols = tables[0]["Symbol"].tolist()
        tickers.extend(symbols)

    tickers = [t.replace(".", "-") for t in tickers]
    return sorted(list(set(tickers)))


def get_eps_estimate(ticker):
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=earningsTrend"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=20)
        data = r.json()

        result = data.get("quoteSummary", {}).get("result")
        if not result:
            return None

        trends = result[0].get("earningsTrend", {}).get("trend", [])

        for t in trends:
            if t.get("period") == "+1y":
                current = t.get("epsTrend", {}).get("current", {}).get("raw")
                if current is not None:
                    return current

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


def get_quote_info(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=20)
        data = r.json()

        results = data.get("quoteResponse", {}).get("result", [])
        if not results:
            return None

        q = results[0]

        price = q.get("regularMarketPrice")
        market_cap = q.get("marketCap")
        ma200 = q.get("twoHundredDayAverage")
        high_52w = q.get("fiftyTwoWeekHigh")

        return {
            "price": price,
            "market_cap": market_cap,
            "ma200": ma200,
            "high_52w": high_52w
        }

    except Exception:
        return None


def get_6m_return(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=20)
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


def save_empty_outputs():
    pd.DataFrame(columns=["ticker", "revision_count"]).to_csv("eps_candidates.csv", index=False)
    pd.DataFrame(columns=["ticker", "revision_count"]).to_csv("top_candidates.csv", index=False)
    pd.DataFrame(columns=[
        "ticker", "revision_count", "market_cap", "price", "ma200",
        "ret_6m", "spy_ret_6m", "high_proximity"
    ]).to_csv("multibagger_candidates.csv", index=False)


def main():
    today = pd.Timestamp(datetime.date.today())

    # 1) S&P1500 전체 EPS 스냅샷
    tickers = get_sp1500_tickers()

    rows = []
    for ticker in tickers:
        eps = get_eps_estimate(ticker)
        rows.append({
            "date": today,
            "ticker": ticker,
            "eps": eps
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

    # 4) 최근 90일 누적 상향 횟수
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

    # 6) 최종 필터 적용
    final_rows = []

    for _, row in stage1.iterrows():
        ticker = row["ticker"]
        revision_count = row["revision_count"]

        quote = get_quote_info(ticker)
        time.sleep(REQUEST_SLEEP)
        ret_6m = get_6m_return(ticker)
        time.sleep(REQUEST_SLEEP)

        if quote is None or ret_6m is None:
            continue

        price = quote.get("price")
        market_cap = quote.get("market_cap")
        ma200 = quote.get("ma200")
        high_52w = quote.get("high_52w")

        if price is None or market_cap is None or ma200 is None or high_52w is None or high_52w == 0:
            continue

        # 공통 필터
        if price <= ma200:
            continue

        if ret_6m <= spy_ret_6m:
            continue

        high_proximity = price / high_52w

        final_rows.append({
            "ticker": ticker,
            "revision_count": revision_count,
            "market_cap": market_cap,
            "price": price,
            "ma200": ma200,
            "ret_6m": ret_6m,
            "spy_ret_6m": spy_ret_6m,
            "high_proximity": high_proximity
        })

    final_df = pd.DataFrame(final_rows)

    if final_df.empty:
        save_empty_outputs()
        print("Done.")
        print(f"Tickers processed: {len(tickers)}")
        print(f"Stage1 candidates: {len(stage1)}")
        print("Final candidates: 0")
        return

    final_df = final_df.sort_values(
        ["revision_count", "ret_6m"],
        ascending=[False, False]
    ).reset_index(drop=True)

    # 7) 전체 후보
    final_df.to_csv("eps_candidates.csv", index=False)

    # 8) 상위 10개
    top_df = final_df.head(10).copy()
    top_df.to_csv("top_candidates.csv", index=False)

    # 9) 멀티배거 후보
    multi_df = final_df[
        (final_df["market_cap"] >= MIN_MULTIBAGGER_CAP) &
        (final_df["market_cap"] <= MAX_MULTIBAGGER_CAP) &
        (final_df["high_proximity"] >= MIN_52W_HIGH_PROXIMITY)
    ].copy()

    multi_df.to_csv("multibagger_candidates.csv", index=False)

    print("Done.")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Stage1 candidates: {len(stage1)}")
    print(f"Final candidates: {len(final_df)}")
    print(f"Top candidates saved: {len(top_df)}")
    print(f"Multibagger candidates saved: {len(multi_df)}")


if __name__ == "__main__":
    main()
