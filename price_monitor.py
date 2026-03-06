import requests
import pandas as pd
import time

REQUEST_SLEEP = 0.15


def load_watchlist():
    dfs = []

    for fname in ["top_candidates.csv", "multibagger_candidates.csv"]:
        try:
            df = pd.read_csv(fname)
            if "ticker" in df.columns and not df.empty:
                dfs.append(df[["ticker"]].copy())
        except Exception:
            continue

    if not dfs:
        return []

    watch = pd.concat(dfs, ignore_index=True).drop_duplicates()
    return watch["ticker"].tolist()


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

        return {
            "price": q.get("regularMarketPrice"),
            "ma200": q.get("twoHundredDayAverage"),
            "high_52w": q.get("fiftyTwoWeekHigh"),
        }

    except Exception:
        return None


def get_20d_high(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=2mo&interval=1d"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=20)
        data = r.json()

        result = data.get("chart", {}).get("result")
        if not result:
            return None

        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [c for c in closes if c is not None]

        if len(closes) < 20:
            return None

        return max(closes[-20:])

    except Exception:
        return None


def main():
    tickers = load_watchlist()

    if not tickers:
        pd.DataFrame(columns=[
            "ticker", "price", "ma200", "high_52w", "high_20d",
            "above_200ma", "near_52w_high", "breakout_20d"
        ]).to_csv("price_alerts.csv", index=False)
        print("No watchlist tickers.")
        return

    rows = []

    for ticker in tickers:
        quote = get_quote_info(ticker)
        time.sleep(REQUEST_SLEEP)
        high_20d = get_20d_high(ticker)
        time.sleep(REQUEST_SLEEP)

        if quote is None:
            continue

        price = quote.get("price")
        ma200 = quote.get("ma200")
        high_52w = quote.get("high_52w")

        if price is None:
            continue

        above_200ma = (ma200 is not None) and (price > ma200)
        near_52w_high = (high_52w is not None) and (high_52w > 0) and (price / high_52w >= 0.98)
        breakout_20d = (high_20d is not None) and (price >= high_20d)

        if above_200ma or near_52w_high or breakout_20d:
            rows.append({
                "ticker": ticker,
                "price": price,
                "ma200": ma200,
                "high_52w": high_52w,
                "high_20d": high_20d,
                "above_200ma": above_200ma,
                "near_52w_high": near_52w_high,
                "breakout_20d": breakout_20d,
            })

    alerts = pd.DataFrame(rows)
    alerts.to_csv("price_alerts.csv", index=False)

    print("Done.")
    print(f"Watchlist tickers: {len(tickers)}")
    print(f"Price alerts: {len(alerts)}")


if __name__ == "__main__":
    main()
