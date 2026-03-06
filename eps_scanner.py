import requests
import pandas as pd
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg
    }
    requests.post(url, data=payload, timeout=20)


def load_universe():
    url = "https://financialmodelingprep.com/api/v3/stock/list?apikey=demo"
    data = requests.get(url, timeout=20).json()

    if not isinstance(data, list):
        return []

    rows = []
    for item in data:
        symbol = item.get("symbol")
        if symbol:
            rows.append(symbol)

    return rows


def scan_eps():
    tickers = load_universe()
    rows = []

    for t in tickers[:1500]:
        try:
            url = f"https://financialmodelingprep.com/api/v3/quote/{t}?apikey=demo"
            data = requests.get(url, timeout=20).json()

            if not isinstance(data, list) or len(data) == 0:
                continue

            quote = data[0]
            price = quote.get("price")

            if price is not None and price > 20:
                rows.append({
                    "ticker": t,
                    "price": price
                })

        except Exception:
            continue

    df = pd.DataFrame(rows)

    if df.empty:
        df = pd.DataFrame(columns=["ticker", "price"])
        df.to_csv("top_candidates.csv", index=False)
        send_telegram("EPS Radar\nNew candidates: 0")
        print("No candidates")
        return

    df.to_csv("top_candidates.csv", index=False)

    msg = f"EPS Radar\nNew candidates: {len(df)}"
    send_telegram(msg)

    print("EPS scan complete")


if __name__ == "__main__":
    scan_eps()
