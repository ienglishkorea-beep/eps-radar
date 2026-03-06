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

    requests.post(url, data=payload, timeout=10)


def load_universe():
    url = "https://financialmodelingprep.com/api/v3/stock/list?apikey=demo"
    data = requests.get(url).json()
    df = pd.DataFrame(data)
    return df["symbol"].dropna().tolist()


def scan_eps():
    tickers = load_universe()

    rows = []

    for t in tickers[:1500]:

        try:

            url = f"https://financialmodelingprep.com/api/v3/quote/{t}?apikey=demo"
            data = requests.get(url).json()

            if not data:
                continue

            price = data[0]["price"]

            if price > 20:
                rows.append({
                    "ticker": t,
                    "price": price
                })

        except:
            continue

    df = pd.DataFrame(rows)

    df.to_csv("top_candidates.csv", index=False)

    if len(df) > 0:

        msg = f"EPS Radar\nNew candidates: {len(df)}"

        send_telegram(msg)

    print("EPS scan complete")


if __name__ == "__main__":
    scan_eps()
