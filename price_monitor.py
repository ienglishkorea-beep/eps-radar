import requests
import pandas as pd
import os

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PROXIMITY_LEVEL = 0.98


def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    requests.post(url, data=payload)


def get_quote(ticker):
    url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
    r = requests.get(url)
    data = r.json()

    result = data["quoteResponse"]["result"]

    if not result:
        return None

    q = result[0]

    return {
        "price": q.get("regularMarketPrice"),
        "high52": q.get("fiftyTwoWeekHigh")
    }


def main():

    try:
        df = pd.read_csv("top_candidates.csv")
    except:
        return

    if df.empty:
        return

    alerts = []

    for ticker in df["ticker"]:

        quote = get_quote(ticker)

        if quote is None:
            continue

        price = quote["price"]
        high52 = quote["high52"]

        if price is None or high52 is None:
            continue

        proximity = price / high52

        if proximity >= 1:
            alerts.append(f"🚀 52W BREAKOUT: {ticker}  Price {price}")

        elif proximity >= PROXIMITY_LEVEL:
            alerts.append(f"⚠️ Near 52W High: {ticker}  Price {price}")

    if alerts:
        message = "Price Alert\n\n" + "\n".join(alerts)
        send_telegram(message)


if __name__ == "__main__":
    main()
