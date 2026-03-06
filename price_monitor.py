import datetime as dt
import os

import pandas as pd
import requests

# =========================================
# ALERT TEXT
# =========================================
PRICE_ALERT_TITLE = "AEGIS Price Radar"
NEAR_PREFIX = "Near 52W High"
BREAKOUT_PREFIX = "52W Breakout"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

PROXIMITY_LEVEL = 0.98
STATE_FILE = "price_alert_state.csv"


def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    requests.post(url, data=payload, timeout=20)


def load_csv_safe(path):
    try:
        df = pd.read_csv(path)
        if df.empty or "ticker" not in df.columns:
            return pd.DataFrame(columns=["ticker"])
        return df
    except Exception:
        return pd.DataFrame(columns=["ticker"])


def load_watchlist():
    frames = [
        load_csv_safe("eps_candidates.csv"),
        load_csv_safe("top_candidates.csv"),
        load_csv_safe("multibagger_candidates.csv"),
    ]

    merged = pd.concat([df[["ticker"]] for df in frames], ignore_index=True)
    merged = merged.dropna().drop_duplicates().reset_index(drop=True)
    return merged


def get_quote(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
        r = requests.get(url, timeout=20)
        data = r.json()

        result = data.get("quoteResponse", {}).get("result", [])
        if not result:
            return None

        q = result[0]

        return {
            "price": q.get("regularMarketPrice"),
            "high52": q.get("fiftyTwoWeekHigh"),
        }
    except Exception:
        return None


def load_state():
    try:
        return pd.read_csv(STATE_FILE)
    except Exception:
        return pd.DataFrame(columns=["date", "ticker", "signal"])


def save_state(df):
    df.to_csv(STATE_FILE, index=False)


def already_sent(state_df, date_str, ticker, signal):
    mask = (
        (state_df["date"] == date_str)
        & (state_df["ticker"] == ticker)
        & (state_df["signal"] == signal)
    )
    return mask.any()


def main():
    watchlist = load_watchlist()

    if watchlist.empty:
        pd.DataFrame(columns=["ticker", "price", "high52", "signal"]).to_csv("price_alerts.csv", index=False)
        print("No watchlist.")
        return

    state_df = load_state()
    today_str = dt.date.today().isoformat()

    alerts_to_send = []
    current_alert_rows = []

    for ticker in watchlist["ticker"]:
        quote = get_quote(ticker)
        if quote is None:
            continue

        price = quote.get("price")
        high52 = quote.get("high52")

        if price is None or high52 is None or high52 == 0:
            continue

        proximity = price / high52

        # 돌파
        if price >= high52:
            current_alert_rows.append({
                "ticker": ticker,
                "price": price,
                "high52": high52,
                "signal": "breakout52",
            })

            if not already_sent(state_df, today_str, ticker, "breakout52"):
                alerts_to_send.append(f"- {BREAKOUT_PREFIX}: {ticker} | price {price:.2f}")
                state_df = pd.concat([
                    state_df,
                    pd.DataFrame([{
                        "date": today_str,
                        "ticker": ticker,
                        "signal": "breakout52",
                    }]),
                ], ignore_index=True)

        # 근접
        elif proximity >= PROXIMITY_LEVEL:
            current_alert_rows.append({
                "ticker": ticker,
                "price": price,
                "high52": high52,
                "signal": "near52",
            })

            if not already_sent(state_df, today_str, ticker, "near52"):
                alerts_to_send.append(f"- {NEAR_PREFIX}: {ticker} | price {price:.2f}")
                state_df = pd.concat([
                    state_df,
                    pd.DataFrame([{
                        "date": today_str,
                        "ticker": ticker,
                        "signal": "near52",
                    }]),
                ], ignore_index=True)

    pd.DataFrame(current_alert_rows).to_csv("price_alerts.csv", index=False)
    save_state(state_df)

    if alerts_to_send:
        message = PRICE_ALERT_TITLE + "\n\n" + "\n".join(alerts_to_send)
        send_telegram(message)
        print("Price alert sent.")
    else:
        print("No new price alerts.")


if __name__ == "__main__":
    main()
