import os
import requests
import pandas as pd

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


def load_csv_safe(path):
    try:
        df = pd.read_csv(path)
        if df.empty:
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()


def format_tickers(df, limit=10):
    if df.empty or "ticker" not in df.columns:
        return []
    return df["ticker"].dropna().astype(str).head(limit).tolist()


def main():
    eps_df = load_csv_safe("eps_candidates.csv")
    top_df = load_csv_safe("top_candidates.csv")
    multi_df = load_csv_safe("multibagger_candidates.csv")

    lines = []
    lines.append("EPS Radar Daily Update")

    if eps_df.empty:
        lines.append("")
        lines.append("EPS candidates: 0")
    else:
        lines.append("")
        lines.append(f"EPS candidates: {len(eps_df)}")

    top_tickers = format_tickers(top_df, limit=10)
    multi_tickers = format_tickers(multi_df, limit=10)

    if top_tickers:
        lines.append("")
        lines.append("Top candidates:")
        for t in top_tickers:
            lines.append(f"- {t}")
    else:
        lines.append("")
        lines.append("Top candidates: none")

    if multi_tickers:
        lines.append("")
        lines.append("Multibagger candidates:")
        for t in multi_tickers:
            lines.append(f"- {t}")
    else:
        lines.append("")
        lines.append("Multibagger candidates: none")

    message = "\n".join(lines)
    send_telegram(message)
    print("EPS alert sent.")


if __name__ == "__main__":
    main()
