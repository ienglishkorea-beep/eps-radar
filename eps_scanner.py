import os
import requests
import pandas as pd

# =========================================
# ALERT TEXT
# =========================================
ALERT_TITLE = "AEGIS EPS Radar"
SECTION_TOP = "Top candidates"
SECTION_MULTI = "Multibagger candidates"
EMPTY_TEXT = "none"

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


def format_line(row):
    ticker = str(row.get("ticker", ""))
    rev = row.get("revision_count", "")
    rev_g = row.get("revenue_growth", None)
    ret6 = row.get("ret_6m", None)
    prox = row.get("high_proximity", None)

    parts = [ticker]

    if rev != "":
        parts.append(f"rev {int(rev)}")

    if rev_g is not None and pd.notna(rev_g):
        parts.append(f"sales {rev_g * 100:.0f}%")

    if ret6 is not None and pd.notna(ret6):
        parts.append(f"6m {ret6 * 100:.0f}%")

    if prox is not None and pd.notna(prox):
        parts.append(f"52w {prox * 100:.0f}%")

    return " | ".join(parts)


def main():
    eps_df = load_csv_safe("eps_candidates.csv")
    top_df = load_csv_safe("top_candidates.csv")
    multi_df = load_csv_safe("multibagger_candidates.csv")

    lines = [ALERT_TITLE, ""]

    lines.append(f"EPS candidates: {len(eps_df)}")
    lines.append("")

    if top_df.empty:
        lines.append(f"{SECTION_TOP}: {EMPTY_TEXT}")
    else:
        lines.append(f"{SECTION_TOP}:")
        for _, row in top_df.head(10).iterrows():
            lines.append(f"- {format_line(row)}")

    lines.append("")

    if multi_df.empty:
        lines.append(f"{SECTION_MULTI}: {EMPTY_TEXT}")
    else:
        lines.append(f"{SECTION_MULTI}:")
        for _, row in multi_df.head(10).iterrows():
            lines.append(f"- {format_line(row)}")

    message = "\n".join(lines)
    send_telegram(message)
    print("EPS alert sent.")


if __name__ == "__main__":
    main()
