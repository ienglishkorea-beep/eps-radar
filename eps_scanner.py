import os
import requests
import pandas as pd

ALERT_TITLE = "AEGIS EPS + MOMENTUM RADAR"
SECTION_TOP = "Top candidates"
SECTION_MULTI = "Multibagger candidates (★)"
EMPTY_TEXT = "none"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
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
    detect_n = row.get("detection_number", None)
    stage = str(row.get("signal_stage", ""))
    action = str(row.get("action", ""))
    gtag = str(row.get("growth_accel_tag", ""))
    gproxy = row.get("growth_accel_proxy", None)
    qtag = str(row.get("quality_proxy_tag", ""))
    sales = row.get("revenue_growth", None)
    price = row.get("price", None)
    entry = row.get("entry_price", None)
    stop = row.get("stop_price", None)
    prox = row.get("high_proximity", None)
    vol = row.get("volume_ratio", None)
    sector = row.get("sector_etf", "")
    score = row.get("score", None)
    is_multi = row.get("is_multibagger", False)

    prefix = "★ " if str(is_multi).lower() == "true" or is_multi is True else ""

    parts = [f"{prefix}{ticker}"]

    if detect_n is not None and pd.notna(detect_n):
        parts.append(f"#{int(detect_n)}")
    if stage:
        parts.append(stage)
    if action:
        parts.append(action)
    if gtag:
        parts.append(f"g:{gtag}")
    if qtag:
        parts.append(f"q:{qtag}")
    if gproxy is not None and pd.notna(gproxy):
        parts.append(f"gp {float(gproxy):.2f}")
    if sales is not None and pd.notna(sales):
        parts.append(f"sales {float(sales) * 100:.0f}%")
    if price is not None and pd.notna(price):
        parts.append(f"px {float(price):.2f}")
    if entry is not None and pd.notna(entry):
        parts.append(f"entry {float(entry):.2f}")
    if stop is not None and pd.notna(stop):
        parts.append(f"stop {float(stop):.2f}")
    if prox is not None and pd.notna(prox):
        parts.append(f"52w {float(prox) * 100:.0f}%")
    if vol is not None and pd.notna(vol):
        parts.append(f"vol {float(vol):.1f}x")
    if sector:
        parts.append(f"sec {sector}")
    if score is not None and pd.notna(score):
        parts.append(f"score {float(score):.1f}")

    return " | ".join(parts)


def main():
    final_df = load_csv_safe("eps_candidates.csv")
    top_df = load_csv_safe("top_candidates.csv")
    multi_df = load_csv_safe("multibagger_candidates.csv")
    stage1_df = load_csv_safe("eps_stage1_raw.csv")

    lines = [ALERT_TITLE, ""]
    lines.append(f"Stage1 raw: {len(stage1_df)}")
    lines.append(f"Final candidates: {len(final_df)}")
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

    send_telegram("\n".join(lines))
    print("EPS alert sent.")


if __name__ == "__main__":
    main()
