import time
import datetime as dt
from typing import Optional, List

import requests
import pandas as pd

# ===== 설정 =====
TICKERS = [
    "NVDA","AVGO","ANET","MSFT","META","AMZN","TSLA","AMD","SMCI",
    "NOW","KLAC","LRCX","AMAT","ASML","CRWD","PANW","SNOW"
]
LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 3
SLEEP_SEC = 0.8

HISTORY_FILE = "eps_history.csv"
TODAY_FILE = "eps_candidates.csv"

UA = {"User-Agent": "Mozilla/5.0"}
YF_QS = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{t}?modules=earningsTrend,price"

def get_eps_estimate_fy1(ticker: str) -> Optional[float]:
    try:
        url = YF_QS.format(t=ticker)
        r = requests.get(url, headers=UA, timeout=20)
        r.raise_for_status()
        data = r.json()

        res = data.get("quoteSummary", {}).get("result", None)
        if not res:
            return None

        trends = res[0].get("earningsTrend", {}).get("trend", []) or []
        for t in trends:
            if t.get("period") == "+1y":
                cur = t.get("epsTrend", {}).get("current", {}).get("raw", None)
                return float(cur) if cur is not None else None
        return None
    except Exception:
        return None

def load_history() -> pd.DataFrame:
    try:
        df = pd.read_csv(HISTORY_FILE)
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception:
        return pd.DataFrame(columns=["date","ticker","eps"])

def main():
    today = pd.to_datetime(dt.date.today().isoformat())

    hist = load_history()

    rows = []
    failed = 0
    for t in TICKERS:
        eps = get_eps_estimate_fy1(t)
        if eps is None:
            failed += 1
        rows.append({"date": today, "ticker": t, "eps": eps})
        time.sleep(SLEEP_SEC)

    df_today = pd.DataFrame(rows)

    # 중복 제거 후 누적
    hist = pd.concat([hist, df_today], ignore_index=True)
    hist = hist.drop_duplicates(subset=["date","ticker"], keep="last")
    hist = hist.sort_values(["ticker","date"])

    # 리비전 계산(전일 대비 eps 상승)
    hist["prev_eps"] = hist.groupby("ticker")["eps"].shift(1)
    hist["up_revision"] = (
        hist["eps"].notna() & hist["prev_eps"].notna() & (hist["eps"] > hist["prev_eps"])
    ).astype(int)

    # 최근 LOOKBACK_DAYS 내 카운트
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=LOOKBACK_DAYS)
    recent = hist[hist["date"] >= cutoff]

    agg = recent.groupby("ticker").agg(
        up_rev_cnt=("up_revision","sum"),
        eps_latest=("eps","last"),
        last_date=("date","max")
    ).reset_index()

    cand = agg[agg["up_rev_cnt"] >= REVISION_THRESHOLD].sort_values(
        ["up_rev_cnt","ticker"], ascending=[False, True]
    )

    # 저장
    hist.drop(columns=["prev_eps"], errors="ignore").to_csv(HISTORY_FILE, index=False)
    cand.to_csv(TODAY_FILE, index=False)

    print("=== EPS Radar ===")
    print(f"Date: {today.date().isoformat()}")
    print(f"Tickers: {len(TICKERS)}, Failed: {failed}")
    print(f"Saved: {HISTORY_FILE}, {TODAY_FILE}")
    if cand.empty:
        print("(no candidates)")
    else:
        print(cand.to_string(index=False))

if __name__ == "__main__":
    main()
