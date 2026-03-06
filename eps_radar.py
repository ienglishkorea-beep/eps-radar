import requests
import pandas as pd
import datetime
from io import StringIO

LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 3

def get_sp1500_tickers():

    urls = [
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"
    ]

    headers = {"User-Agent": "Mozilla/5.0"}

    tickers = []

    for url in urls:
        response = requests.get(url, headers=headers, timeout=20)
        tables = pd.read_html(StringIO(response.text))
        symbols = tables[0]["Symbol"].tolist()
        tickers.extend(symbols)

    tickers = [t.replace(".", "-") for t in tickers]

    return list(set(tickers))



def get_eps_estimate(ticker):
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=earningsTrend"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=20)
        data = r.json()

        trends = data["quoteSummary"]["result"][0]["earningsTrend"]["trend"]

        # +1y 추정치를 우선 사용
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


def main():
    tickers = get_sp1500_tickers()
    today = pd.Timestamp(datetime.date.today())

    # 오늘 스냅샷 수집
    rows = []
    for ticker in tickers:
        eps = get_eps_estimate(ticker)
        rows.append({
            "date": today,
            "ticker": ticker,
            "eps": eps
        })

    today_df = pd.DataFrame(rows)

    # 기존 히스토리 불러오기
    history = load_history()

    # 오늘 데이터 추가
    history = pd.concat([history[["date", "ticker", "eps"]], today_df], ignore_index=True)

    # 같은 날짜/티커 중복 제거
    history = history.drop_duplicates(subset=["date", "ticker"], keep="last")

    # 정렬
    history = history.sort_values(["ticker", "date"]).reset_index(drop=True)

    # 전일 대비 EPS 상향 여부 계산
    history["prev_eps"] = history.groupby("ticker")["eps"].shift(1)
    history["up_revision"] = (
        history["eps"].notna()
        & history["prev_eps"].notna()
        & (history["eps"] > history["prev_eps"])
    ).astype(int)

    # 저장용 history
    save_history = history[["date", "ticker", "eps", "up_revision"]].copy()
    save_history.to_csv("eps_history.csv", index=False)

    # 최근 LOOKBACK_DAYS 내 상향 횟수 계산
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=LOOKBACK_DAYS)
    recent = save_history[save_history["date"] >= cutoff]

    summary = (
        recent.groupby("ticker", as_index=False)["up_revision"]
        .sum()
        .rename(columns={"up_revision": "revision_count"})
    )

    candidates = summary[summary["revision_count"] >= REVISION_THRESHOLD].copy()
    candidates = candidates.sort_values(["revision_count", "ticker"], ascending=[False, True])

    candidates.to_csv("eps_candidates.csv", index=False)
    top_df = final_df.head(10).copy()
    top_df.to_csv("top_candidates.csv", index=False)

multi_df = final_df[
    (final_df["market_cap"] >= 2_000_000_000) &
    (final_df["market_cap"] <= 20_000_000_000)
].copy()

multi_df.to_csv("multibagger_candidates.csv", index=False)
    print("Done.")
    print(f"Tickers processed: {len(tickers)}")
    print(f"Candidates found: {len(candidates)}")


if __name__ == "__main__":
    main()
