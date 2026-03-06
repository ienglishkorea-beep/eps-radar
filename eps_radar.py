import requests
import pandas as pd
import datetime
from io import StringIO
LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 3


def get_sp500_tickers():

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers, timeout=20)

    html = response.text

    tables = pd.read_html(StringIO(html))

    tickers = tables[0]["Symbol"].tolist()

    tickers = [ticker.replace(".", "-") for ticker in tickers]

    return tickers

def get_eps_estimate(ticker):
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=earningsTrend"
        r = requests.get(url)
        data = r.json()

        trends = data["quoteSummary"]["result"][0]["earningsTrend"]["trend"]

        revisions = 0
        eps_values = []

        for t in trends:
            if "earningsEstimate" in t:
                est = t["earningsEstimate"].get("avg", {}).get("raw")
                if est:
                    eps_values.append(est)

        if len(eps_values) >= 2:
            for i in range(1, len(eps_values)):
                if eps_values[i] > eps_values[i-1]:
                    revisions += 1

        return revisions

    except:
        return 0


def main():

    tickers = get_sp500_tickers()

    results = []

    for ticker in tickers:

        revisions = get_eps_estimate(ticker)

        if revisions >= REVISION_THRESHOLD:

            results.append({
                "ticker": ticker,
                "revisions": revisions,
                "date": datetime.date.today()
            })

    df = pd.DataFrame(results)

    df.to_csv("eps_candidates.csv", index=False)

    history = pd.read_csv("eps_history.csv") if \
        requests.get("https://raw.githubusercontent.com").status_code else pd.DataFrame()

    history = pd.concat([history, df])

    history.to_csv("eps_history.csv", index=False)


if __name__ == "__main__":
    main()
