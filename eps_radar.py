import requests
import pandas as pd
import datetime

TICKERS = [
"NVDA","AVGO","ANET","MSFT","META","AMZN","TSLA","AMD","SMCI",
"NOW","KLAC","LRCX","AMAT","ASML","CRWD","PANW","SNOW"
]

LOOKBACK_DAYS = 90
REVISION_THRESHOLD = 3

def get_eps_estimate(ticker):
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=earningsTrend"
        r = requests.get(url)
        data = r.json()

        trends = data["quoteSummary"]["result"][0]["earningsTrend"]["trend"]

        for t in trends:
            if t["period"] == "+1y":
                return t["epsTrend"]["current"]["raw"]

    except:
        return None

def load_history():
    try:
        return pd.read_csv("eps_history.csv")
    except:
        return pd.DataFrame(columns=["date","ticker","eps"])

def main():

    today = str(datetime.date.today())

    history = load_history()

    rows = []

    for t in TICKERS:
        eps = get_eps_estimate(t)
        rows.append([today,t,eps])

    df = pd.DataFrame(rows,columns=["date","ticker","eps"])

    history = pd.concat([history,df])

    history.to_csv("eps_history.csv",index=False)

    history["prev"] = history.groupby("ticker")["eps"].shift(1)

    history["revision"] = (history["eps"] > history["prev"]).astype(int)

    cutoff = pd.Timestamp.today() - pd.Timedelta(days=LOOKBACK_DAYS)

    recent = history[pd.to_datetime(history["date"]) >= cutoff]

    result = recent.groupby("ticker")["revision"].sum()

    candidates = result[result >= REVISION_THRESHOLD]

    print("EPS REVISION CANDIDATES")
    print(candidates)

main()
