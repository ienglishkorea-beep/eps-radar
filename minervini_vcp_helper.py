import pandas as pd
import yfinance as yf


def download_price_history(ticker: str, period: str = "1y") -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval="1d",
        auto_adjust=False,
        progress=False,
    )
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df = df.reset_index()
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    need_cols = ["date", "open", "high", "low", "close", "volume"]
    for col in need_cols:
        if col not in df.columns:
            return pd.DataFrame()

    return df[need_cols].dropna().reset_index(drop=True)


def compute_vcp_features(df: pd.DataFrame) -> dict:
    if df.empty or len(df) < 60:
        return {
            "vcp_ready": False,
            "vcp_score": 0.0,
            "tight_10d": False,
            "near_52w_high": False,
            "price_above_ma50": False,
            "price_above_ma200": False,
            "base_depth": None,
            "range_10d": None,
            "volume_dryup": False,
        }

    x = df.copy()

    x["ma50"] = x["close"].rolling(50).mean()
    x["ma200"] = x["close"].rolling(200).mean()
    x["avg_vol_20"] = x["volume"].rolling(20).mean()
    x["avg_vol_5"] = x["volume"].rolling(5).mean()

    last = x.iloc[-1]

    high_52w = x["high"].tail(252).max() if len(x) >= 252 else x["high"].max()
    low_base = x["low"].tail(60).min()
    high_base = x["high"].tail(60).max()

    base_depth = None
    if high_base and high_base > 0:
        base_depth = (high_base - low_base) / high_base

    range_10d = None
    high_10 = x["high"].tail(10).max()
    low_10 = x["low"].tail(10).min()
    if high_10 and high_10 > 0:
        range_10d = (high_10 - low_10) / high_10

    price_above_ma50 = pd.notna(last["ma50"]) and last["close"] > last["ma50"]
    price_above_ma200 = pd.notna(last["ma200"]) and last["close"] > last["ma200"]
    near_52w_high = high_52w and last["close"] / high_52w >= 0.85
    tight_10d = range_10d is not None and range_10d <= 0.08
    volume_dryup = (
        pd.notna(last["avg_vol_5"])
        and pd.notna(last["avg_vol_20"])
        and last["avg_vol_20"] > 0
        and last["avg_vol_5"] < last["avg_vol_20"]
    )

    score = 0.0
    if price_above_ma50:
        score += 1.0
    if price_above_ma200:
        score += 1.0
    if near_52w_high:
        score += 1.0
    if tight_10d:
        score += 1.0
    if volume_dryup:
        score += 1.0
    if base_depth is not None:
        if base_depth <= 0.20:
            score += 1.0
        elif base_depth <= 0.30:
            score += 0.5

    vcp_ready = (
        price_above_ma50
        and price_above_ma200
        and near_52w_high
        and tight_10d
        and volume_dryup
        and base_depth is not None
        and base_depth <= 0.30
    )

    return {
        "vcp_ready": bool(vcp_ready),
        "vcp_score": round(score, 2),
        "tight_10d": bool(tight_10d),
        "near_52w_high": bool(near_52w_high),
        "price_above_ma50": bool(price_above_ma50),
        "price_above_ma200": bool(price_above_ma200),
        "base_depth": round(base_depth, 4) if base_depth is not None else None,
        "range_10d": round(range_10d, 4) if range_10d is not None else None,
        "volume_dryup": bool(volume_dryup),
    }


def evaluate_vcp_for_ticker(ticker: str) -> dict:
    df = download_price_history(ticker)
    return compute_vcp_features(df)
