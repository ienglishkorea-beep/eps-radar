from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from config import (
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_TIMEOUT_SECONDS,
    PRICE_WINDOWS,
    TIINGO_API_KEY,
    TIINGO_BASE_URL,
    TIINGO_DAILY_FIELDS,
    TIINGO_ENDPOINTS,
    TIINGO_STATEMENT_FIELDS,
    validate_config,
)


@dataclass
class TickerSnapshot:
    ticker: str
    price_df: pd.DataFrame
    daily_df: pd.DataFrame
    statements_df: pd.DataFrame


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _build_url(endpoint_key: str, ticker: Optional[str] = None) -> str:
    path = TIINGO_ENDPOINTS[endpoint_key]
    if ticker is not None:
        path = path.format(ticker=ticker)
    return f"{TIINGO_BASE_URL}{path}"


def _request_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    allowed_error_codes: Optional[List[int]] = None,
) -> Any:
    validate_config()

    final_params = dict(params or {})
    final_params["token"] = TIINGO_API_KEY

    try:
        response = requests.get(
            url,
            params=final_params,
            headers={"Content-Type": "application/json"},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if allowed_error_codes and status in allowed_error_codes:
            return None
        raise


def _deep_get(obj: Dict[str, Any], dotted_key: str) -> Any:
    current: Any = obj
    for part in dotted_key.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def fetch_price_history(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> pd.DataFrame:
    end_dt = pd.Timestamp(end_date).date() if end_date else date.today()
    start_dt = pd.Timestamp(start_date).date() if start_date else (end_dt - timedelta(days=lookback_days))

    raw = _request_json(
        _build_url("eod_price", ticker=ticker),
        params={
            "startDate": start_dt.isoformat(),
            "endDate": end_dt.isoformat(),
            "resampleFreq": "daily",
        },
        allowed_error_codes=[400, 404],
    )

    if not isinstance(raw, list) or not raw:
        return pd.DataFrame(
            columns=[
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "adjOpen",
                "adjHigh",
                "adjLow",
                "adjClose",
                "adjVolume",
            ]
        )

    df = pd.DataFrame(raw).copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_convert(None)

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adjOpen",
        "adjHigh",
        "adjLow",
        "adjClose",
        "adjVolume",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("date").reset_index(drop=True)


def build_price_metrics(price_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if price_df.empty:
        return {
            "last_close": None,
            "ret_3m": None,
            "ret_6m": None,
            "ma_50": None,
            "ma_200": None,
            "high_52w": None,
            "high_proximity": None,
        }

    close_col = "adjClose" if "adjClose" in price_df.columns and price_df["adjClose"].notna().sum() > 0 else "close"
    if close_col not in price_df.columns:
        return {
            "last_close": None,
            "ret_3m": None,
            "ret_6m": None,
            "ma_50": None,
            "ma_200": None,
            "high_52w": None,
            "high_proximity": None,
        }

    series = pd.to_numeric(price_df[close_col], errors="coerce")
    last_close = safe_float(series.iloc[-1]) if len(series) > 0 else None

    ma_50 = safe_float(series.rolling(PRICE_WINDOWS["ma_50"]).mean().iloc[-1]) if len(series) >= PRICE_WINDOWS["ma_50"] else None
    ma_200 = safe_float(series.rolling(PRICE_WINDOWS["ma_200"]).mean().iloc[-1]) if len(series) >= PRICE_WINDOWS["ma_200"] else None

    high_52w = None
    if "high" in price_df.columns:
        high_52w = safe_float(pd.to_numeric(price_df["high"].tail(PRICE_WINDOWS["high_52w"]), errors="coerce").max())
    if high_52w is None:
        high_52w = safe_float(series.tail(PRICE_WINDOWS["high_52w"]).max())

    ret_3m = None
    ret_6m = None

    if len(series) > PRICE_WINDOWS["ret_3m"]:
        base = safe_float(series.iloc[-(PRICE_WINDOWS["ret_3m"] + 1)])
        if base not in [None, 0] and last_close is not None:
            ret_3m = (last_close / base) - 1.0

    if len(series) > PRICE_WINDOWS["ret_6m"]:
        base = safe_float(series.iloc[-(PRICE_WINDOWS["ret_6m"] + 1)])
        if base not in [None, 0] and last_close is not None:
            ret_6m = (last_close / base) - 1.0

    high_proximity = None
    if high_52w not in [None, 0] and last_close is not None:
        high_proximity = last_close / high_52w

    return {
        "last_close": last_close,
        "ret_3m": ret_3m,
        "ret_6m": ret_6m,
        "ma_50": ma_50,
        "ma_200": ma_200,
        "high_52w": high_52w,
        "high_proximity": high_proximity,
    }


def fetch_fundamental_daily(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> pd.DataFrame:
    end_dt = pd.Timestamp(end_date).date() if end_date else date.today()
    start_dt = pd.Timestamp(start_date).date() if start_date else (end_dt - timedelta(days=lookback_days))

    raw = _request_json(
        _build_url("fundamental_daily", ticker=ticker),
        params={
            "startDate": start_dt.isoformat(),
            "endDate": end_dt.isoformat(),
        },
        allowed_error_codes=[400, 404],
    )

    expected_cols = ["date"] + sorted(set(TIINGO_DAILY_FIELDS.values()))
    if not isinstance(raw, list) or not raw:
        return pd.DataFrame(columns=expected_cols)

    df = pd.DataFrame(raw).copy()
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_convert(None)

    for col in TIINGO_DAILY_FIELDS.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values("date").reset_index(drop=True)


def extract_latest_daily_values(daily_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out = {k: None for k in TIINGO_DAILY_FIELDS.keys()}
    if daily_df.empty:
        return out

    latest = daily_df.iloc[-1].to_dict()
    for logical_name, data_code in TIINGO_DAILY_FIELDS.items():
        out[logical_name] = safe_float(latest.get(data_code))
    return out


def fetch_fundamental_statements(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    as_reported: bool = True,
) -> List[Dict[str, Any]]:
    end_dt = pd.Timestamp(end_date).date() if end_date else date.today()
    start_dt = pd.Timestamp(start_date).date() if start_date else (end_dt - timedelta(days=lookback_days))

    raw = _request_json(
        _build_url("fundamental_statements", ticker=ticker),
        params={
            "startDate": start_dt.isoformat(),
            "endDate": end_dt.isoformat(),
            "asReported": "true" if as_reported else "false",
        },
        allowed_error_codes=[400, 404],
    )

    if isinstance(raw, list):
        return raw
    return []


def _flatten_statement_items(raw_statements: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    section_map = [
        ("overview", "statementData.overview"),
        ("incomeStatement", "statementData.incomeStatement"),
        ("cashFlow", "statementData.cashFlow"),
        ("balanceSheet", "statementData.balanceSheet"),
    ]

    for entry in raw_statements:
        base_info = {
            "date": entry.get("date") or entry.get("reportDate") or entry.get("fiscalDate") or entry.get("calendarDate"),
            "year": entry.get("year"),
            "quarter": entry.get("quarter"),
            "statement_type": entry.get("statementType"),
            "ticker": entry.get("ticker"),
        }

        for section_name, dotted_key in section_map:
            section = _deep_get(entry, dotted_key)
            if not isinstance(section, list):
                continue

            for item in section:
                if not isinstance(item, dict):
                    continue

                rows.append(
                    {
                        **base_info,
                        "section": section_name,
                        "dataCode": item.get("dataCode"),
                        "name": item.get("name"),
                        "value": safe_float(item.get("value")),
                        "rawValue": safe_float(item.get("rawValue")),
                        "reportedValue": safe_float(item.get("reportedValue")),
                    }
                )

    if not rows:
        return pd.DataFrame(
            columns=[
                "date",
                "year",
                "quarter",
                "statement_type",
                "ticker",
                "section",
                "dataCode",
                "name",
                "value",
                "rawValue",
                "reportedValue",
            ]
        )

    df = pd.DataFrame(rows).copy()
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_convert(None)
    return df.sort_values(["date", "section", "dataCode"]).reset_index(drop=True)


def extract_statement_series(statements_df: pd.DataFrame, logical_name: str) -> pd.DataFrame:
    if statements_df.empty:
        return pd.DataFrame(columns=["date", "year", "quarter", "value"])

    data_code = TIINGO_STATEMENT_FIELDS.get(logical_name)
    if not data_code:
        return pd.DataFrame(columns=["date", "year", "quarter", "value"])

    subset = statements_df[statements_df["dataCode"] == data_code].copy()
    if subset.empty:
        return pd.DataFrame(columns=["date", "year", "quarter", "value"])

    subset["final_value"] = subset["value"]
    subset.loc[subset["final_value"].isna(), "final_value"] = subset["reportedValue"]
    subset.loc[subset["final_value"].isna(), "final_value"] = subset["rawValue"]

    subset = subset[["date", "year", "quarter", "final_value"]].rename(columns={"final_value": "value"})
    subset = subset.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    return subset


def extract_latest_statement_values(statements_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    out = {k: None for k in TIINGO_STATEMENT_FIELDS.keys()}
    for logical_name in TIINGO_STATEMENT_FIELDS.keys():
        series_df = extract_statement_series(statements_df, logical_name)
        if not series_df.empty:
            out[logical_name] = safe_float(series_df["value"].iloc[-1])
    return out


def pct_change_latest(series_df: pd.DataFrame) -> Optional[float]:
    if series_df.empty or len(series_df) < 2:
        return None

    latest = safe_float(series_df["value"].iloc[-1])
    prev = safe_float(series_df["value"].iloc[-2])
    if latest is None or prev in [None, 0]:
        return None

    return (latest / prev) - 1.0


def avg_pct_change_previous(series_df: pd.DataFrame, window: int = 3) -> Optional[float]:
    if series_df.empty or len(series_df) < window + 1:
        return None

    vals = list(series_df["value"].astype(float))
    changes: List[float] = []

    for i in range(1, len(vals)):
        prev = vals[i - 1]
        cur = vals[i]
        if prev == 0:
            continue
        changes.append((cur / prev) - 1.0)

    if len(changes) < 2:
        return None

    prior_changes = changes[:-1]
    if not prior_changes:
        return None

    tail = prior_changes[-window:]
    return float(sum(tail) / len(tail)) if tail else None


def acceleration_score_inputs(series_df: pd.DataFrame) -> Dict[str, Optional[float]]:
    latest_growth = pct_change_latest(series_df)
    prior_avg_growth = avg_pct_change_previous(series_df, window=3)

    acceleration = None
    if latest_growth is not None and prior_avg_growth is not None:
        acceleration = latest_growth - prior_avg_growth

    return {
        "latest_growth": latest_growth,
        "prior_avg_growth": prior_avg_growth,
        "acceleration": acceleration,
    }


def compute_calculated_metrics(
    last_close: Optional[float],
    latest_daily: Dict[str, Optional[float]],
    latest_stmt: Dict[str, Optional[float]],
    statements_df: pd.DataFrame,
) -> Dict[str, Optional[float]]:
    revenue_df = extract_statement_series(statements_df, "revenue")
    ebit_df = extract_statement_series(statements_df, "ebit")
    cfo_df = extract_statement_series(statements_df, "cfo")
    capex_df = extract_statement_series(statements_df, "capex")

    revenue_growth = pct_change_latest(revenue_df)
    ebit_growth = pct_change_latest(ebit_df)
    cfo_growth = pct_change_latest(cfo_df)
    capex_growth = pct_change_latest(capex_df)

    revenue = latest_stmt.get("revenue")
    ebit = latest_stmt.get("ebit")
    cfo = latest_stmt.get("cfo")
    capex = latest_stmt.get("capex")
    gross_profit = latest_stmt.get("gross_profit")

    market_cap = latest_daily.get("market_cap")
    shares_outstanding = latest_daily.get("shares_outstanding")
    if market_cap is None and last_close not in [None, 0] and shares_outstanding not in [None, 0]:
        market_cap = last_close * shares_outstanding

    enterprise_value = latest_daily.get("enterprise_value")
    debt = latest_stmt.get("debt")
    cash = latest_stmt.get("cash")
    if enterprise_value is None and market_cap is not None:
        enterprise_value = market_cap + (debt or 0.0) - (cash or 0.0)

    ev_sales = None
    if enterprise_value not in [None, 0] and revenue not in [None, 0]:
        ev_sales = enterprise_value / revenue

    ev_ebit = None
    if enterprise_value not in [None, 0] and ebit not in [None, 0]:
        ev_ebit = enterprise_value / ebit

    gross_margin_calc = None
    if gross_profit not in [None, 0] and revenue not in [None, 0]:
        gross_margin_calc = gross_profit / revenue

    free_cash_flow = latest_stmt.get("free_cash_flow")
    if free_cash_flow is None and cfo is not None and capex is not None:
        free_cash_flow = cfo + capex

    revenue_accel = acceleration_score_inputs(revenue_df)
    cfo_accel = acceleration_score_inputs(cfo_df)
    ebit_accel = acceleration_score_inputs(ebit_df)

    return {
        "market_cap": market_cap,
        "enterprise_value": enterprise_value,
        "free_cash_flow": free_cash_flow,
        "revenue_growth": revenue_growth,
        "ebit_growth": ebit_growth,
        "cfo_growth": cfo_growth,
        "capex_growth": capex_growth,
        "ev_sales": ev_sales,
        "ev_ebit": ev_ebit,
        "gross_margin_calc": gross_margin_calc,
        "revenue_latest_growth": revenue_accel["latest_growth"],
        "revenue_prior_avg_growth": revenue_accel["prior_avg_growth"],
        "revenue_acceleration": revenue_accel["acceleration"],
        "cfo_latest_growth": cfo_accel["latest_growth"],
        "cfo_prior_avg_growth": cfo_accel["prior_avg_growth"],
        "cfo_acceleration": cfo_accel["acceleration"],
        "ebit_latest_growth": ebit_accel["latest_growth"],
        "ebit_prior_avg_growth": ebit_accel["prior_avg_growth"],
        "ebit_acceleration": ebit_accel["acceleration"],
    }


def fetch_ticker_snapshot(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> TickerSnapshot:
    price_df = fetch_price_history(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )
    daily_df = fetch_fundamental_daily(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )
    raw_statements = fetch_fundamental_statements(
        ticker=ticker,
        start_date=start_date,
        end_date=end_date,
        lookback_days=lookback_days,
    )
    statements_df = _flatten_statement_items(raw_statements)

    return TickerSnapshot(
        ticker=ticker,
        price_df=price_df,
        daily_df=daily_df,
        statements_df=statements_df,
    )


def build_feature_row(
    ticker: str,
    name: Optional[str],
    sector: Optional[str],
    industry: Optional[str],
    snapshot: TickerSnapshot,
) -> Dict[str, Any]:
    price_metrics = build_price_metrics(snapshot.price_df)
    latest_daily = extract_latest_daily_values(snapshot.daily_df)
    latest_stmt = extract_latest_statement_values(snapshot.statements_df)
    calc = compute_calculated_metrics(
        last_close=price_metrics.get("last_close"),
        latest_daily=latest_daily,
        latest_stmt=latest_stmt,
        statements_df=snapshot.statements_df,
    )

    gross_margin = latest_stmt.get("gross_margin")
    if gross_margin is None:
        gross_margin = calc.get("gross_margin_calc")

    out = {
        "ticker": ticker,
        "name": name,
        "sector": sector,
        "industry": industry,
        **price_metrics,
        **latest_daily,
        **latest_stmt,
        **calc,
    }
    out["gross_margin"] = gross_margin
    return out


def load_universe_csv(path: str) -> pd.DataFrame:
    df = pd.read
