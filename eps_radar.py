from __future__ import annotations

import random
from typing import Any, Dict, List, Optional

import pandas as pd

from config import (
    COMMENT_LIBRARY,
    HEALTH_FLAG_LABELS,
    KOREAN_LABELS,
    MAX_OUTPUT_ROWS,
    OUTPUT_COLUMNS,
    OUTPUT_CSV_PATH,
    SCORE_BANDS,
    UNIVERSE_CSV_PATH,
)
from data_client import (
    build_feature_row,
    load_universe_csv,
    fetch_ticker_snapshot,
    safe_float,
)


# =========================================================
# BASIC HELPERS
# =========================================================
def clamp(value: Optional[float], low: float = 0.0, high: float = 10.0) -> float:
    if value is None:
        return low
    return max(low, min(high, float(value)))


def pct_to_point(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 100.0


def pick_comment(key: str) -> str:
    rows = COMMENT_LIBRARY.get(key, [])
    if not rows:
        return ""
    return rows[0]


def get_signal_tier(total_score: float) -> str:
    for band in SCORE_BANDS:
        if total_score >= float(band["min"]):
            return str(band["label"])
    return "보류"


# =========================================================
# SCORE FUNCTIONS
# =========================================================
def score_acceleration(acceleration: Optional[float]) -> float:
    """
    acceleration is decimal difference.
    e.g. +0.10 == +10%p
    """
    if acceleration is None:
        return 0.0

    ap = acceleration * 100.0

    if ap >= 10:
        return 10.0
    if ap >= 5:
        return 8.0
    if ap >= 0:
        return 6.0
    if ap >= -5:
        return 4.0
    if ap >= -10:
        return 2.0
    return 0.0


def score_margin_level(margin: Optional[float]) -> float:
    if margin is None:
        return 0.0

    m = margin * 100.0
    if m >= 70:
        return 10.0
    if m >= 60:
        return 9.0
    if m >= 50:
        return 8.0
    if m >= 40:
        return 6.0
    if m >= 30:
        return 4.0
    if m >= 20:
        return 2.0
    return 0.0


def score_profit_margin_level(margin: Optional[float]) -> float:
    if margin is None:
        return 0.0

    m = margin * 100.0
    if m >= 30:
        return 10.0
    if m >= 20:
        return 8.0
    if m >= 10:
        return 6.0
    if m >= 5:
        return 4.0
    if m >= 0:
        return 2.0
    return 0.0


def score_growth_level(growth: Optional[float]) -> float:
    if growth is None:
        return 0.0

    g = growth * 100.0
    if g >= 30:
        return 10.0
    if g >= 20:
        return 8.0
    if g >= 10:
        return 6.0
    if g >= 5:
        return 4.0
    if g >= 0:
        return 2.0
    return 0.0


def score_positive_growth(growth: Optional[float]) -> float:
    if growth is None:
        return 0.0

    g = growth * 100.0
    if g >= 30:
        return 10.0
    if g >= 20:
        return 8.0
    if g >= 10:
        return 6.0
    if g >= 0:
        return 4.0
    return 1.0


def score_ebit_growth(growth: Optional[float], ebit: Optional[float]) -> float:
    if ebit is None or ebit <= 0:
        return 0.0
    if growth is None:
        return 6.0
    return score_positive_growth(growth)


def score_pe(pe: Optional[float]) -> float:
    if pe is None or pe <= 0:
        return 0.0
    if pe < 15:
        return 10.0
    if pe < 25:
        return 8.0
    if pe < 35:
        return 6.0
    if pe < 50:
        return 4.0
    return 1.0


def score_ev_sales(ev_sales: Optional[float]) -> float:
    if ev_sales is None or ev_sales <= 0:
        return 0.0
    if ev_sales < 3:
        return 10.0
    if ev_sales < 6:
        return 8.0
    if ev_sales < 10:
        return 6.0
    if ev_sales < 15:
        return 3.0
    return 0.0


def score_ev_ebit(ev_ebit: Optional[float]) -> float:
    if ev_ebit is None or ev_ebit <= 0:
        return 0.0
    if ev_ebit < 10:
        return 10.0
    if ev_ebit < 20:
        return 8.0
    if ev_ebit < 30:
        return 6.0
    if ev_ebit < 40:
        return 3.0
    return 0.0


def score_price_reaction(ret_3m: Optional[float], ret_6m: Optional[float], high_proximity: Optional[float]) -> float:
    score = 0.0

    if ret_3m is not None:
        r3 = ret_3m * 100.0
        if r3 >= 25:
            score += 4.0
        elif r3 >= 15:
            score += 3.0
        elif r3 >= 5:
            score += 2.0
        elif r3 >= 0:
            score += 1.0

    if ret_6m is not None:
        r6 = ret_6m * 100.0
        if r6 >= 40:
            score += 3.0
        elif r6 >= 20:
            score += 2.0
        elif r6 >= 0:
            score += 1.0

    if high_proximity is not None:
        hp = high_proximity * 100.0
        if hp >= 95:
            score += 3.0
        elif hp >= 90:
            score += 2.0
        elif hp >= 80:
            score += 1.0

    return clamp(score, 0.0, 10.0)


def compute_growth_acceleration_score(row: Dict[str, Any]) -> float:
    revenue_accel_score = score_acceleration(row.get("revenue_acceleration"))
    cfo_accel_score = score_acceleration(row.get("cfo_acceleration"))
    ebit_accel_score = score_acceleration(row.get("ebit_acceleration"))

    score = (
        0.50 * revenue_accel_score
        + 0.30 * cfo_accel_score
        + 0.20 * ebit_accel_score
    )
    return round(clamp(score), 2)


def compute_profitability_improvement_score(row: Dict[str, Any]) -> float:
    gross_margin_score = score_margin_level(row.get("gross_margin"))
    profit_margin_value = row.get("profit_margin")
    if profit_margin_value is None:
        profit_margin_value = row.get("net_margin")
    profit_margin_score = score_profit_margin_level(profit_margin_value)
    ebit_score = score_ebit_growth(row.get("ebit_growth"), row.get("ebit"))

    score = (
        0.40 * gross_margin_score
        + 0.35 * profit_margin_score
        + 0.25 * ebit_score
    )
    return round(clamp(score), 2)


def compute_valuation_score(row: Dict[str, Any]) -> float:
    pe_score = score_pe(row.get("pe"))
    ev_sales_score = score_ev_sales(row.get("ev_sales"))
    ev_ebit_score = score_ev_ebit(row.get("ev_ebit"))

    score = (
        0.50 * pe_score
        + 0.30 * ev_sales_score
        + 0.20 * ev_ebit_score
    )
    return round(clamp(score), 2)


def compute_expectation_upgrade_score(
    row: Dict[str, Any],
    growth_acceleration_score: float,
    profitability_improvement_score: float,
    valuation_score: float,
) -> float:
    price_reaction_score = score_price_reaction(
        row.get("ret_3m"),
        row.get("ret_6m"),
        row.get("high_proximity"),
    )

    score = (
        0.45 * growth_acceleration_score
        + 0.30 * profitability_improvement_score
        + 0.15 * price_reaction_score
        + 0.10 * valuation_score
    )
    return round(clamp(score), 2)


def compute_health_flag(row: Dict[str, Any]) -> str:
    cfo = row.get("cfo")
    fcf = row.get("free_cash_flow")
    debt = row.get("debt")
    cash = row.get("cash")
    capex = row.get("capex")

    warnings = 0
    watches = 0

    if cfo is None or cfo <= 0:
        warnings += 1

    if fcf is None or fcf < 0:
        watches += 1

    if debt is not None and cfo not in [None, 0]:
        if debt / cfo > 5:
            warnings += 1
        elif debt / cfo > 3:
            watches += 1

    if cash is not None and debt is not None:
        if cash < 0.4 * debt:
            watches += 1

    if capex is not None and cfo not in [None, 0]:
        try:
            cover = cfo / abs(capex) if capex != 0 else None
        except Exception:
            cover = None
        if cover is not None and cover < 0.7:
            warnings += 1
        elif cover is not None and cover < 1.0:
            watches += 1

    if warnings >= 1:
        return HEALTH_FLAG_LABELS["warning"]
    if watches >= 2:
        return HEALTH_FLAG_LABELS["watch"]
    return HEALTH_FLAG_LABELS["normal"]


def apply_health_penalty(total_score: float, health_flag: str) -> float:
    if health_flag == HEALTH_FLAG_LABELS["warning"]:
        return round(max(0.0, total_score - 1.2), 2)
    if health_flag == HEALTH_FLAG_LABELS["watch"]:
        return round(max(0.0, total_score - 0.5), 2)
    return round(total_score, 2)


# =========================================================
# COMMENTS
# =========================================================
def build_summary_comment(
    row: Dict[str, Any],
    growth_acceleration_score: float,
    profitability_improvement_score: float,
    expectation_upgrade_score: float,
    valuation_score: float,
    health_flag: str,
) -> str:
    lines: List[str] = []

    if growth_acceleration_score >= 8:
        lines.append(pick_comment("growth_accel_strong"))
    elif growth_acceleration_score >= 5:
        lines.append(pick_comment("growth_accel_flat"))
    else:
        lines.append(pick_comment("growth_accel_weak"))

    if profitability_improvement_score >= 8:
        lines.append(pick_comment("profitability_strong"))
    elif profitability_improvement_score >= 5:
        lines.append(pick_comment("profitability_flat"))
    else:
        lines.append(pick_comment("profitability_weak"))

    if valuation_score >= 6:
        lines.append(pick_comment("valuation_ok"))
    else:
        lines.append(pick_comment("valuation_rich"))

    if health_flag == HEALTH_FLAG_LABELS["normal"]:
        lines.append(pick_comment("health_normal"))
    elif health_flag == HEALTH_FLAG_LABELS["watch"]:
        lines.append(pick_comment("health_watch"))
    else:
        lines.append(pick_comment("health_warning"))

    if expectation_upgrade_score >= 8:
        lines.append("다음 실적 기대 상향 가능성이 높다.")
    elif expectation_upgrade_score >= 6:
        lines.append("다음 실적 기대 상향 가능성은 열려 있다.")
    else:
        lines.append("다음 실적 기대 상향 신호는 약하다.")

    lines = [x for x in lines if x]
    return " ".join(lines)


# =========================================================
# DETAIL INTERPRETATION
# =========================================================
def interpret_metric_block(row: Dict[str, Any]) -> str:
    parts: List[str] = []

    revenue_growth = pct_to_point(row.get("revenue_growth"))
    revenue_acc = pct_to_point(row.get("revenue_acceleration"))
    cfo_growth = pct_to_point(row.get("cfo_growth"))
    cfo_acc = pct_to_point(row.get("cfo_acceleration"))
    gross_margin = pct_to_point(row.get("gross_margin"))
    profit_margin = pct_to_point(row.get("profit_margin") if row.get("profit_margin") is not None else row.get("net_margin"))
    ebit_growth = pct_to_point(row.get("ebit_growth"))

    if revenue_growth is not None:
        if revenue_acc is not None:
            parts.append(f"{KOREAN_LABELS['revenue_growth']} {revenue_growth:.1f}%, 가속도 {revenue_acc:+.1f}%p.")
        else:
            parts.append(f"{KOREAN_LABELS['revenue_growth']} {revenue_growth:.1f}%.")

    if cfo_growth is not None:
        if cfo_acc is not None:
            parts.append(f"{KOREAN_LABELS['cfo_growth']} {cfo_growth:.1f}%, 가속도 {cfo_acc:+.1f}%p.")
        else:
            parts.append(f"{KOREAN_LABELS['cfo_growth']} {cfo_growth:.1f}%.")

    if gross_margin is not None:
        parts.append(f"{KOREAN_LABELS['gross_margin']} {gross_margin:.1f}%.")

    if profit_margin is not None:
        parts.append(f"{KOREAN_LABELS['profit_margin']} {profit_margin:.1f}%.")

    if ebit_growth is not None:
        parts.append(f"{KOREAN_LABELS['ebit_growth']} {ebit_growth:.1f}%.")

    return " ".join(parts)


# =========================================================
# MAIN PIPELINE
# =========================================================
def evaluate_row(base_row: Dict[str, Any]) -> Dict[str, Any]:
    growth_acceleration_score = compute_growth_acceleration_score(base_row)
    profitability_improvement_score = compute_profitability_improvement_score(base_row)
    valuation_score = compute_valuation_score(base_row)
    expectation_upgrade_score = compute_expectation_upgrade_score(
        base_row,
        growth_acceleration_score=growth_acceleration_score,
        profitability_improvement_score=profitability_improvement_score,
        valuation_score=valuation_score,
    )

    raw_total_score = (
        0.34 * growth_acceleration_score
        + 0.26 * profitability_improvement_score
        + 0.24 * expectation_upgrade_score
        + 0.16 * valuation_score
    )

    health_flag = compute_health_flag(base_row)
    total_score = apply_health_penalty(raw_total_score, health_flag)
    signal_tier = get_signal_tier(total_score)

    summary_comment = build_summary_comment(
        base_row,
        growth_acceleration_score=growth_acceleration_score,
        profitability_improvement_score=profitability_improvement_score,
        expectation_upgrade_score=expectation_upgrade_score,
        valuation_score=valuation_score,
        health_flag=health_flag,
    )

    detail_comment = interpret_metric_block(base_row)
    if detail_comment:
        summary_comment = f"{summary_comment} {detail_comment}".strip()

    enriched = dict(base_row)
    enriched.update(
        {
            "growth_acceleration_score": round(growth_acceleration_score, 2),
            "profitability_improvement_score": round(profitability_improvement_score, 2),
            "expectation_upgrade_score": round(expectation_upgrade_score, 2),
            "valuation_score": round(valuation_score, 2),
            "health_flag": health_flag,
            "total_score": round(total_score, 2),
            "signal_tier": signal_tier,
            "summary_comment": summary_comment,
        }
    )
    return enriched


def build_all_rows(universe_df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for _, r in universe_df.iterrows():
        ticker = str(r["ticker"]).strip().upper()
        name = None if pd.isna(r["name"]) else r["name"]
        sector = None if pd.isna(r["sector"]) else r["sector"]
        industry = None if pd.isna(r["industry"]) else r["industry"]

        try:
            snapshot = fetch_ticker_snapshot(ticker=ticker)
            feature_row = build_feature_row(
                ticker=ticker,
                name=name,
                sector=sector,
                industry=industry,
                snapshot=snapshot,
            )
            final_row = evaluate_row(feature_row)
            rows.append(final_row)
            print(f"[OK] {ticker}")
        except Exception as e:
            print(f"[FAIL] {ticker} | {e}")

    return rows


def finalize_output(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    df = pd.DataFrame(rows).copy()

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[OUTPUT_COLUMNS].copy()
    df = df.sort_values(
        ["total_score", "expectation_upgrade_score", "growth_acceleration_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    if len(df) > MAX_OUTPUT_ROWS:
        df = df.head(MAX_OUTPUT_ROWS).copy()

    return df


def print_preview(df: pd.DataFrame) -> None:
    if df.empty:
        print("결과 없음")
        return

    cols = [
        "ticker",
        "name",
        "signal_tier",
        "total_score",
        "expectation_upgrade_score",
        "growth_acceleration_score",
        "profitability_improvement_score",
        "valuation_score",
        "health_flag",
    ]
    show_cols = [c for c in cols if c in df.columns]
    print(df[show_cols].to_string(index=False))


def main() -> None:
    universe_df = load_universe_csv(UNIVERSE_CSV_PATH)
    print(f"유니버스 종목 수: {len(universe_df)}")

    rows = build_all_rows(universe_df)
    out_df = finalize_output(rows)
    out_df.to_csv(OUTPUT_CSV_PATH, index=False)

    print("")
    print(f"저장 완료: {OUTPUT_CSV_PATH}")
    print(f"rows: {len(out_df)}")
    print("")
    print_preview(out_df)


if __name__ == "__main__":
    main()
