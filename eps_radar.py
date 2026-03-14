from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from config import (
    COMMENT_LIBRARY,
    HEALTH_FLAG_LABELS,
    MAX_OUTPUT_ROWS,
    OUTPUT_COLUMNS,
    OUTPUT_CSV_PATH,
    SCORE_BANDS,
    UNIVERSE_CSV_PATH,
)
from data_client import (
    build_feature_row,
    fetch_ticker_snapshot,
    load_universe_csv,
)


# =========================================================
# HARD FILTERS
# =========================================================
MIN_LAST_CLOSE = 10.0
MIN_MARKET_CAP = 1_500_000_000  # 15억 달러
REQUIRED_FIELDS = [
    "last_close",
    "market_cap",
    "enterprise_value",
    "pe",
    "revenue",
    "gross_margin",
    "ebit",
    "cfo",
]

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


def fmt_pct(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.{digits}f}%"


def fmt_num(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def fmt_money(value: Optional[float]) -> str:
    if value is None:
        return "-"
    abs_v = abs(value)
    if abs_v >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}조달러"
    if abs_v >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}억달러"
    if abs_v >= 1_000_000:
        return f"{value / 1_000_000:.2f}백만달러"
    return f"{value:.0f}"


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
# HARD FILTERS
# =========================================================
def pass_hard_filters(row: Dict[str, Any]) -> tuple[bool, str]:
    for field in REQUIRED_FIELDS:
        if row.get(field) is None:
            return False, f"필수 데이터 부족: {field}"

    if row["last_close"] < MIN_LAST_CLOSE:
        return False, "최소 가격 미달"

    if row["market_cap"] < MIN_MARKET_CAP:
        return False, "최소 시가총액 미달"

    return True, ""


# =========================================================
# SCORE FUNCTIONS
# =========================================================
def score_acceleration(acceleration: Optional[float]) -> float:
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


def score_high_proximity(high_proximity: Optional[float]) -> float:
    if high_proximity is None:
        return 0.0
    hp = high_proximity * 100.0
    if hp >= 95:
        return 10.0
    if hp >= 90:
        return 8.0
    if hp >= 80:
        return 5.0
    if hp >= 70:
        return 2.0
    return 0.0


def score_ma_position(last_close: Optional[float], ma_50: Optional[float], ma_200: Optional[float]) -> float:
    score = 0.0

    if last_close is not None and ma_50 is not None and last_close > ma_50:
        score += 4.0
    if last_close is not None and ma_200 is not None and last_close > ma_200:
        score += 3.0
    if ma_50 is not None and ma_200 is not None and ma_50 > ma_200:
        score += 3.0

    return clamp(score, 0.0, 10.0)


def compute_price_position_score(row: Dict[str, Any]) -> float:
    ma_position_score = score_ma_position(
        row.get("last_close"),
        row.get("ma_50"),
        row.get("ma_200"),
    )
    high_proximity_score = score_high_proximity(row.get("high_proximity"))

    score = 0.75 * ma_position_score + 0.25 * high_proximity_score
    return round(clamp(score), 2)


def score_price_reaction(
    ret_3m: Optional[float],
    ret_6m: Optional[float],
) -> float:
    score = 0.0

    if ret_3m is not None:
        r3 = ret_3m * 100.0
        if r3 >= 25:
            score += 6.0
        elif r3 >= 15:
            score += 4.5
        elif r3 >= 5:
            score += 3.0
        elif r3 >= 0:
            score += 1.5

    if ret_6m is not None:
        r6 = ret_6m * 100.0
        if r6 >= 40:
            score += 4.0
        elif r6 >= 20:
            score += 3.0
        elif r6 >= 0:
            score += 1.5

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
    pm = row.get("profit_margin") if row.get("profit_margin") is not None else row.get("net_margin")
    profit_margin_score = score_profit_margin_level(pm)
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
    )
    price_position_score = compute_price_position_score(row)

    score = (
        0.45 * growth_acceleration_score
        + 0.30 * profitability_improvement_score
        + 0.10 * price_reaction_score
        + 0.05 * price_position_score
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

    if cash is not None and debt is not None and cash < 0.4 * debt:
        watches += 1

    if capex is not None and cfo not in [None, 0] and capex not in [None, 0]:
        cover = cfo / abs(capex)
        if cover < 0.7:
            warnings += 1
        elif cover < 1.0:
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
    price_position_score: float,
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

    if price_position_score >= 8:
        lines.append("가격 위치는 강하다.")
    elif price_position_score >= 5:
        lines.append("가격 위치는 무난하다.")
    else:
        lines.append("가격 위치는 아직 약하다.")

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


def build_detail_comment(row: Dict[str, Any], price_position_score: float) -> str:
    blocks: List[str] = []

    revenue_growth = row.get("revenue_growth")
    revenue_acc = row.get("revenue_acceleration")
    cfo_growth = row.get("cfo_growth")
    cfo_acc = row.get("cfo_acceleration")
    gross_margin = row.get("gross_margin")
    pm = row.get("profit_margin") if row.get("profit_margin") is not None else row.get("net_margin")
    ebit_growth = row.get("ebit_growth")
    high_proximity = row.get("high_proximity")
    ma_50 = row.get("ma_50")
    ma_200 = row.get("ma_200")
    last_close = row.get("last_close")

    if revenue_growth is not None:
        if revenue_acc is not None:
            blocks.append(f"매출 성장률 {fmt_pct(revenue_growth)}, 가속도 {fmt_pct(revenue_acc)}.")
        else:
            blocks.append(f"매출 성장률 {fmt_pct(revenue_growth)}.")

    if cfo_growth is not None:
        if cfo_acc is not None:
            blocks.append(f"영업현금흐름 성장률 {fmt_pct(cfo_growth)}, 가속도 {fmt_pct(cfo_acc)}.")
        else:
            blocks.append(f"영업현금흐름 성장률 {fmt_pct(cfo_growth)}.")

    if gross_margin is not None:
        blocks.append(f"총이익률 {fmt_pct(gross_margin)}.")
    if pm is not None:
        blocks.append(f"순이익률 {fmt_pct(pm)}.")
    if ebit_growth is not None:
        blocks.append(f"EBIT 성장률 {fmt_pct(ebit_growth)}.")

    if last_close is not None and ma_50 is not None and ma_200 is not None:
        pos_50 = "위" if last_close > ma_50 else "아래"
        pos_200 = "위" if last_close > ma_200 else "아래"
        stack = "정배열" if ma_50 > ma_200 else "역배열"
        blocks.append(
            f"종가는 50일선 {pos_50}, 200일선 {pos_200}, 이평 구조는 {stack}. 가격 위치 점수 {fmt_num(price_position_score)}."
        )

    if high_proximity is not None:
        blocks.append(f"52주 고점 근접도 {fmt_pct(high_proximity)}.")

    return " ".join(blocks)


# =========================================================
# MAIN PIPELINE
# =========================================================
def evaluate_row(base_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    passed, _ = pass_hard_filters(base_row)
    if not passed:
        return None

    growth_acceleration_score = compute_growth_acceleration_score(base_row)
    profitability_improvement_score = compute_profitability_improvement_score(base_row)
    valuation_score = compute_valuation_score(base_row)
    price_position_score = compute_price_position_score(base_row)
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
        base_row=base_row,
        growth_acceleration_score=growth_acceleration_score,
        profitability_improvement_score=profitability_improvement_score,
        expectation_upgrade_score=expectation_upgrade_score,
        valuation_score=valuation_score,
        price_position_score=price_position_score,
        health_flag=health_flag,
    )
    detail_comment = build_detail_comment(base_row, price_position_score)
    if detail_comment:
        summary_comment = f"{summary_comment} {detail_comment}".strip()

    enriched = dict(base_row)
    enriched.update(
        {
            "growth_acceleration_score": round(growth_acceleration_score, 2),
            "profitability_improvement_score": round(profitability_improvement_score, 2),
            "expectation_upgrade_score": round(expectation_upgrade_score, 2),
            "valuation_score": round(valuation_score, 2),
            "price_position_score": round(price_position_score, 2),
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
            if final_row is not None:
                rows.append(final_row)
                print(f"[OK] {ticker}")
            else:
                print(f"[SKIP] {ticker} | 하드필터 탈락")
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

    # 참고용 컬럼 추가
    extra_cols = ["ma_50", "ma_200", "price_position_score"]
    for col in extra_cols:
        if col not in df.columns:
            df[col] = None

    ordered_cols = OUTPUT_COLUMNS.copy()
    insert_after = ordered_cols.index("valuation_score") + 1 if "valuation_score" in ordered_cols else len(ordered_cols)
    for col in ["price_position_score"]:
        if col not in ordered_cols:
            ordered_cols.insert(insert_after, col)
            insert_after += 1

    df = df[ordered_cols + [c for c in extra_cols if c not in ordered_cols]].copy()
    df = df.sort_values(
        ["total_score", "expectation_upgrade_score", "growth_acceleration_score", "price_position_score"],
        ascending=[False, False, False, False],
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
        "price_position_score",
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
