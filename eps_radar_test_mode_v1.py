import pandas as pd

# =========================================
# EPS RADAR TEST MODE (QUANT ONLY)
# - Guidance / earnings call / conference call 제거
# - 정량 신호만으로 ULTRA / MULTIBAGGER / 일반 후보 검증
# =========================================

OUTPUT_COLS = [
    "ticker",
    "is_ultra",
    "is_multibagger",
    "detection_number",
    "signal_stage",
    "action",
    "rev_speed_count_30d",
    "rev_speed_tag",
    "growth_accel_tag",
    "growth_accel_proxy",
    "quality_proxy_tag",
    "score",
    "revision_count",
    "revenue_growth",
    "gross_margin",
    "operating_margin",
    "price",
    "entry_price",
    "stop_price",
    "high_20d",
    "market_cap",
    "sector",
    "sector_etf",
    "ret_6m",
    "sector_ret_6m",
    "spy_ret_6m",
    "ma50",
    "ma200",
    "high_52w",
    "high_proximity",
    "volume_ratio",
    "avg_dollar_volume",
    "earnings_date_note",
]

# -----------------------------
# 기준값
# -----------------------------
MIN_MARKET_CAP = 2_000_000_000
MIN_PRICE = 10.0
MIN_AVG_DOLLAR_VOLUME = 10_000_000
MIN_REVENUE_GROWTH = 0.15
MIN_HIGH_PROXIMITY = 0.80
MIN_VOLUME_RATIO = 1.0
MIN_RS_OVER_SPY = 0.00

MIN_MULTIBAGGER_CAP = 2_000_000_000
MAX_MULTIBAGGER_CAP = 25_000_000_000
MIN_MULTIBAGGER_REVENUE_GROWTH = 0.20
MIN_MULTIBAGGER_HIGH_PROXIMITY = 0.90
MIN_MULTIBAGGER_VOLUME_RATIO = 1.2

SECTOR_MAP = {
    "Technology": "XLK",
    "Semiconductors": "SMH",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Healthcare": "XLV",
    "Financial Services": "XLF",
    "Financial": "XLF",
    "Industrials": "XLI",
    "Energy": "XLE",
    "Basic Materials": "XLB",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Communication Services": "XLC",
}


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def get_signal_stage(revision_count: int) -> str:
    if revision_count == 2:
        return "INITIAL"
    elif revision_count == 3:
        return "EARLY"
    elif revision_count == 4:
        return "MID"
    return "LATE"


def get_action(revision_count: int) -> str:
    if revision_count in [2, 3]:
        return "BUY"
    elif revision_count == 4:
        return "WATCH"
    return "NO_ENTRY"


def get_eps_score(revision_count: int) -> float:
    if revision_count == 2:
        return 1.00
    elif revision_count == 3:
        return 0.90
    elif revision_count == 4:
        return 0.70
    return 0.50


def get_revenue_score(revenue_growth: float) -> float:
    if revenue_growth >= 0.30:
        return 1.00
    elif revenue_growth >= 0.20:
        return 0.80
    elif revenue_growth >= 0.15:
        return 0.60
    elif revenue_growth >= 0.10:
        return 0.30
    return 0.0


def get_breakout_proxy_score(high_proximity: float) -> float:
    if high_proximity >= 0.95:
        return 1.0
    elif high_proximity >= 0.90:
        return 0.7
    elif high_proximity >= 0.85:
        return 0.4
    return 0.0


def get_sector_proxy_score(stock_ret_6m, sector_ret_6m, spy_ret_6m) -> float:
    if sector_ret_6m > spy_ret_6m and stock_ret_6m > sector_ret_6m:
        return 1.0
    elif sector_ret_6m > spy_ret_6m:
        return 0.5
    return 0.0


def get_rev_speed_tag(rev_speed_count_30d: int) -> str:
    if rev_speed_count_30d >= 2:
        return "REV_ACCEL"
    return "NORMAL"


def get_growth_accel_proxy(
    revision_count: int,
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
) -> float:
    eps_part = get_eps_score(revision_count)
    rev_part = get_revenue_score(revenue_growth)
    breakout_part = get_breakout_proxy_score(high_proximity)
    sector_part = get_sector_proxy_score(stock_ret_6m, sector_ret_6m, spy_ret_6m)

    proxy = (
        0.35 * eps_part
        + 0.35 * rev_part
        + 0.15 * breakout_part
        + 0.15 * sector_part
    )
    return round(proxy, 2)


def get_growth_accel_tag(
    revision_count: int,
    revenue_growth: float,
    high_proximity: float,
    stock_ret_6m: float,
    sector_ret_6m: float,
    spy_ret_6m: float,
) -> str:
    strong_sector = sector_ret_6m > spy_ret_6m
    strong_stock = stock_ret_6m > sector_ret_6m

    if (
        revision_count in [2, 3]
        and revenue_growth >= 0.20
        and high_proximity >= 0.90
        and strong_sector
        and strong_stock
    ):
        return "ACCEL"
    elif revision_count in [2, 3] and revenue_growth >= 0.15:
        return "EARLY"
    return "NORMAL"


def get_quality_proxy_tag(revenue_growth, gross_margin, operating_margin):
    if (
        revenue_growth is not None
        and gross_margin is not None
        and operating_margin is not None
        and revenue_growth >= 0.20
        and gross_margin >= 0.45
        and operating_margin >= 0.15
    ):
        return "QUALITY"
    return "NORMAL"


def compute_score(
    revision_count,
    revenue_growth,
    ret_6m,
    sector_ret_6m,
    spy_ret_6m,
    high_proximity,
    volume_ratio,
    price,
    ma50,
    ma200,
    rev_speed_tag,
):
    eps_score = get_eps_score(revision_count)
    rev_score = get_revenue_score(revenue_growth)

    trend_parts = 0.0
    if price > ma50:
        trend_parts += 0.35
    if price > ma200:
        trend_parts += 0.35

    rs_vs_spy = ret_6m - spy_ret_6m
    trend_parts += 0.30 * clamp01(rs_vs_spy / 0.30)
    trend_score = clamp01(trend_parts)

    prox_score = clamp01((high_proximity - 0.80) / 0.20)
    vol_score = clamp01((volume_ratio - 1.0) / 1.5)
    sector_score = 1.0 if sector_ret_6m > spy_ret_6m else 0.0
    stock_sector_score = 1.0 if ret_6m > sector_ret_6m else 0.0
    rev_speed_bonus = 1.0 if rev_speed_tag == "REV_ACCEL" else 0.0

    score = (
        34 * eps_score
        + 26 * rev_score
        + 16 * trend_score
        + 8 * prox_score
        + 5 * vol_score
        + 5 * sector_score
        + 4 * stock_sector_score
        + 2 * rev_speed_bonus
    )
    return round(score, 2)


def build_mock_universe() -> pd.DataFrame:
    # AAA: ULTRA가 떠야 하는 강한 초기 리더
    # BBB: MULTIBAGGER는 가능하지만 ULTRA는 아님
    # CCC: 탈락해야 하는 약한 후보
    rows = [
        {
            "ticker": "AAA",
            "revision_count": 2,
            "rev_speed_count_30d": 2,
            "revenue_growth": 0.32,
            "gross_margin": 0.68,
            "operating_margin": 0.22,
            "price": 118.0,
            "high_20d": 117.0,
            "ma50": 104.0,
            "ma200": 88.0,
            "high_52w": 120.0,
            "market_cap": 12_000_000_000,
            "sector": "Technology",
            "ret_6m": 0.48,
            "sector_ret_6m": 0.24,
            "spy_ret_6m": 0.10,
            "volume_ratio": 1.60,
            "avg_dollar_volume": 220_000_000,
            "earnings_date_note": "2026-03-25",
        },
        {
            "ticker": "BBB",
            "revision_count": 3,
            "rev_speed_count_30d": 1,
            "revenue_growth": 0.23,
            "gross_margin": 0.52,
            "operating_margin": 0.17,
            "price": 72.0,
            "high_20d": 71.0,
            "ma50": 68.0,
            "ma200": 57.0,
            "high_52w": 78.0,
            "market_cap": 9_000_000_000,
            "sector": "Technology",
            "ret_6m": 0.31,
            "sector_ret_6m": 0.18,
            "spy_ret_6m": 0.10,
            "volume_ratio": 1.15,
            "avg_dollar_volume": 95_000_000,
            "earnings_date_note": "2026-04-02",
        },
        {
            "ticker": "CCC",
            "revision_count": 2,
            "rev_speed_count_30d": 0,
            "revenue_growth": 0.08,
            "gross_margin": 0.29,
            "operating_margin": 0.05,
            "price": 41.0,
            "high_20d": 42.0,
            "ma50": 43.0,
            "ma200": 39.0,
            "high_52w": 60.0,
            "market_cap": 5_000_000_000,
            "sector": "Industrials",
            "ret_6m": 0.07,
            "sector_ret_6m": 0.14,
            "spy_ret_6m": 0.10,
            "volume_ratio": 0.90,
            "avg_dollar_volume": 22_000_000,
            "earnings_date_note": "2026-03-18",
        },
    ]
    return pd.DataFrame(rows)


def build_candidates_test(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in df.iterrows():
        ticker = row["ticker"]
        revision_count = int(row["revision_count"])
        rev_speed_count_30d = int(row["rev_speed_count_30d"])
        revenue_growth = float(row["revenue_growth"])
        gross_margin = float(row["gross_margin"])
        operating_margin = float(row["operating_margin"])
        price = float(row["price"])
        high_20d = float(row["high_20d"])
        ma50 = float(row["ma50"])
        ma200 = float(row["ma200"])
        high_52w = float(row["high_52w"])
        market_cap = float(row["market_cap"])
        sector = row["sector"]
        ret_6m = float(row["ret_6m"])
        sector_ret_6m = float(row["sector_ret_6m"])
        spy_ret_6m = float(row["spy_ret_6m"])
        volume_ratio = float(row["volume_ratio"])
        avg_dollar_volume = float(row["avg_dollar_volume"])
        earnings_date_note = row["earnings_date_note"]

        sector_etf = SECTOR_MAP.get(sector)
        if sector_etf is None:
            continue

        # 하드 필터
        if market_cap < MIN_MARKET_CAP:
            continue
        if price < MIN_PRICE:
            continue
        if avg_dollar_volume < MIN_AVG_DOLLAR_VOLUME:
            continue
        if revenue_growth < MIN_REVENUE_GROWTH:
            continue
        if price <= ma50:
            continue
        if price <= ma200:
            continue
        if (ret_6m - spy_ret_6m) <= MIN_RS_OVER_SPY:
            continue
        if sector_ret_6m <= spy_ret_6m:
            continue
        if ret_6m <= sector_ret_6m:
            continue

        high_proximity = price / high_52w
        if high_proximity < MIN_HIGH_PROXIMITY:
            continue
        if volume_ratio < MIN_VOLUME_RATIO:
            continue

        entry_price = max(high_20d * 1.01, price * 1.02)
        stop_price = entry_price * 0.85

        signal_stage = get_signal_stage(revision_count)
        action = get_action(revision_count)
        rev_speed_tag = get_rev_speed_tag(rev_speed_count_30d)

        growth_accel_proxy = get_growth_accel_proxy(
            revision_count=revision_count,
            revenue_growth=revenue_growth,
            high_proximity=high_proximity,
            stock_ret_6m=ret_6m,
            sector_ret_6m=sector_ret_6m,
            spy_ret_6m=spy_ret_6m,
        )

        growth_accel_tag = get_growth_accel_tag(
            revision_count=revision_count,
            revenue_growth=revenue_growth,
            high_proximity=high_proximity,
            stock_ret_6m=ret_6m,
            sector_ret_6m=sector_ret_6m,
            spy_ret_6m=spy_ret_6m,
        )

        quality_proxy_tag = get_quality_proxy_tag(
            revenue_growth=revenue_growth,
            gross_margin=gross_margin,
            operating_margin=operating_margin,
        )

        score = compute_score(
            revision_count=revision_count,
            revenue_growth=revenue_growth,
            ret_6m=ret_6m,
            sector_ret_6m=sector_ret_6m,
            spy_ret_6m=spy_ret_6m,
            high_proximity=high_proximity,
            volume_ratio=volume_ratio,
            price=price,
            ma50=ma50,
            ma200=ma200,
            rev_speed_tag=rev_speed_tag,
        )

        is_multibagger = (
            (market_cap >= MIN_MULTIBAGGER_CAP)
            and (market_cap <= MAX_MULTIBAGGER_CAP)
            and (revenue_growth >= MIN_MULTIBAGGER_REVENUE_GROWTH)
            and (high_proximity >= MIN_MULTIBAGGER_HIGH_PROXIMITY)
            and (volume_ratio >= MIN_MULTIBAGGER_VOLUME_RATIO)
            and (action != "NO_ENTRY")
            and (growth_accel_tag == "ACCEL")
            and (quality_proxy_tag == "QUALITY")
        )

        is_ultra = (
            (revision_count in [2, 3])
            and (rev_speed_count_30d >= 1)
            and (growth_accel_tag == "ACCEL")
            and (quality_proxy_tag == "QUALITY")
            and (action == "BUY")
            and (high_proximity >= 0.85)
            and (volume_ratio >= 1.2)
            and (revenue_growth >= 0.15)
        )

        rows.append(
            {
                "ticker": ticker,
                "is_ultra": bool(is_ultra),
                "is_multibagger": bool(is_multibagger),
                "detection_number": revision_count,
                "signal_stage": signal_stage,
                "action": action,
                "rev_speed_count_30d": rev_speed_count_30d,
                "rev_speed_tag": rev_speed_tag,
                "growth_accel_tag": growth_accel_tag,
                "growth_accel_proxy": growth_accel_proxy,
                "quality_proxy_tag": quality_proxy_tag,
                "score": score,
                "revision_count": revision_count,
                "revenue_growth": revenue_growth,
                "gross_margin": gross_margin,
                "operating_margin": operating_margin,
                "price": price,
                "entry_price": round(entry_price, 2),
                "stop_price": round(stop_price, 2),
                "high_20d": round(high_20d, 2),
                "market_cap": market_cap,
                "sector": sector,
                "sector_etf": sector_etf,
                "ret_6m": ret_6m,
                "sector_ret_6m": sector_ret_6m,
                "spy_ret_6m": spy_ret_6m,
                "ma50": ma50,
                "ma200": ma200,
                "high_52w": high_52w,
                "high_proximity": high_proximity,
                "volume_ratio": volume_ratio,
                "avg_dollar_volume": avg_dollar_volume,
                "earnings_date_note": earnings_date_note,
            }
        )

    return pd.DataFrame(rows)


def main():
    test_df = build_mock_universe()
    final_df = build_candidates_test(test_df)

    if final_df.empty:
        print("테스트 결과: 최종 후보 0개")
        return

    growth_rank = {"ACCEL": 0, "EARLY": 1, "NORMAL": 2}
    action_rank = {"BUY": 0, "WATCH": 1, "NO_ENTRY": 2}

    final_df["ultra_sort"] = final_df["is_ultra"].astype(int) * -1
    final_df["multi_sort"] = final_df["is_multibagger"].astype(int) * -1
    final_df["growth_sort"] = final_df["growth_accel_tag"].map(growth_rank).fillna(9)
    final_df["action_sort"] = final_df["action"].map(action_rank).fillna(9)

    final_df = final_df.sort_values(
        [
            "ultra_sort",
            "multi_sort",
            "growth_sort",
            "action_sort",
            "score",
            "revision_count",
            "ret_6m",
            "high_proximity",
        ],
        ascending=[True, True, True, True, False, True, False, False],
    ).reset_index(drop=True)

    final_df = final_df.drop(columns=["ultra_sort", "multi_sort", "growth_sort", "action_sort"])
    final_df = final_df[OUTPUT_COLS]

    top_df = final_df.head(10).copy()
    multi_df = final_df[final_df["is_multibagger"] == True].copy()
    ultra_df = final_df[final_df["is_ultra"] == True].copy()

    final_df.to_csv("eps_candidates_test.csv", index=False)
    top_df.to_csv("top_candidates_test.csv", index=False)
    multi_df.to_csv("multibagger_candidates_test.csv", index=False)
    ultra_df.to_csv("ultra_candidates_test.csv", index=False)

    print("TEST MODE 완료")
    print()
    print("예상 체크포인트")
    print("- AAA: ULTRA=True, MULTIBAGGER=True, BUY, ACCEL, QUALITY")
    print("- BBB: ULTRA=False, MULTIBAGGER=False 또는 경계, BUY, EARLY/ACCEL 혼합 확인")
    print("- CCC: 탈락")
    print()
    print(f"최종 후보 수: {len(final_df)}")
    print(f"ULTRA 수: {len(ultra_df)}")
    print(f"MULTIBAGGER 수: {len(multi_df)}")
    print()
    print(final_df.to_string(index=False))


if __name__ == "__main__":
    main()
