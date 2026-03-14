from __future__ import annotations

import os
from typing import List

import pandas as pd
import requests

from config import OUTPUT_CSV_PATH

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

TOP_LIST_ROWS = 50
TOP_DETAIL_ROWS = 10
REQUEST_TIMEOUT = 30
MAX_MESSAGE_LEN = 3500


def _format_pct(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{float(value) * 100:.1f}%"
    except Exception:
        return "-"


def _format_num(value, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "-"


def _format_large_money(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    try:
        v = float(value)
    except Exception:
        return "-"

    abs_v = abs(v)
    if abs_v >= 1_000_000_000_000:
        return f"{v / 1_000_000_000_000:.2f}T"
    if abs_v >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B"
    if abs_v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    return f"{v:.0f}"


def _split_message(text: str, max_len: int = MAX_MESSAGE_LEN) -> List[str]:
    chunks: List[str] = []
    current = ""

    for line in text.splitlines(True):
        if len(current) + len(line) > max_len:
            if current:
                chunks.append(current.strip())
            current = line
        else:
            current += line

    if current:
        chunks.append(current.strip())

    return [x for x in chunks if x.strip()]


def build_top50_list_message(df: pd.DataFrame) -> str:
    if df.empty:
        return "Fundamental Direction Radar\n\n결과 없음"

    lines: List[str] = []
    lines.append("Fundamental Direction Radar")
    lines.append("")
    lines.append("상위 50개 리스트")
    lines.append("")

    top = df.head(TOP_LIST_ROWS).copy()

    for i, (_, row) in enumerate(top.iterrows(), start=1):
        ticker = str(row.get("ticker", "") or "")
        name = str(row.get("name", "") or "")
        signal_tier = str(row.get("signal_tier", "") or "")
        total_score = _format_num(row.get("total_score"))
        expectation_score = _format_num(row.get("expectation_upgrade_score"))

        lines.append(
            f"{i:02d}. {ticker} | {name} | 종합 {total_score} | 기대상향 {expectation_score} | {signal_tier}"
        )

    return "\n".join(lines).strip()


def build_top10_detail_message(df: pd.DataFrame) -> str:
    if df.empty:
        return "상세 결과 없음"

    lines: List[str] = []
    lines.append("상위 10개 상세 해석")
    lines.append("")

    top = df.head(TOP_DETAIL_ROWS).copy()

    for i, (_, row) in enumerate(top.iterrows(), start=1):
        ticker = str(row.get("ticker", "") or "")
        name = str(row.get("name", "") or "")
        signal_tier = str(row.get("signal_tier", "") or "")
        total_score = _format_num(row.get("total_score"))
        expectation_score = _format_num(row.get("expectation_upgrade_score"))
        growth_score = _format_num(row.get("growth_acceleration_score"))
        profitability_score = _format_num(row.get("profitability_improvement_score"))
        valuation_score = _format_num(row.get("valuation_score"))
        health_flag = str(row.get("health_flag", "") or "")

        revenue_growth = _format_pct(row.get("revenue_growth"))
        cfo_growth = _format_pct(row.get("cfo_growth"))
        gross_margin = _format_pct(row.get("gross_margin"))
        profit_margin = _format_pct(
            row.get("profit_margin") if pd.notna(row.get("profit_margin")) else row.get("net_margin")
        )
        pe = _format_num(row.get("pe"))
        ev_sales = _format_num(row.get("ev_sales"))
        ev_ebit = _format_num(row.get("ev_ebit"))
        market_cap = _format_large_money(row.get("market_cap"))
        summary_comment = str(row.get("summary_comment", "") or "").strip()

        lines.append(f"{i}) {ticker} | {name}")
        lines.append(f"판정: {signal_tier}")
        lines.append(f"종합 {total_score} | 기대상향 {expectation_score}")
        lines.append(
            f"성장가속 {growth_score} | 수익성개선 {profitability_score} | 밸류허용 {valuation_score}"
        )
        lines.append(f"매출성장률 {revenue_growth} | CFO성장률 {cfo_growth}")
        lines.append(f"총이익률 {gross_margin} | 순이익률 {profit_margin}")
        lines.append(f"PER {pe} | EV/매출 {ev_sales} | EV/EBIT {ev_ebit}")
        lines.append(f"시가총액 {market_cap} | 건전성 {health_flag}")
        if summary_comment:
            lines.append(f"해석: {summary_comment}")
        lines.append("")

    return "\n".join(lines).strip()


def send_telegram_message(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN 환경변수가 비어 있다.")
    if not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_CHAT_ID 환경변수가 비어 있다.")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    for chunk in _split_message(text):
        response = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()


def main() -> None:
    df = pd.read_csv(OUTPUT_CSV_PATH)

    top50_message = build_top50_list_message(df)
    detail_message = build_top10_detail_message(df)

    send_telegram_message(top50_message)
    send_telegram_message(detail_message)

    print("텔레그램 전송 완료")
    print("- 상위 50개 리스트 전송")
    print("- 상위 10개 상세 해석 전송")


if __name__ == "__main__":
    main()
