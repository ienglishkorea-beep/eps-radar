from __future__ import annotations

import os
from typing import List

import pandas as pd
import requests

from config import OUTPUT_CSV_PATH

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

MAX_ROWS_TO_SEND = 10
REQUEST_TIMEOUT = 30


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


def build_message(df: pd.DataFrame) -> str:
    if df.empty:
        return "Fundamental Direction Radar\n\n결과 없음"

    lines: List[str] = []
    lines.append("Fundamental Direction Radar")
    lines.append("")

    top = df.head(MAX_ROWS_TO_SEND).copy()

    for i, (_, row) in enumerate(top.iterrows(), start=1):
        ticker = row.get("ticker", "")
        name = row.get("name", "")
        signal_tier = row.get("signal_tier", "")
        total_score = _format_num(row.get("total_score"))
        expectation_score = _format_num(row.get("expectation_upgrade_score"))
        growth_score = _format_num(row.get("growth_acceleration_score"))
        profitability_score = _format_num(row.get("profitability_improvement_score"))
        valuation_score = _format_num(row.get("valuation_score"))
        health_flag = row.get("health_flag", "")

        revenue_growth = _format_pct(row.get("revenue_growth"))
        cfo_growth = _format_pct(row.get("cfo_growth"))
        gross_margin = _format_pct(row.get("gross_margin"))
        profit_margin = _format_pct(row.get("profit_margin") if pd.notna(row.get("profit_margin")) else row.get("net_margin"))
        pe = _format_num(row.get("pe"))
        ev_sales = _format_num(row.get("ev_sales"))
        ev_ebit = _format_num(row.get("ev_ebit"))
        market_cap = _format_large_money(row.get("market_cap"))
        summary_comment = str(row.get("summary_comment", "") or "").strip()

        lines.append(f"{i}) {ticker} | {name}")
        lines.append(f"판정: {signal_tier}")
        lines.append(f"종합 {total_score} | 기대상향 {expectation_score}")
        lines.append(f"성장가속 {growth_score} | 수익성개선 {profitability_score} | 밸류허용 {valuation_score}")
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

    # 텔레그램 메시지 길이 제한 대응
    chunks: List[str] = []
    current = ""

    for line in text.splitlines(True):
        if len(current) + len(line) > 3500:
            chunks.append(current)
            current = line
        else:
            current += line

    if current:
        chunks.append(current)

    for chunk in chunks:
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
    message = build_message(df)
    send_telegram_message(message)
    print("텔레그램 전송 완료")


if __name__ == "__main__":
    main()
