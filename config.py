from __future__ import annotations

import os
from typing import Dict, List

# =========================================================
# ENV
# =========================================================
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "").strip()

# =========================================================
# PATHS
# =========================================================
UNIVERSE_CSV_PATH = "data/universe.csv"
OUTPUT_CSV_PATH = "eps_radar_output.csv"

# =========================================================
# API
# =========================================================
TIINGO_BASE_URL = "https://api.tiingo.com"

TIINGO_ENDPOINTS = {
    "eod_price": "/tiingo/daily/{ticker}/prices",
    "fundamental_definitions": "/tiingo/fundamentals/definitions",
    "fundamental_daily": "/tiingo/fundamentals/{ticker}/daily",
    "fundamental_statements": "/tiingo/fundamentals/{ticker}/statements",
}

DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_LOOKBACK_DAYS = 1400

# =========================================================
# PRICE WINDOWS
# =========================================================
PRICE_WINDOWS = {
    "ret_3m": 63,
    "ret_6m": 126,
    "ma_50": 50,
    "ma_200": 200,
    "high_52w": 252,
}

# =========================================================
# TIINGO FIELD MAP
# ---------------------------------------------------------
# 원칙:
# 1) definitions endpoint 기준으로 확인된 dataCode만 우선 사용
# 2) daily 에서 직접 내려오는 overview/valuation 성격 필드
# 3) statements 에서 내려오는 incomeStatement / cashFlow / balanceSheet 필드
# 4) 없는 필드는 코드에서 fallback 계산
# =========================================================

# daily 쪽에서 기대하는 필드
TIINGO_DAILY_FIELDS: Dict[str, str] = {
    "market_cap": "marketCap",
    "enterprise_value": "enterpriseVal",
    "pe": "peRatio",
    "shares_outstanding": "sharesOutstanding",
    # 아래는 있으면 쓰고 없으면 None 처리
    "ev": "enterpriseVal",
}

# statements 쪽에서 기대하는 필드
TIINGO_STATEMENT_FIELDS: Dict[str, str] = {
    # overview
    "gross_margin": "grossMargin",
    "net_margin": "netMargin",
    "profit_margin": "profitMargin",

    # incomeStatement
    "revenue": "revenue",
    "gross_profit": "grossProfit",
    "ebit": "ebit",
    "net_income": "netInc",
    "operating_income": "opInc",

    # cashFlow
    "cfo": "ncfo",
    "capex": "capEx",
    "free_cash_flow": "freeCashFlow",

    # balanceSheet
    "debt": "debt",
    "cash": "cashAndCashEquivalents",
    "current_liabilities": "liabilitiesCurrent",
}

# =========================================================
# OUTPUT / SCORE
# =========================================================
MAX_OUTPUT_ROWS = 50

SCORE_BANDS: List[Dict[str, object]] = [
    {"min": 8.5, "label": "최우선 관찰 후보"},
    {"min": 7.0, "label": "선제 관찰 후보"},
    {"min": 5.5, "label": "관찰 유지"},
    {"min": 0.0, "label": "보류"},
]

HEALTH_FLAG_LABELS = {
    "normal": "정상",
    "watch": "주의",
    "warning": "경고",
}

OUTPUT_COLUMNS = [
    "ticker",
    "name",
    "sector",
    "industry",
    "market_cap",
    "enterprise_value",
    "last_close",
    "signal_tier",
    "total_score",
    "expectation_upgrade_score",
    "growth_acceleration_score",
    "profitability_improvement_score",
    "valuation_score",
    "price_position_score",
    "health_flag",
    "revenue",
    "revenue_growth",
    "revenue_acceleration",
    "gross_margin",
    "profit_margin",
    "net_margin",
    "ebit",
    "ebit_growth",
    "ebit_acceleration",
    "cfo",
    "cfo_growth",
    "cfo_acceleration",
    "capex",
    "capex_growth",
    "free_cash_flow",
    "debt",
    "cash",
    "pe",
    "ev_sales",
    "ev_ebit",
    "ret_3m",
    "ret_6m",
    "ma_50",
    "ma_200",
    "high_52w",
    "high_proximity",
    "shares_outstanding",
    "summary_comment",
]

# =========================================================
# COMMENT LIBRARY
# =========================================================
COMMENT_LIBRARY: Dict[str, List[str]] = {
    "growth_accel_strong": [
        "성장 속도가 다시 붙고 있다.",
    ],
    "growth_accel_flat": [
        "성장 흐름은 유지되지만 가속 신호는 강하지 않다.",
    ],
    "growth_accel_weak": [
        "성장 가속 신호가 약하다.",
    ],
    "profitability_strong": [
        "수익성 구조는 강한 편이다.",
    ],
    "profitability_flat": [
        "수익성은 무난하지만 큰 개선 신호는 아직 약하다.",
    ],
    "profitability_weak": [
        "수익성 개선 신호가 약하다.",
    ],
    "valuation_ok": [
        "밸류 부담은 통제 가능한 수준이다.",
    ],
    "valuation_rich": [
        "밸류 부담은 다소 높다.",
    ],
    "health_normal": [
        "재무 구조는 대체로 안정적이다.",
    ],
    "health_watch": [
        "재무 구조는 약간의 점검이 필요하다.",
    ],
    "health_warning": [
        "재무 구조 경고 신호가 있다.",
    ],
}

# =========================================================
# VALIDATION
# =========================================================
def validate_config() -> None:
    if not TIINGO_API_KEY:
        raise RuntimeError("TIINGO_API_KEY 환경변수가 비어 있다.")
