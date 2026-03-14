from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

# =========================================================
# ENV
# =========================================================
TIINGO_API_KEY: str = (
    os.getenv("TIINGO_API_KEY", "").strip()
    or os.getenv("TIINGO_TOKEN", "").strip()
    or os.getenv("TIINGO_KEY", "").strip()
)

TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# =========================================================
# PATHS
# =========================================================
DATA_DIR = Path("data")
UNIVERSE_CSV_PATH = str(DATA_DIR / "universe.csv")
OUTPUT_CSV_PATH = "eps_radar_output.csv"

TIINGO_DEFINITIONS_JSON_PATH = str(DATA_DIR / "tiingo_definitions.json")
TIINGO_DEFINITIONS_GROUPED_JSON_PATH = str(DATA_DIR / "tiingo_definitions_grouped.json")
TIINGO_CORE_FIELD_MAP_JSON_PATH = str(DATA_DIR / "tiingo_core_field_map.json")

# =========================================================
# API
# =========================================================
TIINGO_BASE_URL = "https://api.tiingo.com"

TIINGO_ENDPOINTS: Dict[str, str] = {
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
PRICE_WINDOWS: Dict[str, int] = {
    "ret_3m": 63,
    "ret_6m": 126,
    "ma_50": 50,
    "ma_200": 200,
    "high_52w": 252,
}

# =========================================================
# FALLBACK CORE MAP
# ---------------------------------------------------------
# build_tiingo_definitions.py를 아직 안 돌렸거나
# tiingo_core_field_map.json이 없을 때 사용하는 기본값
# =========================================================
FALLBACK_CORE_FIELD_MAP: Dict[str, Dict[str, Any]] = {
    "market_cap": {
        "matched": True,
        "dataCode": "marketCap",
        "statementType": "overview",
        "units": "$",
        "name": "Market Capitalization",
    },
    "enterprise_value": {
        "matched": True,
        "dataCode": "enterpriseVal",
        "statementType": "overview",
        "units": "$",
        "name": "Enterprise Value",
    },
    "pe": {
        "matched": True,
        "dataCode": "peRatio",
        "statementType": "overview",
        "units": None,
        "name": "Price to Earnings Ratio",
    },
    "shares_outstanding": {
        "matched": True,
        "dataCode": "sharesOutstanding",
        "statementType": "overview",
        "units": None,
        "name": "Shares Outstanding",
    },
    "gross_margin": {
        "matched": True,
        "dataCode": "grossMargin",
        "statementType": "overview",
        "units": "%",
        "name": "Gross Margin",
    },
    "net_margin": {
        "matched": True,
        "dataCode": "netMargin",
        "statementType": "overview",
        "units": "%",
        "name": "Net Margin",
    },
    "profit_margin": {
        "matched": True,
        "dataCode": "profitMargin",
        "statementType": "overview",
        "units": "%",
        "name": "Profit Margin",
    },
    "revenue": {
        "matched": True,
        "dataCode": "revenue",
        "statementType": "incomeStatement",
        "units": "$",
        "name": "Revenue",
    },
    "gross_profit": {
        "matched": True,
        "dataCode": "grossProfit",
        "statementType": "incomeStatement",
        "units": "$",
        "name": "Gross Profit",
    },
    "ebit": {
        "matched": True,
        "dataCode": "ebit",
        "statementType": "incomeStatement",
        "units": "$",
        "name": "EBIT",
    },
    "operating_income": {
        "matched": True,
        "dataCode": "opInc",
        "statementType": "incomeStatement",
        "units": "$",
        "name": "Operating Income",
    },
    "net_income": {
        "matched": True,
        "dataCode": "netInc",
        "statementType": "incomeStatement",
        "units": "$",
        "name": "Net Income",
    },
    "cfo": {
        "matched": True,
        "dataCode": "ncfo",
        "statementType": "cashFlow",
        "units": "$",
        "name": "Net Cash Flow from Operations",
    },
    "capex": {
        "matched": True,
        "dataCode": "capEx",
        "statementType": "cashFlow",
        "units": "$",
        "name": "Capital Expenditure",
    },
    "free_cash_flow": {
        "matched": True,
        "dataCode": "freeCashFlow",
        "statementType": "cashFlow",
        "units": "$",
        "name": "Free Cash Flow",
    },
    "debt": {
        "matched": True,
        "dataCode": "debt",
        "statementType": "balanceSheet",
        "units": "$",
        "name": "Debt",
    },
    "cash": {
        "matched": True,
        "dataCode": "cashAndCashEquivalents",
        "statementType": "balanceSheet",
        "units": "$",
        "name": "Cash And Cash Equivalents",
    },
    "current_liabilities": {
        "matched": True,
        "dataCode": "liabilitiesCurrent",
        "statementType": "balanceSheet",
        "units": "$",
        "name": "Current Liabilities",
    },
}

# =========================================================
# CORE MAP LOAD
# =========================================================
def _load_core_field_map() -> Dict[str, Dict[str, Any]]:
    path = Path(TIINGO_CORE_FIELD_MAP_JSON_PATH)
    if not path.exists():
        return FALLBACK_CORE_FIELD_MAP

    try:
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict):
            return FALLBACK_CORE_FIELD_MAP

        merged = dict(FALLBACK_CORE_FIELD_MAP)
        for k, v in loaded.items():
            if isinstance(v, dict):
                merged[k] = v
        return merged
    except Exception:
        return FALLBACK_CORE_FIELD_MAP


TIINGO_CORE_FIELD_MAP: Dict[str, Dict[str, Any]] = _load_core_field_map()

# =========================================================
# RESOLVED FIELD MAPS
# ---------------------------------------------------------
# data_client.py가 바로 사용할 수 있도록
# logical_name -> actual dataCode 형태로 변환
# statementType이 overview인 경우는 daily 우선
# 나머지는 statements
# =========================================================
TIINGO_DAILY_FIELDS: Dict[str, str] = {}
TIINGO_STATEMENT_FIELDS: Dict[str, str] = {}

for logical_name, info in TIINGO_CORE_FIELD_MAP.items():
    if not info.get("matched"):
        continue

    data_code = info.get("dataCode")
    statement_type = info.get("statementType")

    if not data_code:
        continue

    if statement_type == "overview":
        TIINGO_DAILY_FIELDS[logical_name] = data_code
    else:
        TIINGO_STATEMENT_FIELDS[logical_name] = data_code

# =========================================================
# SCORE / OUTPUT
# =========================================================
MAX_OUTPUT_ROWS = 50

SCORE_BANDS: List[Dict[str, object]] = [
    {"min": 8.5, "label": "최우선 관찰 후보"},
    {"min": 7.0, "label": "선제 관찰 후보"},
    {"min": 5.5, "label": "관찰 유지"},
    {"min": 0.0, "label": "보류"},
]

HEALTH_FLAG_LABELS: Dict[str, str] = {
    "normal": "정상",
    "watch": "주의",
    "warning": "경고",
}

OUTPUT_COLUMNS: List[str] = [
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
# HUMAN LABELS
# =========================================================
KOREAN_LABELS: Dict[str, str] = {
    "ticker": "티커",
    "name": "회사명",
    "sector": "섹터",
    "industry": "산업",
    "market_cap": "시가총액",
    "enterprise_value": "기업가치",
    "last_close": "종가",
    "signal_tier": "판정",
    "total_score": "종합 점수",
    "expectation_upgrade_score": "기대 상향 점수",
    "growth_acceleration_score": "성장 가속 점수",
    "profitability_improvement_score": "수익성 개선 점수",
    "valuation_score": "밸류 허용 점수",
    "price_position_score": "가격 위치 점수",
    "health_flag": "건전성 플래그",
    "revenue": "매출",
    "revenue_growth": "매출 성장률",
    "revenue_acceleration": "매출 가속도",
    "gross_margin": "총이익률",
    "profit_margin": "순이익률",
    "net_margin": "순이익률",
    "ebit": "EBIT",
    "ebit_growth": "EBIT 성장률",
    "ebit_acceleration": "EBIT 가속도",
    "cfo": "영업현금흐름",
    "cfo_growth": "영업현금흐름 성장률",
    "cfo_acceleration": "영업현금흐름 가속도",
    "capex": "설비투자",
    "capex_growth": "설비투자 성장률",
    "free_cash_flow": "자유현금흐름",
    "debt": "부채",
    "cash": "현금 및 현금성자산",
    "pe": "PER",
    "ev_sales": "EV/매출",
    "ev_ebit": "EV/EBIT",
    "ret_3m": "3개월 수익률",
    "ret_6m": "6개월 수익률",
    "ma_50": "50일선",
    "ma_200": "200일선",
    "high_52w": "52주 최고가",
    "high_proximity": "52주 고점 근접도",
    "shares_outstanding": "주식수",
    "summary_comment": "핵심 해석",
}

# =========================================================
# VALIDATION
# =========================================================
def validate_config() -> None:
    if not TIINGO_API_KEY:
        raise RuntimeError("TIINGO_API_KEY 환경변수가 비어 있다.")
