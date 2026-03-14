from __future__ import annotations

import os
from typing import Dict, List

# =========================================================
# ENV
# =========================================================
TIINGO_API_KEY: str = (
    os.getenv("TIINGO_API_KEY", "").strip()
    or os.getenv("TIINGO_TOKEN", "").strip()
    or os.getenv("TIINGO_KEY", "").strip()
)

# =========================================================
# BASIC
# =========================================================
DEFAULT_LOOKBACK_DAYS: int = 1400
DEFAULT_TIMEOUT_SECONDS: int = 30

# 유니버스 파일 경로
UNIVERSE_CSV_PATH: str = "data/universe.csv"

# 결과 파일
OUTPUT_CSV_PATH: str = "eps_radar_output.csv"

# 출력 개수
MAX_OUTPUT_ROWS: int = 50

# =========================================================
# Tiingo API
# =========================================================
TIINGO_BASE_URL: str = "https://api.tiingo.com"

TIINGO_ENDPOINTS: Dict[str, str] = {
    "eod_price": "/tiingo/daily/{ticker}/prices",
    "fundamental_daily": "/tiingo/fundamentals/{ticker}/daily",
    "fundamental_statements": "/tiingo/fundamentals/{ticker}/statements",
    "fundamental_definitions": "/tiingo/fundamentals/definitions",
}

# =========================================================
# CONFIRMED TINGO FIELDS
# 사람이 이해하는 이름 -> 실제 Tiingo dataCode
# =========================================================
TIINGO_DAILY_FIELDS: Dict[str, str] = {
    "pe": "peRatio",
    "enterprise_value": "enterpriseVal",
    "market_cap": "marketCap",
    "shares_outstanding": "sharesOutstanding",
}

TIINGO_STATEMENT_FIELDS: Dict[str, str] = {
    "gross_margin": "grossMargin",
    "profit_margin": "profitMargin",
    "net_margin": "netMargin",
    "revenue": "revenue",
    "gross_profit": "grossProfit",
    "ebit": "ebit",
    "cfo": "ncfo",
    "capex": "capEx",
    "debt": "debt",
    "cash": "cashAndCashEquivalents",
    "free_cash_flow": "freeCashFlow",
}

# 아직 definitions 수준에선 보였지만 직접 활용은 보류
TIINGO_OPTIONAL_FIELDS: Dict[str, str] = {
    "operating_margin": "opMargin",
}

# =========================================================
# HUMAN LABELS
# =========================================================
KOREAN_LABELS: Dict[str, str] = {
    "ticker": "티커",
    "name": "회사명",
    "sector": "섹터",
    "industry": "산업",
    "last_close": "종가",
    "high_52w": "52주 최고가",
    "high_proximity": "52주 고점 근접도",
    "ret_3m": "3개월 수익률",
    "ret_6m": "6개월 수익률",
    "pe": "PER",
    "enterprise_value": "기업가치",
    "market_cap": "시가총액",
    "shares_outstanding": "주식수",
    "gross_margin": "총이익률",
    "profit_margin": "순이익률",
    "net_margin": "순이익률",
    "revenue": "매출",
    "revenue_growth": "매출 성장률",
    "gross_profit": "총이익",
    "ebit": "EBIT",
    "ebit_growth": "EBIT 성장률",
    "cfo": "영업현금흐름",
    "cfo_growth": "영업현금흐름 성장률",
    "capex": "설비투자",
    "capex_growth": "설비투자 성장률",
    "debt": "부채",
    "cash": "현금 및 현금성자산",
    "free_cash_flow": "자유현금흐름",
    "ev_sales": "EV/매출",
    "ev_ebit": "EV/EBIT",
    "growth_acceleration_score": "성장 가속 점수",
    "profitability_improvement_score": "수익성 개선 점수",
    "expectation_upgrade_score": "기대 상향 점수",
    "valuation_score": "밸류 허용 점수",
    "total_score": "종합 점수",
    "health_flag": "건전성 플래그",
    "signal_tier": "판정",
    "summary_comment": "핵심 해석",
}

# =========================================================
# OUTPUT COLUMNS
# =========================================================
OUTPUT_COLUMNS: List[str] = [
    "ticker",
    "name",
    "sector",
    "industry",
    "signal_tier",
    "total_score",
    "expectation_upgrade_score",
    "growth_acceleration_score",
    "profitability_improvement_score",
    "valuation_score",
    "health_flag",
    "last_close",
    "ret_3m",
    "ret_6m",
    "high_proximity",
    "market_cap",
    "enterprise_value",
    "pe",
    "revenue",
    "revenue_growth",
    "gross_margin",
    "profit_margin",
    "gross_profit",
    "ebit",
    "ebit_growth",
    "cfo",
    "cfo_growth",
    "capex",
    "capex_growth",
    "debt",
    "cash",
    "free_cash_flow",
    "ev_sales",
    "ev_ebit",
    "summary_comment",
]

# =========================================================
# PRICE METRIC SETTINGS
# =========================================================
PRICE_WINDOWS: Dict[str, int] = {
    "ret_3m": 63,
    "ret_6m": 126,
    "ma_50": 50,
    "ma_200": 200,
    "high_52w": 252,
}

# =========================================================
# SCORE BAND LABELS
# =========================================================
SCORE_BANDS: List[Dict[str, object]] = [
    {"min": 8.5, "label": "최우선 관찰 후보"},
    {"min": 7.0, "label": "선제 관찰 후보"},
    {"min": 5.5, "label": "관찰 유지"},
    {"min": 0.0, "label": "보류"},
]

# =========================================================
# HEALTH FLAG RULE HINTS
# 실제 계산은 eps_radar.py 또는 data_client.py에서 수행
# =========================================================
HEALTH_FLAG_LABELS: Dict[str, str] = {
    "normal": "정상",
    "watch": "주의",
    "warning": "경고",
}

# =========================================================
# COMMENT TEMPLATES
# 해석 문장용 기본 템플릿
# =========================================================
COMMENT_LIBRARY: Dict[str, List[str]] = {
    "growth_accel_strong": [
        "성장 속도가 직전 구간 대비 뚜렷하게 가속되고 있다.",
        "매출 또는 이익 증가 속도가 빨라지는 구간이다.",
    ],
    "growth_accel_flat": [
        "성장 속도는 유지 구간이다.",
        "성장 절대수준은 유지되지만 가속 신호는 제한적이다.",
    ],
    "growth_accel_weak": [
        "성장 속도가 둔화되고 있다.",
        "최근 숫자 흐름은 가속보다 둔화에 가깝다.",
    ],
    "profitability_strong": [
        "수익성 개선이 동반되고 있다.",
        "외형 성장뿐 아니라 이익의 질도 좋아지고 있다.",
    ],
    "profitability_flat": [
        "수익성은 높은 수준을 유지한다.",
        "수익성의 방향은 대체로 안정적이다.",
    ],
    "profitability_weak": [
        "수익성 개선이 동반되지 않는다.",
        "외형 대비 이익의 질은 약해지고 있다.",
    ],
    "valuation_ok": [
        "밸류 부담은 감내 가능한 구간이다.",
        "과열 부담은 있으나 극단적 수준은 아니다.",
    ],
    "valuation_rich": [
        "밸류 부담이 있다.",
        "숫자 개선이 이어지지 않으면 멀티플 부담이 커질 수 있다.",
    ],
    "health_normal": [
        "현금흐름과 재무구조는 대체로 안정적이다.",
    ],
    "health_watch": [
        "건전성은 추가 확인이 필요하다.",
    ],
    "health_warning": [
        "현금흐름 또는 재무구조 경고 신호가 있다.",
    ],
}

# =========================================================
# SAFE HELPERS
# =========================================================
def validate_config() -> None:
    if not TIINGO_API_KEY:
        raise RuntimeError(
            "Tiingo API 키가 비어 있다. "
            "환경변수 TIINGO_API_KEY 또는 TIINGO_TOKEN 또는 TIINGO_KEY를 설정해야 한다."
        )
