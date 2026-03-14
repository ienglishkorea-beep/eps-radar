from __future__ import annotations

import re
from typing import List

import pandas as pd

from config import UNIVERSE_CSV_PATH

# =========================================================
# SOURCE
# ---------------------------------------------------------
# S&P 1500 = S&P 500 + S&P 400 + S&P 600
# 1차 버전은 공개 constituent table을 합쳐서 유니버스를 생성한다.
# 가격/시총 필터는 여기서 하지 않고, 레이더 실행 단계에서 거는 방식이 더 빠르다.
# =========================================================

SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SP400_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
SP600_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"

# =========================================================
# USER RULES
# =========================================================
EXCLUDED_KEYWORDS_IN_NAME = [
    " ETF",
    " ETN",
    " Fund",
    " Trust",
    " Acquisition",
    " Acquisition Corp",
    " Holdings Corp.",
    " Depositary",
    " Depositary Shares",
    " Income Fund",
    " Royalty",
]

# 외국주식/해외등록/ADR류 제거용
EXCLUDED_KEYWORDS_IN_SECURITY = [
    " ADR",
    " ADS",
    " plc",
    " Limited",
    " Ltd.",
    " S.A.",
    " N.V.",
    " SE",
    " Holdings",
]

# 바이오/제약 제외
EXCLUDED_SECTOR_KEYWORDS = [
    "Health Care",
    "Healthcare",
]

EXCLUDED_INDUSTRY_KEYWORDS = [
    "Biotechnology",
    "Pharmaceuticals",
    "Life Sciences Tools & Services",
    "Health Care Equipment",
    "Health Care Supplies",
    "Health Care Technology",
    "Managed Health Care",
    "Health Care Providers",
    "Biotech",
    "Drug",
]

# 특수 티커 패턴 제거
BAD_TICKER_PATTERNS = [
    r"\.",
    r"/",
    r"\^",
    r"-",
]

# 사용자가 싫어하는 테마 제외
EXCLUDED_NAME_KEYWORDS_EXTRA = [
    "Quantum",
    "Space",
]

# =========================================================
# HELPERS
# =========================================================
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c: str(c).strip() for c in df.columns}
    df = df.rename(columns=cols).copy()

    # 가능한 컬럼명 통일
    mapping = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ["symbol", "ticker"]:
            mapping[c] = "ticker"
        elif lc in ["security", "company", "company name", "name"]:
            mapping[c] = "name"
        elif lc in ["gics sector", "sector"]:
            mapping[c] = "sector"
        elif lc in ["gics sub-industry", "sub-industry", "industry"]:
            mapping[c] = "industry"

    df = df.rename(columns=mapping)

    needed = ["ticker", "name", "sector", "industry"]
    for col in needed:
        if col not in df.columns:
            df[col] = None

    return df[needed].copy()


def _load_table(url: str) -> pd.DataFrame:
    tables = pd.read_html(url)
    if not tables:
        raise RuntimeError(f"표를 읽지 못했다: {url}")

    # constituent 표는 일반적으로 첫 번째 표
    df = tables[0].copy()
    df = _normalize_cols(df)

    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["name"] = df["name"].astype(str).str.strip()
    df["sector"] = df["sector"].astype(str).str.strip()
    df["industry"] = df["industry"].astype(str).str.strip()

    return df


def _contains_any(text: str, keywords: List[str]) -> bool:
    t = str(text or "").strip().lower()
    return any(k.lower() in t for k in keywords)


def _bad_ticker(ticker: str) -> bool:
    t = str(ticker or "").strip().upper()
    if not t:
        return True
    for pattern in BAD_TICKER_PATTERNS:
        if re.search(pattern, t):
            return True
    return False


def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    # 빈 티커 제거
    work = work[work["ticker"].notna()].copy()
    work = work[work["ticker"].astype(str).str.strip() != ""].copy()

    # 특수 티커 제거
    work = work[~work["ticker"].apply(_bad_ticker)].copy()

    # 이름 기반 제거
    work = work[
        ~work["name"].apply(
            lambda x: _contains_any(x, EXCLUDED_KEYWORDS_IN_NAME + EXCLUDED_NAME_KEYWORDS_EXTRA)
        )
    ].copy()

    # 외국주식/ADR류 제거
    work = work[
        ~work["name"].apply(lambda x: _contains_any(x, EXCLUDED_KEYWORDS_IN_SECURITY))
    ].copy()

    # 섹터 제거
    work = work[
        ~work["sector"].apply(lambda x: _contains_any(x, EXCLUDED_SECTOR_KEYWORDS))
    ].copy()

    # 산업 제거
    work = work[
        ~work["industry"].apply(lambda x: _contains_any(x, EXCLUDED_INDUSTRY_KEYWORDS))
    ].copy()

    # 중복 제거
    work = work.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

    return work


def build_universe() -> pd.DataFrame:
    sp500 = _load_table(SP500_URL)
    sp400 = _load_table(SP400_URL)
    sp600 = _load_table(SP600_URL)

    universe = pd.concat([sp500, sp400, sp600], ignore_index=True)
    universe = universe.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    universe = _apply_filters(universe)

    return universe


def main() -> None:
    universe = build_universe()
    universe.to_csv(UNIVERSE_CSV_PATH, index=False)

    print(f"유니버스 저장 완료: {UNIVERSE_CSV_PATH}")
    print(f"rows: {len(universe)}")
    print("")
    print(universe.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
