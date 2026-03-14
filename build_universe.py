from __future__ import annotations

import re
from io import StringIO
from typing import List

import pandas as pd
import requests

from config import UNIVERSE_CSV_PATH

# =========================================================
# SOURCE
# =========================================================
SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SP400_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
SP600_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"

REQUEST_TIMEOUT = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

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
    " Depositary",
    " Depositary Shares",
    " Income Fund",
    " Royalty",
]

EXCLUDED_KEYWORDS_IN_SECURITY = [
    " ADR",
    " ADS",
    " plc",
    " Limited",
    " Ltd.",
    " S.A.",
    " N.V.",
    " SE",
]

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

BAD_TICKER_PATTERNS = [
    r"\.",
    r"/",
    r"\^",
]

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


def _download_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def _load_table(url: str) -> pd.DataFrame:
    html = _download_html(url)
    tables = pd.read_html(StringIO(html))

    if not tables:
        raise RuntimeError(f"표를 읽지 못했다: {url}")

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

    work = work[work["ticker"].notna()].copy()
    work = work[work["ticker"].astype(str).str.strip() != ""].copy()
    work = work[~work["ticker"].apply(_bad_ticker)].copy()

    work = work[
        ~work["name"].apply(
            lambda x: _contains_any(x, EXCLUDED_KEYWORDS_IN_NAME + EXCLUDED_NAME_KEYWORDS_EXTRA)
        )
    ].copy()

    work = work[
        ~work["name"].apply(lambda x: _contains_any(x, EXCLUDED_KEYWORDS_IN_SECURITY))
    ].copy()

    work = work[
        ~work["sector"].apply(lambda x: _contains_any(x, EXCLUDED_SECTOR_KEYWORDS))
    ].copy()

    work = work[
        ~work["industry"].apply(lambda x: _contains_any(x, EXCLUDED_INDUSTRY_KEYWORDS))
    ].copy()

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
