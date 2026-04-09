#!/usr/bin/env python3
"""
Adapter: stock-theme existing data → generate_snapshot.py expected input format.

Reads:
  - public/api/theme_ranking.json
  - public/api/stock_meta.json
  - public/api/theme-details/*.json (for vol_20d computation)

Writes:
  - data/input/market_returns.json
  - data/input/sectors.json
  - data/input/themes.json
  - data/input/constituents.json
"""
import json, math, os, statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RANKING = ROOT / "public" / "api" / "theme_ranking.json"
META = ROOT / "public" / "api" / "stock_meta.json"
DETAILS_DIR = ROOT / "public" / "api" / "theme-details"
OUTPUT_DIR = ROOT / "data" / "input"

# Period key mapping: JP → scaffold
PK = {"1ヶ月": "ret_1m", "3ヶ月": "ret_3m", "半年": "ret_6m"}

# Sector ETF → sector name mapping
SECTOR_MAP = {
    "XLB": "Basic Materials", "XLC": "Communication Services",
    "XLE": "Energy", "XLF": "Financial Services",
    "XLI": "Industrials", "XLK": "Technology",
    "XLP": "Consumer Defensive", "XLRE": "Real Estate",
    "XLU": "Utilities", "XLV": "Healthcare",
    "XLY": "Consumer Cyclical",
}

# MC label mapping: JP → scaffold
MC_MAP = {"超大型": "mega", "大型": "large", "中型": "mid", "小型": "small"}

# Industry JP → EN (for sector_pass matching)
IND_EN = {
    "テクノロジー": "Technology", "ヘルスケア": "Healthcare", "金融": "Financial Services",
    "エネルギー": "Energy", "消費者一般": "Consumer Cyclical", "消費者必需品": "Consumer Defensive",
    "資本財": "Industrials", "素材": "Basic Materials", "不動産": "Real Estate",
    "公益": "Utilities", "通信": "Communication Services", "その他": "Other",
}


def load_data():
    with open(RANKING, encoding="utf-8") as f:
        rk = json.load(f)
    with open(META, encoding="utf-8") as f:
        meta = json.load(f)
    return rk, meta


def compute_vol_20d(slug: str) -> dict:
    """Compute 20-day annualized vol for each ticker from theme-details daily prices."""
    fpath = DETAILS_DIR / f"{slug}.json"
    if not fpath.exists():
        return {}
    with open(fpath, encoding="utf-8") as f:
        detail = json.load(f)
    prices = detail.get("prices", [])
    tickers = detail.get("tickers", [])
    if len(prices) < 22 or not tickers:
        return {}

    result = {}
    for tk in tickers:
        daily_rets = []
        for i in range(1, len(prices)):
            p0 = prices[i - 1].get(tk)
            p1 = prices[i].get(tk)
            if p0 and p1 and p0 > 0:
                daily_rets.append((p1 / p0) - 1.0)
        # Use last 20 trading days
        recent = daily_rets[-20:] if len(daily_rets) >= 20 else daily_rets
        if len(recent) >= 5:
            std = statistics.stdev(recent)
            result[tk] = round(std * math.sqrt(252), 4)
        else:
            result[tk] = 0.0
    return result


def build_market_returns(etfs: list) -> dict:
    """Extract SPY/SHV returns into scaffold format."""
    result = {}
    for tk in ["SPY", "SHV"]:
        etf = next((e for e in etfs if e["name"] == tk), None)
        if etf:
            result[tk] = {
                PK[jp]: round(etf.get(jp, 0) or 0, 6)
                for jp in PK
            }
    return result


def build_sectors(etfs: list) -> list:
    """Extract sector ETF returns into scaffold format."""
    rows = []
    for tk, sector in SECTOR_MAP.items():
        etf = next((e for e in etfs if e["name"] == tk), None)
        if etf:
            row = {"sector": sector, "ticker": tk}
            for jp, en in PK.items():
                row[en] = round(etf.get(jp, 0) or 0, 6)
            rows.append(row)
    return rows


def build_themes(themes: list) -> list:
    """Transform themes into scaffold format."""
    rows = []
    for t in themes:
        row = {
            "theme": t["name"],
            "slug": t.get("slug", ""),
            "sector": IND_EN.get(t.get("industry", ""), t.get("industry", "")),
            "theme1": t.get("theme1", ""),
        }
        for jp, en in PK.items():
            row[en] = round(t.get(jp, 0) or 0, 6)
        rows.append(row)
    return rows


def build_constituents(themes: list, meta: dict) -> list:
    """Build constituent-level data with vol_20d from daily prices."""
    # Pre-compute vol for all themes
    vol_cache = {}
    total = len(themes)
    for i, t in enumerate(themes):
        slug = t.get("slug", "")
        if i % 30 == 0:
            print(f"  Computing vol [{i}/{total}]...")
        if slug:
            vol_cache[slug] = compute_vol_20d(slug)

    rows = []
    for t in themes:
        slug = t.get("slug", "")
        tp = t.get("tickerPerformances", {})
        tickers = [tk.strip() for tk in t.get("related", "").split(",") if tk.strip()]
        vols = vol_cache.get(slug, {})

        for tk in tickers:
            perf = tp.get(tk, {})
            m = meta.get(tk, {})
            mc_ja = m.get("mc", "")
            row = {
                "theme": t["name"],
                "slug": slug,
                "ticker": tk,
                "market_cap_bucket": MC_MAP.get(mc_ja, "unknown"),
                "ret_1m": round(perf.get("1ヶ月", 0) or 0, 6),
                "ret_3m": round(perf.get("3ヶ月", 0) or 0, 6),
                "ret_6m": round(perf.get("半年", 0) or 0, 6),
                "ret_1y": round(perf.get("1年", 0) or 0, 6),
                "vol_20d_annualized": vols.get(tk, 0.0),
                "price": m.get("price", 0),
                "exchange": m.get("exchange", ""),
                "sector": m.get("sector", ""),
                "industry": m.get("industry", ""),
                "indices": m.get("indices", []),
                "name": m.get("name", tk),
                "name_ja": m.get("name_ja", ""),
            }
            rows.append(row)
    return rows


def main():
    print("Loading data...")
    rk, meta = load_data()
    all_themes_raw = rk.get("all_themes", [])
    themes = [t for t in all_themes_raw if t.get("related")]
    etfs = [t for t in all_themes_raw if t.get("isETF")]

    print(f"Themes: {len(themes)}, ETFs: {len(etfs)}, Meta: {len(meta)}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Building market_returns...")
    mr = build_market_returns(etfs)
    with open(OUTPUT_DIR / "market_returns.json", "w", encoding="utf-8") as f:
        json.dump(mr, f, indent=2, ensure_ascii=False)

    print("Building sectors...")
    sec = build_sectors(etfs)
    with open(OUTPUT_DIR / "sectors.json", "w", encoding="utf-8") as f:
        json.dump(sec, f, indent=2, ensure_ascii=False)

    print("Building themes...")
    th = build_themes(themes)
    with open(OUTPUT_DIR / "themes.json", "w", encoding="utf-8") as f:
        json.dump(th, f, indent=2, ensure_ascii=False)

    print("Building constituents (with vol computation)...")
    con = build_constituents(themes, meta)
    with open(OUTPUT_DIR / "constituents.json", "w", encoding="utf-8") as f:
        json.dump(con, f, indent=2, ensure_ascii=False)

    print(f"✓ All inputs written to {OUTPUT_DIR}")
    print(f"  market_returns: {len(mr)} entries")
    print(f"  sectors: {len(sec)} entries")
    print(f"  themes: {len(th)} entries")
    print(f"  constituents: {len(con)} entries")


if __name__ == "__main__":
    main()
