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
    """Compute 20-day annualized vol + gap_down_20d_min for each ticker."""
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
        recent = daily_rets[-20:] if len(daily_rets) >= 20 else daily_rets
        vol = round(statistics.stdev(recent) * math.sqrt(252), 4) if len(recent) >= 5 else 0.0
        gap_min = round(min(recent), 4) if recent else 0.0
        result[tk] = {"vol": vol, "gap_min": gap_min}
    return result


def build_market_returns(etfs: list) -> dict:
    """E1-I1: Extract SPY/SHV + regime observation axes."""
    result = {}
    regime_tickers = ["SPY", "SHV", "GLD", "LQD", "HYG", "TLT", "XLE", "QQQ"]
    for tk in regime_tickers:
        etf = next((e for e in etfs if e["name"] == tk), None)
        if etf:
            result[tk] = {PK[jp]: round(etf.get(jp, 0) or 0, 6) for jp in PK}
    return result


def build_sectors(etfs: list, stocks: list, meta: dict) -> list:
    """E1-I2: Sector ETF returns + breadth metrics."""
    # Map sector → stock performance
    sector_stocks = {}  # sector_en → list of {ret_1m, ret_3m}
    for s in stocks:
        m = meta.get(s["name"], {})
        sec = m.get("sector", "")
        if not sec:
            continue
        perf = {
            "ret_1m": s.get("1ヶ月", 0) or 0,
            "ret_3m": s.get("3ヶ月", 0) or 0,
        }
        sector_stocks.setdefault(sec, []).append(perf)

    rows = []
    for tk, sector in SECTOR_MAP.items():
        etf = next((e for e in etfs if e["name"] == tk), None)
        if not etf:
            continue
        row = {"sector": sector, "ticker": tk}
        for jp, en in PK.items():
            row[en] = round(etf.get(jp, 0) or 0, 6)
        # Breadth
        members = sector_stocks.get(sector, [])
        row["member_count"] = len(members)
        if members:
            row["breadth_1m"] = round(sum(1 for m in members if m["ret_1m"] > 0) / len(members), 3)
            row["breadth_3m"] = round(sum(1 for m in members if m["ret_3m"] > 0) / len(members), 3)
            rets_1m = [m["ret_1m"] for m in members]
            rets_3m = [m["ret_3m"] for m in members]
            row["median_ret_1m"] = round(statistics.median(rets_1m), 6)
            row["median_ret_3m"] = round(statistics.median(rets_3m), 6)
        else:
            row["breadth_1m"] = None
            row["breadth_3m"] = None
            row["median_ret_1m"] = None
            row["median_ret_3m"] = None
        rows.append(row)
    return rows


def build_themes(themes: list) -> list:
    """E1-I3 + E1-I5: Theme returns + breadth/concentration/membership hash."""
    import hashlib
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

        # Constituent tickers
        tickers = sorted([tk.strip() for tk in t.get("related", "").split(",") if tk.strip()])
        tp = t.get("tickerPerformances", {})
        row["member_count"] = len(tickers)

        # E1-I5: Membership hash
        row["member_hash"] = hashlib.sha256(",".join(tickers).encode()).hexdigest()[:16]

        # E1-I3: Breadth
        rets_1m = [(tp.get(tk, {}).get("1ヶ月", 0) or 0) for tk in tickers]
        rets_3m = [(tp.get(tk, {}).get("3ヶ月", 0) or 0) for tk in tickers]
        if tickers:
            row["theme_breadth_1m"] = round(sum(1 for r in rets_1m if r > 0) / len(tickers), 3)
            row["theme_breadth_3m"] = round(sum(1 for r in rets_3m if r > 0) / len(tickers), 3)
        else:
            row["theme_breadth_1m"] = None
            row["theme_breadth_3m"] = None

        # E1-I3: Concentration (top1/top3 contribution proxy)
        theme_ret_3m = t.get("3ヶ月", 0) or 0
        sorted_3m = sorted(rets_3m, reverse=True)
        if len(sorted_3m) >= 1 and theme_ret_3m != 0:
            row["theme_top1_contrib_proxy"] = round(sorted_3m[0] / (theme_ret_3m * len(tickers)), 3) if theme_ret_3m else None
        else:
            row["theme_top1_contrib_proxy"] = None
        if len(sorted_3m) >= 3 and theme_ret_3m != 0:
            top3_avg = sum(sorted_3m[:3]) / 3
            row["theme_top3_contrib_proxy"] = round(top3_avg / (theme_ret_3m * len(tickers) / 3), 3) if theme_ret_3m else None
        else:
            row["theme_top3_contrib_proxy"] = None

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
                "vol_20d_annualized": vols.get(tk, {}).get("vol", 0.0),
                "gap_down_20d_min": vols.get(tk, {}).get("gap_min", 0.0),
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
    stocks = [t for t in all_themes_raw if t.get("isIndividualTicker")]

    print(f"Themes: {len(themes)}, ETFs: {len(etfs)}, Meta: {len(meta)}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Building market_returns...")
    mr = build_market_returns(etfs)
    with open(OUTPUT_DIR / "market_returns.json", "w", encoding="utf-8") as f:
        json.dump(mr, f, indent=2, ensure_ascii=False)

    print("Building sectors...")
    sec = build_sectors(etfs, stocks, meta)
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
