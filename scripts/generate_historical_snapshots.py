#!/usr/bin/env python3
"""
H2: Generate reconstructed historical snapshots from existing daily price data.

CRITICAL RULES (from PRISM_CRITERIA.md):
- NEVER use theme_ranking.json fixed period returns
- Compute rolling ret_1m/ret_3m/ret_6m from daily prices at each date
- Flag all outputs as reconstructed_historical
- Theme membership = current definition fixed (bias acknowledged)
"""
import json, math, os, hashlib, statistics
from pathlib import Path
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DETAILS_DIR = ROOT / "public" / "api" / "theme-details"
ETF_DAILY = ROOT / "data" / "historical" / "etf_daily.json"
RANKING = ROOT / "public" / "api" / "theme_ranking.json"
META = ROOT / "public" / "api" / "stock_meta.json"
OUTPUT_ROOT = ROOT / "data" / "historical" / "snapshots"

SECTOR_MAP = {
    "XLB":"Basic Materials","XLC":"Communication Services","XLE":"Energy",
    "XLF":"Financial Services","XLI":"Industrials","XLK":"Technology",
    "XLP":"Consumer Defensive","XLRE":"Real Estate","XLU":"Utilities",
    "XLV":"Healthcare","XLY":"Consumer Cyclical",
}
IND_EN = {
    "テクノロジー":"Technology","ヘルスケア":"Healthcare","金融":"Financial Services",
    "エネルギー":"Energy","消費者一般":"Consumer Cyclical","消費者必需品":"Consumer Defensive",
    "資本財":"Industrials","素材":"Basic Materials","不動産":"Real Estate",
    "公益":"Utilities","通信":"Communication Services","その他":"Other",
}
MC_MAP = {"超大型":"mega","大型":"large","中型":"mid","小型":"small"}

LOOKBACKS = {"ret_1m": 21, "ret_3m": 63, "ret_6m": 126}

def load_all_data():
    """Load all source data."""
    with open(RANKING, encoding="utf-8") as f:
        rk = json.load(f)
    with open(META, encoding="utf-8") as f:
        meta = json.load(f)
    with open(ETF_DAILY, encoding="utf-8") as f:
        etf_daily = json.load(f)

    themes_raw = [t for t in rk["all_themes"] if t.get("related")]
    stocks_raw = [t for t in rk["all_themes"] if t.get("isIndividualTicker")]

    # Load all theme-details daily prices
    theme_prices = {}
    for t in themes_raw:
        slug = t.get("slug", "")
        fpath = DETAILS_DIR / f"{slug}.json"
        if fpath.exists():
            with open(fpath, encoding="utf-8") as f:
                d = json.load(f)
            theme_prices[slug] = d
    return themes_raw, stocks_raw, meta, etf_daily, theme_prices

def rolling_return(prices_list, idx, lookback):
    """Compute return from prices_list[idx-lookback] to prices_list[idx]."""
    if idx < lookback or idx >= len(prices_list):
        return None
    p_now = prices_list[idx]
    p_prev = prices_list[idx - lookback]
    if p_prev is None or p_now is None or p_prev == 0:
        return None
    return (p_now / p_prev) - 1.0

def classify_gate(excess):
    if excess > 0: return "OPEN", 0.80
    if excess > -0.02: return "MID", 0.50
    return "CLOSED", 0.30

def tf_state(r1, r3, r6):
    if r1 and r3 and r6 and r1>0 and r3>0 and r6>0: return "STRONG"
    if r1 and r3 and r1>0 and r3>0: return "MIXED_UP"
    if r3 and r6 and r3>0 and r6>0: return "LATE"
    return "WEAK"

def safe_z(series):
    std = series.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - series.mean()) / std

def build_snapshot_for_date(date_str, date_idx, trading_dates,
                           themes_raw, meta, etf_daily, theme_prices):
    """Build a complete PRISM snapshot for a single historical date."""

    # 1. K-gate + regime axes
    def etf_ret(tk, lb):
        prices = etf_daily.get(tk, {})
        dates_avail = sorted(prices.keys())
        # Find closest date index
        if date_str not in prices: return None
        di = dates_avail.index(date_str)
        if di < lb: return None
        p_now = prices[dates_avail[di]]
        p_prev = prices[dates_avail[di - lb]]
        if p_prev == 0: return None
        return round((p_now / p_prev) - 1.0, 6)

    spy_3m = etf_ret("SPY", 63) or 0
    shv_3m = etf_ret("SHV", 63) or 0
    excess = round(spy_3m - shv_3m, 6)
    gate_state, atk_cap = classify_gate(excess)

    gate = {
        "benchmark": "SPY", "cash_proxy": "SHV", "lookback_days": 63,
        "benchmark_ret_3m": spy_3m, "cash_ret_3m": shv_3m,
        "excess_3m": excess, "gate_state": gate_state, "atk_cap": atk_cap,
        "equity_axis": {"spy_ret_3m": spy_3m, "qqq_ret_3m": etf_ret("QQQ",63) or 0,
                        "shv_ret_3m": shv_3m, "equity_excess_3m": excess},
        "credit_axis": {"lqd_excess_3m": round((etf_ret("LQD",63) or 0)-shv_3m,6),
                        "hyg_excess_3m": round((etf_ret("HYG",63) or 0)-shv_3m,6),
                        "hyg_minus_lqd_3m": round((etf_ret("HYG",63) or 0)-(etf_ret("LQD",63) or 0),6)},
        "hard_asset_axis": {"gld_excess_3m": round((etf_ret("GLD",63) or 0)-shv_3m,6),
                           "xle_minus_spy_3m": round((etf_ret("XLE",63) or 0)-spy_3m,6)},
        "duration_axis": {"tlt_ret_3m": etf_ret("TLT",63) or 0},
    }

    # 2. Sector layer
    sectors = []
    for tk, sec_name in SECTOR_MAP.items():
        r1m = etf_ret(tk, 21) or 0
        r3m = etf_ret(tk, 63) or 0
        r6m = etf_ret(tk, 126) or 0
        sectors.append({"sector": sec_name, "ticker": tk,
                        "ret_1m": r1m, "ret_3m": r3m, "ret_6m": r6m})
    sec_df = pd.DataFrame(sectors)
    sec_df["rank_3m"] = sec_df["ret_3m"].rank(ascending=False, method="first").astype(int)
    sec_df["pass_abs"] = sec_df["ret_3m"] > 0
    sec_df["pass_rel"] = sec_df["rank_3m"] <= 5
    sec_df["pass_layer1"] = sec_df["pass_abs"] & sec_df["pass_rel"]
    sec_df["tf_state"] = [tf_state(a,b,c) for a,b,c in zip(sec_df["ret_1m"],sec_df["ret_3m"],sec_df["ret_6m"])]
    sector_pass_map = sec_df.set_index("sector")["pass_layer1"].to_dict()

    # 3. Theme layer - compute rolling returns from daily prices
    theme_rows = []
    constituent_rows = []
    for t in themes_raw:
        slug = t.get("slug", "")
        tp_data = theme_prices.get(slug, {})
        prices = tp_data.get("prices", [])
        tickers = tp_data.get("tickers", [])
        if not prices or date_idx >= len(prices):
            continue

        # Per-ticker rolling returns at this date
        ticker_rets = {}
        for tk in tickers:
            tk_prices = [p.get(tk) for p in prices]
            r1m = rolling_return(tk_prices, date_idx, 21)
            r3m = rolling_return(tk_prices, date_idx, 63)
            r6m = rolling_return(tk_prices, date_idx, 126)
            # vol_20d
            daily_r = []
            for i in range(max(1, date_idx-19), date_idx+1):
                p0 = tk_prices[i-1]; p1 = tk_prices[i]
                if p0 and p1 and p0 > 0: daily_r.append(p1/p0 - 1)
            vol = round(statistics.stdev(daily_r)*math.sqrt(252),4) if len(daily_r)>=5 else 0
            gap_min = round(min(daily_r),4) if daily_r else 0
            ticker_rets[tk] = {"r1m": r1m, "r3m": r3m, "r6m": r6m, "vol": vol, "gap": gap_min}

        # Theme-level returns (equal-weight average)
        valid_1m = [v["r1m"] for v in ticker_rets.values() if v["r1m"] is not None]
        valid_3m = [v["r3m"] for v in ticker_rets.values() if v["r3m"] is not None]
        valid_6m = [v["r6m"] for v in ticker_rets.values() if v["r6m"] is not None]
        theme_r1m = statistics.mean(valid_1m) if valid_1m else 0
        theme_r3m = statistics.mean(valid_3m) if valid_3m else 0
        theme_r6m = statistics.mean(valid_6m) if valid_6m else 0
        sector_en = IND_EN.get(t.get("industry",""), t.get("industry",""))

        # Breadth / concentration
        breadth_1m = sum(1 for v in ticker_rets.values() if v["r1m"] and v["r1m"]>0)/max(len(tickers),1)
        breadth_3m = sum(1 for v in ticker_rets.values() if v["r3m"] and v["r3m"]>0)/max(len(tickers),1)
        sorted_3m = sorted([v["r3m"] or 0 for v in ticker_rets.values()], reverse=True)
        top1 = sorted_3m[0]/(theme_r3m*len(tickers)) if theme_r3m and len(sorted_3m)>=1 else None
        theme_vol = statistics.mean([v["vol"] for v in ticker_rets.values()]) if ticker_rets else 0

        theme_rows.append({
            "theme": t["name"], "slug": slug, "sector": sector_en,
            "ret_1m": round(theme_r1m,6), "ret_3m": round(theme_r3m,6), "ret_6m": round(theme_r6m,6),
            "theme_breadth_1m": round(breadth_1m,3), "theme_breadth_3m": round(breadth_3m,3),
            "theme_top1_contrib_proxy": round(top1,3) if top1 else None,
            "theme_vol_proxy": round(theme_vol,4), "member_count": len(tickers),
            "member_hash": hashlib.sha256(",".join(sorted(tickers)).encode()).hexdigest()[:16],
        })

        # Constituents
        for tk in tickers:
            tr = ticker_rets.get(tk, {})
            m = meta.get(tk, {})
            constituent_rows.append({
                "theme": t["name"], "slug": slug, "ticker": tk,
                "market_cap_bucket": MC_MAP.get(m.get("mc",""),"unknown"),
                "ret_1m": round(tr.get("r1m") or 0, 6),
                "ret_3m": round(tr.get("r3m") or 0, 6),
                "vol_20d_annualized": tr.get("vol", 0),
                "gap_down_20d_min": tr.get("gap", 0),
                "exchange": m.get("exchange",""), "sector": m.get("sector",""),
                "indices": m.get("indices",[]),
            })

    # 4. Theme scoring (same as generate_snapshot.py)
    th_df = pd.DataFrame(theme_rows)
    if th_df.empty:
        return None
    th_df["sector_pass"] = th_df["sector"].map(sector_pass_map).fillna(False)
    th_df["consensus_flag"] = (th_df["ret_1m"]>0) & (th_df["ret_3m"]>0) & (th_df["ret_6m"]>0)
    th_df["acceleration_flag"] = (th_df["ret_1m"]*12) > (th_df["ret_3m"]*4)
    th_df["tf_state"] = [tf_state(a,b,c) for a,b,c in zip(th_df["ret_1m"],th_df["ret_3m"],th_df["ret_6m"])]
    th_df["high_vol_excluded"] = th_df["theme_vol_proxy"] > 0.60
    # Score
    th_df["theme_score"] = (
        1.0*safe_z(th_df["ret_3m"]) + 0.6*safe_z(th_df["ret_1m"]*12 - th_df["ret_3m"]*4)
        + 0.75*th_df["consensus_flag"].astype(int) + 0.5*th_df["sector_pass"].astype(int)
        - 0.6*safe_z(th_df["theme_vol_proxy"])
    )
    th_df = th_df.sort_values("theme_score",ascending=False).reset_index(drop=True)
    th_df["score_rank"] = np.arange(1, len(th_df)+1)
    th_df["zone"] = th_df["score_rank"].map(lambda r: "ENTRY_ZONE" if r<=20 else "HOLD_ZONE" if r<=30 else "OUT_ZONE")
    th_df["entry_candidate"] = (
        (th_df["zone"]=="ENTRY_ZONE") & th_df["consensus_flag"] & th_df["acceleration_flag"]
        & th_df["sector_pass"] & ~th_df["high_vol_excluded"]
    )
    selected_themes = th_df.loc[th_df["entry_candidate"],"theme"].tolist()

    # 5. Stock selection
    con_df = pd.DataFrame(constituent_rows)
    con_df["theme_selected"] = con_df["theme"].isin(selected_themes)
    con_df["large_or_better"] = con_df["market_cap_bucket"].isin({"mega","large"})
    con_df["rank_within_theme"] = (
        con_df.sort_values(["theme","large_or_better","ret_1m"],ascending=[True,False,False])
        .groupby("theme").cumcount()+1
    )
    con_df["stock_selected"] = con_df["theme_selected"] & (con_df["rank_within_theme"]<=2)
    selected_stocks = con_df.loc[con_df["stock_selected"],"ticker"].drop_duplicates().tolist()

    # 6. Signals
    signals = {
        "gate_state": gate_state, "atk_cap": atk_cap,
        "selected_sectors": sec_df.loc[sec_df["pass_layer1"],"ticker"].tolist(),
        "selected_themes": selected_themes,
        "selected_stocks": selected_stocks,
        "selected_theme_count": len(selected_themes),
        "selected_stock_count": len(selected_stocks),
    }
    meta_out = {
        "snapshot_date": date_str,
        "snapshot_mode": "reconstructed_historical",
        "theme_membership_mode": "current_definition_fixed",
        "schema_version": "v1",
        "bias_notes": [
            "Theme membership may differ from actual historical composition",
            "Suitable for relative benchmarking, not absolute performance claims"
        ],
    }
    return {"meta": meta_out, "gate": gate, "sectors": sec_df.to_dict("records"),
            "themes": th_df.to_dict("records"), "constituents": con_df.to_dict("records"),
            "signals": signals}

def main():
    print("Loading all data...")
    themes_raw, stocks_raw, meta, etf_daily, theme_prices = load_all_data()
    print(f"  Themes: {len(themes_raw)}, ETFs: {len(etf_daily)}, Theme price files: {len(theme_prices)}")

    # Get trading dates from first theme-detail file
    sample = next(iter(theme_prices.values()))
    all_dates = [p["date"] for p in sample["prices"]]
    total_days = len(all_dates)
    start_idx = 126  # 6M lookback
    print(f"  Trading days: {total_days}, Start index: {start_idx} ({all_dates[start_idx]})")
    print(f"  Will generate {total_days - start_idx} snapshots")

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    generated = 0
    for date_idx in range(start_idx, total_days):
        date_str = all_dates[date_idx]
        if date_idx % 20 == 0:
            print(f"  [{date_idx-start_idx}/{total_days-start_idx}] {date_str}...")
        try:
            snapshot = build_snapshot_for_date(
                date_str, date_idx, all_dates, themes_raw, meta, etf_daily, theme_prices
            )
            if snapshot is None:
                continue
            snap_dir = OUTPUT_ROOT / date_str
            snap_dir.mkdir(parents=True, exist_ok=True)
            for key in ["meta","gate","sectors","themes","constituents","signals"]:
                data = snapshot[key]
                with open(snap_dir / f"{key}.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, default=str)
            generated += 1
        except Exception as e:
            print(f"  ERROR at {date_str}: {e}")

    print(f"\n✓ Generated {generated} historical snapshots in {OUTPUT_ROOT}")
    print(f"  Range: {all_dates[start_idx]} ~ {all_dates[-1]}")

if __name__ == "__main__":
    main()
