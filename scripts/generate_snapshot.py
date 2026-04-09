#!/usr/bin/env python3
"""
PRISM daily snapshot generator for a stock-theme universe.
Adapted from ChatGPT scaffold with stock-theme data adapter.
"""
from __future__ import annotations
import json, math, os
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import numpy as np
import pandas as pd

JST = timezone(timedelta(hours=9))

def _env_float(name, default):
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default

def _env_int(name, default):
    raw = os.getenv(name)
    return int(raw) if raw not in (None, "") else default

def ensure_dir(path):
    path.mkdir(parents=True, exist_ok=True)

def safe_z(series):
    std = series.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(np.zeros(len(series)), index=series.index)
    return (series - series.mean()) / std

@dataclass
class GateState:
    benchmark: str
    cash_proxy: str
    lookback_days: int
    benchmark_ret_3m: float
    cash_ret_3m: float
    excess_3m: float
    gate_state: str
    atk_cap: float
    # E1-I1: Regime observation axes (no judgment, record only)
    equity_axis: dict
    credit_axis: dict
    hard_asset_axis: dict
    duration_axis: dict

def classify_gate(excess_3m, open_threshold, mid_threshold):
    if excess_3m > open_threshold:
        return "OPEN", 0.80
    if excess_3m > mid_threshold:
        return "MID", 0.50
    return "CLOSED", 0.30

def tf_state(r1, r3, r6):
    if r1 > 0 and r3 > 0 and r6 > 0: return "STRONG"
    if r1 > 0 and r3 > 0 and r6 <= 0: return "MIXED_UP"
    if r1 <= 0 and r3 > 0 and r6 > 0: return "LATE"
    return "WEAK"

def acceleration_flag(r1, r3):
    return (r1 * 12.0) > (r3 * 4.0)

def zone_from_rank(rank_3m):
    if rank_3m <= 20: return "ENTRY_ZONE"
    if rank_3m <= 30: return "HOLD_ZONE"
    return "OUT_ZONE"


def theme_score(df):
    score = (
        1.00 * safe_z(df["ret_3m"])
        + 0.60 * safe_z((df["ret_1m"] * 12.0) - (df["ret_3m"] * 4.0))
        + 0.75 * df["consensus_flag"].astype(int)
        + 0.50 * df["sector_pass"].astype(int)
        - 0.60 * safe_z(df["theme_vol_proxy"])
    )
    return score

def load_frames():
    input_dir = Path(os.getenv("SNAPSHOT_INPUT_DIR", "data/input"))
    with open(input_dir / "market_returns.json", encoding="utf-8") as f:
        market_returns = json.load(f)
    sectors_df = pd.read_json(input_dir / "sectors.json")
    themes_df = pd.read_json(input_dir / "themes.json")
    constituents_df = pd.read_json(input_dir / "constituents.json")
    return sectors_df, themes_df, constituents_df, market_returns

def build_gate(market_returns):
    benchmark = os.getenv("MARKET_BENCHMARK", "SPY")
    cash_proxy = os.getenv("CASH_PROXY", "SHV")
    lookback_days = _env_int("K_GATE_LOOKBACK_DAYS", 63)
    open_threshold = _env_float("GATE_OPEN_THRESHOLD", 0.0)
    mid_threshold = _env_float("GATE_MID_THRESHOLD", -0.02)

    def _get(tk, field): return float(market_returns.get(tk, {}).get(field, 0))

    bench_ret = _get(benchmark, "ret_3m")
    cash_ret = _get(cash_proxy, "ret_3m")
    excess = bench_ret - cash_ret
    state, atk = classify_gate(excess, open_threshold, mid_threshold)

    equity_axis = {
        "spy_ret_3m": _get("SPY","ret_3m"), "qqq_ret_3m": _get("QQQ","ret_3m"),
        "shv_ret_3m": _get("SHV","ret_3m"), "equity_excess_3m": round(excess, 6),
    }
    credit_axis = {
        "lqd_excess_3m": round(_get("LQD","ret_3m") - cash_ret, 6),
        "hyg_excess_3m": round(_get("HYG","ret_3m") - cash_ret, 6),
        "hyg_minus_lqd_3m": round(_get("HYG","ret_3m") - _get("LQD","ret_3m"), 6),
    }
    hard_asset_axis = {
        "gld_excess_3m": round(_get("GLD","ret_3m") - cash_ret, 6),
        "xle_minus_spy_3m": round(_get("XLE","ret_3m") - _get("SPY","ret_3m"), 6),
    }
    duration_axis = {"tlt_ret_3m": _get("TLT","ret_3m")}

    return GateState(benchmark, cash_proxy, lookback_days, bench_ret, cash_ret, excess, state, atk,
                     equity_axis, credit_axis, hard_asset_axis, duration_axis)

def build_sector_layer(sectors_df):
    df = sectors_df.copy()
    df["rank_3m"] = df["ret_3m"].rank(method="first", ascending=False).astype(int)
    df["pass_abs"] = df["ret_3m"] > 0
    df["pass_rel"] = df["rank_3m"] <= 5
    df["pass_layer1"] = df["pass_abs"] & df["pass_rel"]
    df["tf_state"] = [tf_state(a, b, c) for a, b, c in zip(df["ret_1m"], df["ret_3m"], df["ret_6m"])]
    return df.sort_values("rank_3m").reset_index(drop=True)

def build_theme_layer(themes_df, sector_layer, constituents_df):
    df = themes_df.copy()
    sector_pass_map = sector_layer.set_index("sector")["pass_layer1"].to_dict()
    df["sector_pass"] = df["sector"].map(sector_pass_map).fillna(False)
    df["consensus_flag"] = (df["ret_1m"] > 0) & (df["ret_3m"] > 0) & (df["ret_6m"] > 0)
    df["acceleration_flag"] = [acceleration_flag(a, b) for a, b in zip(df["ret_1m"], df["ret_3m"])]
    df["tf_state"] = [tf_state(a, b, c) for a, b, c in zip(df["ret_1m"], df["ret_3m"], df["ret_6m"])]
    df["rank_3m"] = df["ret_3m"].rank(method="first", ascending=False).astype(int)
    vol_proxy = constituents_df.groupby("theme")["vol_20d_annualized"].mean()
    df["theme_vol_proxy"] = df["theme"].map(vol_proxy).fillna(0)
    df["high_vol_excluded"] = df["theme_vol_proxy"] > 0.60
    df["theme_score"] = theme_score(df)
    df = df.sort_values("theme_score", ascending=False).reset_index(drop=True)
    df["score_rank"] = np.arange(1, len(df) + 1)
    df["zone"] = df["score_rank"].map(zone_from_rank)
    df["entry_candidate"] = (
        (df["zone"] == "ENTRY_ZONE")
        & df["consensus_flag"]
        & df["acceleration_flag"]
        & df["sector_pass"]
        & ~df["high_vol_excluded"]
    )
    return df

def build_constituent_layer(constituents_df, selected_themes):
    df = constituents_df.copy()
    df["duplicate_theme_count"] = df.groupby("ticker")["theme"].transform("nunique")
    df["large_or_better"] = df["market_cap_bucket"].str.lower().isin({"mega", "large"})
    df["theme_selected"] = df["theme"].isin(list(selected_themes))
    df["rank_within_theme"] = (
        df.sort_values(["theme", "large_or_better", "ret_1m"], ascending=[True, False, False])
          .groupby("theme").cumcount() + 1
    )
    df["stock_selected"] = df["theme_selected"] & (df["rank_within_theme"] <= 2)
    return df.sort_values(["theme", "rank_within_theme"]).reset_index(drop=True)

def diff_list(prev, curr):
    ps, cs = set(prev), set(curr)
    return {"added": sorted(cs - ps), "removed": sorted(ps - cs), "unchanged": sorted(ps & cs)}

def build_trigger_candidates(gate, theme_df, constituents_df, prev_dir):
    prev_gate = {}; prev_signals = {}
    if prev_dir:
        gp = prev_dir / "gate.json"
        sp = prev_dir / "signals.json"
        if gp.exists(): prev_gate = json.loads(gp.read_text(encoding="utf-8"))
        if sp.exists(): prev_signals = json.loads(sp.read_text(encoding="utf-8"))

    curr_themes = theme_df.loc[theme_df["entry_candidate"], "theme"].tolist()
    prev_themes = prev_signals.get("selected_themes", []) if prev_signals else []
    curr_stocks = constituents_df.loc[constituents_df["stock_selected"], "ticker"].drop_duplicates().tolist()

    t1 = bool(prev_gate) and prev_gate.get("gate_state") != gate.gate_state
    rank_map = theme_df.set_index("theme")["score_rank"].to_dict()
    accel_map = theme_df.set_index("theme")["acceleration_flag"].to_dict()
    t2 = sorted([t for t in prev_themes if rank_map.get(t, 999) > 30])
    t3 = sorted([t for t in prev_themes if t in accel_map and not bool(accel_map[t])])
    t5 = sorted([t for t in curr_themes if t not in set(prev_themes)])

    return {
        "T1_gate_flip": t1, "T2_theme_drop": t2, "T3_accel_flip": t3,
        "T4_dd": [], "T5_new_entry": t5,
        "theme_membership_diff": diff_list(prev_themes, curr_themes),
        "prev_snapshot_available": prev_dir is not None,
        "selected_theme_count": len(curr_themes),
        "selected_stock_count": len(curr_stocks),
    }

def write_json(path, payload):
    import math
    def clean_nan(obj):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        if isinstance(obj, dict):
            return {k: clean_nan(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean_nan(v) for v in obj]
        return obj
    path.write_text(json.dumps(clean_nan(payload), indent=2, ensure_ascii=False, default=str), encoding="utf-8")

def main():
    now = datetime.now(JST)
    snapshot_root = Path(os.getenv("SNAPSHOT_ROOT", "data/snapshots"))
    snapshot_dir = snapshot_root / now.date().isoformat()
    ensure_dir(snapshot_dir)

    sectors_df, themes_df, constituents_df, market_returns = load_frames()
    gate = build_gate(market_returns)
    sector_layer = build_sector_layer(sectors_df)
    theme_layer = build_theme_layer(themes_df, sector_layer, constituents_df)
    selected = theme_layer.loc[theme_layer["entry_candidate"], "theme"].tolist()
    constituent_layer = build_constituent_layer(constituents_df, selected)

    all_dirs = sorted([p for p in snapshot_root.iterdir() if p.is_dir() and p != snapshot_dir])
    prev_dir = all_dirs[-1] if all_dirs else None
    triggers = build_trigger_candidates(gate, theme_layer, constituent_layer, prev_dir)

    meta = {"snapshot_date": now.date().isoformat(), "snapshot_time_jst": now.isoformat(),
            "schema_version": "v1", "generator": "generate_snapshot.py"}
    selected_stocks = constituent_layer.loc[constituent_layer["stock_selected"], "ticker"].drop_duplicates().tolist()

    # Production portfolio: atk_cap → sector_cap → single_name_cap → SHV
    SECTOR_CAP = 0.35
    MAX_POSITIONS = 35
    SINGLE_NAME_CAP = 0.08
    prod_stocks = selected_stocks[:MAX_POSITIONS]
    sec_map = {}
    for _, row in constituent_layer.iterrows():
        sec_map[row["ticker"]] = row.get("sector", "Unknown")
    if prod_stocks:
        # Step 1: atk_cap（攻撃比率上限）
        n = len(prod_stocks)
        weights = {tk: gate.atk_cap / n for tk in prod_stocks}
        # Step 2: sector_cap
        sec_tot = {}
        for tk, wt in weights.items():
            s = sec_map.get(tk, "Unknown")
            sec_tot[s] = sec_tot.get(s, 0) + wt
        for s, tot in sec_tot.items():
            if tot > SECTOR_CAP:
                scale = SECTOR_CAP / tot
                for tk in list(weights.keys()):
                    if sec_map.get(tk, "Unknown") == s:
                        weights[tk] = weights[tk] * scale
        # Step 3: single_name_cap
        for tk in list(weights.keys()):
            if weights[tk] > SINGLE_NAME_CAP:
                weights[tk] = SINGLE_NAME_CAP
        # Step 4: residual → SHV
        eq_total = sum(weights.values())
        weights["SHV"] = round(1.0 - eq_total, 4)
        # Collect prices for production portfolio
        price_map = {}
        for _, row in constituent_layer.iterrows():
            if row.get("price") and row["price"] > 0:
                price_map[row["ticker"]] = float(row["price"])
        # Add SHV price from stock_meta if available
        meta_path = Path(os.getenv("SNAPSHOT_INPUT_DIR", "data/input")).parent.parent / "public" / "api" / "stock_meta.json"
        if meta_path.exists():
            import json as _json
            with open(meta_path) as _f: _meta = _json.load(_f)
            for etf_tk in ["SHV","GLD","XLU","TLT","SPY"]:
                if etf_tk in _meta and _meta[etf_tk].get("price"):
                    price_map[etf_tk] = float(_meta[etf_tk]["price"])
        prod_portfolio = {"weights": {k: round(v, 4) for k, v in weights.items()},
                         "prices": price_map,
                         "sector_map": {tk: sec_map.get(tk, "Unknown") for tk in weights if tk != "SHV"},
                         "sector_cap": SECTOR_CAP, "max_positions": MAX_POSITIONS,
                         "single_name_cap": SINGLE_NAME_CAP,
                         "strategy": "PRISM_MH20_CAP35",
                         "summary": {"gross_equity_weight": round(eq_total, 4),
                                    "cash_proxy_weight": round(1.0 - eq_total, 4),
                                    "n_equity_positions": len(prod_stocks),
                                    "gate_state": gate.gate_state, "atk_cap": gate.atk_cap}}
    else:
        prod_portfolio = {"weights": {"SHV": 1.0}, "sector_cap": SECTOR_CAP,
                         "max_positions": MAX_POSITIONS, "strategy": "PRISM_MH20_CAP35"}

    signals = {"gate_state": gate.gate_state, "atk_cap": gate.atk_cap,
               "selected_sectors": sector_layer.loc[sector_layer["pass_layer1"], "ticker"].tolist(),
               "selected_themes": selected,
               "selected_stocks": selected_stocks,
               "production_portfolio": prod_portfolio,
               "trigger_candidates": triggers}

    write_json(snapshot_dir / "meta.json", meta)
    write_json(snapshot_dir / "gate.json", asdict(gate))
    write_json(snapshot_dir / "sectors.json", sector_layer.to_dict(orient="records"))
    write_json(snapshot_dir / "themes.json", theme_layer.to_dict(orient="records"))
    write_json(snapshot_dir / "constituents.json", constituent_layer.to_dict(orient="records"))
    write_json(snapshot_dir / "signals.json", signals)

    # Copy to latest/
    latest = snapshot_root / "latest"
    ensure_dir(latest)
    for f in ["meta.json","gate.json","sectors.json","themes.json","constituents.json","signals.json"]:
        (latest / f).write_text((snapshot_dir / f).read_text(encoding="utf-8"), encoding="utf-8")

    print(f"✓ Snapshot: {snapshot_dir}")
    print(f"  Gate: {gate.gate_state} (atk_cap={gate.atk_cap})")
    print(f"  Sectors: {signals['selected_sectors']}")
    print(f"  Themes: {len(selected)} selected")
    print(f"  Stocks: {len(signals['selected_stocks'])} selected")
    if triggers.get("T1_gate_flip"): print(f"  ⚠ T1: K-gate FLIPPED")
    if triggers.get("T2_theme_drop"): print(f"  ⚠ T2: Themes dropped: {triggers['T2_theme_drop']}")
    if triggers.get("T5_new_entry"): print(f"  ⚠ T5: New entries: {triggers['T5_new_entry']}")

if __name__ == "__main__":
    main()
