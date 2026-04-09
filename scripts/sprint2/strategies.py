#!/usr/bin/env python3
"""Benchmark strategies for Sprint 2."""
from __future__ import annotations
from typing import Dict, Any

class BM2SpyShvSwitch:
    name = "BM2_SPY_SHV"
    def build_target_portfolio(self, date_str, snap):
        gate = snap["gate"]
        excess = gate.get("excess_3m")
        target = "SPY" if (excess is not None and float(excess) > 0) else "SHV"
        return {"weights": {target: 1.0}, "reasons": {target: "SPY>SHV" if target=="SPY" else "SPY<=SHV"}}

class BM3SectorRotation:
    name = "BM3_SECTOR_ROTATION"
    def __init__(self, top_n=3): self.top_n = int(top_n)
    def build_target_portfolio(self, date_str, snap):
        passed = [r for r in snap["sectors"] if r.get("pass_layer1")]
        if not passed:
            return {"weights": {"SHV": 1.0}, "reasons": {"SHV": "no passed sectors"}}
        top = sorted(passed, key=lambda r: float(r.get("ret_3m", -999)), reverse=True)[:self.top_n]
        w = 1.0 / len(top)
        return {"weights": {r["ticker"]: w for r in top}, "reasons": {r["ticker"]: f"sector rank={r.get('rank_3m')}" for r in top}}

class BM5DirectStockMomentum:
    name = "BM5_DIRECT_STOCK"
    def __init__(self, top_n=10): self.top_n = int(top_n)
    def build_target_portfolio(self, date_str, snap):
        cands = []
        for r in snap["constituents"]:
            r1=r.get("ret_1m"); r3=r.get("ret_3m")
            if r1 is None or r3 is None: continue
            if float(r1)<=0 or float(r3)<=0: continue
            mc = str(r.get("market_cap_bucket","")).lower()
            bonus = 1 if mc in {"large","mega"} else 0
            cands.append((r, bonus))
        if not cands:
            return {"weights": {"SHV": 1.0}, "reasons": {"SHV": "no candidates"}}
        cands = sorted(cands, key=lambda x: (float(x[0].get("ret_1m",-999)), x[1]), reverse=True)[:self.top_n]
        seen = set(); unique = []
        for r,_ in cands:
            if r["ticker"] not in seen: unique.append(r); seen.add(r["ticker"])
        w = 1.0 / len(unique)
        return {"weights": {r["ticker"]: w for r in unique}, "reasons": {r["ticker"]: "direct momentum" for r in unique}}

class PrismV1Replay:
    name = "PRISM_V1"
    def build_target_portfolio(self, date_str, snap):
        picks = [p for p in snap["signals"].get("selected_stocks", []) if p]
        if not picks:
            return {"weights": {"SHV": 1.0}, "reasons": {"SHV": "no PRISM picks"}}
        seen = set(); unique = []
        for p in picks:
            if p not in seen: unique.append(p); seen.add(p)
        w = 1.0 / len(unique)
        return {"weights": {p: w for p in unique}, "reasons": {p: "PRISM selected" for p in unique}}
