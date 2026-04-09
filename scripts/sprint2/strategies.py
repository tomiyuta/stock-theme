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

class PrismV1WithMinHold:
    """PRISM v1 with minimum holding period. Stocks held < min_days are NOT sold."""
    def __init__(self, min_days=20):
        self.min_days = int(min_days)
        self.name = f"PRISM_V1_MH{self.min_days}"
        self._held = {}  # ticker -> days_held
        self._prev_date = None

    def build_target_portfolio(self, date_str, snap):
        # Advance holding counters
        if self._prev_date and self._prev_date != date_str:
            for tk in self._held:
                self._held[tk] += 1
        self._prev_date = date_str

        new_picks = [p for p in snap["signals"].get("selected_stocks", []) if p]
        seen = set(); unique_new = []
        for p in new_picks:
            if p not in seen: unique_new.append(p); seen.add(p)

        # Keep stocks under min hold
        kept = {tk: self._held[tk] for tk in self._held if self._held[tk] < self.min_days}
        # Remove expired stocks not in new picks
        expired = {tk for tk in self._held if self._held[tk] >= self.min_days and tk not in seen}
        for tk in expired:
            del self._held[tk]

        # Add new picks
        for tk in unique_new:
            if tk not in self._held:
                self._held[tk] = 0

        # Remove kept stocks that expired AND are not new picks
        final_tickers = list(self._held.keys())

        if not final_tickers:
            return {"weights": {"SHV": 1.0}, "reasons": {"SHV": "no holdings"}}

        w = 1.0 / len(final_tickers)
        reasons = {}
        for tk in final_tickers:
            if tk in kept and tk not in seen:
                reasons[tk] = f"min_hold({self._held[tk]}d/{self.min_days}d)"
            else:
                reasons[tk] = "PRISM selected"
        return {"weights": {tk: w for tk in final_tickers}, "reasons": reasons}

    def reset(self):
        self._held = {}
        self._prev_date = None

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
