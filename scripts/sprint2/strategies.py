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

class PrismMH20SectorCap:
    """MH20 baseline + sector concentration cap."""
    def __init__(self, min_days=20, sector_cap=0.35):
        self.min_days=min_days; self.cap=sector_cap
        self.name=f"MH{min_days}_CAP{int(sector_cap*100)}"
        self._held={}; self._prev_date=None

    def build_target_portfolio(self, date_str, snap):
        if self._prev_date and self._prev_date!=date_str:
            for tk in self._held: self._held[tk]+=1
        self._prev_date=date_str

        new_picks=[p for p in snap["signals"].get("selected_stocks",[]) if p]
        seen=set(); unique=[]
        for p in new_picks:
            if p not in seen: unique.append(p); seen.add(p)
        kept={tk:self._held[tk] for tk in self._held if self._held[tk]<self.min_days}
        expired={tk for tk in self._held if self._held[tk]>=self.min_days and tk not in set(unique)}
        for tk in expired: del self._held[tk]
        for tk in unique:
            if tk not in self._held: self._held[tk]=0
        final=list(self._held.keys())
        if not final:
            return {"weights":{"SHV":1.0},"reasons":{"SHV":"no holdings"}}

        # Build sector map from constituents
        sec_map={}
        for c in snap.get("constituents",[]):
            sec_map[c["ticker"]]=c.get("sector","Unknown")

        # Equal weight then cap
        w=1.0/len(final)
        weights={tk:w for tk in final}
        # Compute sector totals
        sec_tot={}
        for tk,wt in weights.items():
            s=sec_map.get(tk,"Unknown")
            sec_tot[s]=sec_tot.get(s,0)+wt
        # Scale down over-cap sectors
        excess_total=0
        for s,tot in sec_tot.items():
            if tot>self.cap:
                scale=self.cap/tot
                for tk in list(weights.keys()):
                    if sec_map.get(tk,"Unknown")==s:
                        old=weights[tk]; weights[tk]=old*scale
                        excess_total+=(old-weights[tk])
        # Redistribute excess to cash (SHV)
        if excess_total>0.001:
            weights["SHV"]=weights.get("SHV",0)+excess_total

        reasons={tk:f"cap({sec_map.get(tk,'?')})" if sec_map.get(tk) in [s for s,t in sec_tot.items() if t>self.cap] else "selected" for tk in weights}
        return {"weights":weights,"reasons":reasons}

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


class PrismHysteresis:
    """PRISM with configurable hysteresis zones + MH20. Themes use different thresholds for entry vs hold."""
    def __init__(self, entry_rank=15, hold_rank=30, min_days=20, label=""):
        self.entry_rank = entry_rank
        self.hold_rank = hold_rank
        self.min_days = min_days
        self.name = label or f"PRISM_HYS_E{entry_rank}_H{hold_rank}"
        self._held_themes = set()  # currently held themes
        self._held_stocks = {}     # ticker -> days_held
        self._prev_date = None

    def build_target_portfolio(self, date_str, snap):
        if self._prev_date and self._prev_date != date_str:
            for tk in self._held_stocks:
                self._held_stocks[tk] += 1
        self._prev_date = date_str

        themes = snap.get("themes", [])
        constituents = snap.get("constituents", [])

        # Build theme rank map
        rank_map = {}  # theme_name -> score_rank
        theme_data = {}
        for t in themes:
            name = t.get("theme", "")
            rank = t.get("score_rank", 999)
            rank_map[name] = rank
            theme_data[name] = t

        # Decide which themes to hold
        new_held = set()
        for name, rank in rank_map.items():
            t = theme_data[name]
            in_held = name in self._held_themes
            if in_held:
                # HOLD zone: more lenient
                if rank <= self.hold_rank:
                    new_held.add(name)
            else:
                # ENTRY zone: stricter
                if (rank <= self.entry_rank
                    and t.get("consensus_flag", False)
                    and t.get("acceleration_flag", False)
                    and t.get("sector_pass", False)
                    and not t.get("high_vol_excluded", False)):
                    new_held.add(name)
        self._held_themes = new_held

        # Select stocks from held themes (top 2 per theme by ret_1m, large priority)
        stocks = []
        for theme_name in new_held:
            theme_cons = [c for c in constituents if c.get("theme") == theme_name]
            theme_cons.sort(key=lambda c: (
                c.get("market_cap_bucket", "") in ("large", "mega"),
                c.get("ret_1m", 0) or 0
            ), reverse=True)
            for c in theme_cons[:2]:
                tk = c["ticker"]
                if tk not in [s["ticker"] for s in stocks]:
                    stocks.append(c)

        # Apply min hold
        new_tickers = {s["ticker"] for s in stocks}
        # Keep stocks under min hold even if their theme dropped
        kept = {tk for tk, days in self._held_stocks.items() if days < self.min_days and tk not in new_tickers}
        all_tickers = new_tickers | kept

        # Update held stocks
        expired = [tk for tk in self._held_stocks if tk not in all_tickers]
        for tk in expired:
            del self._held_stocks[tk]
        for tk in all_tickers:
            if tk not in self._held_stocks:
                self._held_stocks[tk] = 0

        if not all_tickers:
            return {"weights": {"SHV": 1.0}, "reasons": {"SHV": "no holdings"}}

        w = 1.0 / len(all_tickers)
        reasons = {}
        for tk in all_tickers:
            if tk in kept:
                reasons[tk] = f"min_hold({self._held_stocks[tk]}d/{self.min_days}d)"
            elif tk in new_tickers:
                reasons[tk] = "hysteresis selected"
            else:
                reasons[tk] = "held"
        return {"weights": {tk: w for tk in all_tickers}, "reasons": reasons}
