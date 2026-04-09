#!/usr/bin/env python3
"""Replay engine adapted for PRISM historical snapshots with external price sources."""
from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

class PriceProvider:
    def __init__(self, project_root):
        self.root = Path(project_root)
        self._etf = self._load_etf()
        self._stock = self._load_stocks()
    def _load_etf(self):
        with open(self.root/"data"/"historical"/"etf_daily.json") as f:
            return json.load(f)
    def _load_stocks(self):
        result = {}
        for fp in (self.root/"public"/"api"/"theme-details").glob("*.json"):
            with open(fp) as f: d = json.load(f)
            for p in d.get("prices",[]):
                dt = p.get("date")
                if not dt: continue
                for tk in d.get("tickers",[]):
                    v = p.get(tk)
                    if v and v > 0: result.setdefault(tk,{})[dt] = float(v)
        return result
    def get_price(self, sym, date):
        if sym in self._etf: return self._etf[sym].get(date)
        if sym in self._stock: return self._stock[sym].get(date)
        return None

class SnapshotStore:
    def __init__(self, root):
        self.root = Path(root)
        self.dates = sorted([p.name for p in self.root.iterdir() if p.is_dir() and p.name[:4].isdigit()])
    def iter_dates(self): return self.dates
    def next_date(self, d):
        i = self.dates.index(d)
        return self.dates[i+1] if i+1 < len(self.dates) else None
    def load_all(self, d):
        r = {}
        for k in ["meta","gate","sectors","themes","constituents","signals"]:
            with open(self.root/d/f"{k}.json","r",encoding="utf-8") as f: r[k]=json.load(f)
        return r

class ReplayEngine:
    def __init__(self, store, prices, initial_capital=1_000_000.0):
        self.store=store; self.prices=prices; self.initial_capital=float(initial_capital)
    def run(self, strategy):
        cash=self.initial_capital; holdings={}; entry_dates={}
        daily_rows,trade_rows,holding_rows=[],[],[]
        dates=self.store.iter_dates()
        for cur in dates[:-1]:
            nxt=self.store.next_date(cur)
            if nxt is None: break
            snap=self.store.load_all(cur)
            tgt=strategy.build_target_portfolio(cur,snap)
            tw=tgt.get("weights",{}); reasons=tgt.get("reasons",{})
            eq=cash
            for s,sh in holdings.items():
                px=self.prices.get_price(s,nxt)
                if px: eq+=sh*px

            for s,sh in list(holdings.items()):
                px=self.prices.get_price(s,nxt)
                if px is None: continue
                cash+=sh*px
                trade_rows.append({"signal_date":cur,"exec_date":nxt,"action":"SELL","symbol":s,"shares":sh,"price":px,"notional":sh*px,"reason":"rebalance","strategy":strategy.name})
                del holdings[s]; entry_dates.pop(s,None)
            for s,w in tw.items():
                px=self.prices.get_price(s,nxt)
                if px is None or px<=0 or w<=0: continue
                notional=eq*float(w); sh=notional/px
                holdings[s]=sh; cash-=notional; entry_dates[s]=nxt
                trade_rows.append({"signal_date":cur,"exec_date":nxt,"action":"BUY","symbol":s,"shares":sh,"price":px,"notional":notional,"reason":reasons.get(s,""),"strategy":strategy.name})
            meq=cash
            for s,sh in holdings.items():
                px=self.prices.get_price(s,nxt)
                if px: mv=sh*px; meq+=mv; holding_rows.append({"date":nxt,"symbol":s,"shares":sh,"price":px,"market_value":mv,"entry_date":entry_dates.get(s),"strategy":strategy.name})
            daily_rows.append({"date":nxt,"equity":meq,"cash":cash,"n_positions":len(holdings),"strategy":strategy.name})
        daily=pd.DataFrame(daily_rows); trades=pd.DataFrame(trade_rows); hdf=pd.DataFrame(holding_rows)
        if not daily.empty:
            daily["ret"]=daily["equity"].pct_change().fillna(0.0)
            daily["cum_return"]=daily["equity"]/self.initial_capital-1.0
            daily["peak"]=daily["equity"].cummax()
            daily["drawdown"]=daily["equity"]/daily["peak"]-1.0
        return {"daily":daily,"trades":trades,"holdings":hdf}

def summarize_performance(daily, trades):
    if daily.empty:
        return {"CAGR":None,"MaxDD":None,"Sharpe_daily":None,"Turnover_trades":0,"AvgHoldingDays_proxy":None,"WorstDay":None,"Rebalance_days":None,"Avg_Jaccard":None,"Max_sector_pct":None}
    n=len(daily); years=max(n/252.0,1e-9)
    end=float(daily["equity"].iloc[-1]); first=float(daily["equity"].iloc[0])
    cagr=(end/first)**(1/years)-1 if first>0 else None
    maxdd=float(daily["drawdown"].min())
    std=float(daily["ret"].std(ddof=0))
    sharpe=(float(daily["ret"].mean())/std*(252**0.5)) if std>0 else None
    worst=float(daily["ret"].min())
    # Position set change metrics
    rebal_days=0; jaccard_vals=[]
    if not trades.empty:
        buy_by_date=trades[trades["action"]=="BUY"].groupby("exec_date")["symbol"].apply(set).to_dict()
        dates_sorted=sorted(buy_by_date.keys())
        prev_set=set()
        for d in dates_sorted:
            cur_set=buy_by_date[d]
            if prev_set and cur_set!=prev_set:
                rebal_days+=1
                union=prev_set|cur_set; inter=prev_set&cur_set
                jaccard_vals.append(len(inter)/len(union) if union else 1.0)
            prev_set=cur_set
    avg_jaccard=sum(jaccard_vals)/len(jaccard_vals) if jaccard_vals else None
    avg_hold=None
    if not trades.empty:
        buys=trades[trades["action"]=="BUY"].copy(); sells=trades[trades["action"]=="SELL"].copy()
        if not buys.empty and not sells.empty:
            buys["exec_date"]=pd.to_datetime(buys["exec_date"]); sells["exec_date"]=pd.to_datetime(sells["exec_date"])
            m=buys.merge(sells[["symbol","exec_date"]],on="symbol",suffixes=("_b","_s"))
            m=m[m["exec_date_s"]>=m["exec_date_b"]]
            if not m.empty: avg_hold=float((m["exec_date_s"]-m["exec_date_b"]).dt.days.mean())
    return {"CAGR":cagr,"MaxDD":maxdd,"Sharpe_daily":sharpe,"Turnover_trades":int(len(trades)),"AvgHoldingDays_proxy":avg_hold,"WorstDay":worst,"Rebalance_days":rebal_days,"Avg_Jaccard":round(avg_jaccard,3) if avg_jaccard else None,"Max_sector_pct":None}
