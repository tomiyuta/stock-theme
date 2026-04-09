#!/usr/bin/env python3
"""Robustness: sub-period split + transaction cost sensitivity."""
import pandas as pd, json
from pathlib import Path
ROOT = Path("/Users/yutatomi/Downloads/stock-theme")
OUT = ROOT/"output"/"benchmarks"
df = pd.read_csv(OUT/"MH20_CAP35_daily.csv")
df["date"] = pd.to_datetime(df["date"])
bm3 = pd.read_csv(OUT/"BM3_SECTOR_ROTATION_daily.csv")
bm3["date"] = pd.to_datetime(bm3["date"])
snap_root = ROOT/"data"/"historical"/"snapshots"
gate_map = {}
for d in snap_root.iterdir():
    if d.is_dir() and d.name[:4].isdigit():
        with open(d/"gate.json") as f: g=json.load(f)
        gate_map[d.name] = g["gate_state"]
df["gate"] = df["date"].dt.strftime("%Y-%m-%d").map(gate_map)

def metrics(series):
    if len(series)<2: return {}
    rets = series.pct_change().dropna()
    n=len(rets); years=max(n/252,0.01)
    cagr = (series.iloc[-1]/series.iloc[0])**(1/years)-1
    peak = series.cummax(); dd = (series/peak-1).min()
    std = rets.std(ddof=0)
    sharpe = (rets.mean()/std*(252**0.5)) if std>0 else 0
    worst = rets.min()
    return {"CAGR":f"{cagr:.1%}","MaxDD":f"{dd:.1%}","Sharpe":f"{sharpe:.2f}","WorstDay":f"{worst:.1%}","Days":n}

mid = len(df)//2
print("=== MH20_CAP35 SUB-PERIOD ===")
for label, slc in [("First Half",df.iloc[:mid]),("Second Half",df.iloc[mid:])]:
    print(f"  {label}: {metrics(slc['equity'])}")

print("\n=== REGIME SPLIT ===")
for state in ["OPEN","MID","CLOSED"]:
    mask = df["gate"]==state
    if mask.sum()>1:
        sub = df[mask]
        print(f"  {state} ({mask.sum()}d): {metrics(sub['equity'].reset_index(drop=True))}")

print("\n=== TRANSACTION COST SENSITIVITY ===")
trades = pd.read_csv(OUT/"MH20_CAP35_trades.csv")
total_notional = trades["notional"].abs().sum()
avg_eq = df["equity"].mean()
annual_factor = 252/len(df)
annual_to = total_notional / avg_eq * annual_factor
print(f"  Annual turnover ratio: {annual_to:.1f}x")
for bps in [5, 10, 20]:
    drag = annual_to * bps/10000
    adj = 0.339193 - drag
    print(f"  {bps}bps: drag={drag:.1%}, adj CAGR={adj:.1%} (BM3=19.4%)")

print("\n=== BM3 SUB-PERIOD ===")
mid3 = len(bm3)//2
for label, slc in [("First Half",bm3.iloc[:mid3]),("Second Half",bm3.iloc[mid3:])]:
    print(f"  {label}: {metrics(slc['equity'])}")
print("\n✓ Done")
