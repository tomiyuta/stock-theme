#!/usr/bin/env python3
"""K-gate sensitivity analysis: NoGate / CurrentGate / SoftGate / LinearGate
Using BM3 (sector ETF) on Norgate 24-year data.
Purpose: quantify CAGR cost vs DD benefit of each gate model."""
import pandas as pd, numpy as np
from pathlib import Path

ETF_PATH = Path("/Users/yutatomi/Downloads/norgate_full_bundle.parquet")
OUT = Path("/Users/yutatomi/Downloads/stock-theme/output/longterm")
OUT.mkdir(parents=True, exist_ok=True)

etf = pd.read_parquet(ETF_PATH)
etf['Date'] = pd.to_datetime(etf['Date'])
etf = etf.set_index('Date').sort_index()

SECTORS = ['XLE','XLU','XLB','XLK','XLV','XLF','XLI','XLC','XLRE']
CASH = 'SHY'
LB = 63; TOP_N = 3; HOLD_MIN = 20; SECTOR_CAP = 0.35

tickers = SECTORS + [CASH, 'SPY']
daily_ret, rolling_3m = {}, {}
for tk in tickers:
    col = f'{tk}_AdjClose'
    if col in etf.columns:
        px = etf[col].dropna()
        daily_ret[tk] = px.pct_change()
        rolling_3m[tk] = px / px.shift(LB) - 1

spy_r3m = rolling_3m.get('SPY', pd.Series(dtype=float))
shy_r3m = rolling_3m.get(CASH, pd.Series(dtype=float))
excess = spy_r3m - shy_r3m

start = pd.Timestamp('2002-08-01')
dates = [d for d in etf.index if d >= start]
print(f"Period: {dates[0].date()} ~ {dates[-1].date()} ({len(dates)} days, {len(dates)/252:.1f}y)")

# === 4 Gate Models ===
def gate_none(exc):    return 1.0
def gate_current(exc):
    if exc > 0: return 0.80
    if exc > -0.02: return 0.50
    return 0.30
def gate_soft(exc):
    if exc > 0.02: return 1.0
    if exc > 0: return 0.80
    if exc > -0.02: return 0.60
    return 0.40
def gate_linear(exc):
    return min(1.0, max(0.40, 0.40 + 0.30 * (exc / 0.02)))

MODELS = {
    'M0_NoGate': gate_none,
    'M1_Current': gate_current,
    'M2_Soft': gate_soft,
    'M3_Linear': gate_linear,
}

def avail_sectors(d):
    out = []
    for tk in SECTORS:
        if tk in rolling_3m and d in rolling_3m[tk].index:
            v = rolling_3m[tk].loc[d]
            if not np.isnan(v): out.append((tk, v))
    return sorted(out, key=lambda x: -x[1])

class S:
    def __init__(self): self.eq=1e6; self.w={}; self.hd={}; self.rows=[]

strats = {n: S() for n in MODELS}

for i in range(len(dates)-1):
    d, d1 = dates[i], dates[i+1]
    exc_val = excess.get(d, np.nan)
    if np.isnan(exc_val): exc_val = -0.05
    avail = avail_sectors(d)
    passed = [(tk, r) for tk, r in avail if r > 0]
    top = [tk for tk, _ in passed[:TOP_N]]

    for mname, gate_fn in MODELS.items():
        s = strats[mname]
        atk = gate_fn(exc_val)
        if top:
            w_each = min(atk / len(top), SECTOR_CAP)
            tw = {tk: w_each for tk in top}
            tw[CASH] = 1.0 - sum(tw.values())
        else:
            tw = {CASH: 1.0}
        # MinHold
        for tk, days in s.hd.items():
            if days < HOLD_MIN and tk != CASH and tk not in tw:
                old = s.w.get(tk, 0)
                if old > 0:
                    tw[tk] = old
                    tw[CASH] = tw.get(CASH, 0) - old
                    if tw[CASH] < 0: tw[CASH] = 0

        # Portfolio return
        pr = 0.0
        for tk, w in tw.items():
            if tk in daily_ret and d1 in daily_ret[tk].index:
                r = daily_ret[tk].loc[d1]
                if not np.isnan(r): pr += w * r
        s.eq *= (1.0 + pr)
        nhd = {}
        for tk in tw:
            if tk != CASH and tw[tk] > 0: nhd[tk] = s.hd.get(tk, -1) + 1
        s.hd = nhd; s.w = dict(tw)
        s.rows.append({'date': d1, 'equity': s.eq, 'ret': pr, 'atk_cap': atk})
    if i % 1000 == 0:
        print(f"  [{i}/{len(dates)-1}] {d.date()} exc={exc_val:.3f}")

# === Summary ===
print("\n" + "="*100)
print(f"{'Model':20s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Sortino':>8s} {'WorstMo':>8s} {'AvgAtk':>8s} {'ΔCAGR':>8s} {'ΔMaxDD':>8s}")
print("="*100)

ref_cagr = ref_dd = None
for mname, s in strats.items():
    df = pd.DataFrame(s.rows); df['date'] = pd.to_datetime(df['date'])
    df['peak'] = df['equity'].cummax(); df['dd'] = df['equity']/df['peak']-1
    years = len(df)/252
    cagr = (df['equity'].iloc[-1]/df['equity'].iloc[0])**(1/years)-1
    maxdd = df['dd'].min()
    std = df['ret'].std(ddof=0)
    sharpe = df['ret'].mean()/std*(252**0.5) if std>0 else 0
    neg = df['ret'][df['ret']<0]
    sortino = df['ret'].mean()/np.sqrt((neg**2).mean())*(252**0.5) if len(neg)>0 else 0
    df['ym'] = df['date'].dt.to_period('M')
    wmo = df.groupby('ym')['ret'].apply(lambda x:(1+x).prod()-1).min()
    avg_atk = df['atk_cap'].mean()
    if ref_cagr is None: ref_cagr, ref_dd = cagr, maxdd
    dc = cagr - ref_cagr; dd_d = maxdd - ref_dd
    print(f"{mname:20s} {cagr:>+7.1%} {maxdd:>+7.1%} {sharpe:>8.2f} {sortino:>8.2f} {wmo:>+7.1%} {avg_atk:>7.0%} {dc:>+7.1%} {dd_d:>+7.1%}")
    df.to_csv(OUT/f"kgate_{mname}_daily.csv", index=False)

print("="*100)
print("ΔCAGR/ΔMaxDD = NoGate基準。正のΔMaxDD = NoGateよりDD改善。")
