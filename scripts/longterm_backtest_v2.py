#!/usr/bin/env python3
"""Long-term backtest v2: BM2/BM3/BM3_MH20_CAP35/SPY_BH using new Norgate PIT ETF dataset.
Replaces external norgate_full_bundle.parquet with self-contained norgate_etf_prices.parquet.
"""
import pandas as pd
import numpy as np
from pathlib import Path

SCB = Path("/Users/yutatomi/Downloads/stock-theme/research/scb")
OUT_DIR = Path("/Users/yutatomi/Downloads/stock-theme/output/longterm")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# === Load new ETF data (long format) → convert to wide format for compatibility ===
print("Loading ETF prices...")
etf_long = pd.read_parquet(SCB / "norgate_etf_prices.parquet", columns=["date","ticker","close"])
etf = etf_long.pivot_table(index="date", columns="ticker", values="close", aggfunc="first").sort_index()
# Rename columns to match old format: TICKER → TICKER_AdjClose
etf.columns = [f"{c}_AdjClose" for c in etf.columns]
print(f"ETF: {etf.shape}, {etf.index.min().date()} ~ {etf.index.max().date()}")

SECTOR_ETFS = ['XLE','XLU','XLB','XLK','XLV','XLF','XLI']
SECTOR_ETFS_LATE = ['XLC','XLRE']
CASH = 'SHY'
LB = 63  # 3-month lookback
HOLD_MIN = 20
SECTOR_CAP = 0.35
TOP_N = 3

# Precompute daily returns for all tickers
tickers_all = SECTOR_ETFS + SECTOR_ETFS_LATE + [CASH, 'SPY']
daily_ret = {}
rolling_3m = {}
for tk in tickers_all:
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
print(f"Backtest: {dates[0].date()} ~ {dates[-1].date()} ({len(dates)} days)")

def gate(exc):
    if np.isnan(exc): return 'CLOSED', 0.30
    if exc > 0: return 'OPEN', 0.80
    if exc > -0.02: return 'MID', 0.50
    return 'CLOSED', 0.30

def available_sectors(d):
    out = []
    for tk in SECTOR_ETFS + SECTOR_ETFS_LATE:
        if tk in rolling_3m and d in rolling_3m[tk].index:
            v = rolling_3m[tk].loc[d]
            if not np.isnan(v):
                out.append((tk, v))
    return sorted(out, key=lambda x: -x[1])

class Strategy:
    def __init__(self, name):
        self.name = name
        self.equity = 1_000_000.0
        self.weights = {}
        self.hold_days = {}
        self.rows = []

strats = {n: Strategy(n) for n in ['BM2','BM3','BM3_MH20_CAP35','SPY_BH']}

for i in range(len(dates) - 1):
    d, d1 = dates[i], dates[i+1]
    exc_val = excess.get(d, np.nan)
    g_state, atk_cap = gate(exc_val)
    avail = available_sectors(d)
    passed = [(tk, r) for tk, r in avail if r > 0]
    top = [tk for tk, _ in passed[:TOP_N]]

    targets = {}
    targets['BM2'] = {'SPY': 1.0} if g_state == 'OPEN' else {CASH: 1.0}

    if top:
        w = 1.0 / len(top)
        targets['BM3'] = {tk: w for tk in top}
    else:
        targets['BM3'] = {CASH: 1.0}

    s = strats['BM3_MH20_CAP35']
    if top:
        w_each = min(atk_cap / len(top), SECTOR_CAP)
        cap35 = {tk: w_each for tk in top}
        eq_sum = sum(cap35.values())
        cap35[CASH] = 1.0 - eq_sum
    else:
        cap35 = {CASH: 1.0}
    for tk, days in s.hold_days.items():
        if days < HOLD_MIN and tk != CASH and tk not in cap35:
            old_w = s.weights.get(tk, 0)
            if old_w > 0:
                cap35[tk] = old_w
                cap35[CASH] = cap35.get(CASH, 0) - old_w
                if cap35[CASH] < 0:
                    cap35[CASH] = 0
    targets['BM3_MH20_CAP35'] = cap35
    targets['SPY_BH'] = {'SPY': 1.0}

    for sname, s in strats.items():
        tw = targets[sname]
        port_ret = 0.0
        for tk, w in tw.items():
            if tk in daily_ret and d1 in daily_ret[tk].index:
                r = daily_ret[tk].loc[d1]
                if not np.isnan(r):
                    port_ret += w * r
        s.equity *= (1.0 + port_ret)
        new_hd = {}
        for tk in tw:
            if tk == CASH: continue
            if tw[tk] > 0:
                new_hd[tk] = s.hold_days.get(tk, -1) + 1
        s.hold_days = new_hd
        s.weights = dict(tw)
        s.rows.append({'date': d1, 'equity': s.equity, 'port_ret': port_ret})

    if i % 1000 == 0:
        print(f"  [{i}/{len(dates)-1}] {d.date()} gate={g_state} eq={strats['BM3_MH20_CAP35'].equity:,.0f}")

print("\n" + "="*90)
print(f"{'Strategy':25s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Sortino':>8s} {'WorstMo':>8s}")
print("="*90)
for sname, s in strats.items():
    df = pd.DataFrame(s.rows)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['peak'] = df['equity'].cummax()
    df['dd'] = df['equity'] / df['peak'] - 1
    n = len(df)
    years = max(n / 252, 0.01)
    cagr = (df['equity'].iloc[-1] / df['equity'].iloc[0]) ** (1/years) - 1
    maxdd = df['dd'].min()
    rets = df['port_ret']
    std = rets.std(ddof=0)
    sharpe = (rets.mean() / std * (252**0.5)) if std > 0 else 0
    neg = rets[rets < 0]
    semi_std = np.sqrt((neg**2).mean()) if len(neg) > 0 else 1e-9
    sortino = rets.mean() / semi_std * (252**0.5)
    df['ym'] = df['date'].dt.to_period('M')
    monthly = df.groupby('ym')['port_ret'].apply(lambda x: (1+x).prod()-1)
    worst_mo = monthly.min()
    print(f"{sname:25s} {cagr:>+7.1%} {maxdd:>+7.1%} {sharpe:>8.2f} {sortino:>8.2f} {worst_mo:>+7.1%}")
    df.to_csv(OUT_DIR / f"{sname}_daily.csv", index=False)
print("="*90)
print(f"Period: {dates[0].date()} ~ {dates[-1].date()} ({len(dates)} days, {len(dates)/252:.1f} years)")
print(f"Saved to {OUT_DIR}")
