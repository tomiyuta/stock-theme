#!/usr/bin/env python3
"""Phase 2: BM5 (direct stock momentum) × MH20 × CAP35 using Norgate PIT S&P500 (2002-2026)."""
import pandas as pd
import numpy as np
from pathlib import Path

HG_PATH = Path("/Users/yutatomi/Downloads/01_投資・定量分析/99_archive/HolyGrail_v4/holygrail_v4 2/data/holygrail_with_universe.parquet")
ETF_PATH = Path("/Users/yutatomi/Downloads/norgate_full_bundle.parquet")
OUT_DIR = Path("/Users/yutatomi/Downloads/stock-theme/output/longterm")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load universe data (PIT S&P500)
print("Loading PIT S&P500 universe...")
hg = pd.read_parquet(HG_PATH)
print(f"Universe: {hg.shape}, {hg.index.min().date()} ~ {hg.index.max().date()}")

# Extract ticker names
adj_cols = [c for c in hg.columns if '_AdjClose' in c]
univ_cols = [c for c in hg.columns if '_Universe_SP500' in c]
tickers = [c.replace('_AdjClose', '') for c in adj_cols]
print(f"Tickers: {len(tickers)} with AdjClose, {len(univ_cols)} with SP500 membership")

# Load ETF data for K-gate
etf = pd.read_parquet(ETF_PATH)
etf['Date'] = pd.to_datetime(etf['Date'])
etf = etf.set_index('Date').sort_index()
spy_px = etf['SPY_AdjClose']
shy_px = etf['SHY_AdjClose']

LB = 63  # 3M lookback for sector; use 126 for 6M stock momentum
STOCK_LB = 126  # 6M momentum for stock selection (matching PRISM Layer 3)
TOP_K = 5  # Top K stocks
HOLD_MIN = 20
SECTOR_CAP = 0.35
SINGLE_CAP = 0.08

# Precompute monthly rebalance dates (last trading day of each month)
hg_dates = hg.index.sort_values()
start = pd.Timestamp('2002-08-01')
all_dates = hg_dates[hg_dates >= start]

# Use month-end rebalance (matching PRISM design)
etf_me = etf[etf['is_month_end_trading'] == True].index
me_dates = [d for d in etf_me if d >= start and d in hg_dates]
print(f"Monthly rebalance dates: {len(me_dates)} months, {me_dates[0].date()} ~ {me_dates[-1].date()}")

def gate(d):
    if d not in spy_px.index or d not in shy_px.index:
        return 'CLOSED', 0.30
    lb_d = spy_px.index[max(0, spy_px.index.get_loc(d) - LB)]
    spy_r = spy_px.loc[d] / spy_px.loc[lb_d] - 1 if spy_px.loc[lb_d] > 0 else 0
    shy_r = shy_px.loc[d] / shy_px.loc[lb_d] - 1 if shy_px.loc[lb_d] > 0 else 0
    exc = spy_r - shy_r
    if exc > 0: return 'OPEN', 0.80
    if exc > -0.02: return 'MID', 0.50
    return 'CLOSED', 0.30

def get_momentum(d, lb=STOCK_LB):
    """Get 6M momentum for all PIT S&P500 members at date d."""
    idx = hg_dates.get_loc(d)
    if idx < lb: return {}
    lb_d = hg_dates[idx - lb]
    moms = {}
    for tk in tickers:
        univ_col = f'{tk}_Universe_SP500'
        adj_col = f'{tk}_AdjClose'
        if univ_col not in hg.columns: continue
        # Check PIT membership
        if d in hg.index and hg.loc[d, univ_col] == 1:
            p_now = hg.loc[d, adj_col]
            p_lb = hg.loc[lb_d, adj_col] if lb_d in hg.index else np.nan
            if p_now > 0 and p_lb > 0 and not np.isnan(p_now) and not np.isnan(p_lb):
                moms[tk] = p_now / p_lb - 1
    return moms

# Strategies
class Strat:
    def __init__(self, name):
        self.name = name
        self.equity = 1_000_000.0
        self.weights = {}
        self.hold_days = {}
        self.rows = []

strats = {n: Strat(n) for n in ['BM5','BM5_MH20','BM5_MH20_CAP35','SPY_BH']}

print("Running monthly backtest...")
for mi in range(len(me_dates) - 1):
    d = me_dates[mi]
    d_next = me_dates[mi + 1]

    g_state, atk_cap = gate(d)
    moms = get_momentum(d)
    if not moms:
        # No data: all cash
        for s in strats.values():
            s.rows.append({'date': d_next, 'equity': s.equity, 'month_ret': 0.0})
        continue
    sorted_moms = sorted(moms.items(), key=lambda x: -x[1])
    top = [tk for tk, _ in sorted_moms[:TOP_K]]

    targets = {}
    # BM5: equal weight top K, no constraints
    w = 1.0 / TOP_K if top else 1.0
    targets['BM5'] = {tk: w for tk in top} if top else {'SHY': 1.0}

    # BM5_MH20: with min hold (monthly rebalance → hold_min doesn't bind at 20d < 21d month)
    targets['BM5_MH20'] = dict(targets['BM5'])

    # BM5_MH20_CAP35: with atk_cap + single_name_cap
    if top:
        w_each = min(atk_cap / TOP_K, SINGLE_CAP)
        cap_w = {tk: w_each for tk in top}
        eq_sum = sum(cap_w.values())
        cap_w['SHY'] = 1.0 - eq_sum
        targets['BM5_MH20_CAP35'] = cap_w
    else:
        targets['BM5_MH20_CAP35'] = {'SHY': 1.0}

    # SPY_BH
    targets['SPY_BH'] = {'SPY': 1.0}

    # Compute monthly return for each strategy
    for sname, s in strats.items():
        tw = targets[sname]
        port_ret = 0.0
        for tk, w in tw.items():
            adj_col = f'{tk}_AdjClose'
            # Try hg first, then etf
            p_now = p_next = np.nan
            if adj_col in hg.columns:
                if d in hg.index: p_now = hg.loc[d, adj_col]
                if d_next in hg.index: p_next = hg.loc[d_next, adj_col]
            elif adj_col in etf.columns:
                if d in etf.index: p_now = etf.loc[d, adj_col]
                if d_next in etf.index: p_next = etf.loc[d_next, adj_col]
            if p_now > 0 and p_next > 0 and not np.isnan(p_now) and not np.isnan(p_next):
                port_ret += w * (p_next / p_now - 1)
        s.equity *= (1.0 + port_ret)
        s.rows.append({'date': d_next, 'equity': s.equity, 'month_ret': port_ret})

    if mi % 20 == 0:
        print(f"  [{mi}/{len(me_dates)-1}] {d.date()} gate={g_state} top={top[:3]} bm5eq={strats['BM5'].equity:,.0f}")

# Summary
print("\n" + "="*90)
print(f"{'Strategy':25s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Sortino':>8s} {'WorstMo':>8s}")
print("="*90)
for sname, s in strats.items():
    df = pd.DataFrame(s.rows)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    df['peak'] = df['equity'].cummax()
    df['dd'] = df['equity'] / df['peak'] - 1
    n_months = len(df)
    years = max(n_months / 12, 0.01)
    cagr = (df['equity'].iloc[-1] / df['equity'].iloc[0]) ** (1/years) - 1
    maxdd = df['dd'].min()
    rets = df['month_ret']
    std = rets.std(ddof=0)
    sharpe = (rets.mean() / std * (12**0.5)) if std > 0 else 0
    neg = rets[rets < 0]
    semi_std = np.sqrt((neg**2).mean()) if len(neg) > 0 else 1e-9
    sortino = rets.mean() / semi_std * (12**0.5)
    worst = rets.min()
    print(f"{sname:25s} {cagr:>+7.1%} {maxdd:>+7.1%} {sharpe:>8.2f} {sortino:>8.2f} {worst:>+7.1%}")
    df.to_csv(OUT_DIR / f"{sname}_monthly.csv", index=False)
print("="*90)
print(f"Period: {me_dates[0].date()} ~ {me_dates[-1].date()} ({len(me_dates)} months, {len(me_dates)/12:.1f} years)")
