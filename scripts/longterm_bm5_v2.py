#!/usr/bin/env python3
"""Phase 2 v2: BM5 (direct stock momentum) × MH20 × CAP35 using new Norgate PIT dataset.
Replaces external dependencies (HolyGrail_v4, norgate_full_bundle) with self-contained parquets.
"""
import pandas as pd
import numpy as np
from pathlib import Path

SCB = Path("/Users/yutatomi/Downloads/stock-theme/research/scb")
OUT_DIR = Path("/Users/yutatomi/Downloads/stock-theme/output/longterm")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# === Load new PIT datasets (long format) ===
print("Loading PIT datasets...")
prices = pd.read_parquet(SCB / "norgate_us_prices.parquet", columns=["date","ticker","close"])
membership = pd.read_parquet(SCB / "norgate_index_membership.parquet",
                             columns=["date","ticker","in_sp500","on_major_exchange"])
etf_prices = pd.read_parquet(SCB / "norgate_etf_prices.parquet", columns=["date","ticker","close"])
metadata = pd.read_parquet(SCB / "norgate_us_metadata.parquet", columns=["ticker","gics_sector","is_common_stock"])

print(f"  Prices: {len(prices):,} rows, {prices.ticker.nunique():,} tickers")
print(f"  Membership: {len(membership):,} rows")
print(f"  ETFs: {etf_prices.ticker.nunique()} tickers")

# === Build pivot tables for fast lookup ===
print("Building lookup tables...")

# Stock prices: pivot to wide for fast access
# Filter to common stocks on major exchanges with SP500 membership at any point
sp500_ever = set(membership[membership.in_sp500 == 1].ticker.unique())
print(f"  SP500 ever-members: {len(sp500_ever)}")

# Only load prices for SP500 ever-members (saves memory)
stk_prices = prices[prices.ticker.isin(sp500_ever)].copy()
stk_prices['ticker'] = stk_prices['ticker'].astype(str)
stk_wide = stk_prices.pivot_table(index="date", columns="ticker", values="close", aggfunc="first")
stk_wide = stk_wide.sort_index()
print(f"  Stock price matrix: {stk_wide.shape}")

# SP500 membership: pivot to wide
sp500_mem = membership[membership.ticker.isin(sp500_ever)][["date","ticker","in_sp500"]].copy()
sp500_mem['ticker'] = sp500_mem['ticker'].astype(str)
sp500_wide = sp500_mem.pivot_table(index="date", columns="ticker", values="in_sp500", aggfunc="first").fillna(0)
sp500_wide = sp500_wide.sort_index().reindex(stk_wide.index, method='ffill').fillna(0)

# ETF prices: pivot
etf_prices['ticker'] = etf_prices['ticker'].astype(str)
etf_wide = etf_prices.pivot_table(index="date", columns="ticker", values="close", aggfunc="first").sort_index()

spy_px = etf_wide["SPY"].dropna()
shy_px = etf_wide["SHY"].dropna()
all_dates = stk_wide.index

# === Parameters (matching original longterm_bm5.py) ===
LB = 63          # 3M lookback for K-gate
STOCK_LB = 126   # 6M momentum for stock selection
TOP_K = 5
HOLD_MIN = 20
SECTOR_CAP = 0.35
SINGLE_CAP = 0.08

# Month-end rebalance dates (last trading day of each month)
dates_series = pd.Series(all_dates)
month_groups = dates_series.groupby([dates_series.dt.year, dates_series.dt.month])
me_dates_all = [g.iloc[-1] for _, g in month_groups]
start = pd.Timestamp('2002-08-01')
me_dates = [d for d in me_dates_all if d >= start and d in spy_px.index and d in shy_px.index]
print(f"Monthly rebalance dates: {len(me_dates)} months, {me_dates[0].date()} ~ {me_dates[-1].date()}")

def gate(d):
    if d not in spy_px.index or d not in shy_px.index:
        return 'CLOSED', 0.30
    loc_spy = spy_px.index.get_loc(d)
    loc_shy = shy_px.index.get_loc(d)
    if loc_spy < LB or loc_shy < LB: return 'CLOSED', 0.30
    lb_spy = spy_px.index[loc_spy - LB]
    lb_shy = shy_px.index[loc_shy - LB]
    spy_r = spy_px.loc[d] / spy_px.loc[lb_spy] - 1
    shy_r = shy_px.loc[d] / shy_px.loc[lb_shy] - 1
    exc = spy_r - shy_r
    if exc > 0: return 'OPEN', 0.80
    if exc > -0.02: return 'MID', 0.50
    return 'CLOSED', 0.30

def get_momentum(d, lb=STOCK_LB):
    """Get 6M momentum for all PIT S&P500 members at date d."""
    loc = stk_wide.index.get_loc(d) if d in stk_wide.index else -1
    if loc < lb: return {}
    lb_d = stk_wide.index[loc - lb]
    
    # Current PIT S&P500 members
    if d not in sp500_wide.index: return {}
    members = sp500_wide.loc[d]
    member_tks = members[members == 1].index.tolist()
    
    moms = {}
    for tk in member_tks:
        if tk not in stk_wide.columns: continue
        p_now = stk_wide.loc[d, tk]
        p_lb = stk_wide.loc[lb_d, tk] if lb_d in stk_wide.index else np.nan
        if p_now > 0 and p_lb > 0 and not np.isnan(p_now) and not np.isnan(p_lb):
            moms[tk] = p_now / p_lb - 1
    return moms

class Strat:
    def __init__(self, name):
        self.name = name
        self.equity = 1_000_000.0
        self.rows = []

strats = {n: Strat(n) for n in ['BM5','BM5_MH20','BM5_MH20_CAP35','SPY_BH']}

print("Running monthly backtest...")
for mi in range(len(me_dates) - 1):
    d = me_dates[mi]
    d_next = me_dates[mi + 1]
    g_state, atk_cap = gate(d)
    moms = get_momentum(d)
    
    if not moms:
        for s in strats.values():
            s.rows.append({'date': d_next, 'equity': s.equity, 'month_ret': 0.0})
        continue
    
    sorted_moms = sorted(moms.items(), key=lambda x: -x[1])
    top = [tk for tk, _ in sorted_moms[:TOP_K]]
    
    targets = {}
    w = 1.0 / TOP_K if top else 1.0
    targets['BM5'] = {tk: w for tk in top} if top else {'SHY': 1.0}
    targets['BM5_MH20'] = dict(targets['BM5'])
    
    if top:
        w_each = min(atk_cap / TOP_K, SINGLE_CAP)
        cap_w = {tk: w_each for tk in top}
        cap_w['SHY'] = 1.0 - sum(cap_w.values())
        targets['BM5_MH20_CAP35'] = cap_w
    else:
        targets['BM5_MH20_CAP35'] = {'SHY': 1.0}
    targets['SPY_BH'] = {'SPY': 1.0}
    
    for sname, s in strats.items():
        tw = targets[sname]
        port_ret = 0.0
        for tk, w in tw.items():
            p_now = p_next = np.nan
            if tk in stk_wide.columns:
                if d in stk_wide.index: p_now = stk_wide.loc[d, tk]
                if d_next in stk_wide.index: p_next = stk_wide.loc[d_next, tk]
            if (np.isnan(p_now) or np.isnan(p_next)) and tk in etf_wide.columns:
                if d in etf_wide.index: p_now = etf_wide.loc[d, tk]
                if d_next in etf_wide.index: p_next = etf_wide.loc[d_next, tk]
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
