#!/usr/bin/env python3
"""SCB v0.1 — Structural-Catalyst Bottleneck Strategy Backtest
Separate research track from PRISM. Do NOT integrate into PRISM."""
import pandas as pd
import numpy as np
import json, csv
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

# === Load config ===
config = pd.read_csv(ROOT / "static_config.csv")
with open(ROOT / "driver_weights.json") as f:
    driver_weights = json.load(f)

TICKERS = config["ticker"].tolist()
DRIVER_COLS = [c for c in config.columns if c.startswith("driver_")]
print(f"Universe: {len(TICKERS)} tickers")
print(f"Drivers: {len(DRIVER_COLS)}")

# === Fetch price data ===
import yfinance as yf

print("Fetching price data...")
start_date = "2024-01-01"
end_date = "2026-04-08"
raw = yf.download(TICKERS, start=start_date, end=end_date, auto_adjust=True, progress=False)
prices = raw["Close"].dropna(how="all")
prices.to_parquet(ROOT / "prices.parquet")
print(f"Prices: {prices.shape}, {prices.index.min().date()} ~ {prices.index.max().date()}")

# === Precompute returns ===
ret_1m = prices / prices.shift(21) - 1
ret_3m = prices / prices.shift(63) - 1
vol_20d = prices.pct_change().rolling(20).std() * (252**0.5)

# === Build structural scores (static) ===
bottleneck = config.set_index("ticker")["bottleneck_score"]
driver_exposure = {}
for _, row in config.iterrows():
    tk = row["ticker"]
    d_score = sum(row[dc] * driver_weights[dc] for dc in DRIVER_COLS if dc in driver_weights)
    driver_exposure[tk] = d_score
driver_exp = pd.Series(driver_exposure)
print(f"\nBottleneck range: {bottleneck.min():.1f} ~ {bottleneck.max():.1f}")
print(f"Driver exposure range: {driver_exp.min():.2f} ~ {driver_exp.max():.2f}")

# === Score function ===
def compute_scores(date, ret1m, ret3m, vol):
    """Compute SCB scores for all tickers at a given date."""
    scores = {}
    valid_tickers = []
    for tk in TICKERS:
        r1 = ret1m.get(tk, np.nan)
        r3 = ret3m.get(tk, np.nan)
        v = vol.get(tk, np.nan)
        if np.isnan(r1) or np.isnan(r3) or np.isnan(v):
            continue
        if r1 <= 0 or r3 <= 0:  # momentum filter
            continue
        valid_tickers.append(tk)
        scores[tk] = {"ret_1m": r1, "ret_3m": r3, "vol": v}

    if len(valid_tickers) < 2:
        return {}
    # Z-scores across valid tickers
    vals_1m = np.array([scores[tk]["ret_1m"] for tk in valid_tickers])
    vals_3m = np.array([scores[tk]["ret_3m"] for tk in valid_tickers])
    vals_vol = np.array([scores[tk]["vol"] for tk in valid_tickers])
    def zscore(arr):
        s = arr.std()
        return (arr - arr.mean()) / s if s > 1e-9 else np.zeros_like(arr)
    z1m = zscore(vals_1m)
    z3m = zscore(vals_3m)
    zvol = zscore(vals_vol)
    # Normalize B and D to z-scale
    b_vals = np.array([bottleneck.get(tk, 0) for tk in valid_tickers])
    d_vals = np.array([driver_exp.get(tk, 0) for tk in valid_tickers])
    zb = zscore(b_vals)
    zd = zscore(d_vals)
    result = {}
    for i, tk in enumerate(valid_tickers):
        M = z1m[i] + 0.8 * z3m[i]
        B = zb[i]
        D = zd[i]
        R = zvol[i]
        scb = 1.0 * M + 0.7 * B + 0.5 * D - 0.5 * R
        mom_only = 1.0 * M - 0.5 * R  # pure momentum baseline
        result[tk] = {"scb": scb, "mom": mom_only, "M": M, "B": B, "D": D, "R": R}
    return result

# === Backtest ===
N_HOLD = 8
MIN_HOLD = 10
MAX_HOLD = 60
DD_STOP = -0.12
REBAL_FREQ = 21  # monthly rebalance

class Portfolio:
    def __init__(self, name, score_key):
        self.name = name
        self.score_key = score_key
        self.equity = 1_000_000.0
        self.holdings = {}  # ticker -> {"weight", "entry_idx", "entry_price"}
        self.rows = []

strats = {
    "SCB": Portfolio("SCB", "scb"),
    "MOM_ONLY": Portfolio("MOM_ONLY", "mom"),
    "EQUAL_ALL": Portfolio("EQUAL_ALL", None),  # equal weight all 17
}

trade_dates = prices.index.tolist()
rebal_dates = trade_dates[63::REBAL_FREQ]  # start after 3M lookback, monthly
print(f"\nBacktest: {len(trade_dates)} days, {len(rebal_dates)} rebalance points")

for ri in range(len(rebal_dates) - 1):
    d = rebal_dates[ri]
    d_next = rebal_dates[ri + 1]
    # Get holding period daily dates
    d_idx = trade_dates.index(d)
    d_next_idx = trade_dates.index(d_next)
    period_dates = trade_dates[d_idx:d_next_idx + 1]

    r1m_row = ret_1m.loc[d] if d in ret_1m.index else pd.Series(dtype=float)
    r3m_row = ret_3m.loc[d] if d in ret_3m.index else pd.Series(dtype=float)
    vol_row = vol_20d.loc[d] if d in vol_20d.index else pd.Series(dtype=float)
    scores = compute_scores(d, r1m_row, r3m_row, vol_row)

    for sname, port in strats.items():
        if sname == "EQUAL_ALL":
            # Equal weight all tickers with positive 1M
            picks = [tk for tk in TICKERS if r1m_row.get(tk, -1) > 0]
            if not picks: picks = TICKERS
            tw = {tk: 1.0 / len(picks) for tk in picks}
        else:
            if not scores:
                tw = {}
            else:
                ranked = sorted(scores.items(), key=lambda x: -x[1][port.score_key])
                top = ranked[:N_HOLD]
                tw = {tk: 1.0 / len(top) for tk, _ in top}

        # Daily mark-to-market for the holding period
        for di in range(len(period_dates) - 1):
            day = period_dates[di]
            day_next = period_dates[di + 1]
            port_ret = 0.0
            for tk, w in tw.items():
                if tk in prices.columns and day in prices.index and day_next in prices.index:
                    p0 = prices.loc[day, tk]
                    p1 = prices.loc[day_next, tk]
                    if p0 > 0 and p1 > 0 and not np.isnan(p0) and not np.isnan(p1):
                        port_ret += w * (p1 / p0 - 1)
            port.equity *= (1.0 + port_ret)
            port.rows.append({"date": day_next, "equity": port.equity, "ret": port_ret})

    if ri % 5 == 0:
        scb_eq = strats["SCB"].equity
        mom_eq = strats["MOM_ONLY"].equity
        print(f"  [{ri}/{len(rebal_dates)-1}] {d.date()} SCB=${scb_eq:,.0f} MOM=${mom_eq:,.0f}")

# === Summary ===
print("\n" + "="*100)
print(f"{'Strategy':20s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Sortino':>8s} {'WorstMo':>8s} {'HitRate':>8s}")
print("="*100)

for sname, port in strats.items():
    df = pd.DataFrame(port.rows)
    if df.empty:
        print(f"{sname:20s} NO DATA"); continue
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates(subset="date", keep="last")
    df["peak"] = df["equity"].cummax()
    df["dd"] = df["equity"] / df["peak"] - 1
    n = len(df)
    years = max(n / 252, 0.01)
    cagr = (df["equity"].iloc[-1] / df["equity"].iloc[0]) ** (1 / years) - 1
    maxdd = df["dd"].min()
    std = df["ret"].std(ddof=0)
    sharpe = (df["ret"].mean() / std * (252**0.5)) if std > 0 else 0
    neg = df["ret"][df["ret"] < 0]
    semi = np.sqrt((neg**2).mean()) if len(neg) > 0 else 1e-9
    sortino = df["ret"].mean() / semi * (252**0.5)
    df["ym"] = df["date"].dt.to_period("M")
    monthly = df.groupby("ym")["ret"].apply(lambda x: (1 + x).prod() - 1)
    worst_mo = monthly.min()
    hit = (monthly > 0).sum() / len(monthly) if len(monthly) > 0 else 0
    print(f"{sname:20s} {cagr:>+7.1%} {maxdd:>+7.1%} {sharpe:>8.2f} {sortino:>8.2f} {worst_mo:>+7.1%} {hit:>7.0%}")
    df.to_csv(OUT / f"{sname}_daily.csv", index=False)

print("="*100)
print(f"Period: {rebal_dates[0].date()} ~ {rebal_dates[-1].date()}")
print(f"N_HOLD={N_HOLD}, REBAL={REBAL_FREQ}d, Universe={len(TICKERS)}")
print(f"\nSaved to {OUT}")

# === Latest SCB Score Snapshot ===
latest = rebal_dates[-1]
r1m_latest = ret_1m.loc[latest] if latest in ret_1m.index else pd.Series(dtype=float)
r3m_latest = ret_3m.loc[latest] if latest in ret_3m.index else pd.Series(dtype=float)
vol_latest = vol_20d.loc[latest] if latest in vol_20d.index else pd.Series(dtype=float)
sc = compute_scores(latest, r1m_latest, r3m_latest, vol_latest)
if sc:
    print(f"\n{'='*80}")
    print(f"SCB Score Snapshot @ {latest.date()}")
    print(f"{'Ticker':8s} {'SCB':>7s} {'MOM':>7s} {'M':>6s} {'B':>6s} {'D':>6s} {'R':>6s} {'BN':>4s} {'DrvExp':>6s}")
    print(f"{'='*80}")
    for tk, s in sorted(sc.items(), key=lambda x: -x[1]["scb"]):
        bn = bottleneck.get(tk, 0)
        de = driver_exp.get(tk, 0)
        print(f"{tk:8s} {s['scb']:>+6.2f} {s['mom']:>+6.2f} {s['M']:>+5.2f} {s['B']:>+5.2f} {s['D']:>+5.2f} {s['R']:>+5.2f} {bn:>4.1f} {de:>5.2f}")
