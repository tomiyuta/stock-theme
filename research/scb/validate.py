#!/usr/bin/env python3
"""SCB v0.1 — Validation: Shuffle Test + Beta Separation
Purpose: Determine if SCB alpha is real or artifact of sizing/beta."""
import pandas as pd
import numpy as np
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"

# === Load data ===
config = pd.read_csv(ROOT / "static_config.csv")
with open(ROOT / "driver_weights.json") as f:
    driver_weights = json.load(f)
prices = pd.read_parquet(ROOT / "prices.parquet")

TICKERS = config["ticker"].tolist()
DRIVER_COLS = [c for c in config.columns if c.startswith("driver_")]
bottleneck = config.set_index("ticker")["bottleneck_score"]
driver_exp = {}
for _, row in config.iterrows():
    driver_exp[row["ticker"]] = sum(row[dc] * driver_weights[dc] for dc in DRIVER_COLS if dc in driver_weights)
driver_exp = pd.Series(driver_exp)

ret_1m = prices / prices.shift(21) - 1
ret_3m = prices / prices.shift(63) - 1
vol_20d = prices.pct_change().rolling(20).std() * (252**0.5)

# Fetch SPY for beta separation
import yfinance as yf
spy = yf.download("SPY", start=prices.index.min(), end=prices.index.max(), auto_adjust=True, progress=False)["Close"]
if isinstance(spy, pd.DataFrame): spy = spy.iloc[:, 0]
spy_ret = spy.pct_change()

N_HOLD = 8
REBAL_FREQ = 21
N_SHUFFLE = 30  # 30 trials for statistical power

def compute_scores(date, shuffle_b=None, shuffle_d=None):
    r1m_row = ret_1m.loc[date] if date in ret_1m.index else pd.Series(dtype=float)
    r3m_row = ret_3m.loc[date] if date in ret_3m.index else pd.Series(dtype=float)
    vol_row = vol_20d.loc[date] if date in vol_20d.index else pd.Series(dtype=float)
    valid = []
    for tk in TICKERS:
        r1 = r1m_row.get(tk, np.nan)
        r3 = r3m_row.get(tk, np.nan)
        v = vol_row.get(tk, np.nan)
        if np.isnan(r1) or np.isnan(r3) or np.isnan(v): continue
        if r1 <= 0 or r3 <= 0: continue
        valid.append({"tk": tk, "r1": r1, "r3": r3, "v": v})
    if len(valid) < 2: return {}

    tks = [x["tk"] for x in valid]
    arr_1m = np.array([x["r1"] for x in valid])
    arr_3m = np.array([x["r3"] for x in valid])
    arr_vol = np.array([x["v"] for x in valid])
    def zs(a):
        s = a.std(); return (a - a.mean()) / s if s > 1e-9 else np.zeros_like(a)
    z1, z3, zv = zs(arr_1m), zs(arr_3m), zs(arr_vol)
    b_vals = np.array([bottleneck.get(tk, 0) for tk in tks]) if shuffle_b is None else shuffle_b[:len(tks)]
    d_vals = np.array([driver_exp.get(tk, 0) for tk in tks]) if shuffle_d is None else shuffle_d[:len(tks)]
    zb, zd = zs(b_vals), zs(d_vals)
    result = {}
    for i, tk in enumerate(tks):
        M = z1[i] + 0.8 * z3[i]
        result[tk] = {
            "scb": 1.0*M + 0.7*zb[i] + 0.5*zd[i] - 0.5*zv[i],
            "mom": 1.0*M - 0.5*zv[i],
            "M": M, "B": zb[i], "D": zd[i], "R": zv[i]
        }
    return result

def run_backtest(score_key, shuffle_mode=False, seed=None):
    """Run backtest with optional B/D shuffle."""
    trade_dates = prices.index.tolist()
    rebal_dates = trade_dates[63::REBAL_FREQ]
    equity = 1_000_000.0
    rows = []
    holdings_history = []

    for ri in range(len(rebal_dates) - 1):
        d = rebal_dates[ri]
        d_next = rebal_dates[ri + 1]
        d_idx = trade_dates.index(d)
        d_next_idx = trade_dates.index(d_next)
        period = trade_dates[d_idx:d_next_idx + 1]

        # Shuffle B and D if requested
        sb, sd = None, None
        if shuffle_mode:
            rng = np.random.RandomState(seed * 1000 + ri if seed else None)
            sb = rng.permutation(bottleneck.reindex(TICKERS).fillna(0).values)
            sd = rng.permutation(pd.Series(driver_exp).reindex(TICKERS).fillna(0).values)

        scores = compute_scores(d, shuffle_b=sb, shuffle_d=sd)
        if not scores:
            tw = {TICKERS[0]: 1.0 / len(TICKERS) for _ in TICKERS}
        elif score_key == "equal":
            r1m_row = ret_1m.loc[d] if d in ret_1m.index else pd.Series(dtype=float)
            picks = [tk for tk in TICKERS if r1m_row.get(tk, -1) > 0] or TICKERS
            tw = {tk: 1.0 / len(picks) for tk in picks}
        else:
            ranked = sorted(scores.items(), key=lambda x: -x[1][score_key])
            top = ranked[:N_HOLD]
            tw = {tk: 1.0 / len(top) for tk, _ in top}
            holdings_history.append(set(tw.keys()))

        for di in range(len(period) - 1):
            day, day1 = period[di], period[di + 1]
            pr = 0.0
            for tk, w in tw.items():
                if tk in prices.columns and day in prices.index and day1 in prices.index:
                    p0, p1 = prices.loc[day, tk], prices.loc[day1, tk]
                    if p0 > 0 and p1 > 0 and not np.isnan(p0) and not np.isnan(p1):
                        pr += w * (p1 / p0 - 1)
            equity *= (1.0 + pr)
            rows.append({"date": day1, "equity": equity, "ret": pr})

    df = pd.DataFrame(rows)
    if df.empty: return None
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date", keep="last")
    df["peak"] = df["equity"].cummax()
    df["dd"] = df["equity"] / df["peak"] - 1
    n = len(df); years = max(n / 252, 0.01)
    cagr = (df["equity"].iloc[-1] / df["equity"].iloc[0]) ** (1/years) - 1
    maxdd = df["dd"].min()
    std = df["ret"].std(ddof=0)
    sharpe = df["ret"].mean() / std * (252**0.5) if std > 0 else 0
    neg = df["ret"][df["ret"] < 0]
    sortino = df["ret"].mean() / np.sqrt((neg**2).mean()) * (252**0.5) if len(neg) > 0 else 0
    # Jaccard between consecutive rebalances
    jaccards = []
    for i in range(1, len(holdings_history)):
        a, b = holdings_history[i-1], holdings_history[i]
        if a and b: jaccards.append(len(a & b) / len(a | b))
    avg_jaccard = np.mean(jaccards) if jaccards else 0
    return {"cagr": cagr, "maxdd": maxdd, "sharpe": sharpe, "sortino": sortino,
            "jaccard": avg_jaccard, "daily_rets": df.set_index("date")["ret"]}

# ============================
# EXPERIMENT 1: SHUFFLE TEST
# ============================
print("=" * 100)
print("EXPERIMENT 1: SHUFFLE TEST (B/D randomized, M preserved)")
print("=" * 100)

res_scb = run_backtest("scb")
res_mom = run_backtest("mom")
res_eq = run_backtest("equal")

shuffle_results = []
for trial in range(N_SHUFFLE):
    r = run_backtest("scb", shuffle_mode=True, seed=trial)
    if r: shuffle_results.append(r)

shuf_sharpes = [r["sharpe"] for r in shuffle_results]
shuf_cagrs = [r["cagr"] for r in shuffle_results]
shuf_dds = [r["maxdd"] for r in shuffle_results]

print(f"\n{'Strategy':20s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Sortino':>8s} {'Jaccard':>8s}")
print("-" * 70)
for name, r in [("SCB", res_scb), ("MOM_ONLY", res_mom), ("EQUAL_ALL", res_eq)]:
    if r: print(f"{name:20s} {r['cagr']:>+7.1%} {r['maxdd']:>+7.1%} {r['sharpe']:>8.2f} {r['sortino']:>8.2f} {r['jaccard']:>7.2f}")
print(f"{'SHUFFLE_MEAN':20s} {np.mean(shuf_cagrs):>+7.1%} {np.mean(shuf_dds):>+7.1%} {np.mean(shuf_sharpes):>8.2f} {'':>8s} {'':>8s}")
print(f"{'SHUFFLE_STD':20s} {np.std(shuf_cagrs):>7.1%} {np.std(shuf_dds):>7.1%} {np.std(shuf_sharpes):>8.2f}")
print(f"{'SHUFFLE_MIN':20s} {min(shuf_cagrs):>+7.1%} {min(shuf_dds):>+7.1%} {min(shuf_sharpes):>8.2f}")
print(f"{'SHUFFLE_MAX':20s} {max(shuf_cagrs):>+7.1%} {max(shuf_dds):>+7.1%} {max(shuf_sharpes):>8.2f}")

# Statistical test
scb_sharpe = res_scb["sharpe"]
wins = sum(1 for s in shuf_sharpes if scb_sharpe > s)
print(f"\nSCB Sharpe ({scb_sharpe:.3f}) > Shuffle in {wins}/{N_SHUFFLE} trials ({wins/N_SHUFFLE:.0%})")
print(f"SCB - Shuffle_mean = {scb_sharpe - np.mean(shuf_sharpes):+.3f} Sharpe")
z_score = (scb_sharpe - np.mean(shuf_sharpes)) / np.std(shuf_sharpes) if np.std(shuf_sharpes) > 0 else 0
print(f"Z-score: {z_score:.2f} (>1.64 = significant at 5%)")

# ============================
# EXPERIMENT 2: BETA SEPARATION
# ============================
print("\n" + "=" * 100)
print("EXPERIMENT 2: BETA SEPARATION (SPY regression)")
print("=" * 100)

def compute_alpha_stats(strat_rets, spy_daily, label):
    """Regress strategy returns on SPY, extract alpha."""
    aligned = pd.DataFrame({"strat": strat_rets, "spy": spy_daily}).dropna()
    if len(aligned) < 30: return None
    s, m = aligned["strat"].values, aligned["spy"].values
    beta = np.cov(s, m)[0, 1] / np.var(m) if np.var(m) > 0 else 1.0
    alpha_series = s - beta * m
    alpha_ann = np.mean(alpha_series) * 252
    alpha_std = np.std(alpha_series, ddof=0)
    alpha_sharpe = np.mean(alpha_series) / alpha_std * (252**0.5) if alpha_std > 0 else 0
    alpha_cum = np.cumsum(alpha_series)
    alpha_maxdd = np.min(alpha_cum - np.maximum.accumulate(alpha_cum))
    return {"label": label, "beta": beta, "alpha_ann": alpha_ann,
            "alpha_sharpe": alpha_sharpe, "alpha_maxdd": alpha_maxdd}

print(f"\n{'Strategy':20s} {'β(SPY)':>8s} {'α(ann)':>8s} {'Sh(α)':>8s} {'DD(α)':>8s}")
print("-" * 60)
for name, r in [("SCB", res_scb), ("MOM_ONLY", res_mom), ("EQUAL_ALL", res_eq)]:
    if r:
        a = compute_alpha_stats(r["daily_rets"], spy_ret, name)
        if a: print(f"{name:20s} {a['beta']:>8.2f} {a['alpha_ann']:>+7.1%} {a['alpha_sharpe']:>8.2f} {a['alpha_maxdd']:>+7.1%}")

# Shuffle alpha distribution
shuf_alpha_sharpes = []
for sr in shuffle_results:
    a = compute_alpha_stats(sr["daily_rets"], spy_ret, "shuf")
    if a: shuf_alpha_sharpes.append(a["alpha_sharpe"])

if shuf_alpha_sharpes:
    scb_alpha = compute_alpha_stats(res_scb["daily_rets"], spy_ret, "SCB")
    print(f"\n{'SHUFFLE_α_MEAN':20s} {'':>8s} {'':>8s} {np.mean(shuf_alpha_sharpes):>8.2f}")
    print(f"{'SHUFFLE_α_STD':20s} {'':>8s} {'':>8s} {np.std(shuf_alpha_sharpes):>8.2f}")
    if scb_alpha:
        z_alpha = (scb_alpha["alpha_sharpe"] - np.mean(shuf_alpha_sharpes)) / np.std(shuf_alpha_sharpes) if np.std(shuf_alpha_sharpes) > 0 else 0
        print(f"\nα Sharpe: SCB={scb_alpha['alpha_sharpe']:.3f} vs Shuffle_mean={np.mean(shuf_alpha_sharpes):.3f}")
        print(f"Z-score(α): {z_alpha:.2f} (>1.64 = significant at 5%)")

# ============================
# EXPERIMENT 3: CAUSALITY CHECK
# ============================
print("\n" + "=" * 100)
print("EXPERIMENT 3: CAUSALITY — N-fixed comparison + overlap")
print("=" * 100)

# Both SCB and MOM select N=8 stocks. Compare overlap.
trade_dates = prices.index.tolist()
rebal_dates = trade_dates[63::REBAL_FREQ]
scb_picks_all, mom_picks_all = [], []
for d in rebal_dates[:-1]:
    sc = compute_scores(d)
    if not sc: continue
    scb_ranked = sorted(sc.items(), key=lambda x: -x[1]["scb"])[:N_HOLD]
    mom_ranked = sorted(sc.items(), key=lambda x: -x[1]["mom"])[:N_HOLD]
    scb_set = set(tk for tk, _ in scb_ranked)
    mom_set = set(tk for tk, _ in mom_ranked)
    scb_picks_all.append(scb_set)
    mom_picks_all.append(mom_set)

overlaps = [len(a & b) / len(a | b) for a, b in zip(scb_picks_all, mom_picks_all) if a and b]
unique_to_scb = []
for a, b in zip(scb_picks_all, mom_picks_all):
    unique_to_scb.extend(a - b)

from collections import Counter
scb_unique_counts = Counter(unique_to_scb)
print(f"\nSCB vs MOM overlap (Jaccard): mean={np.mean(overlaps):.2f} min={min(overlaps):.2f} max={max(overlaps):.2f}")
print(f"Tickers unique to SCB (not in MOM top-8):")
for tk, cnt in scb_unique_counts.most_common(10):
    bn = bottleneck.get(tk, 0)
    de = driver_exp.get(tk, 0)
    print(f"  {tk:8s} appeared {cnt:3d}x  BN={bn:.1f} DrvExp={de:.1f}")

# ============================
# FINAL VERDICT
# ============================
print("\n" + "=" * 100)
print("VERDICT")
print("=" * 100)
if z_score > 1.64:
    print("✅ SHUFFLE TEST: PASS — SCB score provides statistically significant improvement")
else:
    print(f"❌ SHUFFLE TEST: FAIL — Z={z_score:.2f} < 1.64 (not significant at 5%)")
if scb_alpha and z_alpha > 1.64:
    print("✅ ALPHA TEST: PASS — SCB alpha (net of SPY beta) is significant")
elif scb_alpha:
    print(f"❌ ALPHA TEST: FAIL — Z(α)={z_alpha:.2f} < 1.64")
if np.mean(overlaps) < 0.7:
    print(f"✅ DIFFERENTIATION: SCB selects different stocks from MOM (Jaccard={np.mean(overlaps):.2f})")
else:
    print(f"⚠️  LOW DIFFERENTIATION: SCB ≈ MOM (Jaccard={np.mean(overlaps):.2f})")
