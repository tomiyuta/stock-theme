#!/usr/bin/env python3
"""SCB v0.2 — Zukai-driven, 25-theme cross-sector validation.
All B/D scores derived from zukai exposure data (no manual scores).
"""
import pandas as pd
import numpy as np
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

# === Load zukai-derived B/D scores ===
with open(ROOT / "v2_scores.json") as f:
    v2 = json.load(f)
B_dict = v2["B"]
D_dict = v2["D"]
TICKERS = sorted(B_dict.keys())
print(f"Universe: {len(TICKERS)} tickers from zukai data")

# === Fetch price data ===
import yfinance as yf
print("Fetching prices...")
start_date = "2024-01-01"
end_date = "2026-04-08"
raw = yf.download(TICKERS, start=start_date, end=end_date, auto_adjust=True, progress=False)
prices = raw["Close"].dropna(how="all")
if isinstance(prices, pd.DataFrame):
    valid_tickers = [tk for tk in TICKERS if tk in prices.columns]
else:
    valid_tickers = TICKERS
prices = prices[valid_tickers]
print(f"Prices: {prices.shape}, {prices.index[0].date()} ~ {prices.index[-1].date()}")
print(f"Valid tickers: {len(valid_tickers)}")

# Precompute
ret_1m = prices / prices.shift(21) - 1
ret_3m = prices / prices.shift(63) - 1
vol_20d = prices.pct_change().rolling(20).std() * (252**0.5)

# SPY for beta separation
spy_raw = yf.download("SPY", start=start_date, end=end_date, auto_adjust=True, progress=False)["Close"]
if isinstance(spy_raw, pd.DataFrame): spy_raw = spy_raw.iloc[:,0]
spy_ret = spy_raw.pct_change()

# === Score function ===
N_HOLD = 10
REBAL_FREQ = 21
N_SHUFFLE = 30

def compute_scores(date, shuffle_b=None, shuffle_d=None):
    r1m = ret_1m.loc[date] if date in ret_1m.index else pd.Series(dtype=float)
    r3m = ret_3m.loc[date] if date in ret_3m.index else pd.Series(dtype=float)
    vol = vol_20d.loc[date] if date in vol_20d.index else pd.Series(dtype=float)
    valid = []
    for tk in valid_tickers:
        r1,r3,v = r1m.get(tk,np.nan), r3m.get(tk,np.nan), vol.get(tk,np.nan)
        if np.isnan(r1) or np.isnan(r3) or np.isnan(v): continue
        if r1 <= 0 or r3 <= 0: continue
        valid.append({"tk":tk,"r1":r1,"r3":r3,"v":v})
    if len(valid) < 3: return {}
    tks = [x["tk"] for x in valid]
    def zs(a):
        s=a.std(); return (a-a.mean())/s if s>1e-9 else np.zeros_like(a)
    z1 = zs(np.array([x["r1"] for x in valid]))
    z3 = zs(np.array([x["r3"] for x in valid]))
    zv = zs(np.array([x["v"] for x in valid]))
    if shuffle_b is not None:
        b_vals = shuffle_b[:len(tks)]
        d_vals = shuffle_d[:len(tks)]
    else:
        b_vals = np.array([B_dict.get(tk,0) for tk in tks])
        d_vals = np.array([D_dict.get(tk,0) for tk in tks])
    zb = zs(b_vals)
    zd = zs(d_vals)
    result = {}
    for i,tk in enumerate(tks):
        M = z1[i] + 0.8*z3[i]
        result[tk] = {"scb":1.0*M+0.7*zb[i]+0.5*zd[i]-0.5*zv[i],
                       "mom":1.0*M-0.5*zv[i], "M":M}
    return result

def run_backtest(score_key, shuffle_mode=False, seed=None):
    trade_dates = prices.index.tolist()
    rebal_dates = trade_dates[63::REBAL_FREQ]
    equity = 1_000_000.0
    rows_out = []
    holdings_hist = []
    for ri in range(len(rebal_dates)-1):
        d = rebal_dates[ri]; d_next = rebal_dates[ri+1]
        di = trade_dates.index(d); dni = trade_dates.index(d_next)
        period = trade_dates[di:dni+1]
        sb,sd = None,None
        if shuffle_mode:
            rng = np.random.RandomState(seed*1000+ri if seed else None)
            all_b = np.array([B_dict.get(tk,0) for tk in valid_tickers])
            all_d = np.array([D_dict.get(tk,0) for tk in valid_tickers])
            sb = rng.permutation(all_b)
            sd = rng.permutation(all_d)
        scores = compute_scores(d, shuffle_b=sb, shuffle_d=sd)
        if not scores:
            tw = {}
        elif score_key == "equal":
            r1m_row = ret_1m.loc[d] if d in ret_1m.index else pd.Series(dtype=float)
            picks = [tk for tk in valid_tickers if r1m_row.get(tk,-1)>0] or valid_tickers
            tw = {tk:1.0/len(picks) for tk in picks}
        else:
            ranked = sorted(scores.items(), key=lambda x:-x[1][score_key])[:N_HOLD]
            tw = {tk:1.0/len(ranked) for tk,_ in ranked}
            holdings_hist.append(set(tw.keys()))
        for pi in range(len(period)-1):
            day,day1 = period[pi],period[pi+1]
            pr = sum(tw.get(tk,0)*(prices.loc[day1,tk]/prices.loc[day,tk]-1)
                     for tk in tw if tk in prices.columns
                     and day in prices.index and day1 in prices.index
                     and prices.loc[day,tk]>0 and not np.isnan(prices.loc[day,tk])
                     and not np.isnan(prices.loc[day1,tk]))
            equity *= (1+pr)
            rows_out.append({"date":day1,"equity":equity,"ret":pr})
    df = pd.DataFrame(rows_out)
    if df.empty: return None
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date",keep="last")
    n=len(df); yrs=max(n/252,0.01)
    cagr=(df["equity"].iloc[-1]/df["equity"].iloc[0])**(1/yrs)-1
    maxdd=(df["equity"]/df["equity"].cummax()-1).min()
    std=df["ret"].std(ddof=0)
    sharpe=df["ret"].mean()/std*(252**0.5) if std>0 else 0
    neg=df["ret"][df["ret"]<0]
    sortino=df["ret"].mean()/np.sqrt((neg**2).mean())*(252**0.5) if len(neg)>0 else 0
    return {"cagr":cagr,"maxdd":maxdd,"sharpe":sharpe,"sortino":sortino,
            "daily_rets":df.set_index("date")["ret"],"holdings":holdings_hist}

# ==========================================
# RUN ALL EXPERIMENTS
# ==========================================
print("\n" + "="*80)
print("SCB v0.2 — Zukai-driven, 25-theme, 331 tickers")
print("="*80)

res_scb = run_backtest("scb")
res_mom = run_backtest("mom")
res_eq = run_backtest("equal")
print("Main strategies done.", flush=True)

shuffle_results = []
for trial in range(N_SHUFFLE):
    r = run_backtest("scb", shuffle_mode=True, seed=trial)
    if r: shuffle_results.append(r)
    if (trial+1)%10==0: print(f"  Shuffle {trial+1}/{N_SHUFFLE}", flush=True)

# === EXPERIMENT 1: SHUFFLE TEST ===
print("\n" + "="*80)
print("EXP 1: SHUFFLE TEST")
print("="*80)
shuf_sh = [r["sharpe"] for r in shuffle_results]
shuf_cagr = [r["cagr"] for r in shuffle_results]
scb_sh = res_scb["sharpe"]
wins = sum(1 for s in shuf_sh if scb_sh > s)
z1 = (scb_sh - np.mean(shuf_sh))/np.std(shuf_sh) if np.std(shuf_sh)>0 else 0

print(f"{'Strategy':20s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Sortino':>8s}")
print("-"*60)
for nm,r in [("SCB_v2",res_scb),("MOM_ONLY",res_mom),("EQUAL_ALL",res_eq)]:
    if r: print(f"{nm:20s} {r['cagr']:>+7.1%} {r['maxdd']:>+7.1%} {r['sharpe']:>8.2f} {r['sortino']:>8.2f}")
print(f"{'SHUFFLE_MEAN':20s} {np.mean(shuf_cagr):>+7.1%} {'':>8s} {np.mean(shuf_sh):>8.2f}")
print(f"{'SHUFFLE_STD':20s} {np.std(shuf_cagr):>7.1%} {'':>8s} {np.std(shuf_sh):>8.2f}")
print(f"\nSCB > Shuffle: {wins}/{N_SHUFFLE} ({wins/N_SHUFFLE:.0%})")
print(f"Z-score: {z1:.2f} (>1.64 required)")

# === EXPERIMENT 2: BETA SEPARATION ===
print("\n" + "="*80)
print("EXP 2: BETA SEPARATION (SPY regression)")
print("="*80)
def alpha_stats(strat_rets, label):
    aligned = pd.DataFrame({"s":strat_rets,"spy":spy_ret}).dropna()
    if len(aligned)<30: return None
    s,m = aligned["s"].values, aligned["spy"].values
    beta = np.cov(s,m)[0,1]/np.var(m) if np.var(m)>0 else 1
    alpha = s - beta*m
    ash = np.mean(alpha)/np.std(alpha,ddof=0)*(252**0.5) if np.std(alpha)>0 else 0
    return {"label":label,"beta":beta,"alpha_ann":np.mean(alpha)*252,"alpha_sharpe":ash}

print(f"{'Strategy':20s} {'β(SPY)':>8s} {'α(ann)':>8s} {'Sh(α)':>8s}")
print("-"*50)
for nm,r in [("SCB_v2",res_scb),("MOM_ONLY",res_mom),("EQUAL_ALL",res_eq)]:
    if r:
        a=alpha_stats(r["daily_rets"],nm)
        if a: print(f"{nm:20s} {a['beta']:>8.2f} {a['alpha_ann']:>+7.1%} {a['alpha_sharpe']:>8.2f}")

shuf_ash = [alpha_stats(r["daily_rets"],"sh")["alpha_sharpe"] for r in shuffle_results if alpha_stats(r["daily_rets"],"sh")]
scb_a = alpha_stats(res_scb["daily_rets"],"SCB")
mom_a = alpha_stats(res_mom["daily_rets"],"MOM")
z2 = (scb_a["alpha_sharpe"]-np.mean(shuf_ash))/np.std(shuf_ash) if np.std(shuf_ash)>0 else 0
print(f"\nα Sharpe: SCB={scb_a['alpha_sharpe']:.3f} vs MOM={mom_a['alpha_sharpe']:.3f} (diff={scb_a['alpha_sharpe']-mom_a['alpha_sharpe']:+.3f})")
print(f"α Sharpe: SCB={scb_a['alpha_sharpe']:.3f} vs Shuffle_mean={np.mean(shuf_ash):.3f}")
print(f"Z-score(α): {z2:.2f}")

# === EXPERIMENT 3: PER-THEME WIN RATE ===
print("\n" + "="*80)
print("EXP 3: PER-THEME WIN RATE (SCB vs MOM)")
print("="*80)

# Load theme→ticker mapping from zukai data
D = Path(ROOT).parent / "stock_themes_data"
theme_tickers = {}
RNAMES = {"optical":"光接続","server":"AIサーバー","semi_manufacturing":"半導体製造",
    "semi_equip":"半導体装置","agentic_ai":"エージェントAI","compute_providors":"AIコンピュート",
    "dc_infra":"DCインフラ","dc_power":"DC電力","dc_server":"DCサーバー","natgas":"天然ガス",
    "oil_us":"石油US","oil_global":"石油GL","nuclear":"原子力","pe_credit":"PEクレジット",
    "software_enterprise":"エンタプライズSW","software_specialized":"業種特化SW",
    "software_cyber":"サイバー","space":"宇宙","humanoids":"ヒューマノイド","agg":"農業",
    "coal":"石炭","rare_earth":"レアアース","construction":"インフラ建設",
    "finance_consumer":"消費者金融","travel":"旅行"}

for f in sorted(D.glob("zukai_tickers_*.json")):
    rid = f.stem.replace("zukai_tickers_","")
    td = json.load(open(f))
    tks = [item["ticker"] for item in td.get("items",[]) if item.get("ticker") and item["ticker"] in set(valid_tickers)]
    if tks: theme_tickers[rid] = tks

# Per-theme: compare SCB vs MOM returns using the full backtest holdings
# Simple approach: for each theme, calculate avg return of its tickers when held by SCB vs MOM
trade_dates = prices.index.tolist()
rebal_dates = trade_dates[63::REBAL_FREQ]

theme_wins = {}
for rid, tks in theme_tickers.items():
    if len(tks) < 3: continue
    scb_ret_sum = 0; mom_ret_sum = 0
    n_periods = 0
    for ri in range(len(rebal_dates)-1):
        d = rebal_dates[ri]; d_next = rebal_dates[ri+1]
        scores = compute_scores(d)
        if not scores: continue
        # Theme tickers that have scores
        theme_scored = {tk:scores[tk] for tk in tks if tk in scores}
        if len(theme_scored) < 2: continue
        # Top by SCB vs top by MOM
        scb_top = sorted(theme_scored.items(), key=lambda x:-x[1]["scb"])[:min(5,len(theme_scored))]
        mom_top = sorted(theme_scored.items(), key=lambda x:-x[1]["mom"])[:min(5,len(theme_scored))]
        # Calculate period returns
        di = trade_dates.index(d); dni = trade_dates.index(d_next)
        for picks, acc in [(scb_top,"scb"),(mom_top,"mom")]:
            pr = 0
            for tk,_ in picks:
                if tk in prices.columns and d in prices.index and d_next in prices.index:
                    p0,p1 = prices.loc[d,tk], prices.loc[d_next,tk]
                    if p0>0 and p1>0: pr += (p1/p0-1)/len(picks)
            if acc=="scb": scb_ret_sum += pr
            else: mom_ret_sum += pr
        n_periods += 1
    if n_periods > 0:
        name = RNAMES.get(rid, rid)
        win = scb_ret_sum > mom_ret_sum
        theme_wins[rid] = {"name":name,"scb":scb_ret_sum/n_periods,"mom":mom_ret_sum/n_periods,"win":win,"n":n_periods}

n_win = sum(1 for v in theme_wins.values() if v["win"])
n_total = len(theme_wins)
wr = n_win/n_total if n_total>0 else 0
print(f"{'Theme':20s} {'SCB ret':>8s} {'MOM ret':>8s} {'Winner':>8s}")
print("-"*50)
for rid in sorted(theme_wins, key=lambda k:-theme_wins[k]["scb"]):
    tw = theme_wins[rid]
    w = "SCB" if tw["win"] else "MOM"
    print(f"{tw['name']:20s} {tw['scb']:>+7.2%} {tw['mom']:>+7.2%} {w:>8s}")
print(f"\nTheme win rate: {n_win}/{n_total} ({wr:.0%})")

# === FINAL VERDICT ===
print("\n" + "="*80)
print("VERDICT — SCB v0.2")
print("="*80)
c1 = z1 > 1.64
c2 = scb_a["alpha_sharpe"] > mom_a["alpha_sharpe"] + 0.1
c3 = wr > 0.60
passes = sum([c1,c2,c3])
print(f"① Shuffle Z = {z1:.2f} {'> 1.64 ✅ PASS' if c1 else '< 1.64 ❌ FAIL'}")
print(f"② α Sh(SCB)-α Sh(MOM) = {scb_a['alpha_sharpe']-mom_a['alpha_sharpe']:+.3f} {'> +0.1 ✅ PASS' if c2 else '< +0.1 ❌ FAIL'}")
print(f"③ Theme win rate = {wr:.0%} {'> 60% ✅ PASS' if c3 else '< 60% ❌ FAIL'}")
print(f"\nResult: {passes}/3 → ", end="")
if passes >= 3: print("✅ SCB v0.2 VALIDATED — proceed to integration research")
elif passes >= 2: print("⚠️ CONTINUE RESEARCH — promising but not confirmed")
else: print("❌ INSUFFICIENT EVIDENCE — consider redesign or discard")
