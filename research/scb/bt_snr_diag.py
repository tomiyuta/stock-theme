"""Additional diagnostics requested by ChatGPT reviewers"""
import pandas as pd, numpy as np, json, warnings
warnings.filterwarnings('ignore')

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
psec = panel[['theme','ticker']].drop_duplicates().merge(meta[['ticker','sector']], on='ticker', how='left')
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')
ticker_sector = dict(zip(meta['ticker'], meta['sector']))
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

def ols_full(y, x):
    mask = np.isfinite(y) & np.isfinite(x)
    y, x = y[mask], x[mask]
    n = len(y)
    if n < 20: return np.nan, np.nan, np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean()
    vx = np.var(x, ddof=1)
    if vx < 1e-15: return np.nan, np.nan, np.nan, np.nan, np.nan
    b = np.dot(x-xm, y-ym)/(n-1)/vx; a = ym - b*xm
    resid = y - a - b*x
    ss_res = float(np.sum(resid**2)); ss_tot = float(np.sum((y-ym)**2))
    r2 = 1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n, b, r2, float(np.std(resid,ddof=1)*np.sqrt(n)), float(np.sqrt(np.sum(resid**2)))

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
STRATS = ['a5lite','snrb','quality']

# Track portfolios per rebalance for diagnostics
all_ports = {s: [] for s in STRATS}  # list of (date, {ticker: weight})
all_daily = {s: [] for s in STRATS}
all_daily_contrib = {s: [] for s in STRATS}  # per-ticker daily contribution

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]; dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1]); dt21 = set(dates_all[max(0,j-20):j+1])
    sub = panel[panel['date'].isin(dt63)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm>=MIN_M].index.tolist()
    tm_mom = {}
    for th in elig:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        tm_mom[th] = cumret(td.values)
    ms = pd.Series(tm_mom).dropna().sort_values(ascending=False)
    dc = {}
    for th in ms.index:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)<63: continue
        r021=cumret(td[-21:]); r2142=cumret(td[-42:-21]); r4263=cumret(td[-63:-42])
        if all(np.isfinite([r021,r2142,r4263])): dc[th]=-(r021-0.5*(r2142+r4263))
    dcs = pd.Series(dc); common = list(set(ms.index)&set(dcs.index))
    hold_dates = tk_wide.index[j+1:j_next+1]
    if not common:
        for s in STRATS:
            all_daily[s].extend([0.0]*len(hold_dates))
            all_ports[s].append((dt, {}))
        continue
    ts = pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['r_mom']=ts['mom63'].rank(pct=True); ts['r_dec']=ts['decel'].rank(pct=True)
    ts['score']=0.70*ts['r_mom']+0.30*ts['r_dec']
    ts = ts.sort_values('score', ascending=False)
    sel=[]; sc_cnt={}
    for th in ts.index:
        s2=theme_sector.get(th,'Unk')
        if sc_cnt.get(s2,0)>=SEC_MAX: continue
        sel.append(th); sc_cnt[s2]=sc_cnt.get(s2,0)+1
        if len(sel)>=TOP_T: break
    ports = {s: {} for s in STRATS}; used = {s: set() for s in STRATS}
    for th in sel:
        ths = sub[(sub['theme']==th)&sub['ret'].notna()]; tks = ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        scores = {s: {} for s in STRATS}
        for tk in tks:
            tkd = ths[ths['ticker']==tk].sort_values('date')
            a63,b63,r2_63,rvol,npath = ols_full(tkd['ret'].values, tkd['theme_ex_self'].values)
            shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
            scores['a5lite'][tk] = a63*shrk if np.isfinite(a63) else -999
            scores['snrb'][tk] = (a63/rvol)*shrk if np.isfinite(a63) and np.isfinite(rvol) and rvol>1e-8 else -999
            q = abs(a63)/(abs(a63)+npath) if np.isfinite(a63) and np.isfinite(npath) and npath>1e-8 else 0
            scores['quality'][tk] = a63*q if np.isfinite(a63) else -999
        for s in STRATS:
            for tk,sc in sorted(scores[s].items(), key=lambda x:-x[1]):
                if tk not in used[s] and sc>-999: ports[s][tk]=1.0; used[s].add(tk); break
    for s in STRATS:
        total=sum(ports[s].values())
        if total>0:
            for k in ports[s]: ports[s][k]/=total
        all_ports[s].append((dt, dict(ports[s])))
    for s in STRATS:
        if not ports[s]:
            all_daily[s].extend([0.0]*len(hold_dates)); continue
        ws = pd.Series(ports[s])
        dr = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1)
        all_daily[s].extend(dr.sum(axis=1).values.tolist())
        # Per-ticker contribution
        for d_idx, d_date in enumerate(hold_dates):
            contrib = {}
            for tk in ws.index:
                r = dr.loc[d_date, tk] if d_date in dr.index else 0.0
                contrib[tk] = float(r)
            all_daily_contrib[s].append((d_date, contrib))

# === DIAGNOSTICS ===
eq_dates = tk_wide.index[-len(all_daily['a5lite']):]

print("=" * 60)
print("DIAGNOSTIC 1: MaxDD Window Analysis")
print("=" * 60)
for s in STRATS:
    arr = np.array(all_daily[s])
    eq = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    mdd_idx = np.argmin(dd)
    mdd_val = dd[mdd_idx]
    # Find peak before trough
    peak_idx = np.argmax(eq[:mdd_idx+1])
    print(f"\n  {s}: MaxDD = {mdd_val:.4f}")
    print(f"    Peak: {eq_dates[peak_idx].date()} (eq={eq[peak_idx]:.2f})")
    print(f"    Trough: {eq_dates[mdd_idx].date()} (eq={eq[mdd_idx]:.2f})")
    # Portfolio at peak
    port_at_peak = None
    for dt, p in all_ports[s]:
        if dt <= eq_dates[peak_idx]:
            port_at_peak = p
    if port_at_peak:
        print(f"    Holdings at peak: {list(port_at_peak.keys())}")

print("\n" + "=" * 60)
print("DIAGNOSTIC 2: Tech Share & Concentration by Year")
print("=" * 60)
for s in STRATS:
    print(f"\n  === {s} ===")
    # Build per-rebalance tech count and contribution
    yearly = {}
    for dt, port in all_ports[s]:
        yr = dt.year
        if yr not in yearly: yearly[yr] = {'tech_count':[], 'total_count':[], 'holdings':[]}
        n_tech = sum(1 for tk in port if ticker_sector.get(tk,'')=='Technology')
        yearly[yr]['tech_count'].append(n_tech)
        yearly[yr]['total_count'].append(len(port))
        yearly[yr]['holdings'].extend(port.keys())
    for yr in sorted(yearly.keys()):
        tc = yearly[yr]['tech_count']
        nc = yearly[yr]['total_count']
        avg_tech = np.mean(tc) if tc else 0
        avg_n = np.mean(nc) if nc else 0
        print(f"    {yr}: avg Tech={avg_tech:.1f}/{avg_n:.0f} ({avg_tech/avg_n:.0%})" if avg_n>0 else f"    {yr}: N/A")

print("\n" + "=" * 60)
print("DIAGNOSTIC 3: Top-5 Active Contributor Share (cumulative)")
print("=" * 60)
for s in STRATS:
    # Aggregate per-ticker total contribution
    tk_total = {}
    for d_date, contrib in all_daily_contrib[s]:
        for tk, r in contrib.items():
            tk_total[tk] = tk_total.get(tk, 0) + r
    total_ret = sum(tk_total.values())
    if total_ret > 0:
        top5 = sorted(tk_total.items(), key=lambda x: -x[1])[:5]
        top5_sum = sum(v for _,v in top5)
        print(f"\n  {s}: Top5 = {top5_sum/total_ret:.1%} of total return")
        for tk, v in top5:
            sec = ticker_sector.get(tk, 'Unk')
            print(f"    {tk:6s} ({sec:12s}): {v/total_ret:.1%}")

print("\n" + "=" * 60)
print("DIAGNOSTIC 4: 2021 Quality Deep Dive")
print("=" * 60)
# Filter 2021 contributions for quality
tk_2021 = {}
for d_date, contrib in all_daily_contrib['quality']:
    if d_date.year == 2021:
        for tk, r in contrib.items():
            tk_2021[tk] = tk_2021.get(tk, 0) + r
total_2021 = sum(tk_2021.values())
if total_2021 > 0:
    top5_2021 = sorted(tk_2021.items(), key=lambda x: -x[1])[:5]
    top1_share = top5_2021[0][1] / total_2021 if top5_2021 else 0
    top3_share = sum(v for _,v in top5_2021[:3]) / total_2021
    top5_share = sum(v for _,v in top5_2021) / total_2021
    print(f"  Quality 2021 total return contribution: {total_2021:.4f}")
    print(f"  Top-1 share: {top1_share:.1%}")
    print(f"  Top-3 share: {top3_share:.1%}")
    print(f"  Top-5 share: {top5_share:.1%}")
    n_tech_2021 = sum(1 for tk,_ in top5_2021 if ticker_sector.get(tk,'')=='Technology')
    print(f"  Tech in Top-5: {n_tech_2021}/5")
    for tk, v in top5_2021:
        sec = ticker_sector.get(tk, 'Unk')
        print(f"    {tk:6s} ({sec:12s}): {v/total_2021:.1%} ({v:.4f})")

# 2021 overlap quality vs a5lite
print("\n  2021 overlap quality vs a5lite:")
for dt, port_q in all_ports['quality']:
    if dt.year == 2021:
        port_a5 = None
        for dt2, p2 in all_ports['a5lite']:
            if dt2 == dt: port_a5 = p2; break
        if port_a5:
            overlap = set(port_q.keys()) & set(port_a5.keys())
            print(f"    {dt.date()}: overlap {len(overlap)}/{len(port_q)} ({len(overlap)/max(len(port_q),1):.0%})")

print("\n" + "=" * 60)
print("DIAGNOSTIC 5: SNRb Tech Share vs A5-lite")
print("=" * 60)
for s in ['a5lite', 'snrb']:
    tk_contrib_by_sector = {}
    for d_date, contrib in all_daily_contrib[s]:
        for tk, r in contrib.items():
            sec = ticker_sector.get(tk, 'Unk')
            tk_contrib_by_sector[sec] = tk_contrib_by_sector.get(sec, 0) + r
    total = sum(tk_contrib_by_sector.values())
    if total > 0:
        tech_share = tk_contrib_by_sector.get('Technology', 0) / total
        nontech = total - tk_contrib_by_sector.get('Technology', 0)
        print(f"  {s}: Tech share of return = {tech_share:.1%}, Non-Tech return = {nontech:.4f}")

print("\nDone.")
