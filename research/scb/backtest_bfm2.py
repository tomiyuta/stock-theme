"""
BFM-v2 backtest — Quality Filter (veto型)
Stage 1: Current L1 top 25
Stage 2: Veto breadth<30pct / concentration>80pct / vol>80pct
Stage 3: Remaining top 10
Layer 2 = A5-SNRb fixed.
"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes | {panel.ticker.nunique()} tickers')

panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
psec = panel[['theme','ticker']].drop_duplicates().merge(meta[['ticker','sector']], on='ticker', how='left')
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

def ols_full(y, x):
    mask = np.isfinite(y) & np.isfinite(x); y, x = y[mask], x[mask]; n = len(y)
    if n < 20: return np.nan, np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean(); vx = np.var(x, ddof=1)
    if vx < 1e-15: return np.nan, np.nan, np.nan, np.nan
    b = np.dot(x-xm, y-ym)/(n-1)/vx; a = ym - b*xm
    resid = y - a - b*x
    ss_res = float(np.sum(resid**2)); ss_tot = float(np.sum((y-ym)**2))
    r2 = 1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n, b, r2, float(np.std(resid, ddof=1)*np.sqrt(n))

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def score_snrb(a63, rvol, r2):
    if not np.isfinite(a63) or not np.isfinite(rvol) or rvol < 1e-8: return -999
    return (a63 / rvol) * (shrink_r2(r2) if np.isfinite(r2) else 0)

def pick_snrb(sel_themes, sub, dt21_set, MIN_M=4):
    ports = {}; used = set()
    for th in sel_themes:
        ths = sub[(sub['theme']==th)&sub['ret'].notna()]
        tks = ths['ticker'].unique()
        if len(tks) < MIN_M: continue
        scores = {}
        for tk in tks:
            tkd = ths[ths['ticker']==tk].sort_values('date')
            a63, b63, r2_63, rvol = ols_full(tkd['ret'].values, tkd['theme_ex_self'].values)
            scores[tk] = score_snrb(a63, rvol, r2_63)
        for tk, sc in sorted(scores.items(), key=lambda x:-x[1]):
            if tk not in used and sc > -999: ports[tk]=1.0; used.add(tk); break
    total = sum(ports.values())
    if total > 0:
        for k in ports: ports[k] /= total
    return ports

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3; CAND_N=25
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

STRATS = ['base', 'bfm2']
daily_ret = {s: [] for s in STRATS}
detail_log = []

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]; dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1])
    dt21 = set(dates_all[max(0,j-20):j+1])
    sub = panel[panel['date'].isin(dt63)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm>=MIN_M].index.tolist()
    # Current Layer 1 theme scoring
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
    dcs=pd.Series(dc); common=list(set(ms.index)&set(dcs.index))
    hold_dates = tk_wide.index[j+1:j_next+1]
    if not common:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    ts = pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['r_mom']=ts['mom63'].rank(pct=True); ts['r_dec']=ts['decel'].rank(pct=True)
    ts['score']=0.70*ts['r_mom']+0.30*ts['r_dec']
    ts = ts.sort_values('score', ascending=False)
    # Base: current L1 top 10
    sel_base=[]; sc_cnt={}
    for th in ts.index:
        s2=theme_sector.get(th,'Unk')
        if sc_cnt.get(s2,0)>=SEC_MAX: continue
        sel_base.append(th); sc_cnt[s2]=sc_cnt.get(s2,0)+1
        if len(sel_base)>=TOP_T: break

    # BFM-v2: top 25 candidates → veto filter → top 10
    candidates=[]; sc_cnt2={}
    for th in ts.index:
        s2=theme_sector.get(th,'Unk')
        # No sector cap on candidates (apply after filter)
        candidates.append(th)
        if len(candidates)>=CAND_N: break

    # Compute quality features for candidates
    cand_feat = {}
    for th in candidates:
        ths = sub[sub['theme']==th]
        tks = ths['ticker'].unique()
        if len(tks) < MIN_M: continue
        # breadth63
        tk_r63 = {}
        for tk in tks:
            tkd = ths[ths['ticker']==tk].sort_values('date')
            if len(tkd)>=20: tk_r63[tk] = cumret(tkd['ret'].values[-63:])
        if len(tk_r63) < MIN_M: continue
        breadth63 = sum(1 for v in tk_r63.values() if np.isfinite(v) and v>0)/len(tk_r63)
        # concentration63
        abs_c = np.array([abs(v) for v in tk_r63.values() if np.isfinite(v)])
        tot_abs = abs_c.sum()
        conc63 = float(np.sum((abs_c/tot_abs)**2)) if tot_abs>1e-10 else 1.0
        # theme_vol63
        td_vals = ths.groupby('date')['theme_ret'].first().sort_index().values
        tvol = float(np.std(td_vals[-63:],ddof=1)*np.sqrt(252)) if len(td_vals)>=63 else np.nan
        if np.isfinite(breadth63) and np.isfinite(conc63) and np.isfinite(tvol):
            cand_feat[th] = {'breadth63':breadth63,'concentration63':conc63,'theme_vol63':tvol}

    # Apply veto filter
    if len(cand_feat) >= 5:
        cf = pd.DataFrame(cand_feat).T
        # Veto: breadth < 30th pct, concentration > 80th pct, vol > 80th pct
        b_thresh = cf['breadth63'].quantile(0.30)
        c_thresh = cf['concentration63'].quantile(0.80)
        v_thresh = cf['theme_vol63'].quantile(0.80)
        vetoed = set()
        for th in cf.index:
            if cf.loc[th,'breadth63'] < b_thresh: vetoed.add(th)
            if cf.loc[th,'concentration63'] > c_thresh: vetoed.add(th)
            if cf.loc[th,'theme_vol63'] > v_thresh: vetoed.add(th)
        survivors = [th for th in ts.index if th in candidates and th not in vetoed]
    else:
        survivors = candidates; vetoed = set()
    # Select top 10 from survivors with sector cap
    sel_bfm2=[]; sc_cnt3={}
    for th in survivors:
        s2=theme_sector.get(th,'Unk')
        if sc_cnt3.get(s2,0)>=SEC_MAX: continue
        sel_bfm2.append(th); sc_cnt3[s2]=sc_cnt3.get(s2,0)+1
        if len(sel_bfm2)>=TOP_T: break

    # Layer 2: A5-SNRb for both
    ports_base = pick_snrb(sel_base, sub, dt21)
    ports_bfm2 = pick_snrb(sel_bfm2, sub, dt21)

    for s, port in [('base',ports_base),('bfm2',ports_bfm2)]:
        if not port: daily_ret[s].extend([0.0]*len(hold_dates)); continue
        ws = pd.Series(port)
        dr = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1).sum(axis=1)
        daily_ret[s].extend(dr.values.tolist())

    theme_overlap = len(set(sel_base)&set(sel_bfm2))
    stock_overlap = len(set(ports_base.keys())&set(ports_bfm2.keys()))
    detail_log.append({
        'date':str(dt.date()), 'theme_overlap':theme_overlap, 'stock_overlap':stock_overlap,
        'n_vetoed':len(vetoed), 'n_survivors':len(survivors),
        'avg_breadth': round(np.mean([cand_feat[t]['breadth63'] for t in sel_bfm2 if t in cand_feat]),3) if cand_feat else None,
        'avg_conc': round(np.mean([cand_feat[t]['concentration63'] for t in sel_bfm2 if t in cand_feat]),3) if cand_feat else None,
    })

# === Metrics ===
eq_dates = tk_wide.index[-len(daily_ret['base']):]
def calc_metrics(dr, name):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    neg=arr[arr<0]; dd=np.sqrt(np.mean(neg**2))*np.sqrt(252) if len(neg)>0 else 1e-8
    sortino=cagr/dd; calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    return {'name':name,'cagr':cagr,'vol':vol,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd}

print("\n"+"="*70)
print(f"{'Metric':<18} {'Base+SNRb':>14} {'BFM-v2+SNRb':>14} {'差':>12}")
print("="*70)
m1=calc_metrics(daily_ret['base'],'Base'); m2=calc_metrics(daily_ret['bfm2'],'BFM-v2')
for key,label in [('cagr','CAGR'),('vol','Vol'),('sharpe','Sharpe'),('sortino','Sortino'),('calmar','Calmar'),('maxdd','MaxDD')]:
    v1,v2=m1[key],m2[key]; diff=v2-v1
    fmt=lambda v: f"{v:.1%}" if key in ['cagr','vol','maxdd'] else f"{v:.3f}"
    print(f"  {label:<16} {fmt(v1):>13} {fmt(v2):>13} {'+' if diff>=0 else ''}{fmt(diff):>11}")
print("="*70)

# Direction check
diff_arr = np.array(daily_ret['bfm2'])-np.array(daily_ret['base'])
monthly = pd.Series(diff_arr, index=eq_dates).resample('M').sum()
print(f"\n=== BFM-v2 vs Base ===")
print(f"  Median monthly diff: {monthly.median():+.4f} ({'↑' if monthly.median()>0 else '↓'})")
print(f"  Positive months: {(monthly>0).sum()}/{len(monthly)} ({(monthly>0).sum()/len(monthly):.0%})")

dl=pd.DataFrame(detail_log)
print(f"\n=== OVERLAP ===")
print(f"  Theme: {dl['theme_overlap'].mean():.1f}/{TOP_T} ({dl['theme_overlap'].mean()/TOP_T:.0%})")
print(f"  Stock: {dl['stock_overlap'].mean():.1f}/{TOP_T} ({dl['stock_overlap'].mean()/TOP_T:.0%})")
print(f"  Avg vetoed/rebal: {dl['n_vetoed'].mean():.1f}")
print(f"  Avg breadth63: {dl['avg_breadth'].mean():.3f}")
print(f"  Avg concentration63: {dl['avg_conc'].mean():.3f}")

# Annual
print(f"\n=== ANNUAL ===")
for s in STRATS:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[s])),index=eq_dates)
    annual=eq.resample('YE').last().pct_change().dropna()
    print(f"  {s}:")
    for dt_y,r in annual.items(): print(f"    {dt_y.year}: {r:+.1%}")

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_bfm2_results.json','w') as f:
    json.dump({'metrics':{'base':m1,'bfm2':m2},'detail_log':detail_log},f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
