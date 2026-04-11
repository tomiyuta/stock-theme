"""G2-MAX multi-period momentum weighting test
W0: equal weight (baseline)
W5: momentum consistency (all periods agree → higher weight)
W6: acceleration (R21/R63 ratio → accelerating themes get more)
W7: long-term dominant (R126 proportional)
W8: geometric mean momentum (√(R21×R63×R126))
W9: composite edge/vol (theme_score / theme_vol)
"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum',n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret']/panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1,(panel['sum_ret']-panel['ret'])/(panel['n_day']-1),np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
def ols_alpha(y,x):
    mask=np.isfinite(y)&np.isfinite(x); y,x=y[mask],x[mask]
    if len(y)<20: return np.nan
    xm,ym=x.mean(),y.mean(); vx=np.var(x,ddof=1)
    if vx<1e-15: return np.nan
    b=np.dot(x-xm,y-ym)/(len(y)-1)/vx; a=ym-b*xm
    return a*len(y)
def corr_select(ranked, sub, max_n, max_corr=0.80):
    tdr={}
    for th in ranked:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        if len(td)>=20: tdr[th]=td
    if len(tdr)<2: return ranked[:max_n]
    cdf=pd.DataFrame(tdr).dropna().corr(); sel=[]
    for th in ranked:
        if th not in cdf.index: continue
        ok=all(abs(cdf.loc[th,s])<max_corr for s in sel if s in cdf.columns)
        if ok: sel.append(th)
        if len(sel)>=max_n: break
    return sel

WARMUP=126; REBAL=20; MIN_M=4; N_TH=6
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

STRATS = ['W0_equal','W5_consist','W6_accel','W7_longterm','W8_geomean','W9_edgevol']
daily_ret = {s:[] for s in STRATS}

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    sub=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    tm=sub.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme momentum features
    tm_m63,tm_m126,tm_m21={},{},{}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21: tm_m21[th]=cumret(td[-21:])
        if len(td)>=63: tm_m63[th]=cumret(td)
        td126=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126)>=63: tm_m126[th]=cumret(td126)
    tdf=pd.DataFrame({'m63':pd.Series(tm_m63),'m126':pd.Series(tm_m126),'m21':pd.Series(tm_m21)}).dropna(subset=['m63'])
    if len(tdf)<3:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    tdf['score']=(0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom'))
    ranked=list(tdf.sort_values('score',ascending=False).index)
    sel=corr_select(ranked, sub, N_TH)
    # Pick stocks + collect per-theme momentum features
    port = []
    for th in sel:
        ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        scores={}
        for tk in tks:
            tkd=ths[ths['ticker']==tk].sort_values('date')
            a=ols_alpha(tkd['ret'].values, tkd['theme_ex_self'].values)
            scores[tk]=a if np.isfinite(a) else -999
        best=max(scores, key=scores.get) if scores else None
        if best and scores[best]>-999:
            r21=tdf.loc[th,'m21'] if th in tdf.index and np.isfinite(tdf.loc[th,'m21']) else 0
            r63=tdf.loc[th,'m63'] if th in tdf.index else 0
            r126=tdf.loc[th,'m126'] if th in tdf.index and np.isfinite(tdf.loc[th,'m126']) else 0
            tvol=float(np.std(ths.groupby('date')['theme_ret'].first().values,ddof=1)*np.sqrt(252))
            tscore=tdf.loc[th,'score'] if th in tdf.index else 0
            port.append({'tk':best,'th':th,'r21':r21,'r63':r63,'r126':r126,'vol':max(tvol,0.01),'score':tscore})
    if not port:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    n=len(port); tickers=[p['tk'] for p in port]
    # Compute weights
    weights = {}
    # W0: equal
    weights['W0_equal'] = np.ones(n)/n
    
    # W5: momentum consistency (all 3 periods positive & ranked high → more weight)
    consist = []
    for p in port:
        pos_count = sum(1 for r in [p['r21'],p['r63'],p['r126']] if r > 0)
        avg_ret = np.mean([max(p['r21'],0), max(p['r63'],0), max(p['r126'],0)])
        consist.append(pos_count * (1 + avg_ret))  # 0-3 periods positive × magnitude
    ca = np.array(consist); weights['W5_consist'] = ca/ca.sum() if ca.sum()>0 else np.ones(n)/n
    
    # W6: acceleration (R21/R63 ratio; >1 = accelerating)
    accel = []
    for p in port:
        if p['r63'] > 0.01:
            ratio = max((1+p['r21'])**(63/21) / (1+p['r63']), 0.1)  # annualized R21 vs R63
        else:
            ratio = 1.0
        accel.append(ratio)
    aa = np.array(accel); weights['W6_accel'] = aa/aa.sum() if aa.sum()>0 else np.ones(n)/n
    
    # W7: long-term dominant (R126 proportional, shifted to positive)
    r126s = np.array([max(p['r126'], 0) for p in port])
    weights['W7_longterm'] = r126s/r126s.sum() if r126s.sum()>0 else np.ones(n)/n
    
    # W8: geometric mean of positive returns
    geom = []
    for p in port:
        vals = [max(1+p['r21'],0.5), max(1+p['r63'],0.5), max(1+p['r126'],0.5)]
        geom.append(np.prod(vals)**(1/3))
    ga = np.array(geom); weights['W8_geomean'] = ga/ga.sum() if ga.sum()>0 else np.ones(n)/n
    
    # W9: composite edge/vol (theme_score / theme_vol)
    ev = np.array([max(p['score'],0.01)/p['vol'] for p in port])
    weights['W9_edgevol'] = ev/ev.sum() if ev.sum()>0 else np.ones(n)/n
    
    # Apply cap: no single theme > 30%
    for sname in STRATS:
        w = weights[sname].copy()
        for _ in range(5):  # iterate to redistribute
            excess = np.maximum(w - 0.30, 0)
            if excess.sum() < 1e-6: break
            w = np.minimum(w, 0.30)
            w += excess.sum() * (w / w.sum()) * (w < 0.30)
            w = w / w.sum()
        weights[sname] = w
    
    for sname in STRATS:
        ws = pd.Series(weights[sname], index=tickers)
        dr = tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ws,axis=1).sum(axis=1)
        daily_ret[sname].extend(dr.values.tolist())

# === Results ===
eq_dates=tk_wide.index[-len(daily_ret['W0_equal']):]
def calc(dr):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    terminal=float(eq[-1])
    return {'cagr':cagr,'vol':vol,'sharpe':sharpe,'calmar':calmar,'maxdd':maxdd,'terminal':terminal}

print("\n"+"="*95)
print(f"{'Weight Scheme':<16} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} {'Calmar':>8} {'MaxDD':>8} {'Term$':>8}")
print("="*95)
results={}
for sname in STRATS:
    m=calc(daily_ret[sname]); results[sname]=m
    star=' ★' if sname=='W0_equal' else ''
    print(f"  {sname:<14}{star:2s} {m['cagr']:>7.1%} {m['vol']:>7.1%} {m['sharpe']:>7.3f} {m['calmar']:>7.3f} {m['maxdd']:>7.1%} {m['terminal']:>7.1f}x")
print("="*95)

w0=results['W0_equal']
print("\n=== vs 等ウェイト(W0) ===")
for s in STRATS[1:]:
    m=results[s]
    print(f"  {s:<14} ΔCAGR={m['cagr']-w0['cagr']:>+6.1%} ΔSharpe={m['sharpe']-w0['sharpe']:>+6.3f} ΔMaxDD={m['maxdd']-w0['maxdd']:>+6.1%} ΔCalmar={m['calmar']-w0['calmar']:>+6.3f}")

print("\n=== ANNUAL ===")
for s in STRATS:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[s])),index=eq_dates)
    results[s]['annual']={str(d.year):round(float(r),3) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
for yr in ['2021','2022','2023','2024','2025','2026']:
    row=f"  {yr:<4}"
    for s in STRATS: row+=f"  {results[s].get('annual',{}).get(yr,0):>+10.1%}"
    print(row)

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_mpmw_results.json','w') as f:
    json.dump(results,f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
