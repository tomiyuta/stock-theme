"""W5 period variant test:
W5_orig: R21/R63/R126 (current)
W5a: R42/R63/R126 (short-term reversal avoidance)
W5b: R63/R126/R252_ex1m (academic 12-1 momentum)
W0: equal weight (reference)
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
print(f'Dates: {dates_all[0].strftime("%Y-%m-%d")} ~ {dates_all[-1].strftime("%Y-%m-%d")} ({len(dates_all)}d)')

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

WARMUP=252; REBAL=20; MIN_M=4; N_TH=6
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1} (WARMUP={WARMUP}, start~{dates_all[WARMUP].strftime("%Y-%m-%d")})')

STRATS = ['W0_equal','W5_orig','W5a_R42','W5b_252ex1m']
daily_ret = {s:[] for s in STRATS}

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    dt252=set(dates_all[max(0,j-251):j+1])
    sub=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    sub252=panel[panel['date'].isin(dt252)]
    tm=sub.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme momentum at all horizons
    tm_m={h:{} for h in [21,42,63,126,252]}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21: tm_m[21][th]=cumret(td[-21:])
        if len(td)>=42: tm_m[42][th]=cumret(td[-42:])
        if len(td)>=63: tm_m[63][th]=cumret(td)
        td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126v)>=63: tm_m[126][th]=cumret(td126v)
        td252v=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td252v)>=126: tm_m[252][th]=cumret(td252v)
    tdf=pd.DataFrame({f'm{h}':pd.Series(tm_m[h]) for h in [21,42,63,126,252]}).dropna(subset=['m63'])
    if len(tdf)<3:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    # R252_ex1m = (1+R252)/(1+R21) - 1
    tdf['m252ex1m']=np.where(tdf['m252'].notna()&tdf['m21'].notna(),(1+tdf['m252'])/(1+tdf['m21'])-1,np.nan)
    # Theme score for selection (G2-MAX L1 unchanged)
    tdf['score']=(0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom'))
    ranked=list(tdf.sort_values('score',ascending=False).index)
    sel=corr_select(ranked, sub, N_TH)
    # Pick stocks (identical for all variants)
    port=[]
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
            port.append({'tk':best,'th':th,
                'r21':tdf.loc[th,'m21'] if th in tdf.index else np.nan,
                'r42':tdf.loc[th,'m42'] if th in tdf.index else np.nan,
                'r63':tdf.loc[th,'m63'] if th in tdf.index else np.nan,
                'r126':tdf.loc[th,'m126'] if th in tdf.index else np.nan,
                'r252ex1m':tdf.loc[th,'m252ex1m'] if th in tdf.index else np.nan})
    if not port:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    n=len(port); tickers=[p['tk'] for p in port]
    def w5_calc(keys):
        ws=[]
        for p in port:
            vals=[p[k] for k in keys]
            valid=[v for v in vals if np.isfinite(v)]
            if len(valid)<2: ws.append(1.0)
            else:
                pc=sum(1 for v in valid if v>0)
                ar=np.mean([max(v,0) for v in valid])
                ws.append(pc*(1+ar))
        wa=np.array(ws,dtype=float)
        if wa.sum()<=0: return np.ones(n)/n
        wa=wa/wa.sum()
        for _ in range(5):
            exc=np.maximum(wa-0.30,0)
            if exc.sum()<1e-6: break
            under=wa<0.30; wa=np.minimum(wa,0.30)
            if under.any(): wa[under]+=exc.sum()*(wa[under]/wa[under].sum())
            wa=wa/wa.sum()
        return wa
    weights={}
    weights['W0_equal']=np.ones(n)/n
    weights['W5_orig']=w5_calc(['r21','r63','r126'])
    weights['W5a_R42']=w5_calc(['r42','r63','r126'])
    weights['W5b_252ex1m']=w5_calc(['r63','r126','r252ex1m'])
    for sname in STRATS:
        ws=pd.Series(weights[sname],index=tickers)
        dr=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ws,axis=1).sum(axis=1)
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

print("\n"+"="*100)
print(f"{'Variant':<16} {'Periods':<22} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} {'Calmar':>8} {'MaxDD':>8} {'Term$':>8}")
print("="*100)
results={}
labs={'W0_equal':'equal(ref)','W5_orig':'R21/R63/R126','W5a_R42':'R42/R63/R126','W5b_252ex1m':'R63/R126/R252ex1m'}
for s in STRATS:
    m=calc(daily_ret[s]); results[s]=m
    print(f"  {s:<14}  {labs[s]:<22} {m['cagr']:>7.1%} {m['vol']:>7.1%} {m['sharpe']:>7.3f} {m['calmar']:>7.3f} {m['maxdd']:>7.1%} {m['terminal']:>7.1f}x")
print("="*100)

w0=results['W0_equal']; w5=results['W5_orig']
print("\n=== vs W0 (equal weight) ===")
for s in STRATS[1:]:
    m=results[s]
    print(f"  {s:<16} ΔCAGR={m['cagr']-w0['cagr']:>+7.1%} ΔSharpe={m['sharpe']-w0['sharpe']:>+6.3f} ΔMaxDD={m['maxdd']-w0['maxdd']:>+6.1%} ΔCalmar={m['calmar']-w0['calmar']:>+6.3f}")
print("\n=== vs W5_orig (R21/R63/R126) ===")
for s in ['W5a_R42','W5b_252ex1m']:
    m=results[s]
    print(f"  {s:<16} ΔCAGR={m['cagr']-w5['cagr']:>+7.1%} ΔSharpe={m['sharpe']-w5['sharpe']:>+6.3f} ΔMaxDD={m['maxdd']-w5['maxdd']:>+6.1%} ΔCalmar={m['calmar']-w5['calmar']:>+6.3f}")

print("\n=== ANNUAL ===")
for s in STRATS:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[s])),index=eq_dates)
    results[s]['annual']={str(d.year):round(float(r),3) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
header=f"{'Year':<6}"+"".join(f"{s:>18}" for s in STRATS)
print(header)
for yr in ['2021','2022','2023','2024','2025','2026']:
    row=f"  {yr:<4}"
    for s in STRATS: row+=f"  {results[s].get('annual',{}).get(yr,0):>+15.1%}"
    print(row)

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_period_variant_results.json','w') as f:
    json.dump(results,f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
