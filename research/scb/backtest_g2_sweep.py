"""G2-MAX theme count sweep: 3/4/5/6/7 themes with corr budget + raw α63"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

def ols_full(y, x):
    mask=np.isfinite(y)&np.isfinite(x); y,x=y[mask],x[mask]; n=len(y)
    if n<20: return np.nan,np.nan,np.nan,np.nan
    xm,ym=x.mean(),y.mean(); vx=np.var(x,ddof=1)
    if vx<1e-15: return np.nan,np.nan,np.nan,np.nan
    b=np.dot(x-xm,y-ym)/(n-1)/vx; a=ym-b*xm; resid=y-a-b*x
    ss_res=float(np.sum(resid**2)); ss_tot=float(np.sum((y-ym)**2))
    r2=1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n, b, r2, float(np.std(resid,ddof=1)*np.sqrt(n))
def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
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

WARMUP=126; REBAL=20; MIN_M=4
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

THEMES = [3, 4, 5, 6, 7]
daily_ret = {f'G2_{n}th': [] for n in THEMES}

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    sub=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    tm=sub.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme scoring (G2 growth mode)
    tm_mom63,tm_mom126,tm_mom21={},{},{}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21: tm_mom21[th]=cumret(td[-21:])
        if len(td)>=63: tm_mom63[th]=cumret(td)
        td126=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126)>=63: tm_mom126[th]=cumret(td126)
    tdf=pd.DataFrame({'m63':pd.Series(tm_mom63),'m126':pd.Series(tm_mom126),'m21':pd.Series(tm_mom21)}).dropna(subset=['m63'])
    if len(tdf)<3:
        for n in THEMES: daily_ret[f'G2_{n}th'].extend([0.0]*len(hold_dates))
        continue
    tdf['score']=0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom')
    ranked=list(tdf.sort_values('score',ascending=False).index)
    for n_th in THEMES:
        sel=corr_select(ranked, sub, n_th)
        port={}; used=set()
        for th in sel:
            ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
            if len(tks)<MIN_M: continue
            scores={}
            for tk in tks:
                tkd=ths[ths['ticker']==tk].sort_values('date')
                a63,b63,r2,rvol=ols_full(tkd['ret'].values, tkd['theme_ex_self'].values)
                scores[tk]=a63 if np.isfinite(a63) else -999
            for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
                if tk not in used and sc>-999: port[tk]=1.0; used.add(tk); break
        if not port:
            daily_ret[f'G2_{n_th}th'].extend([0.0]*len(hold_dates)); continue
        ws=pd.Series({k:1.0/len(port) for k in port})
        dr=tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1).sum(axis=1)
        daily_ret[f'G2_{n_th}th'].extend(dr.values.tolist())

# === Results ===
eq_dates=tk_wide.index[-len(daily_ret['G2_3th']):]
def calc(dr):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    terminal=float(eq[-1])
    wm=float(pd.Series(arr,index=eq_dates[:len(arr)]).resample('ME').apply(lambda x:float(np.expm1(np.log1p(x).sum()))).min())
    return {'cagr':cagr,'vol':vol,'sharpe':sharpe,'maxdd':maxdd,'terminal':terminal,'worst_month':wm}

print("\n"+"="*85)
print(f"{'Themes':>8} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} {'MaxDD':>8} {'Terminal':>9} {'WorstM':>8}")
print("="*85)
results={}
for n in THEMES:
    k=f'G2_{n}th'; m=calc(daily_ret[k]); results[k]=m
    star=' ★' if n==5 else ''
    print(f"  {n}テーマ{star:2s} {m['cagr']:>7.1%} {m['vol']:>7.1%} {m['sharpe']:>7.3f} {m['maxdd']:>7.1%} {m['terminal']:>8.1f}x {m['worst_month']:>7.1%}")
print("="*85)

# Annual
print("\n=== ANNUAL ===")
for k in results:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[k])),index=eq_dates)
    results[k]['annual']={str(d.year):round(float(r),3) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
header=f"{'Year':<6}"+"".join(f"  {n}テーマ " for n in THEMES)
print(header)
for yr in ['2021','2022','2023','2024','2025','2026']:
    row=f"  {yr:<4}"
    for n in THEMES: row+=f"  {results[f'G2_{n}th'].get('annual',{}).get(yr,0):>+7.1%}"
    print(row)

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_g2_sweep.json','w') as f:
    json.dump(results,f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
