"""G2-MAX weight allocation test: equal vs alternatives (6 themes, raw α63)
W0: equal weight (16.7% × 6)
W1: score-weighted (theme score proportional)
W2: rank-weighted (1st=25%, 2nd=20%, 3rd=18%, 4th=15%, 5th=12%, 6th=10%)
W3: top-heavy (1st=30%, 2nd=22%, 3rd=18%, 4th=14%, 5th=10%, 6th=6%)
W4: inverse-vol weighted (1/theme_vol proportional)
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

RANK_W = [0.25, 0.20, 0.18, 0.15, 0.12, 0.10]  # W2
TOP_HEAVY = [0.30, 0.22, 0.18, 0.14, 0.10, 0.06]  # W3
STRATS = ['W0_equal','W1_score','W2_rank','W3_heavy','W4_invvol']
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
    # Theme scoring
    tm_mom63,tm_mom126,tm_mom21={},{},{}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21: tm_mom21[th]=cumret(td[-21:])
        if len(td)>=63: tm_mom63[th]=cumret(td)
        td126=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126)>=63: tm_mom126[th]=cumret(td126)
    tdf=pd.DataFrame({'m63':pd.Series(tm_mom63),'m126':pd.Series(tm_mom126),'m21':pd.Series(tm_mom21)}).dropna(subset=['m63'])
    if len(tdf)<3:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    tdf['score']=(0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom'))
    ranked=list(tdf.sort_values('score',ascending=False).index)
    sel=corr_select(ranked, sub, N_TH)
    # Pick stocks + compute theme-level features for weighting
    port = []  # list of (theme, ticker, theme_score, theme_vol)
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
            th_vol = float(np.std(ths.groupby('date')['theme_ret'].first().values, ddof=1)*np.sqrt(252))
            th_score = tdf.loc[th,'score'] if th in tdf.index else 0
            port.append((th, best, th_score, max(th_vol, 0.01)))
    if not port:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    n = len(port)
    tickers = [p[1] for p in port]
    th_scores = np.array([p[2] for p in port])
    th_vols = np.array([p[3] for p in port])
    # Compute weights per strategy
    weights = {}
    # W0: equal
    weights['W0_equal'] = np.ones(n)/n
    # W1: score-proportional
    sc = np.maximum(th_scores, 0); weights['W1_score'] = sc/sc.sum() if sc.sum()>0 else np.ones(n)/n
    # W2: rank-weighted (fixed schedule)
    rw = np.array(RANK_W[:n]); weights['W2_rank'] = rw/rw.sum()
    # W3: top-heavy
    tw = np.array(TOP_HEAVY[:n]); weights['W3_heavy'] = tw/tw.sum()
    # W4: inverse-vol
    iv = 1.0/th_vols; weights['W4_invvol'] = iv/iv.sum()
    # Compute daily returns for each
    for sname in STRATS:
        ws = pd.Series(weights[sname], index=tickers)
        dr = tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ws, axis=1).sum(axis=1)
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
    print(f"  {s:<14} ΔCAGR={m['cagr']-w0['cagr']:>+6.1%} ΔSharpe={m['sharpe']-w0['sharpe']:>+6.3f} ΔMaxDD={m['maxdd']-w0['maxdd']:>+6.1%}")

# Annual
print("\n=== ANNUAL ===")
for s in STRATS:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[s])),index=eq_dates)
    results[s]['annual']={str(d.year):round(float(r),3) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
for yr in ['2021','2022','2023','2024','2025','2026']:
    row=f"  {yr:<4}"
    for s in STRATS: row+=f"  {results[s].get('annual',{}).get(yr,0):>+11.1%}"
    print(row)

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_weight_results.json','w') as f:
    json.dump(results,f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
