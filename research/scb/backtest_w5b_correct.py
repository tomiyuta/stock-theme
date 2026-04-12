"""W5b/BEAST test with CORRECT PRISM/PRISM-R/PRISM-RQ scoring
Matches generate_bt_returns.py exactly:
  PRISM:    theme=0.70*mom63+0.30*decel, stock=raw_1m, sector_cap=3, NO corr_select
  PRISM-R:  theme=0.70*mom63+0.30*decel, stock=α63*shrink_r2, sector_cap=3, NO corr_select
  PRISM-RQ: theme=0.70*mom63+0.30*decel, stock=SNRb, sector_cap=3, NO corr_select
"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()
panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum',n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret']/panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1,(panel['sum_ret']-panel['ret'])/(panel['n_day']-1),np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')
meta_sec = meta.set_index('ticker')['sector'].to_dict()
psec = panel[['theme','ticker']].drop_duplicates()
psec['sector'] = psec['ticker'].map(meta_sec)
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')

def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def ols_ab(y, x):
    """Matches generate_bt_returns.py exactly."""
    mask=np.isfinite(y)&np.isfinite(x); y,x=y[mask],x[mask]; n=len(y)
    if n<10: return np.nan, np.nan, np.nan
    xm,ym=x.mean(),y.mean(); xd=x-xm; vx=np.dot(xd,xd)/(n-1)
    if vx<1e-12: return np.nan, np.nan, np.nan
    b=np.dot(xd,y-ym)/(n-1)/vx; a=ym-b*xm
    ss_res=float(np.sum((y-a-b*x)**2)); ss_tot=float(np.sum((y-ym)**2))
    r2=1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    resid=y-a-b*x; sigma=float(np.std(resid,ddof=2))
    return a*n, sigma, r2

def shrink_r2(r2v):
    """Matches generate_bt_returns.py exactly."""
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

def w5b_weights(port, cap=0.30):
    ws=[]
    for p in port:
        vals=[p.get('r63',np.nan),p.get('r126',np.nan),p.get('r252ex1m',np.nan)]
        valid=[v for v in vals if np.isfinite(v)]
        if len(valid)<2: ws.append(1.0)
        else:
            pc=sum(1 for v in valid if v>0)
            ar=np.mean([max(v,0) for v in valid])
            ws.append(pc*(1+ar))
    wa=np.array(ws,dtype=float)
    if wa.sum()<=0: return np.ones(len(port))/len(port)
    wa=wa/wa.sum()
    if cap is not None:
        for _ in range(5):
            exc=np.maximum(wa-cap,0)
            if exc.sum()<1e-6: break
            under=wa<cap; wa=np.minimum(wa,cap)
            if under.any(): wa[under]+=exc.sum()*(wa[under]/wa[under].sum())
            wa=wa/wa.sum()
    return wa

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance: {len(rebal_idx)-1} periods')

STRATS=['PRISM','PRISM-R','PRISM-RQ']
WMODES=['W0','W5b','BEAST']
dr={f'{s}_{w}':[] for s in STRATS for w in WMODES}

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt21=set(dates_all[max(0,j-20):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    dt252=set(dates_all[max(0,j-251):j+1])
    sub=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    sub252=panel[panel['date'].isin(dt252)]
    tm=sub.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    # === Theme scoring: 0.70*mom63 + 0.30*decel (matches generate_bt_returns.py) ===
    tm_mom={}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=63: tm_mom[th]=cumret(td)
    ms=pd.Series(tm_mom).dropna().sort_values(ascending=False)
    # Deceleration
    dc={}
    for th in ms.index:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)<63: continue
        r021=cumret(td[-21:]); r2142=cumret(td[-42:-21]); r4263=cumret(td[-63:-42])
        if all(np.isfinite([r021,r2142,r4263])): dc[th]=-(r021-0.5*(r2142+r4263))
    dcs=pd.Series(dc); common=list(set(ms.index)&set(dcs.index))
    if not common:
        for k in dr: dr[k].extend([0.0]*len(hold_dates)); continue
    ts=pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['r_mom']=ts['mom63'].rank(pct=True); ts['r_dec']=ts['decel'].rank(pct=True)
    ts['score']=0.70*ts['r_mom']+0.30*ts['r_dec']
    ts=ts.sort_values('score',ascending=False)
    # Theme selection: sector cap, NO corr_select
    sel=[]; sc_cnt={}
    for th in ts.index:
        s=theme_sector.get(th,'Unk')
        if sc_cnt.get(s,0)>=SEC_MAX: continue
        sel.append(th); sc_cnt[s]=sc_cnt.get(s,0)+1
        if len(sel)>=TOP_T: break
    # === Collect W5b momentum data per theme ===
    tm_m63,tm_m126,tm_m252={},{},{}
    for th in sel:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=63: tm_m63[th]=cumret(td)
        td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126v)>=63: tm_m126[th]=cumret(td126v)
        td252v=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td252v)>=126: tm_m252[th]=cumret(td252v)
        td21=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        # R21 for R252_ex1m calc
        tm_m63.setdefault(th+'_r21', cumret(td21[-21:]) if len(td21)>=21 else np.nan)

    # === Stock selection per strategy ===
    for strat in STRATS:
        port=[]; used=set()
        for th in sel:
            ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
            if len(tks)<MIN_M: continue
            scores={}
            for tk in tks:
                tkd=ths[ths['ticker']==tk].sort_values('date')
                if strat=='PRISM':
                    # raw_1m
                    r21d=tkd[tkd['date'].isin(dt21)]
                    raw_1m=cumret(r21d['ret'].values) if len(r21d)>=10 else np.nan
                    scores[tk]=raw_1m if np.isfinite(raw_1m) else -999
                elif strat=='PRISM-R':
                    # α63 * shrink_r2
                    a63,sigma,r2_63=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
                    shrk=shrink_r2(r2_63) if np.isfinite(r2_63) else 0
                    scores[tk]=a63*shrk if np.isfinite(a63) else -999
                else:  # PRISM-RQ = SNRb
                    a63,sigma,r2_63=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
                    scores[tk]=a63/sigma if np.isfinite(a63) and sigma>1e-8 else -999
            # Pick best unused
            for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
                if tk not in used and sc>-999:
                    r63=tm_m63.get(th,np.nan)
                    r126=tm_m126.get(th,np.nan)
                    r252=tm_m252.get(th,np.nan)
                    r21=tm_m63.get(th+'_r21',np.nan)
                    r252ex1m=((1+r252)/(1+r21)-1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21)>1e-8 else np.nan
                    port.append({'tk':tk,'th':th,'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                    used.add(tk); break
        if not port:
            for w in WMODES: dr[f'{strat}_{w}'].extend([0.0]*len(hold_dates)); continue
        n=len(port); tickers=[p['tk'] for p in port]
        weights={
            'W0': np.ones(n)/n,
            'W5b': w5b_weights(port, cap=0.30),
            'BEAST': w5b_weights(port, cap=None),
        }
        for w in WMODES:
            ws=pd.Series(weights[w],index=tickers)
            d=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ws,axis=1).sum(axis=1)
            dr[f'{strat}_{w}'].extend(d.values.tolist())

# === Results ===
eq_dates=tk_wide.index[-len(dr[f'PRISM_W0']):]
def calc(d):
    arr=np.array(d);arr=arr[np.isfinite(arr)];n=len(arr);yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1;vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr);peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    terminal=float(eq[-1])
    neg=arr[arr<0];dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    return {'cagr':cagr,'vol':vol,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'terminal':terminal}

# Verify W0 matches dashboard
print("\n=== W0 VALIDATION (should match dashboard BT) ===")
for s in STRATS:
    m=calc(dr[f'{s}_W0'])
    print(f'  {s:<10} CAGR={m["cagr"]:.1%} Sharpe={m["sharpe"]:.3f} MaxDD={m["maxdd"]:.1%} Terminal={m["terminal"]:.1f}x')

for strat in STRATS:
    print(f"\n{'='*100}")
    print(f"  {strat}")
    print(f"{'='*100}")
    print(f"  {'Weight':<14} {'CAGR':>8} {'MaxDD':>8} {'Sharpe':>8} {'Sortino':>8} {'Calmar':>8} {'Vol':>8} {'Term':>8}")
    print(f"  {'-'*85}")
    results={}
    for w in WMODES:
        m=calc(dr[f'{strat}_{w}']); results[w]=m
        star=' ★' if w=='W0' else ''
        print(f"  {w:<12}{star:2s} {m['cagr']:>7.1%} {m['maxdd']:>7.1%} {m['sharpe']:>7.3f} {m['sortino']:>7.3f} {m['calmar']:>7.3f} {m['vol']:>7.1%} {m['terminal']:>7.1f}x")
    w0=results['W0']
    print(f"\n  vs W0:")
    for w in ['W5b','BEAST']:
        m=results[w]
        print(f"    {w:<10} ΔCAGR={m['cagr']-w0['cagr']:>+7.1%} ΔSharpe={m['sharpe']-w0['sharpe']:>+6.3f} ΔMaxDD={m['maxdd']-w0['maxdd']:>+6.1%} ΔCalmar={m['calmar']-w0['calmar']:>+6.3f}")

    # Annual
    print(f"\n  Annual:")
    for w in WMODES:
        eq=pd.Series(np.cumprod(1+np.array(dr[f'{strat}_{w}'])),index=eq_dates)
        results[w]['annual']={str(d.year):round(float(r),3) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
    for yr in ['2021','2022','2023','2024','2025','2026']:
        row=f"    {yr}"
        for w in WMODES: row+=f"  {results[w].get('annual',{}).get(yr,0):>+11.1%}"
        print(row)

# Cross comparison
print(f"\n{'='*100}")
print("  CROSS-COMPARISON")
print(f"{'='*100}")
print(f"  {'Strategy':<10} {'W0 Shrp':>8} {'W5b Shrp':>9} {'BST Shrp':>9} {'W5b ΔShrp':>10} {'W5b ΔCAGR':>10} {'W5b ΔDD':>8}")
print(f"  {'-'*65}")
for s in STRATS:
    w0=calc(dr[f'{s}_W0']);w5=calc(dr[f'{s}_W5b']);be=calc(dr[f'{s}_BEAST'])
    print(f"  {s:<10} {w0['sharpe']:>7.3f} {w5['sharpe']:>8.3f} {be['sharpe']:>8.3f} {w5['sharpe']-w0['sharpe']:>+9.3f} {w5['cagr']-w0['cagr']:>+9.1%} {w5['maxdd']-w0['maxdd']:>+7.1%}")

all_r={}
for k in dr: all_r[k]=calc(dr[k])
with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_w5b_correct_results.json','w') as f:
    json.dump(all_r,f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
