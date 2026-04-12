#!/usr/bin/env python3
"""G2-MAX Split-Window A/B BT
Compares: G2_before (OLS=63d raw α) vs G2_after (split β=126d, α=63d raw α)
Uses G2-MAX theme scoring (0.50×R63 + 0.30×R126 + 0.20×R21) + correlation filter
"""
import pandas as pd, numpy as np, time, warnings
warnings.filterwarnings('ignore')
t0 = time.time()
panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum',n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret']/panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1,(panel['sum_ret']-panel['ret'])/(panel['n_day']-1),np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')
import yfinance as yf
spy=yf.download('SPY',start='2018-01-01',end='2027-01-01',progress=False)
spy_close=(spy['Adj Close'] if 'Adj Close' in spy.columns else spy['Close']).squeeze()
spy_close.index=spy_close.index.tz_localize(None)
spy_ret=spy_close.pct_change().dropna()
print(f'Loaded: {len(panel):,} rows | {time.time()-t0:.1f}s')
def cumret(a):
    a=np.asarray(a,dtype=float);a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
def ols_ab(y,x):
    mask=np.isfinite(y)&np.isfinite(x);y,x=y[mask],x[mask];n=len(y)
    if n<10:return np.nan,np.nan,np.nan
    xm,ym=x.mean(),y.mean();xd=x-xm;vx=np.dot(xd,xd)/(n-1)
    if vx<1e-12:return np.nan,np.nan,np.nan
    b=np.dot(xd,y-ym)/(n-1)/vx;a=ym-b*xm
    ss=float(np.sum((y-a-b*x)**2));st=float(np.sum((y-ym)**2))
    return a*n,b,(1-ss/st if st>1e-12 else np.nan)
def split_alpha(y_long, x_long, y_short, x_short):
    mask=np.isfinite(y_long)&np.isfinite(x_long)
    yl,xl=y_long[mask],x_long[mask];nl=len(yl)
    if nl<20:return np.nan,np.nan,np.nan
    xm,ym=xl.mean(),yl.mean();vx=np.var(xl,ddof=1)
    if vx<1e-15:return np.nan,np.nan,np.nan
    b_long=np.dot(xl-xm,yl-ym)/(nl-1)/vx
    a_long=ym-b_long*xm
    resid=yl-a_long-b_long*xl
    ss=float(np.sum(resid**2));st=float(np.sum((yl-ym)**2))
    r2=1-ss/st if st>1e-12 else np.nan
    mask_s=np.isfinite(y_short)&np.isfinite(x_short)
    ys,xs=y_short[mask_s],x_short[mask_s];ns=len(ys)
    if ns<10:return np.nan,b_long,r2
    alpha_daily=np.mean(ys-b_long*xs)
    return alpha_daily*ns,b_long,r2
def w5b_w(port,cap=0.30):
    ws=[]
    for p in port:
        vals=[p.get('r63',np.nan),p.get('r126',np.nan),p.get('r252ex1m',np.nan)]
        valid=[v for v in vals if np.isfinite(v)]
        if len(valid)<2:ws.append(1.0)
        else:pc=sum(1 for v in valid if v>0);ar=np.mean([max(v,0) for v in valid]);ws.append(pc*(1+ar))
    wa=np.array(ws,dtype=float)
    if wa.sum()<=0:return np.ones(len(port))/len(port)
    wa=wa/wa.sum()
    if cap:
        for _ in range(5):
            exc=np.maximum(wa-cap,0)
            if exc.sum()<1e-6:break
            under=wa<cap;wa=np.minimum(wa,cap)
            if under.any():wa[under]+=exc.sum()*(wa[under]/wa[under].sum())
            wa=wa/wa.sum()
    return wa
def calc_stats(arr):
    arr=np.array(arr);arr=arr[np.isfinite(arr)];n=len(arr);yrs=n/252
    if n<20:return {}
    cum=float(np.expm1(np.log1p(arr).sum()));cagr=(1+cum)**(1/yrs)-1
    vol=float(np.std(arr,ddof=1)*np.sqrt(252));sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr);peak=np.maximum.accumulate(eq);maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    neg=arr[arr<0];dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    return {'cagr':cagr,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd}

# G2-MAX params
WARMUP=252;REBAL=20;MIN_M=4;TOP_T=6;MAX_CORR=0.80
VARIANTS=['G2_before','G2_after','G2_before_w5b','G2_after_w5b']
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1:rebal_idx.append(len(dates_all)-1)
results={k:[] for k in VARIANTS}
eq_dates=[]
print(f'Rebalance: {len(rebal_idx)-1} periods')
for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos];j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    dt252=set(dates_all[max(0,j-251):j+1])
    sub63=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    sub252=panel[panel['date'].isin(dt252)]
    hold_dates=tk_wide.index[j+1:j_next+1]
    # G2-MAX theme scoring
    tm=sub63.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    tm63,tm126,tm21={},{},{}
    for th in elig:
        td=sub63[sub63['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21:tm21[th]=cumret(td[-21:])
        if len(td)>=63:tm63[th]=cumret(td)
        td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126v)>=63:tm126[th]=cumret(td126v)
    tdf=pd.DataFrame({'m63':pd.Series(tm63),'m126':pd.Series(tm126),'m21':pd.Series(tm21)}).dropna(subset=['m63'])
    if len(tdf)<3:
        for v in VARIANTS:results[v].extend([0.0]*len(hold_dates))
        eq_dates.extend(hold_dates.tolist());continue
    tdf['score']=0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom')
    tdf=tdf.sort_values('score',ascending=False)
    # Correlation-distinct selection
    theme_daily={}
    for th in tdf.index[:20]:
        td=sub63[sub63['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        if len(td)>=20:theme_daily[th]=td
    corr_mat=pd.DataFrame(theme_daily).dropna().corr() if len(theme_daily)>=2 else pd.DataFrame()
    sel=[]
    for th in tdf.index:
        if th not in corr_mat.index:continue
        conflict=False
        for s in sel:
            if s in corr_mat.columns and abs(corr_mat.loc[th,s])>MAX_CORR:conflict=True;break
        if not conflict:sel.append(th)
        if len(sel)>=TOP_T:break
    # Stock scoring: before (OLS=63d) vs after (split β=126d, α=63d)
    picks_before={};picks_after={};used_b=set();used_a=set()
    port_info_b=[];port_info_a=[]
    for th in sel:
        ths63=sub63[(sub63['theme']==th)&sub63['ret'].notna()]
        ths126=sub126[(sub126['theme']==th)&sub126['ret'].notna()]
        tks=ths63['ticker'].unique()
        if len(tks)<MIN_M:continue
        scores_b={};scores_a={}
        for tk in tks:
            tkd63=ths63[ths63['ticker']==tk].sort_values('date')
            tkd126=ths126[ths126['ticker']==tk].sort_values('date')
            if len(tkd63)<10:continue
            # Before: raw α63 (no shrink)
            a_b,_,_=ols_ab(tkd63['ret'].values,tkd63['theme_ex_self'].values)
            scores_b[tk]=a_b if np.isfinite(a_b) else -999
            # After: split-window raw α (β=126d, α=63d, no shrink)
            if len(tkd126)>=20:
                a_a,_,_=split_alpha(tkd126['ret'].values,tkd126['theme_ex_self'].values,
                                    tkd63['ret'].values,tkd63['theme_ex_self'].values)
                scores_a[tk]=a_a if np.isfinite(a_a) else -999
            else:
                scores_a[tk]=scores_b.get(tk,-999)
        # Select best per theme
        for tk,sc in sorted(scores_b.items(),key=lambda x:-x[1]):
            if tk not in used_b and sc>-999:
                picks_before[tk]=1.0;used_b.add(tk)
                # W5b info
                tkd=sub252[(sub252['ticker']==tk)&sub252['ret'].notna()].sort_values('date')
                r63=cumret(tkd['ret'].values[-63:]) if len(tkd)>=63 else np.nan
                r126=cumret(tkd['ret'].values[-126:]) if len(tkd)>=126 else np.nan
                r252=cumret(tkd['ret'].values) if len(tkd)>=200 else np.nan
                r21=cumret(tkd['ret'].values[-21:]) if len(tkd)>=21 else np.nan
                r252ex1m=(1+r252)/(1+r21)-1 if np.isfinite(r252) and np.isfinite(r21) else np.nan
                port_info_b.append({'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                break
        for tk,sc in sorted(scores_a.items(),key=lambda x:-x[1]):
            if tk not in used_a and sc>-999:
                picks_after[tk]=1.0;used_a.add(tk)
                tkd=sub252[(sub252['ticker']==tk)&sub252['ret'].notna()].sort_values('date')
                r63=cumret(tkd['ret'].values[-63:]) if len(tkd)>=63 else np.nan
                r126=cumret(tkd['ret'].values[-126:]) if len(tkd)>=126 else np.nan
                r252=cumret(tkd['ret'].values) if len(tkd)>=200 else np.nan
                r21=cumret(tkd['ret'].values[-21:]) if len(tkd)>=21 else np.nan
                r252ex1m=(1+r252)/(1+r21)-1 if np.isfinite(r252) and np.isfinite(r21) else np.nan
                port_info_a.append({'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                break
    # Equal weight
    for d in [picks_before,picks_after]:
        total=sum(d.values())
        if total>0:
            for k in d:d[k]/=total
    # W5b weights
    w5b_b=w5b_w(port_info_b) if port_info_b else np.array([])
    w5b_a=w5b_w(port_info_a) if port_info_a else np.array([])
    picks_before_w5b={tk:float(w5b_b[i]) for i,tk in enumerate(picks_before)} if len(w5b_b)==len(picks_before) else dict(picks_before)
    picks_after_w5b={tk:float(w5b_a[i]) for i,tk in enumerate(picks_after)} if len(w5b_a)==len(picks_after) else dict(picks_after)
    # Hold period
    for d in hold_dates:
        eq_dates.append(d)
        dr=tk_wide.loc[d] if d in tk_wide.index else pd.Series(dtype=float)
        for vname,port in [('G2_before',picks_before),('G2_after',picks_after),
                           ('G2_before_w5b',picks_before_w5b),('G2_after_w5b',picks_after_w5b)]:
            r=sum(w*dr.get(tk,0) for tk,w in port.items())
            results[vname].append(float(r) if np.isfinite(r) else 0.0)
    if (pos+1)%20==0:print(f'  [{pos+1}/{len(rebal_idx)-1}] {dates_all[j].strftime("%Y-%m-%d")}')

print(f'\nBT done in {time.time()-t0:.1f}s')

# === Results ===
eq_dates_pd=pd.to_datetime(eq_dates)
print(f'\n{"="*90}')
print(f'  G2-MAX SPLIT-WINDOW A/B TEST')
print(f'{"="*90}')
print(f'  {"Variant":<18} {"CAGR":>7} {"Shrp":>6} {"Sort":>6} {"Cal":>6} {"MaxDD":>7}')
print(f'  {"-"*55}')
for v in VARIANTS:
    st=calc_stats(np.array(results[v]))
    if st:print(f'  {v:<18} {st["cagr"]:>6.1%} {st["sharpe"]:>5.3f} {st["sortino"]:>5.2f} {st["calmar"]:>5.2f} {st["maxdd"]:>6.1%}')

print(f'\n  Delta (after - before):')
for pair in [('G2_before','G2_after'),('G2_before_w5b','G2_after_w5b')]:
    sb=calc_stats(np.array(results[pair[0]]));sa=calc_stats(np.array(results[pair[1]]))
    if sb and sa:
        print(f'    {pair[1]:18s} dCAGR={sa["cagr"]-sb["cagr"]:+.1%} dShrp={sa["sharpe"]-sb["sharpe"]:+.3f} dDD={sa["maxdd"]-sb["maxdd"]:+.1%}')

# Annual
print(f'\n  Annual returns:')
eq_s={v:pd.Series(np.cumprod(1+np.array(results[v])),index=eq_dates_pd) for v in VARIANTS}
for v in ['G2_before_w5b','G2_after_w5b']:
    ann={str(d.year):round(float(r),4) for d,r in eq_s[v].resample('YE').last().pct_change().dropna().items()}
    years=sorted(ann.keys())
    vals=' '.join(f'{y}={ann[y]*100:+6.1f}%' for y in years)
    print(f'    {v:18s} {vals}')

print(f'\n  Total elapsed: {time.time()-t0:.1f}s')
print('  === G2-MAX SPLIT-WINDOW BT COMPLETE ===')
