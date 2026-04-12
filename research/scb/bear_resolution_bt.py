#!/usr/bin/env python3
"""Bear Problem Resolution — Phase 1-2: Intermediate Horizon + Residual Momentum
Tests whether switching from recent-heavy to intermediate/residual scoring improves Bear Sharpe.
"""
import pandas as pd, numpy as np, time, warnings
warnings.filterwarnings('ignore')
t0 = time.time()
panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum',n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret']/panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1,(panel['sum_ret']-panel['ret'])/(panel['n_day']-1),np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')
# Market return (equal-weight all stocks per day)
mkt_ret = tk_wide.mean(axis=1)
meta_sec = meta.set_index('ticker')['sector'].to_dict()
psec = panel[['theme','ticker']].drop_duplicates()
psec['sector'] = psec['ticker'].map(meta_sec)
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')
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
def ols_resid(y, X):
    """Multi-factor OLS, returns residuals."""
    mask = np.all(np.isfinite(np.column_stack([y,X])),axis=1)
    if mask.sum()<10: return np.full(len(y),np.nan)
    ym,Xm = y[mask],X[mask]
    Xb = np.column_stack([np.ones(len(Xm)),Xm])
    try:
        beta = np.linalg.lstsq(Xb,ym,rcond=None)[0]
        resid = np.full(len(y),np.nan)
        resid[mask] = ym - Xb @ beta
        return resid
    except: return np.full(len(y),np.nan)
def shrink_r2(v):
    if np.isnan(v) or v<0:return 0.0
    if v<0.10:return v*2
    if v<=0.50:return 0.20+(v-0.10)*2.0
    return 1.0
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
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1;vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr);peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    neg=arr[arr<0];dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    return {'cagr':cagr,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'terminal':float(eq[-1]),'n':n}

# === Scoring Variants ===
# Theme scoring: how to rank themes
# Stock scoring: how to pick best stock per theme
VARIANTS = {
    # Baseline (current PRISM-R)
    'A_current':     {'theme':'mom63+decel', 'stock':'alpha63_shrink'},
    # Phase 1: Intermediate horizon theme scoring
    'B_theme_12_7':  {'theme':'mom252_ex_recent', 'stock':'alpha63_shrink'},
    'C_theme_12_2':  {'theme':'mom252_ex42', 'stock':'alpha63_shrink'},
    'D_theme_6_2':   {'theme':'mom126_ex42', 'stock':'alpha63_shrink'},
    # Phase 1b: Intermediate stock scoring
    'E_stock_12_7':  {'theme':'mom63+decel', 'stock':'alpha_12_7'},
    'F_stock_6_2':   {'theme':'mom63+decel', 'stock':'alpha_6_2'},
    # Phase 1c: Both intermediate
    'G_both_12_7':   {'theme':'mom252_ex_recent', 'stock':'alpha_12_7'},
    'H_both_6_2':    {'theme':'mom126_ex42', 'stock':'alpha_6_2'},
    # Phase 2: Residual momentum (remove market + theme factor)
    'I_resid_63':    {'theme':'mom63+decel', 'stock':'resid_alpha63'},
    'J_resid_12_7':  {'theme':'mom252_ex_recent', 'stock':'resid_alpha_12_7'},
    # Phase 2b: Formation-vol penalty
    'K_volpen_63':   {'theme':'mom63+decel', 'stock':'alpha63_volpen'},
    'L_resid_volpen':{'theme':'mom252_ex_recent', 'stock':'resid_alpha_12_7_volpen'},
}

WARMUP=252;REBAL=20;MIN_M=4;TOP_T=10;SEC_MAX=3
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1:rebal_idx.append(len(dates_all)-1)
results={k:[] for k in VARIANTS}
print(f'Rebalance: {len(rebal_idx)-1} periods')

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos];j_next=rebal_idx[pos+1]
    # Lookback windows
    dt63=set(dates_all[max(0,j-62):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    dt252=set(dates_all[max(0,j-251):j+1])
    dt21=set(dates_all[max(0,j-20):j+1])
    dt42=set(dates_all[max(0,j-41):j+1])
    # Intermediate windows
    dt_7_12 = set(dates_all[max(0,j-251):max(0,j-146)])  # months 7-12 ago
    dt_2_12 = set(dates_all[max(0,j-251):max(0,j-41)])   # months 2-12 ago
    dt_2_6  = set(dates_all[max(0,j-125):max(0,j-41)])   # months 2-6 ago
    sub63=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    sub252=panel[panel['date'].isin(dt252)]
    sub_7_12=panel[panel['date'].isin(dt_7_12)]
    sub_2_12=panel[panel['date'].isin(dt_2_12)]
    sub_2_6=panel[panel['date'].isin(dt_2_6)]
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Market returns for residual computation
    mkt_63=mkt_ret.reindex(sorted(dt63)).values
    mkt_252=mkt_ret.reindex(sorted(dt252)).values
    mkt_7_12=mkt_ret.reindex(sorted(dt_7_12)).values
    # === Theme eligible ===
    tm=sub63.groupby('theme')['ticker'].nunique();elig=tm[tm>=MIN_M].index.tolist()
    # Theme momentum for different horizons
    tm_scores = {}  # {scoring_method: {theme: score}}
    tm_mom63={};tm_mom126={};tm_mom252={};tm_mom_7_12={};tm_mom_2_12={};tm_mom_2_6={}
    dc={}
    for th in elig:
        td63=sub63[sub63['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td63)>=42: tm_mom63[th]=cumret(td63)
        td126=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126)>=63: tm_mom126[th]=cumret(td126)
        td252=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td252)>=126: tm_mom252[th]=cumret(td252)
        td_7_12=sub_7_12[sub_7_12['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td_7_12)>=42: tm_mom_7_12[th]=cumret(td_7_12)
        td_2_12=sub_2_12[sub_2_12['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td_2_12)>=63: tm_mom_2_12[th]=cumret(td_2_12)
        td_2_6=sub_2_6[sub_2_6['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td_2_6)>=42: tm_mom_2_6[th]=cumret(td_2_6)
        # Deceleration (for current scoring)
        if len(td63)>=63:
            r021=cumret(td63[-21:]);r2142=cumret(td63[-42:-21]);r4263=cumret(td63[-63:-42])
            if all(np.isfinite([r021,r2142,r4263])):dc[th]=-(r021-0.5*(r2142+r4263))
    # Build theme scores per method
    def build_theme_score(method):
        if method=='mom63+decel':
            ms=pd.Series(tm_mom63).dropna();dcs=pd.Series(dc)
            common=list(set(ms.index)&set(dcs.index))
            if not common:return pd.Series(dtype=float)
            ts=pd.DataFrame({'mom':ms[common],'dec':dcs[common]})
            ts['score']=0.70*ts['mom'].rank(pct=True)+0.30*ts['dec'].rank(pct=True)
            return ts['score'].sort_values(ascending=False)
        elif method=='mom252_ex_recent':
            # 12-7 month momentum (skip recent 6 months)
            ms=pd.Series(tm_mom_7_12).dropna()
            if len(ms)<3:return pd.Series(dtype=float)
            return ms.rank(pct=True).sort_values(ascending=False)
        elif method=='mom252_ex42':
            # 12-2 month momentum
            ms=pd.Series(tm_mom_2_12).dropna()
            if len(ms)<3:return pd.Series(dtype=float)
            return ms.rank(pct=True).sort_values(ascending=False)
        elif method=='mom126_ex42':
            # 6-2 month momentum
            ms=pd.Series(tm_mom_2_6).dropna()
            if len(ms)<3:return pd.Series(dtype=float)
            return ms.rank(pct=True).sort_values(ascending=False)
        return pd.Series(dtype=float)
    def select_themes(score_series):
        sel=[];sc_cnt={}
        for th in score_series.index:
            if th not in elig:continue
            s=theme_sector.get(th,'Unk')
            if sc_cnt.get(s,0)>=SEC_MAX:continue
            sel.append(th);sc_cnt[s]=sc_cnt.get(s,0)+1
            if len(sel)>=TOP_T:break
        return sel
    def score_stock(tk, th, method):
        """Score a stock for a given theme using the specified method."""
        if method=='alpha63_shrink':
            tkd=sub63[(sub63['theme']==th)&(sub63['ticker']==tk)].sort_values('date')
            if len(tkd)<10:return -999
            a,b,r2=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
            return a*shrink_r2(r2) if np.isfinite(a) else -999
        elif method=='alpha_12_7':
            tkd=sub_7_12[(sub_7_12['theme']==th)&(sub_7_12['ticker']==tk)].sort_values('date')
            if len(tkd)<10:return -999
            a,b,r2=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
            return a*shrink_r2(r2) if np.isfinite(a) else -999
        elif method=='alpha_6_2':
            tkd=sub_2_6[(sub_2_6['theme']==th)&(sub_2_6['ticker']==tk)].sort_values('date')
            if len(tkd)<10:return -999
            a,b,r2=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
            return a*shrink_r2(r2) if np.isfinite(a) else -999
        elif method=='resid_alpha63':
            tkd=sub63[(sub63['theme']==th)&(sub63['ticker']==tk)].sort_values('date')
            if len(tkd)<10:return -999
            mk=mkt_ret.reindex(tkd['date']).values
            resid=ols_resid(tkd['ret'].values, mk.reshape(-1,1))
            return cumret(resid) if np.any(np.isfinite(resid)) else -999
        elif method=='resid_alpha_12_7':
            tkd=sub_7_12[(sub_7_12['theme']==th)&(sub_7_12['ticker']==tk)].sort_values('date')
            if len(tkd)<10:return -999
            mk=mkt_ret.reindex(tkd['date']).values
            resid=ols_resid(tkd['ret'].values, mk.reshape(-1,1))
            return cumret(resid) if np.any(np.isfinite(resid)) else -999
        elif method=='alpha63_volpen':
            tkd=sub63[(sub63['theme']==th)&(sub63['ticker']==tk)].sort_values('date')
            if len(tkd)<10:return -999
            a,b,r2=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
            if not np.isfinite(a):return -999
            # Penalize formation-period volatility
            vol63=float(np.std(tkd['ret'].values,ddof=1)*np.sqrt(252))
            score=a*shrink_r2(r2)
            return score / max(vol63, 0.1)  # vol-adjusted alpha
        elif method=='resid_alpha_12_7_volpen':
            tkd=sub_7_12[(sub_7_12['theme']==th)&(sub_7_12['ticker']==tk)].sort_values('date')
            if len(tkd)<10:return -999
            mk=mkt_ret.reindex(tkd['date']).values
            resid=ols_resid(tkd['ret'].values, mk.reshape(-1,1))
            if not np.any(np.isfinite(resid)):return -999
            vol=float(np.std(tkd['ret'].values,ddof=1)*np.sqrt(252))
            return cumret(resid) / max(vol, 0.1)
        return -999
    # === Run each variant ===
    for vname, vcfg in VARIANTS.items():
        theme_scores = build_theme_score(vcfg['theme'])
        if len(theme_scores)<3:
            results[vname].extend([0.0]*len(hold_dates));continue
        sel = select_themes(theme_scores)
        if not sel:
            results[vname].extend([0.0]*len(hold_dates));continue
        # Stock selection
        port=[];used=set()
        for th in sel:
            sub_for_stock = sub63 if '63' in vcfg['stock'] or vcfg['stock']=='alpha63_shrink' else (sub_7_12 if '12_7' in vcfg['stock'] else sub_2_6)
            ths=sub_for_stock[(sub_for_stock['theme']==th)&sub_for_stock['ret'].notna()]
            tks=ths['ticker'].unique()
            if len(tks)<MIN_M:continue
            scores={tk:score_stock(tk,th,vcfg['stock']) for tk in tks}
            for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
                if tk not in used and sc>-999:
                    # W5b momentum data
                    r63=tm_mom63.get(th,np.nan);r126=tm_mom126.get(th,np.nan)
                    r252=tm_mom252.get(th,np.nan)
                    td21v=sub63[sub63['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                    r21=cumret(td21v[-21:]) if len(td21v)>=21 else np.nan
                    r252ex1m=((1+r252)/(1+r21)-1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21)>1e-8 else np.nan
                    port.append({'tk':tk,'th':th,'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                    used.add(tk);break
        if not port:
            results[vname].extend([0.0]*len(hold_dates));continue
        ws=w5b_w(port,cap=0.30)
        tickers=[p['tk'] for p in port]
        ww=pd.Series(ws,index=tickers)
        d=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ww,axis=1).sum(axis=1)
        results[vname].extend(d.values.tolist())
    if (pos+1)%20==0:print(f'  [{pos+1}/{len(rebal_idx)-1}]')

# === Results ===
print(f'\nBT done in {time.time()-t0:.1f}s')
eq_dates=tk_wide.index[-len(results['A_current']):]
spy_aligned=spy_ret.reindex(eq_dates).fillna(0)
bear_mask=(spy_aligned.rolling(63).apply(lambda x:np.expm1(np.log1p(x).sum()),raw=True)<=0).fillna(False)

print(f'\n{"="*120}')
print(f'  BEAR RESOLUTION: Phase 1-2 Results')
print(f'{"="*120}')
print(f'  {"Variant":<18} {"CAGR":>7} {"Shrp":>6} {"Sort":>5} {"Cal":>5} {"MaxDD":>7} {"BearS":>7} {"Term":>7}')
print(f'  {"-"*75}')
base_s = None
for vname in VARIANTS:
    dr=results[vname]; s=calc_stats(dr)
    if not s:print(f'  {vname:<18} (insufficient data)');continue
    if base_s is None: base_s=s
    bs_arr=np.array(dr)[bear_mask.values[:len(dr)]]
    bear_s=calc_stats(bs_arr).get('sharpe',0) if len(bs_arr)>20 else 0
    print(f'  {vname:<18} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} {s["maxdd"]:>6.1%} {bear_s:>+6.3f} {s["terminal"]:>6.1f}x')

# Delta vs baseline
print(f'\n  Delta vs A_current:')
a_s=calc_stats(results['A_current'])
a_bear=calc_stats(np.array(results['A_current'])[bear_mask.values[:len(results['A_current'])]]).get('sharpe',0)
for vname in list(VARIANTS.keys())[1:]:
    dr=results[vname];s=calc_stats(dr)
    if not s:continue
    bs_arr=np.array(dr)[bear_mask.values[:len(dr)]]
    bear_s=calc_stats(bs_arr).get('sharpe',0) if len(bs_arr)>20 else 0
    print(f'    {vname:<18} ΔCAGR={s["cagr"]-a_s["cagr"]:>+6.1%} ΔShrp={s["sharpe"]-a_s["sharpe"]:>+6.3f} ΔMaxDD={s["maxdd"]-a_s["maxdd"]:>+5.1%} ΔBear={bear_s-a_bear:>+6.3f}')

# Annual
print(f'\n  2022 (worst year):')
for vname in VARIANTS:
    dr=results[vname];eq=pd.Series(np.cumprod(1+np.array(dr)),index=eq_dates)
    ann={str(d.year):round(float(r),4) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
    y22=ann.get('2022',0)
    print(f'    {vname:<18} 2022={y22:>+6.1%}')

print(f'\n  Total elapsed: {time.time()-t0:.1f}s')
print('  === BEAR RESOLUTION BT COMPLETE ===')
