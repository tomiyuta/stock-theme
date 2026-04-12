#!/usr/bin/env python3
"""Bear Resolution Phase 4: Fmix blend + partial de-theming + quality
6 variants from ChatGPT spec:
  Fmix_50:  0.5×(6-2) + 0.5×(12-7)
  Fmix_75:  0.75×(6-2) + 0.25×(12-7)
  Fmix_25:  0.25×(6-2) + 0.75×(12-7)
  Fmix_50_rel02: subtract 0.2×theme z-score
  Fmix_50_rel04: subtract 0.4×theme z-score
  Fmix_50_Q: 0.7×price + 0.3×quality (ROE/profitability proxy)
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

# Variants: baseline + Phase 3 winners + Phase 4 new
VARIANTS = {
    'A_current':   {'mix_6_2': 0.0, 'mix_12_7': 0.0, 'use_current': True, 'lambda_rel': 0, 'quality_w': 0},
    'F_stk6_2':    {'mix_6_2': 1.0, 'mix_12_7': 0.0, 'use_current': False,'lambda_rel': 0, 'quality_w': 0},
    'G_both12_7':  {'mix_6_2': 0.0, 'mix_12_7': 1.0, 'use_current': False,'lambda_rel': 0, 'quality_w': 0},
    'Fmix_75':     {'mix_6_2': 0.75,'mix_12_7': 0.25,'use_current': False,'lambda_rel': 0, 'quality_w': 0},
    'Fmix_50':     {'mix_6_2': 0.50,'mix_12_7': 0.50,'use_current': False,'lambda_rel': 0, 'quality_w': 0},
    'Fmix_25':     {'mix_6_2': 0.25,'mix_12_7': 0.75,'use_current': False,'lambda_rel': 0, 'quality_w': 0},
    'Fmix50_r02':  {'mix_6_2': 0.50,'mix_12_7': 0.50,'use_current': False,'lambda_rel': 0.2,'quality_w': 0},
    'Fmix50_r04':  {'mix_6_2': 0.50,'mix_12_7': 0.50,'use_current': False,'lambda_rel': 0.4,'quality_w': 0},
    'Fmix50_Q':    {'mix_6_2': 0.50,'mix_12_7': 0.50,'use_current': False,'lambda_rel': 0, 'quality_w': 0.3},
}
WARMUP=252;REBAL=20;MIN_M=4;TOP_T=10;SEC_MAX=3
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1:rebal_idx.append(len(dates_all)-1)
results={k:[] for k in VARIANTS}
print(f'Rebalance: {len(rebal_idx)-1} periods | {len(VARIANTS)} variants')

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos];j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1]);dt21=set(dates_all[max(0,j-20):j+1])
    dt126=set(dates_all[max(0,j-125):j+1]);dt252=set(dates_all[max(0,j-251):j+1])
    dt_7_12=set(dates_all[max(0,j-251):max(0,j-146)])
    dt_2_6=set(dates_all[max(0,j-125):max(0,j-41)])
    sub63=panel[panel['date'].isin(dt63)]
    sub_7_12=panel[panel['date'].isin(dt_7_12)]
    sub_2_6=panel[panel['date'].isin(dt_2_6)]
    sub126=panel[panel['date'].isin(dt126)];sub252=panel[panel['date'].isin(dt252)]
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme scoring (current: mom63+decel) + theme momentum for different horizons
    tm=sub63.groupby('theme')['ticker'].nunique();elig=tm[tm>=MIN_M].index.tolist()
    tm_mom63={};dc={};tm_mom_7_12={};tm_mom126={};tm_mom252={}
    for th in elig:
        td=sub63[sub63['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=42:tm_mom63[th]=cumret(td)
        if len(td)>=63:
            r021=cumret(td[-21:]);r2142=cumret(td[-42:-21]);r4263=cumret(td[-63:-42])
            if all(np.isfinite([r021,r2142,r4263])):dc[th]=-(r021-0.5*(r2142+r4263))
        td712=sub_7_12[sub_7_12['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td712)>=42:tm_mom_7_12[th]=cumret(td712)
        td126=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126)>=63:tm_mom126[th]=cumret(td126)
        td252=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td252)>=126:tm_mom252[th]=cumret(td252)
    # Theme selection (current method for all variants — theme layer preserved)
    ms=pd.Series(tm_mom63).dropna().sort_values(ascending=False)
    dcs=pd.Series(dc);common=list(set(ms.index)&set(dcs.index))
    if not common:
        for k in results:results[k].extend([0.0]*len(hold_dates));continue
    ts=pd.DataFrame({'mom':ms[common],'dec':dcs[common]})
    ts['score']=0.70*ts['mom'].rank(pct=True)+0.30*ts['dec'].rank(pct=True)
    ts=ts.sort_values('score',ascending=False)
    sel=[];sc_cnt={}
    for th in ts.index:
        s=theme_sector.get(th,'Unk')
        if sc_cnt.get(s,0)>=SEC_MAX:continue
        sel.append(th);sc_cnt[s]=sc_cnt.get(s,0)+1
        if len(sel)>=TOP_T:break
    # For G_both12_7: use 12-7 theme scoring instead
    ms712=pd.Series(tm_mom_7_12).dropna()
    sel_712=[]
    if len(ms712)>=3:
        sc_cnt2={}
        for th in ms712.sort_values(ascending=False).index:
            if th not in elig:continue
            s2=theme_sector.get(th,'Unk')
            if sc_cnt2.get(s2,0)>=SEC_MAX:continue
            sel_712.append(th);sc_cnt2[s2]=sc_cnt2.get(s2,0)+1
            if len(sel_712)>=TOP_T:break
    # Per-variant stock scoring
    for vname, vcfg in VARIANTS.items():
        use_sel = sel_712 if vname=='G_both12_7' and sel_712 else sel
        port=[];used=set()
        for th in use_sel:
            # Score stocks per theme
            tks_63=sub63[(sub63['theme']==th)&sub63['ret'].notna()]['ticker'].unique()
            tks_26=sub_2_6[(sub_2_6['theme']==th)&sub_2_6['ret'].notna()]['ticker'].unique()
            tks_712=sub_7_12[(sub_7_12['theme']==th)&sub_7_12['ret'].notna()]['ticker'].unique()
            all_tks=set(tks_63)|set(tks_26)|set(tks_712)
            if len(all_tks)<MIN_M:continue
            scores={}
            for tk in all_tks:
                if vcfg['use_current']:
                    # Current: α63*shrink_r2
                    tkd=sub63[(sub63['theme']==th)&(sub63['ticker']==tk)].sort_values('date')
                    if len(tkd)<10:continue
                    a,b,r2=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
                    scores[tk]=a*shrink_r2(r2) if np.isfinite(a) else -999
                else:
                    # Blended: mix_6_2 × α(6-2) + mix_12_7 × α(12-7)
                    s_62=0;s_712=0
                    if vcfg['mix_6_2']>0:
                        tkd=sub_2_6[(sub_2_6['theme']==th)&(sub_2_6['ticker']==tk)].sort_values('date')
                        if len(tkd)>=10:
                            a,b,r2=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
                            s_62=a*shrink_r2(r2) if np.isfinite(a) else 0
                    if vcfg['mix_12_7']>0:
                        tkd=sub_7_12[(sub_7_12['theme']==th)&(sub_7_12['ticker']==tk)].sort_values('date')
                        if len(tkd)>=10:
                            a,b,r2=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
                            s_712=a*shrink_r2(r2) if np.isfinite(a) else 0
                    sc = vcfg['mix_6_2']*s_62 + vcfg['mix_12_7']*s_712
                    # Partial de-theming: subtract λ × theme z-score
                    if vcfg['lambda_rel']>0:
                        theme_mom = tm_mom63.get(th, 0)
                        sc -= vcfg['lambda_rel'] * theme_mom
                    # Quality overlay: profitability proxy (Sharpe of returns = consistency)
                    if vcfg['quality_w']>0:
                        tkd_full=sub63[(sub63['theme']==th)&(sub63['ticker']==tk)].sort_values('date')
                        if len(tkd_full)>=20:
                            rets=tkd_full['ret'].dropna().values
                            # Quality proxy: mean/vol ratio × positive-day ratio
                            q_sharpe=float(np.mean(rets)/np.std(rets,ddof=1)) if np.std(rets)>1e-8 else 0
                            q_hitrate=float(np.mean(rets>0))
                            q_score = q_sharpe * q_hitrate
                            sc = (1-vcfg['quality_w'])*sc + vcfg['quality_w']*q_score
                    scores[tk] = sc if sc != 0 else -999
            # Pick best unused
            for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
                if tk not in used and sc>-999:
                    r63=tm_mom63.get(th,np.nan);r126=tm_mom126.get(th,np.nan)
                    r252=tm_mom252.get(th,np.nan)
                    td21v=sub63[sub63['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                    r21=cumret(td21v[-21:]) if len(td21v)>=21 else np.nan
                    r252ex1m=((1+r252)/(1+r21)-1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21)>1e-8 else np.nan
                    port.append({'tk':tk,'th':th,'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                    used.add(tk);break
        if not port:results[vname].extend([0.0]*len(hold_dates));continue
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
print(f'  BEAR RESOLUTION: Phase 4 Results')
print(f'{"="*120}')
print(f'  {"Variant":<16} {"CAGR":>7} {"Shrp":>6} {"Sort":>5} {"Cal":>5} {"MaxDD":>7} {"BearS":>7} {"Term":>7}')
print(f'  {"-"*70}')
a_s=calc_stats(results['A_current'])
a_bear=calc_stats(np.array(results['A_current'])[bear_mask.values[:len(results['A_current'])]]).get('sharpe',0)
for vname in VARIANTS:
    dr=results[vname];s=calc_stats(dr)
    if not s:print(f'  {vname:<16} (insufficient data)');continue
    bs_arr=np.array(dr)[bear_mask.values[:len(dr)]]
    bear_s=calc_stats(bs_arr).get('sharpe',0) if len(bs_arr)>20 else 0
    print(f'  {vname:<16} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} {s["maxdd"]:>6.1%} {bear_s:>+6.3f} {s["terminal"]:>6.1f}x')

print(f'\n  Delta vs A_current:')
for vname in list(VARIANTS.keys())[1:]:
    dr=results[vname];s=calc_stats(dr)
    if not s:continue
    bs_arr=np.array(dr)[bear_mask.values[:len(dr)]]
    bear_s=calc_stats(bs_arr).get('sharpe',0) if len(bs_arr)>20 else 0
    print(f'    {vname:<16} ΔCAGR={s["cagr"]-a_s["cagr"]:>+6.1%} ΔShrp={s["sharpe"]-a_s["sharpe"]:>+6.3f} ΔMaxDD={s["maxdd"]-a_s["maxdd"]:>+5.1%} ΔBear={bear_s-a_bear:>+6.3f}')

print(f'\n  2022:')
for vname in VARIANTS:
    dr=results[vname];eq=pd.Series(np.cumprod(1+np.array(dr)),index=eq_dates)
    ann={str(d.year):round(float(r),4) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
    print(f'    {vname:<16} 2022={ann.get("2022",0):>+6.1%} 2025={ann.get("2025",0):>+6.1%}')

print(f'\n  Total elapsed: {time.time()-t0:.1f}s')
print('  === PHASE 4 COMPLETE ===')
