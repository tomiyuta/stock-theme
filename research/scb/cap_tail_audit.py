#!/usr/bin/env python3
"""B_CAP_GRID + C_TAIL_DECOMP combined audit.
B1: Static cap ladder (8 levels)
C1: Best/worst day removal
C4: ES/CED block
C5: ES contribution
C7: Regime-conditioned tail map
"""
import pandas as pd, numpy as np, time, warnings, json
from scipy import stats as sp_stats
from scipy.stats import norm
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
print(f'Panel loaded: {len(panel):,} rows | {time.time()-t0:.1f}s')

def cumret(a):
    a=np.asarray(a,dtype=float);a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
def ols_ab(y,x):
    mask=np.isfinite(y)&np.isfinite(x);y,x=y[mask],x[mask];n=len(y)
    if n<10:return np.nan,np.nan,np.nan
    xm,ym=x.mean(),y.mean();xd=x-xm;vx=np.dot(xd,xd)/(n-1)
    if vx<1e-12:return np.nan,np.nan,np.nan
    b=np.dot(xd,y-ym)/(n-1)/vx;a=ym-b*xm
    ss_res=float(np.sum((y-a-b*x)**2));ss_tot=float(np.sum((y-ym)**2))
    r2=1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n,b,r2
def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0:return 0.0
    if r2v<0.10:return r2v*2
    if r2v<=0.50:return 0.20+(r2v-0.10)*2.0
    return 1.0
def w5b_w(port,cap=None):
    ws=[]
    for p in port:
        vals=[p.get('r63',np.nan),p.get('r126',np.nan),p.get('r252ex1m',np.nan)]
        valid=[v for v in vals if np.isfinite(v)]
        if len(valid)<2:ws.append(1.0)
        else:pc=sum(1 for v in valid if v>0);ar=np.mean([max(v,0) for v in valid]);ws.append(pc*(1+ar))
    wa=np.array(ws,dtype=float)
    if wa.sum()<=0:return np.ones(len(port))/len(port)
    wa=wa/wa.sum()
    if cap and cap<1.0:
        for _ in range(5):
            exc=np.maximum(wa-cap,0)
            if exc.sum()<1e-6:break
            under=wa<cap;wa=np.minimum(wa,cap)
            if under.any():wa[under]+=exc.sum()*(wa[under]/wa[under].sum())
            wa=wa/wa.sum()
    return wa
def calc_stats(arr):
    arr=np.array(arr);arr=arr[np.isfinite(arr)];n=len(arr);yrs=n/252
    if n<20:return None
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1;vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr);peak=np.maximum.accumulate(eq);maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    neg=arr[arr<0];dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    sk=float(sp_stats.skew(arr));ku=float(sp_stats.kurtosis(arr))
    e_max=norm.ppf(1-1/216)*np.sqrt(1/n)*np.sqrt(252)
    sr_std=np.sqrt((1+0.5*sharpe**2-sk*sharpe+(ku/4)*sharpe**2)/(n-1))*np.sqrt(252)
    dsr=1-norm.cdf((sharpe-e_max)/sr_std) if sr_std>0 else 1
    return {'cagr':cagr,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'dsr':1-dsr,'n':n,'skew':sk,'kurt':ku,'terminal':float(eq[-1])}

def run_engine(cap=None):
    """Run full BT with given cap. Returns daily_rets, weight_history, per_ticker_contrib."""
    WARMUP=126;REBAL=20;N=len(dates_all)
    rebal_idx=list(range(WARMUP,N,REBAL))
    if rebal_idx[-1]!=N-1:rebal_idx.append(N-1)
    daily_rets=[];wh=[];tk_contribs=[];prev_tks=set();turnover_list=[]
    for pos in range(len(rebal_idx)-1):
        j=rebal_idx[pos];j_next=rebal_idx[pos+1]
        dt63=set(dates_all[max(0,j-62):j+1]);dt21=set(dates_all[max(0,j-20):j+1])
        dt126=set(dates_all[max(0,j-125):j+1]);dt252=set(dates_all[max(0,j-251):j+1])
        sub=panel[panel['date'].isin(dt63)];sub126=panel[panel['date'].isin(dt126)];sub252=panel[panel['date'].isin(dt252)]
        tm=sub.groupby('theme')['ticker'].nunique();elig=tm[tm>=4].index.tolist()
        hold_dates=tk_wide.index[j+1:j_next+1]
        tm_mom={};dc={}
        for th in elig:
            td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            if len(td)>=63:tm_mom[th]=cumret(td)
            if len(td)>=63:
                r021=cumret(td[-21:]);r2142=cumret(td[-42:-21]);r4263=cumret(td[-63:-42])
                if all(np.isfinite([r021,r2142,r4263])):dc[th]=-(r021-0.5*(r2142+r4263))
        ms=pd.Series(tm_mom).dropna().sort_values(ascending=False)
        dcs=pd.Series(dc);common=list(set(ms.index)&set(dcs.index))
        if not common:daily_rets.extend([0.0]*len(hold_dates));continue
        ts=pd.DataFrame({'mom':ms[common],'dec':dcs[common]})
        ts['score']=0.70*ts['mom'].rank(pct=True)+0.30*ts['dec'].rank(pct=True)
        ts=ts.sort_values('score',ascending=False)
        sel=[];sc_cnt={}
        for th in ts.index:
            s=theme_sector.get(th,'Unk')
            if sc_cnt.get(s,0)>=3:continue
            sel.append(th);sc_cnt[s]=sc_cnt.get(s,0)+1
            if len(sel)>=10:break
        port=[];used=set()
        for th in sel:
            ths=sub[(sub['theme']==th)&sub['ret'].notna()];tks=ths['ticker'].unique()
            if len(tks)<4:continue
            scores={}
            for tk in tks:
                tkd=ths[ths['ticker']==tk].sort_values('date')
                a63,b63,r2_63=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
                shrk=shrink_r2(r2_63) if np.isfinite(r2_63) else 0
                scores[tk]=a63*shrk if np.isfinite(a63) else -999
            for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
                if tk not in used and sc>-999:
                    r63=tm_mom.get(th,np.nan)
                    td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                    r126=cumret(td126v) if len(td126v)>=63 else np.nan
                    td252v=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                    r252=cumret(td252v) if len(td252v)>=126 else np.nan
                    r21v=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                    r21=cumret(r21v[-21:]) if len(r21v)>=21 else np.nan
                    r252ex1m=((1+r252)/(1+r21)-1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21)>1e-8 else np.nan
                    port.append({'tk':tk,'th':th,'r63':r63,'r126':r126,'r252ex1m':r252ex1m});used.add(tk);break
        if not port:daily_rets.extend([0.0]*len(hold_dates));continue
        tickers=[p['tk'] for p in port];ws=w5b_w(port,cap=cap)
        wh.append({'tickers':tickers,'weights':ws.tolist(),'themes':[p['th'] for p in port]})
        curr=set(tickers);to=len(curr-prev_tks)+len(prev_tks-curr)
        turnover_list.append(to/(len(curr)+len(prev_tks)) if (len(curr)+len(prev_tks))>0 else 0)
        prev_tks=curr
        ww=pd.Series(ws,index=tickers)
        rets_block=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0)
        port_ret=rets_block.mul(ww,axis=1).sum(axis=1)
        daily_rets.extend(port_ret.values.tolist())
        for d in hold_dates:
            row={}
            for tk,w in zip(tickers,ws):
                r=rets_block.loc[d,tk] if tk in rets_block.columns else 0
                row[tk]=r*w
            tk_contribs.append(row)
    return np.array(daily_rets),turnover_list,wh,tk_contribs

# === SPY benchmark ===
import yfinance as yf
spy=yf.download('SPY',start='2019-01-01',end='2026-12-31',progress=False)
spy_close=(spy['Adj Close'] if 'Adj Close' in spy.columns else spy['Close']).squeeze()
spy_ret=spy_close.pct_change().dropna();spy_ret.index=spy_ret.index.tz_localize(None)
print(f'Setup done: {time.time()-t0:.1f}s')

# =====================================================================
# B1: STATIC CAP LADDER
# =====================================================================
print('\n' + '='*120)
print('  B1: STATIC CAP LADDER')
print('='*120)
CAPS = [None, 0.50, 0.40, 0.35, 0.30, 0.25, 0.20, 0.15]
cap_results = {}
cap_daily = {}
cap_wh = {}
cap_contribs = {}
for c in CAPS:
    label = 'nocap' if c is None else f'cap{int(c*100)}'
    dr, to, wh, contribs = run_engine(cap=c)
    s = calc_stats(dr)
    if not s: continue
    # Concentration
    max_ws = [max(w['weights']) for w in wh] if wh else [0]
    top3_ws = [sum(sorted(w['weights'],reverse=True)[:3]) for w in wh] if wh else [0]
    hhis = [sum(x**2 for x in w['weights']) for w in wh] if wh else [0]
    avg_to = np.mean(to) if to else 0
    # Bear Sharpe
    eq_dates = tk_wide.index[-len(dr):]
    spy_al = spy_ret.reindex(eq_dates).fillna(0)
    spy63 = spy_al.rolling(63).apply(lambda x:np.expm1(np.log1p(x).sum()),raw=True)
    bear = spy63 <= 0
    bear_rets = dr[bear.reindex(eq_dates).fillna(False).values]
    bear_s = calc_stats(bear_rets)
    s['bear_sharpe'] = bear_s['sharpe'] if bear_s else 0
    s['top1_mean'] = np.mean(max_ws); s['top1_p95'] = np.percentile(max_ws,95)
    s['top3_mean'] = np.mean(top3_ws); s['hhi_mean'] = np.mean(hhis)
    s['eff_n'] = 1/np.mean(hhis) if np.mean(hhis)>0 else 10
    s['turnover'] = avg_to * (252/20)
    s['bind_rate'] = sum(1 for w in wh if max(w['weights'])>=((c or 1)-0.001))/max(len(wh),1) if c else 0
    cap_results[label] = s; cap_daily[label] = dr; cap_wh[label] = wh; cap_contribs[label] = contribs
    print(f'  {label:7s} CAGR={s["cagr"]:>7.1%} Shrp={s["sharpe"]:>6.3f} Sort={s["sortino"]:>5.2f} '
          f'Cal={s["calmar"]:>5.2f} MaxDD={s["maxdd"]:>6.1%} DSR={s["dsr"]:>5.1%} '
          f'BearS={s["bear_sharpe"]:>+6.3f} top1={s["top1_mean"]:>5.1%} HHI={s["hhi_mean"]:>.3f} '
          f'effN={s["eff_n"]:>4.1f} TO={s["turnover"]:>6.0%} bind={s["bind_rate"]:>4.0%}')

# =====================================================================
# C1: BEST/WORST DAY REMOVAL (on cap30 winner)
# =====================================================================
print('\n' + '='*120)
print('  C1: BEST/WORST DAY REMOVAL SENSITIVITY (cap30)')
print('='*120)
ref_dr = cap_daily.get('cap30', cap_daily.get('nocap'))
ref_s = calc_stats(ref_dr)
sorted_idx = np.argsort(ref_dr)
for label, remove_idx in [
    ('top1d', sorted_idx[-1:]), ('top3d', sorted_idx[-3:]),
    ('top5d', sorted_idx[-5:]), ('top10d', sorted_idx[-10:]),
    ('bot1d', sorted_idx[:1]), ('bot3d', sorted_idx[:3]),
    ('bot5d', sorted_idx[:5]), ('bot10d', sorted_idx[:10]),
]:
    mask = np.ones(len(ref_dr), dtype=bool); mask[remove_idx] = False
    s = calc_stats(ref_dr[mask])
    if s:
        cr = s['cagr']/ref_s['cagr'] if ref_s['cagr']!=0 else 0
        sr = s['sharpe']/ref_s['sharpe'] if ref_s['sharpe']!=0 else 0
        clr = s['calmar']/ref_s['calmar'] if ref_s['calmar']!=0 else 0
        print(f'  {label:7s} CAGR={s["cagr"]:>7.1%}({cr:>5.0%}) Shrp={s["sharpe"]:>6.3f}({sr:>5.0%}) Cal={s["calmar"]:>5.2f}({clr:>5.0%}) MaxDD={s["maxdd"]:>6.1%}')
# Monthly removal
ref_ser = pd.Series(ref_dr, index=tk_wide.index[-len(ref_dr):])
monthly = ref_ser.resample('ME').apply(lambda x: np.expm1(np.log1p(x).sum()))
for label, idx in [('top1m', monthly.nlargest(1).index), ('top3m', monthly.nlargest(3).index),
                   ('bot1m', monthly.nsmallest(1).index), ('bot3m', monthly.nsmallest(3).index)]:
    periods = idx.to_period('M')
    mask = np.array([d.to_period('M') not in periods for d in ref_ser.index])
    s = calc_stats(ref_dr[mask])
    if s:
        cr = s['cagr']/ref_s['cagr'] if ref_s['cagr']!=0 else 0
        print(f'  {label:7s} CAGR={s["cagr"]:>7.1%}({cr:>5.0%}) Shrp={s["sharpe"]:>6.3f} Cal={s["calmar"]:>5.2f} MaxDD={s["maxdd"]:>6.1%}')

# =====================================================================
# C4: ES / CED BLOCK
# =====================================================================
print('\n' + '='*120)
print('  C4: ES / CED BLOCK')
print('='*120)
for label in ['nocap','cap30','cap25','cap20']:
    dr = cap_daily.get(label)
    if dr is None: continue
    arr = dr[np.isfinite(dr)]
    # ES (Expected Shortfall)
    es95 = -np.mean(arr[arr <= np.percentile(arr, 5)]) * np.sqrt(252)
    es99 = -np.mean(arr[arr <= np.percentile(arr, 1)]) * np.sqrt(252)
    # CED (Conditional Expected Drawdown)
    eq = np.cumprod(1+arr); peak = np.maximum.accumulate(eq)
    dd_ser = (eq-peak)/peak
    ced90 = -np.mean(dd_ser[dd_ser <= np.percentile(dd_ser, 10)])
    ced95 = -np.mean(dd_ser[dd_ser <= np.percentile(dd_ser, 5)])
    ced_es = ced95 / es95 if es95 > 0 else 0
    print(f'  {label:7s} ES95={es95:.1%} ES99={es99:.1%} CED90={ced90:.1%} CED95={ced95:.1%} CED/ES={ced_es:.2f}')

# =====================================================================
# C5: ES CONTRIBUTION (cap30, per ticker)
# =====================================================================
print('\n' + '='*120)
print('  C5: ES CONTRIBUTION BY TICKER (cap30)')
print('='*120)
contribs = cap_contribs.get('cap30', [])
if contribs:
    cdf = pd.DataFrame(contribs).fillna(0)
    port_ret = cdf.sum(axis=1)
    threshold = np.percentile(port_ret, 5)
    tail_mask = port_ret <= threshold
    tail_contribs = cdf[tail_mask].mean()
    total_tail = tail_contribs.sum()
    if abs(total_tail) > 1e-10:
        es_share = (tail_contribs / total_tail * 100).sort_values()
        print(f'  ES95 tail days: {tail_mask.sum()} | Total tail mean: {total_tail:.4f}')
        print(f'  Top ES contributors (worst = largest share):')
        for tk, sh in es_share.head(5).items():
            print(f'    {tk:6s}: {sh:>6.1f}% of ES')
        print(f'  Top 3 ES share: {es_share.head(3).sum():.1f}%')
        print(f'  Top 5 ES share: {es_share.head(5).sum():.1f}%')

# =====================================================================
# C7: REGIME-CONDITIONED TAIL MAP (cap30)
# =====================================================================
print('\n' + '='*120)
print('  C7: REGIME-CONDITIONED TAIL MAP (cap30)')
print('='*120)
dr30 = cap_daily.get('cap30', ref_dr)
eq_dates = tk_wide.index[-len(dr30):]
ser30 = pd.Series(dr30, index=eq_dates)
spy_al = spy_ret.reindex(eq_dates).fillna(0)
spy63 = spy_al.rolling(63).apply(lambda x:np.expm1(np.log1p(x).sum()),raw=True)
spy_vol21 = spy_al.rolling(21).std()*np.sqrt(252)
vol_med = spy_vol21.median()
spy21 = spy_al.rolling(21).apply(lambda x:np.expm1(np.log1p(x).sum()),raw=True)
rebound = (spy63<-0.05)&(spy21>0.03)
regimes = {
    'Bull+LowVol': (spy63>0)&(spy_vol21<=vol_med),
    'Bull+HighVol': (spy63>0)&(spy_vol21>vol_med),
    'Bear+LowVol': (spy63<=0)&(spy_vol21<=vol_med),
    'Bear+HighVol': (spy63<=0)&(spy_vol21>vol_med),
    'Rebound': rebound,
}
print(f'  {"Regime":<16} {"n":>5} {"CAGR":>8} {"Sharpe":>8} {"Skew":>6} {"Kurt":>6} {"ES95":>8} {"HitRate":>8}')
print(f'  {"-"*70}')
for rname, rmask in regimes.items():
    m = rmask.reindex(eq_dates).fillna(False)
    sub = ser30[m].values; n = len(sub)
    if n < 20:
        print(f'  {rname:<16} {n:>5} insufficient'); continue
    s = calc_stats(sub)
    es95 = -np.mean(sub[sub<=np.percentile(sub,5)])*np.sqrt(252) if len(sub[sub<=np.percentile(sub,5)])>0 else 0
    hit = np.mean(sub>0)
    sk = float(sp_stats.skew(sub)); ku = float(sp_stats.kurtosis(sub))
    if s: print(f'  {rname:<16} {n:>5} {s["cagr"]:>7.1%} {s["sharpe"]:>7.3f} {sk:>+5.1f} {ku:>5.0f} {es95:>7.1%} {hit:>7.1%}')

# Worst month regime concentration
monthly30 = ser30.resample('ME').apply(lambda x:np.expm1(np.log1p(x).sum()))
worst10pct = monthly30.nsmallest(max(1,int(len(monthly30)*0.10)))
spy63_m = spy63.resample('ME').last()
bear_months = spy63_m[spy63_m<=0].index.to_period('M')
worst_in_bear = sum(1 for d in worst10pct.index if d.to_period('M') in bear_months)
print(f'\n  Worst 10% months ({len(worst10pct)}): {worst_in_bear}/{len(worst10pct)} in bear regime ({worst_in_bear/max(len(worst10pct),1):.0%})')

# =====================================================================
# FINAL COMPOSITE SCOREBOARD
# =====================================================================
print('\n' + '='*120)
print('  COMPOSITE SCOREBOARD')
print('='*120)
print(f'  {"Cap":>7} {"CAGR":>7} {"Shrp":>6} {"Sort":>5} {"Cal":>5} {"MaxDD":>7} {"DSR":>5} {"BearS":>7} {"top1":>5} {"HHI":>5} {"effN":>4} {"TO":>6}')
print(f'  {"-"*85}')
for label in ['nocap','cap50','cap40','cap35','cap30','cap25','cap20','cap15']:
    s = cap_results.get(label)
    if not s: continue
    print(f'  {label:>7} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} '
          f'{s["maxdd"]:>6.1%} {s["dsr"]:>4.1%} {s["bear_sharpe"]:>+6.3f} {s["top1_mean"]:>4.1%} '
          f'{s["hhi_mean"]:>4.3f} {s["eff_n"]:>3.1f} {s["turnover"]:>5.0%}')
# Winner determination
best_composite = None; best_score = -999
for label, s in cap_results.items():
    perf = 0.30*(s['sharpe']/3 + s['calmar']/5 + s['cagr']/2) / 3
    risk = 0.30*(1+s['maxdd'])/0.6 + 0.30*(1-abs(s['bear_sharpe'])/2)
    tail = 0.20*s['dsr'] + 0.20*(1-s['hhi_mean']/0.3)
    comp = perf + risk + tail
    if comp > best_score: best_score=comp; best_composite=label
print(f'\n  COMPOSITE WINNER: {best_composite} (score={best_score:.3f})')
c30=cap_results.get('cap30',{}); c25=cap_results.get('cap25',{})
if c30 and c25:
    print(f'\n  cap30 vs cap25 head-to-head:')
    for m in ['cagr','sharpe','calmar','maxdd','dsr','bear_sharpe','top1_mean','hhi_mean']:
        v30=c30.get(m,0);v25=c25.get(m,0)
        better='cap30' if (v30>v25 if m!='maxdd' else v30>v25) else 'cap25'
        print(f'    {m:15s} cap30={v30:>+8.3f} cap25={v25:>+8.3f} → {better}')
print(f'\n  Total elapsed: {time.time()-t0:.1f}s')
with open('/Users/yutatomi/Downloads/stock-theme/research/scb/cap_tail_results.json','w') as f:
    json.dump({k:{kk:round(vv,6) if isinstance(vv,float) else vv for kk,vv in v.items()} for k,v in cap_results.items()},f,indent=2)
print('  === AUDIT COMPLETE ===')
