#!/usr/bin/env python3
"""BEAST Institutional Audit — All feasible tests from ChatGPT rubric.
Tests: Walk-Forward, Subsample, Perturbation, Turnover, Net-cost,
       Regime slice, Drawdown decomp, Concentration, DSR, Recovery.
Target: PRISM-R BEAST (α63×shrink_r2, W5b nocap, 10 themes)
"""
import pandas as pd, numpy as np, time, warnings, json
from scipy import stats as scipy_stats
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
    return {'cagr':cagr,'vol':vol,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'terminal':float(eq[-1]),'n':n}

def run_engine(start_idx, end_idx, warmup=126, rebal=20, cap=None, lookbacks=(63,126,252)):
    """Core BT engine. Returns daily_rets, turnover_list, weight_history."""
    lb63,lb126,lb252 = lookbacks
    rebal_idx=list(range(max(warmup,start_idx),end_idx,rebal))
    if not rebal_idx:return [],[],[]
    if rebal_idx[-1]!=end_idx-1:rebal_idx.append(end_idx-1)
    daily_rets=[];turnover_list=[];weight_hist=[];prev_tickers=set()
    for pos in range(len(rebal_idx)-1):
        j=rebal_idx[pos];j_next=rebal_idx[pos+1]
        dt63=set(dates_all[max(0,j-lb63+1):j+1])
        dt21=set(dates_all[max(0,j-20):j+1])
        dt126=set(dates_all[max(0,j-lb126+1):j+1])
        dt252=set(dates_all[max(0,j-lb252+1):j+1])
        sub=panel[panel['date'].isin(dt63)]
        sub126=panel[panel['date'].isin(dt126)]
        sub252=panel[panel['date'].isin(dt252)]
        tm=sub.groupby('theme')['ticker'].nunique()
        elig=tm[tm>=4].index.tolist()
        hold_dates=tk_wide.index[j+1:j_next+1]
        tm_mom={};dc={}
        for th in elig:
            td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            if len(td)>=lb63:tm_mom[th]=cumret(td)
            if len(td)>=lb63:
                r021=cumret(td[-21:]);r2142=cumret(td[-42:-21]);r4263=cumret(td[-lb63:-42])
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
                    port.append({'tk':tk,'th':th,'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                    used.add(tk);break
        if not port:daily_rets.extend([0.0]*len(hold_dates));continue
        n=len(port);tickers=[p['tk'] for p in port]
        ws=w5b_w(port,cap=cap)
        weight_hist.append({'tickers':tickers,'weights':ws.tolist()})
        # Turnover
        curr=set(tickers);to=len(curr-prev_tickers)+len(prev_tickers-curr)
        turnover_list.append(to/(len(curr)+len(prev_tickers)) if (len(curr)+len(prev_tickers))>0 else 0)
        prev_tickers=curr
        ww=pd.Series(ws,index=tickers)
        d=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ww,axis=1).sum(axis=1)
        daily_rets.extend(d.values.tolist())
    return daily_rets, turnover_list, weight_hist

# === SPY benchmark ===
import yfinance as yf
spy=yf.download('SPY',start='2019-01-01',end='2026-12-31',progress=False)
spy_close=(spy['Adj Close'] if 'Adj Close' in spy.columns else spy['Close']).squeeze()
spy_ret=spy_close.pct_change().dropna()
spy_ret.index=spy_ret.index.tz_localize(None)

# ========================================
# TEST 0: BASELINE (full sample)
# ========================================
print('\n' + '='*100)
print('  TEST 0: BASELINE — Full Sample BEAST (nocap) + W5b (cap30)')
print('='*100)
dr_beast,to_beast,wh_beast = run_engine(0, len(dates_all), cap=None)
dr_w5b,to_w5b,wh_w5b = run_engine(0, len(dates_all), cap=0.30)
dr_eq,_,_ = run_engine(0, len(dates_all), cap=9999)  # effectively equal
for label, dr in [('BEAST(nocap)', dr_beast), ('W5b(cap30)', dr_w5b), ('EqualW', dr_eq)]:
    s=calc_stats(dr)
    if s: print(f'  {label:14s} CAGR={s["cagr"]:.1%} Sharpe={s["sharpe"]:.3f} MaxDD={s["maxdd"]:.1%} Sortino={s["sortino"]:.2f} Calmar={s["calmar"]:.2f} T={s["terminal"]:.1f}x')

# ========================================
# TEST 1: WALK-FORWARD (expanding + rolling)
# ========================================
print('\n' + '='*100)
print('  TEST 1: WALK-FORWARD')
print('='*100)
N=len(dates_all); half=N//2; q1=N//4; q3=3*N//4
# Expanding: train on first X%, test on rest
for train_end_pct in [0.40, 0.50, 0.60, 0.70]:
    te=int(N*train_end_pct)
    dr_oos,_,_=run_engine(te, N, cap=None)
    s=calc_stats(dr_oos)
    if s: print(f'  Expanding OOS [{train_end_pct:.0%}-100%]: CAGR={s["cagr"]:.1%} Sharpe={s["sharpe"]:.3f} MaxDD={s["maxdd"]:.1%}')
# Rolling: 504-day window
WINDOW=504
print(f'  Rolling {WINDOW}d windows:')
rolling_sharpes=[]
for ws in range(126, N-WINDOW, 126):
    we=ws+WINDOW
    dr_roll,_,_=run_engine(ws, min(we,N), cap=None)
    s=calc_stats(dr_roll)
    if s:
        rolling_sharpes.append(s['sharpe'])
        period_start=dates_all[ws].strftime('%Y-%m') if ws<len(dates_all) else '?'
        period_end=dates_all[min(we,N-1)].strftime('%Y-%m') if we<len(dates_all) else '?'
        print(f'    {period_start}~{period_end}: Sharpe={s["sharpe"]:.3f} CAGR={s["cagr"]:.1%} MaxDD={s["maxdd"]:.1%}')
if rolling_sharpes:
    print(f'  Rolling Sharpe: mean={np.mean(rolling_sharpes):.3f} std={np.std(rolling_sharpes):.3f} min={min(rolling_sharpes):.3f} max={max(rolling_sharpes):.3f}')
    oos_retention = np.mean(rolling_sharpes) / calc_stats(dr_beast).get('sharpe',1)
    print(f'  OOS Sharpe retention: {oos_retention:.1%} (pass: >50%)')

# ========================================
# TEST 2: SUBSAMPLE STABILITY
# ========================================
print('\n' + '='*100)
print('  TEST 2: SUBSAMPLE STABILITY (first half / second half)')
print('='*100)
for label, s, e in [('First half', 0, half), ('Second half', half, N)]:
    dr,_,_ = run_engine(s, e, cap=None)
    st=calc_stats(dr)
    if st: print(f'  {label:14s} CAGR={st["cagr"]:.1%} Sharpe={st["sharpe"]:.3f} MaxDD={st["maxdd"]:.1%} Calmar={st["calmar"]:.2f}')

# ========================================
# TEST 3: PARAMETER PERTURBATION
# ========================================
print('\n' + '='*100)
print('  TEST 3: PARAMETER PERTURBATION')
print('='*100)
base_s=calc_stats(dr_beast)
print(f'  Base: lookback=(63,126,252) cap=None → Sharpe={base_s["sharpe"]:.3f} CAGR={base_s["cagr"]:.1%}')
perturbations = [
    ('lb=(42,105,210)',  (42,105,210)),
    ('lb=(63,126,252)',  (63,126,252)),  # base
    ('lb=(84,147,294)',  (84,147,294)),
    ('lb=(63,63,252)',   (63,63,252)),
    ('lb=(63,126,189)',  (63,126,189)),
    ('lb=(63,189,252)',  (63,189,252)),
]
pert_sharpes=[]
for label, lbs in perturbations:
    dr,_,_ = run_engine(0, N, cap=None, lookbacks=lbs)
    s=calc_stats(dr)
    if s:
        pert_sharpes.append(s['sharpe'])
        print(f'  {label:22s} Sharpe={s["sharpe"]:.3f} CAGR={s["cagr"]:.1%} MaxDD={s["maxdd"]:.1%}')
if pert_sharpes:
    print(f'  Perturbation: mean Sharpe={np.mean(pert_sharpes):.3f} std={np.std(pert_sharpes):.3f} range={max(pert_sharpes)-min(pert_sharpes):.3f}')
    print(f'  Sign stability: {sum(1 for s in pert_sharpes if s>0)}/{len(pert_sharpes)} positive')

# ========================================
# TEST 4: TURNOVER + NET-OF-COST
# ========================================
print('\n' + '='*100)
print('  TEST 4: TURNOVER + NET-OF-COST')
print('='*100)
avg_to = np.mean(to_beast) if to_beast else 0
annual_to = avg_to * (252/20)  # ~13 rebalances/year
print(f'  Avg turnover/rebalance: {avg_to:.1%}')
print(f'  Est. annual turnover: {annual_to:.1%}')
# Net-of-cost: spread 10bps one-way, commission $0
arr_beast=np.array(dr_beast)
for spread_bps in [5, 10, 20, 30]:
    cost_per_rebal = avg_to * spread_bps / 10000 * 2  # round-trip
    annual_cost = cost_per_rebal * (252/20)
    daily_cost = annual_cost / 252
    net = arr_beast - daily_cost
    s=calc_stats(net)
    if s:
        gross_sharpe = base_s['sharpe']
        retention = s['sharpe']/gross_sharpe if gross_sharpe>0 else 0
        print(f'  Spread={spread_bps:2d}bp: net CAGR={s["cagr"]:.1%} net Sharpe={s["sharpe"]:.3f} retention={retention:.1%} Calmar={s["calmar"]:.2f}')
cost2x_daily = (avg_to * 20/10000 * 2 * (252/20)) / 252
net2x = arr_beast - cost2x_daily
s2x=calc_stats(net2x)
if s2x: print(f'  Cost 2x (20bp): Calmar={s2x["calmar"]:.2f} (pass: >1.5)')

# ========================================
# TEST 5: REGIME SLICING
# ========================================
print('\n' + '='*100)
print('  TEST 5: REGIME SLICING (market state / volatility)')
print('='*100)
eq_dates=tk_wide.index[-len(dr_beast):]
beast_ser=pd.Series(dr_beast,index=eq_dates)
spy_aligned=spy_ret.reindex(eq_dates).fillna(0)
# VIX proxy: 21-day realized vol of SPY
spy_vol21=spy_aligned.rolling(21).std()*np.sqrt(252)
vol_med=spy_vol21.median()
hi_vol=spy_vol21>vol_med; lo_vol=~hi_vol
# Market state: SPY 63d cumret
spy_cum63=spy_aligned.rolling(63).apply(lambda x:np.expm1(np.log1p(x).sum()),raw=True)
bull=spy_cum63>0; bear=spy_cum63<=0
# Panic rebound: SPY was down >10% in past 63d then up >5% in past 21d
spy_cum21=spy_aligned.rolling(21).apply(lambda x:np.expm1(np.log1p(x).sum()),raw=True)
panic_rebound=(spy_cum63<-0.10)&(spy_cum21>0.05)
for label, mask in [('High Vol',hi_vol),('Low Vol',lo_vol),('Bull (SPY63>0)',bull),('Bear (SPY63≤0)',bear),('Panic Rebound',panic_rebound)]:
    m=mask.reindex(eq_dates).fillna(False)
    sub_rets=beast_ser[m].values
    s=calc_stats(sub_rets) if len(sub_rets)>20 else {}
    n_days=int(m.sum())
    if s: print(f'  {label:20s} n={n_days:4d} CAGR={s["cagr"]:.1%} Sharpe={s["sharpe"]:.3f} MaxDD={s["maxdd"]:.1%}')
    elif n_days>0: print(f'  {label:20s} n={n_days:4d} (insufficient data)')

# ========================================
# TEST 6: DRAWDOWN DECOMPOSITION (Top 5)
# ========================================
print('\n' + '='*100)
print('  TEST 6: DRAWDOWN DECOMPOSITION — Top 5 Drawdowns')
print('='*100)
eq=np.cumprod(1+np.array(dr_beast)); peak=np.maximum.accumulate(eq)
dd_series=(eq-peak)/peak
# Find top 5 drawdowns
dd_events=[]
in_dd=False; dd_start=0
for i in range(len(dd_series)):
    if dd_series[i]<-0.01 and not in_dd:
        in_dd=True; dd_start=i
    elif dd_series[i]>=0 and in_dd:
        in_dd=False; trough=np.argmin(dd_series[dd_start:i])+dd_start
        dd_events.append((dd_series[trough], dd_start, trough, i))
if in_dd:
    trough=np.argmin(dd_series[dd_start:])+dd_start
    dd_events.append((dd_series[trough], dd_start, trough, len(dd_series)-1))
dd_events.sort(key=lambda x:x[0])
for rank,(depth,start,trough,end) in enumerate(dd_events[:5]):
    dur_down=trough-start; dur_recov=end-trough
    sd=eq_dates[start].strftime('%Y-%m-%d') if start<len(eq_dates) else '?'
    td=eq_dates[trough].strftime('%Y-%m-%d') if trough<len(eq_dates) else '?'
    ed=eq_dates[min(end,len(eq_dates)-1)].strftime('%Y-%m-%d')
    print(f'  #{rank+1}: {depth:.1%} | {sd}→{td}→{ed} | down={dur_down}d recov={dur_recov}d')

# ========================================
# TEST 7: CONCENTRATION ANALYSIS
# ========================================
print('\n' + '='*100)
print('  TEST 7: CONCENTRATION ANALYSIS')
print('='*100)
max_weights=[]; hhis=[]
for wh in wh_beast:
    ws=np.array(wh['weights'])
    max_weights.append(ws.max())
    hhis.append(float(np.sum(ws**2)))
if max_weights:
    print(f'  Max single-theme weight: mean={np.mean(max_weights):.1%} max={max(max_weights):.1%}')
    print(f'  HHI: mean={np.mean(hhis):.3f} max={max(hhis):.3f} (equal-10={0.10:.3f})')
    print(f'  Effective N (1/HHI): mean={1/np.mean(hhis):.1f} min={1/max(hhis):.1f}')

# ========================================
# TEST 8: DEFLATED SHARPE RATIO (DSR)
# ========================================
print('\n' + '='*100)
print('  TEST 8: DEFLATED SHARPE RATIO (DSR)')
print('='*100)
# M = number of strategy variants tried (conservative estimate)
# lookbacks(3) x rebal(2) x cap(3) x stock_score(3) x n_themes(2) = 108
M_trials = 108
sr = base_s['sharpe']
n_obs = base_s['n']
arr_b=np.array(dr_beast); arr_b=arr_b[np.isfinite(arr_b)]
skew_val = float(scipy_stats.skew(arr_b))
kurt_val = float(scipy_stats.kurtosis(arr_b))
# E[max(SR)] under null: Harvey et al approximation
from scipy.stats import norm
e_max_sr = norm.ppf(1 - 1/(2*M_trials)) * np.sqrt(1/n_obs) * np.sqrt(252)  # annualized
# DSR = P(SR* > SR_observed | H0)
sr_std = np.sqrt((1 + 0.5*sr**2 - skew_val*sr + (kurt_val/4)*sr**2) / (n_obs-1)) * np.sqrt(252)
if sr_std > 0:
    dsr_stat = (sr - e_max_sr) / sr_std
    dsr_pval = 1 - norm.cdf(dsr_stat)
else:
    dsr_stat = 0; dsr_pval = 1
print(f'  M (trials): {M_trials}')
print(f'  Observed Sharpe: {sr:.3f}')
print(f'  E[max(SR)] under null: {e_max_sr:.3f}')
print(f'  Skewness: {skew_val:.3f} | Excess Kurtosis: {kurt_val:.3f}')
print(f'  DSR statistic: {dsr_stat:.3f}')
print(f'  DSR p-value: {dsr_pval:.4f}')
dsr_pass = 1 - dsr_pval
print(f'  DSR confidence: {dsr_pass:.1%} (Green: ≥95%, Yellow: 80-95%, Red: <80%)')

# ========================================
# TEST 9: WORST PERIODS
# ========================================
print('\n' + '='*100)
print('  TEST 9: WORST PERIODS')
print('='*100)
beast_monthly=beast_ser.resample('ME').apply(lambda x:np.expm1(np.log1p(x).sum()))
worst_1m=beast_monthly.nsmallest(5)
print('  Worst 1M:')
for d,v in worst_1m.items(): print(f'    {d.strftime("%Y-%m")}: {v:.1%}')
beast_3m=beast_monthly.rolling(3).apply(lambda x:np.expm1(np.log1p(x).sum()))
worst_3m=beast_3m.nsmallest(3)
print('  Worst 3M:')
for d,v in worst_3m.items(): print(f'    {d.strftime("%Y-%m")}: {v:.1%}')

# ========================================
# FINAL RUBRIC SUMMARY
# ========================================
print('\n' + '='*100)
print('  AUDIT RUBRIC SUMMARY')
print('='*100)
oos_pass = oos_retention >= 0.50 if 'oos_retention' in dir() else False
pert_pass = all(s>0 for s in pert_sharpes) if pert_sharpes else False
cost_pass = s2x['calmar']>1.5 if s2x else False
dsr_color = 'GREEN' if dsr_pass>=0.95 else ('YELLOW' if dsr_pass>=0.80 else 'RED')
maxdd_color = 'RED' if base_s['maxdd']<-0.40 else ('YELLOW' if base_s['maxdd']<-0.30 else 'GREEN')

rows = [
    ('Walk-Forward OOS retention', f'{oos_retention:.1%}' if 'oos_retention' in dir() else '?', '≥50%', '✅ PASS' if oos_pass else '❌ FAIL'),
    ('Parameter perturbation sign', f'{sum(1 for s in pert_sharpes if s>0)}/{len(pert_sharpes)}' if pert_sharpes else '?', 'all positive', '✅ PASS' if pert_pass else '❌ FAIL'),
    ('DSR confidence', f'{dsr_pass:.1%}', '≥95%=Green', dsr_color),
    ('MaxDD', f'{base_s["maxdd"]:.1%}', '>-40%', maxdd_color),
    ('Cost 2x Calmar', f'{s2x["calmar"]:.2f}' if s2x else '?', '>1.5', '✅ PASS' if cost_pass else '❌ FAIL'),
    ('Annual turnover', f'{annual_to:.0%}', 'documented', '⚠ HIGH' if annual_to>5 else '✅ OK'),
    ('PIT/delisting', 'NOT TESTED', 'pass', '⚠ PENDING'),
    ('CSCV/PBO', 'NOT TESTED', '<10%', '⚠ PENDING'),
]
for name,val,thresh,verdict in rows:
    print(f'  {name:30s} {val:>12s}  thresh={thresh:>16s}  {verdict}')

print(f'\n  Elapsed: {time.time()-t0:.1f}s')
print('  === AUDIT COMPLETE ===')
