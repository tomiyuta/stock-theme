#!/usr/bin/env python3
"""Kill Switch BT — NORMAL/CAUTION/KILL state machine
Based on ChatGPT spec (Daniel-Moskowitz panic state model)
Tests: static cap25 baseline vs kill switch variants
"""
import pandas as pd, numpy as np, time, warnings, json
from scipy import stats as scipy_stats
warnings.filterwarnings('ignore')
t0 = time.time()
panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
print(f'Panel loaded: {len(panel):,} rows')
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

# === SPY + Market State Indicators ===
import yfinance as yf
spy = yf.download('SPY', start='2018-01-01', end='2027-01-01', progress=False)
spy_close = (spy['Adj Close'] if 'Adj Close' in spy.columns else spy['Close']).squeeze()
spy_close.index = spy_close.index.tz_localize(None)
spy_ret = spy_close.pct_change().dropna()

# Precompute market indicators on SPY index
spy_sma200 = spy_close.rolling(200).mean()
spy_m63 = spy_close.pct_change(63)
spy_rv21 = spy_ret.rolling(21).std() * np.sqrt(252)
# RV21 percentile over trailing 756 days (3yr)
spy_rv21_pct = spy_rv21.rolling(756, min_periods=252).rank(pct=True) * 100
# Rebound from 20-day low
spy_low20 = spy_close.rolling(20).min()
spy_rebound20 = (spy_close - spy_low20) / spy_low20

print(f'SPY data: {spy_close.index[0].date()} ~ {spy_close.index[-1].date()}')

# === Helpers ===
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
def w5b_w(port,cap=0.25):
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

# === Kill Switch State Machine ===
def get_market_state(dt, strat_rets=None):
    """Evaluate market indicators at date dt."""
    if dt not in spy_close.index:
        idx = spy_close.index.searchsorted(dt)
        if idx >= len(spy_close.index): idx = len(spy_close.index)-1
        dt = spy_close.index[idx]
    trend200 = 1 if spy_close.loc[:dt].iloc[-1] > spy_sma200.loc[:dt].iloc[-1] else 0
    m63 = spy_m63.loc[:dt].iloc[-1] if dt in spy_m63.index or len(spy_m63.loc[:dt])>0 else 0
    rv21_pct = spy_rv21_pct.loc[:dt].iloc[-1] if len(spy_rv21_pct.loc[:dt])>0 else 50
    rebound20 = spy_rebound20.loc[:dt].iloc[-1] if len(spy_rebound20.loc[:dt])>0 else 0
    # Breadth: % of themes with positive 63d return (from panel)
    breadth = 0.5  # default
    # Strategy vol/DD
    strat_rv20 = 0; strat_dd20 = 0
    if strat_rets is not None and len(strat_rets) >= 20:
        sr = np.array(strat_rets[-20:])
        strat_rv20 = float(np.std(sr, ddof=1) * np.sqrt(252))
        eq = np.cumprod(1 + sr); peak = np.maximum.accumulate(eq)
        strat_dd20 = float(((eq - peak) / peak).min())
    return {
        'trend200': trend200, 'm63': float(m63) if np.isfinite(m63) else 0,
        'rv21_pct': float(rv21_pct) if np.isfinite(rv21_pct) else 50,
        'rebound20': float(rebound20) if np.isfinite(rebound20) else 0,
        'breadth': breadth, 'strat_rv20': strat_rv20, 'strat_dd20': strat_dd20
    }

def determine_state(ms, prev_state='NORMAL'):
    """State machine: NORMAL → CAUTION → KILL with hysteresis."""
    # KILL conditions (panic state)
    kill_main = (ms['m63'] < 0 and ms['rv21_pct'] >= 75 and ms['rebound20'] >= 0.08)
    kill_sub_count = sum([ms['strat_dd20'] <= -0.10, ms['breadth'] < 0.25, ms['trend200'] == 0])
    kill_sub = kill_sub_count >= 2
    if kill_main or kill_sub:
        return 'KILL'
    # CAUTION conditions (2/4 triggers)
    caution_count = sum([ms['trend200'] == 0, ms['m63'] < 0, ms['rv21_pct'] >= 75, ms['breadth'] < 0.35])
    if caution_count >= 2:
        return 'CAUTION'
    # NORMAL recovery (with hysteresis for KILL→NORMAL)
    if prev_state == 'KILL':
        # Need strong recovery to exit KILL → go to CAUTION first
        return 'CAUTION'
    if prev_state == 'CAUTION':
        # Need 3/4 normal conditions
        normal_count = sum([ms['trend200'] == 1, ms['m63'] >= 0, ms['rv21_pct'] < 60, ms['breadth'] >= 0.45])
        if normal_count >= 3:
            return 'NORMAL'
        return 'CAUTION'
    return 'NORMAL'

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1:rebal_idx.append(len(dates_all)-1)

# Variants to test
VARIANTS = {
    'B0_cap30':      {'cap': 0.30, 'kill': False, 'sma_filter': False, 'vol_scale': False},
    'B1_cap25':      {'cap': 0.25, 'kill': False, 'sma_filter': False, 'vol_scale': False},
    'K1_sma200':     {'cap': 0.25, 'kill': False, 'sma_filter': True,  'vol_scale': False},
    'K2_volscale':   {'cap': 0.25, 'kill': False, 'sma_filter': False, 'vol_scale': True},
    'K3_sma+vol':    {'cap': 0.25, 'kill': False, 'sma_filter': True,  'vol_scale': True},
    'K4_fullKS':     {'cap': 0.25, 'kill': True,  'sma_filter': False, 'vol_scale': False},
    'K5_fullKS+vol': {'cap': 0.25, 'kill': True,  'sma_filter': False, 'vol_scale': True},
}
results = {k: {'daily':[], 'states':[], 'gross_hist':[]} for k in VARIANTS}
VOL_TARGET = 0.40  # annualized vol target for vol scaling

prev_states = {k: 'NORMAL' for k in VARIANTS}
for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos];j_next=rebal_idx[pos+1]
    dt=dates_all[j]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt21=set(dates_all[max(0,j-20):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    dt252=set(dates_all[max(0,j-251):j+1])
    sub=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    sub252=panel[panel['date'].isin(dt252)]
    tm=sub.groupby('theme')['ticker'].nunique();elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme scoring (PRISM-R style)
    tm_mom={};dc={}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=63:tm_mom[th]=cumret(td)
        if len(td)>=63:
            r021=cumret(td[-21:]);r2142=cumret(td[-42:-21]);r4263=cumret(td[-63:-42])
            if all(np.isfinite([r021,r2142,r4263])):dc[th]=-(r021-0.5*(r2142+r4263))
    ms_s=pd.Series(tm_mom).dropna().sort_values(ascending=False)
    dcs=pd.Series(dc);common=list(set(ms_s.index)&set(dcs.index))
    if not common:
        for k in results:results[k]['daily'].extend([0.0]*len(hold_dates))
        continue
    ts=pd.DataFrame({'mom':ms_s[common],'dec':dcs[common]})
    ts['score']=0.70*ts['mom'].rank(pct=True)+0.30*ts['dec'].rank(pct=True)
    ts=ts.sort_values('score',ascending=False)
    sel=[];sc_cnt={}
    for th in ts.index:
        s=theme_sector.get(th,'Unk')
        if sc_cnt.get(s,0)>=SEC_MAX:continue
        sel.append(th);sc_cnt[s]=sc_cnt.get(s,0)+1
        if len(sel)>=TOP_T:break
    # Stock selection (PRISM-R: α63*shrink_r2) + W5b momentum data
    port=[];used=set()
    for th in sel:
        ths=sub[(sub['theme']==th)&sub['ret'].notna()];tks=ths['ticker'].unique()
        if len(tks)<MIN_M:continue
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
    if not port:
        for k in results:results[k]['daily'].extend([0.0]*len(hold_dates))
        continue
    # Apply each variant
    tickers=[p['tk'] for p in port]; n=len(port)
    for vname, vcfg in VARIANTS.items():
        ws = w5b_w(port, cap=vcfg['cap'])
        gross = 1.0
        state = 'NORMAL'
        # Market state evaluation
        ms = get_market_state(dt, results[vname]['daily'])
        # Breadth from theme momentum
        n_pos = sum(1 for th in tm_mom if tm_mom[th] > 0)
        n_tot = max(len(tm_mom), 1)
        ms['breadth'] = n_pos / n_tot
        if vcfg['kill']:
            state = determine_state(ms, prev_states[vname])
            prev_states[vname] = state
            if state == 'KILL':
                gross = 0.0
            elif state == 'CAUTION':
                gross = 0.50
                ws = w5b_w(port, cap=0.20)  # tighter cap in CAUTION
        elif vcfg['sma_filter']:
            if ms['trend200'] == 0:
                gross = 0.0; state = 'KILL'
            elif ms['m63'] < 0:
                gross = 0.50; state = 'CAUTION'
        # Vol scaling
        if vcfg['vol_scale'] and len(results[vname]['daily']) >= 20:
            recent = np.array(results[vname]['daily'][-20:])
            rv = float(np.std(recent, ddof=1) * np.sqrt(252))
            if rv > 0.01:
                vol_scale = min(VOL_TARGET / rv, 1.5)  # cap at 150%
                gross *= vol_scale
        gross = max(0, min(gross, 1.5))
        results[vname]['states'].append(state)
        results[vname]['gross_hist'].append(round(gross, 3))
        ww = pd.Series(ws * gross, index=tickers)
        d = tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ww, axis=1).sum(axis=1)
        results[vname]['daily'].extend(d.values.tolist())

# === Results ===
print(f'\n{"="*120}')
print(f'  KILL SWITCH BT RESULTS')
print(f'{"="*120}')
eq_dates = tk_wide.index[-len(results['B0_cap30']['daily']):]
spy_aligned = spy_ret.reindex(eq_dates).fillna(0)
spy_cum63 = spy_aligned.rolling(63).apply(lambda x: np.expm1(np.log1p(x).sum()), raw=True)
bear_mask = spy_cum63 <= 0

print(f'\n  {"Variant":<16} {"CAGR":>7} {"Shrp":>6} {"Sort":>5} {"Cal":>5} {"MaxDD":>7} {"BearS":>7} {"Off%":>6} {"Whip":>5}')
print(f'  {"-"*80}')
base_s = None
for vname in VARIANTS:
    dr = results[vname]['daily']
    s = calc_stats(dr)
    if not s: continue
    if base_s is None: base_s = s
    # Bear Sharpe
    beast_ser = pd.Series(dr, index=eq_dates)
    bear_rets = beast_ser[bear_mask.reindex(eq_dates).fillna(False)].values
    bear_s = calc_stats(bear_rets).get('sharpe', 0) if len(bear_rets) > 20 else 0
    # Off%: fraction of time gross < 100%
    states = results[vname]['states']
    off_pct = sum(1 for g in results[vname]['gross_hist'] if g < 0.99) / max(len(results[vname]['gross_hist']), 1)
    # Whipsaw: KILL→NORMAL transitions
    whip = sum(1 for i in range(1, len(states)) if states[i] != states[i-1])
    print(f'  {vname:<16} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} {s["maxdd"]:>6.1%} {bear_s:>+6.3f} {off_pct:>5.1%} {whip:>4d}')

# Delta vs baseline
print(f'\n  Delta vs B1_cap25:')
b1 = calc_stats(results['B1_cap25']['daily'])
b1_bear = pd.Series(results['B1_cap25']['daily'], index=eq_dates)
b1_bear_s = calc_stats(b1_bear[bear_mask.reindex(eq_dates).fillna(False)].values).get('sharpe',0)
for vname in ['K1_sma200','K2_volscale','K3_sma+vol','K4_fullKS','K5_fullKS+vol']:
    s = calc_stats(results[vname]['daily'])
    if not s: continue
    bs = pd.Series(results[vname]['daily'], index=eq_dates)
    bsr = calc_stats(bs[bear_mask.reindex(eq_dates).fillna(False)].values).get('sharpe',0)
    cagr_ret = s['cagr'] / b1['cagr'] if b1['cagr'] > 0 else 0
    print(f'    {vname:<16} ΔCAGR={s["cagr"]-b1["cagr"]:>+6.1%} ΔShrp={s["sharpe"]-b1["sharpe"]:>+6.3f} ΔMaxDD={s["maxdd"]-b1["maxdd"]:>+5.1%} ΔBear={bsr-b1_bear_s:>+6.3f} CAGR_ret={cagr_ret:>5.1%}')

# State transition log for K4_fullKS
print(f'\n  K4_fullKS state transitions:')
k4s = results['K4_fullKS']['states']
k4g = results['K4_fullKS']['gross_hist']
for i, (st, gr) in enumerate(zip(k4s, k4g)):
    if i == 0 or st != k4s[i-1]:
        ri = rebal_idx[i] if i < len(rebal_idx) else len(dates_all)-1
        dt_str = dates_all[ri].strftime('%Y-%m-%d') if ri < len(dates_all) else '?'
        prev = k4s[i-1] if i > 0 else 'INIT'
        print(f'    {dt_str}: {prev} → {st} (gross={gr})')

# Annual comparison
print(f'\n  Annual:')
print(f'  {"Year":>6}', end='')
for vn in VARIANTS: print(f' {vn:>14}', end='')
print()
for vn in VARIANTS:
    eq = pd.Series(np.cumprod(1+np.array(results[vn]['daily'])), index=eq_dates)
    results[vn]['annual'] = {str(d.year): round(float(r),4) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
for yr in ['2021','2022','2023','2024','2025','2026']:
    row = f'  {yr:>6}'
    for vn in VARIANTS:
        v = results[vn].get('annual',{}).get(yr, 0)
        row += f' {v:>+13.1%}'
    print(row)

# ChatGPT pass criteria
print(f'\n  ChatGPT Pass Criteria:')
for vn in ['K4_fullKS','K5_fullKS+vol']:
    s = calc_stats(results[vn]['daily'])
    bs = pd.Series(results[vn]['daily'], index=eq_dates)
    bsr = calc_stats(bs[bear_mask.reindex(eq_dates).fillna(False)].values).get('sharpe',0)
    off = sum(1 for g in results[vn]['gross_hist'] if g < 0.99) / max(len(results[vn]['gross_hist']),1)
    cagr_ret = s['cagr'] / b1['cagr'] if b1['cagr'] > 0 else 0
    print(f'    {vn}:')
    print(f'      Bear Sharpe: {bsr:+.3f} (vs -0.40 baseline, {"✅ PASS" if bsr > -0.20 else "❌ FAIL"})')
    print(f'      MaxDD: {s["maxdd"]:.1%} (target <-35%, {"✅ PASS" if s["maxdd"] > -0.35 else "❌ FAIL"})')
    print(f'      CAGR retention: {cagr_ret:.1%} (target ≥80%, {"✅ PASS" if cagr_ret >= 0.80 else "❌ FAIL"})')
    print(f'      Off rate: {off:.1%}')

print(f'\n  Total elapsed: {time.time()-t0:.1f}s')
print('  === KILL SWITCH BT COMPLETE ===')
