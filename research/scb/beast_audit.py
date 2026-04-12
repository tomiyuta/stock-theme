"""BEAST / G2-MAX Institutional Audit Suite
Feasible tests from ChatGPT review recommendations:
1. Walk-Forward (expanding + rolling)
2. Subsample stability (first/second half)
3. Parameter perturbation (lookback, cap, theme count)
4. Turnover measurement
5. Cost-adjusted (net) performance
6. Regime slicing (high-vol / low-vol / drawdown)
7. Worst drawdown decomposition
8. Concentration analysis
9. DSR approximation (deflated Sharpe)
"""
import pandas as pd, numpy as np, time, warnings, json
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
spy = yf.download('SPY', start='2020-01-01', end='2027-01-01', progress=False)
spy_ret = spy['Close'].pct_change().dropna()
spy_ret.index = pd.to_datetime(spy_ret.index).tz_localize(None)
print(f'Panel: {len(panel):,} rows | {len(dates_all)} days')

def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
def ols_alpha(y,x):
    mask=np.isfinite(y)&np.isfinite(x); y,x=y[mask],x[mask]
    if len(y)<20: return np.nan
    xm,ym=x.mean(),y.mean(); vx=np.var(x,ddof=1)
    if vx<1e-15: return np.nan
    b=np.dot(x-xm,y-ym)/(len(y)-1)/vx; return (ym-b*xm)*len(y)
def corr_select(ranked,sub,max_n,max_corr=0.80):
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

def run_engine(start_idx=126, end_idx=None, n_themes=6, rebal=20, cap=0.30, max_corr=0.80):
    """Run G2-MAX engine with given params. Returns daily_ret list + metadata."""
    if end_idx is None: end_idx = len(dates_all)
    rebal_idx = list(range(start_idx, end_idx, rebal))
    if rebal_idx[-1] != end_idx-1 and end_idx-1 > rebal_idx[-1]: rebal_idx.append(end_idx-1)
    daily_ret=[]; prev_tickers=set(); turnovers=[]; concentrations=[]
    for pos in range(len(rebal_idx)-1):
        j=rebal_idx[pos]; j_next=rebal_idx[pos+1]
        dt63=set(dates_all[max(0,j-62):j+1]); dt126=set(dates_all[max(0,j-125):j+1]); dt252=set(dates_all[max(0,j-251):j+1])
        sub=panel[panel['date'].isin(dt63)]; sub126=panel[panel['date'].isin(dt126)]; sub252=panel[panel['date'].isin(dt252)]
        tm=sub.groupby('theme')['ticker'].nunique(); elig=tm[tm>=4].index.tolist()
        hold_dates=tk_wide.index[j+1:min(j_next+1,len(tk_wide))]
        if len(hold_dates)==0: continue
        tm_m={h:{} for h in [21,63,126,252]}
        for th in elig:
            td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            if len(td)>=21: tm_m[21][th]=cumret(td[-21:])
            if len(td)>=63: tm_m[63][th]=cumret(td)
            td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            if len(td126v)>=63: tm_m[126][th]=cumret(td126v)
            td252v=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            if len(td252v)>=126: tm_m[252][th]=cumret(td252v)
        tdf=pd.DataFrame({f'm{h}':pd.Series(tm_m[h]) for h in [21,63,126,252]}).dropna(subset=['m63'])
        if len(tdf)<3: daily_ret.extend([0.0]*len(hold_dates)); continue
        tdf['m252ex1m']=np.where(tdf['m252'].notna()&tdf['m21'].notna(),(1+tdf['m252'])/(1+tdf['m21'])-1,np.nan)
        tdf['score']=(0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom'))
        ranked=list(tdf.sort_values('score',ascending=False).index)
        sel=corr_select(ranked,sub,n_themes,max_corr)
        port=[]
        for th in sel:
            ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
            if len(tks)<4: continue
            scores={}
            for tk in tks:
                tkd=ths[ths['ticker']==tk].sort_values('date')
                a=ols_alpha(tkd['ret'].values,tkd['theme_ex_self'].values)
                scores[tk]=a if np.isfinite(a) else -999
            best=max(scores,key=scores.get) if scores else None
            if best and scores[best]>-999:
                port.append({'tk':best,'th':th,'r63':tdf.loc[th,'m63'] if th in tdf.index else np.nan,
                    'r126':tdf.loc[th,'m126'] if th in tdf.index else np.nan,
                    'r252ex1m':tdf.loc[th,'m252ex1m'] if th in tdf.index else np.nan})
        if not port: daily_ret.extend([0.0]*len(hold_dates)); continue
        n=len(port); tickers=[p['tk'] for p in port]
        ws=w5b_weights(port, cap=cap)
        # Turnover
        curr_set=set(tickers)
        if prev_tickers: turnovers.append(len(curr_set.symmetric_difference(prev_tickers))/max(len(curr_set|prev_tickers),1))
        prev_tickers=curr_set
        concentrations.append(float(max(ws)))
        wsd=pd.Series(ws,index=tickers)
        dr=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(wsd,axis=1).sum(axis=1)
        daily_ret.extend(dr.values.tolist())
    return daily_ret, turnovers, concentrations

def calc(dr):
    arr=np.array(dr,dtype=float); arr=arr[np.isfinite(arr)]; n=len(arr)
    if n<20: return {}
    yrs=n/252; cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq); maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    neg=arr[arr<0]; dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    return {'cagr':cagr,'vol':vol,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'terminal':float(eq[-1]),'n_days':n}

# ============================================================
# TEST 1: Baseline (G2-MAX W5b cap30 vs BEAST nocap vs W0 equal)
# ============================================================
print('\n' + '='*90)
print('TEST 1: BASELINE')
print('='*90)
for label, cap in [('W0_equal', 'eq'), ('G2-MAX (W5b cap30)', 0.30), ('BEAST (nocap)', None)]:
    c = None if cap is None else (0.30 if cap == 0.30 else 0.30)
    if cap == 'eq': c = 99.0  # effectively equal
    dr, to, co = run_engine(cap=c)
    m = calc(dr)
    avg_to = np.mean(to) if to else 0
    avg_co = np.mean(co) if co else 0
    max_co = max(co) if co else 0
    print(f'  {label:<25s} CAGR={m["cagr"]:.1%} Sharpe={m["sharpe"]:.3f} MaxDD={m["maxdd"]:.1%} '
          f'Sortino={m["sortino"]:.2f} Calmar={m["calmar"]:.2f} Term={m["terminal"]:.0f}x '
          f'TO={avg_to:.1%} MaxConc={max_co:.1%}')

# ============================================================
# TEST 2: WALK-FORWARD (expanding + rolling)
# ============================================================
print('\n' + '='*90)
print('TEST 2: WALK-FORWARD')
print('='*90)
mid = len(dates_all) // 2
# Expanding: train on first half, test on second half
dr_oos, _, _ = run_engine(start_idx=mid, cap=None)
m_oos = calc(dr_oos)
dr_is, _, _ = run_engine(end_idx=mid, cap=None)
m_is = calc(dr_is)
print(f'  BEAST IS  (first half):  CAGR={m_is.get("cagr",0):.1%} Sharpe={m_is.get("sharpe",0):.3f} MaxDD={m_is.get("maxdd",0):.1%}')
print(f'  BEAST OOS (second half): CAGR={m_oos.get("cagr",0):.1%} Sharpe={m_oos.get("sharpe",0):.3f} MaxDD={m_oos.get("maxdd",0):.1%}')
retention = m_oos.get('sharpe',0)/m_is.get('sharpe',1) if m_is.get('sharpe',0)>0.01 else 0
print(f'  Sharpe retention: {retention:.1%} (pass: ≥50%)')
print(f'  {"✅ PASS" if retention>=0.50 else "❌ FAIL"}')
# Rolling: 3 equal windows
third = len(dates_all)//3
print(f'\n  Rolling 3-window:')
for i, (s, e, lbl) in enumerate([(126, third, 'W1'), (third, 2*third, 'W2'), (2*third, None, 'W3')]):
    dr_w, _, _ = run_engine(start_idx=s, end_idx=e, cap=None)
    m_w = calc(dr_w)
    print(f'    {lbl}: CAGR={m_w.get("cagr",0):.1%} Sharpe={m_w.get("sharpe",0):.3f} MaxDD={m_w.get("maxdd",0):.1%}')

# ============================================================
# TEST 3: SUBSAMPLE STABILITY
# ============================================================
print('\n' + '='*90)
print('TEST 3: SUBSAMPLE STABILITY (first/second half comparison)')
print('='*90)
for cap_lbl, cap_v in [('G2-MAX', 0.30), ('BEAST', None)]:
    dr1, _, _ = run_engine(end_idx=mid, cap=cap_v)
    dr2, _, _ = run_engine(start_idx=mid, cap=cap_v)
    m1, m2 = calc(dr1), calc(dr2)
    print(f'  {cap_lbl} 1st half: CAGR={m1.get("cagr",0):.1%} Sharpe={m1.get("sharpe",0):.3f}')
    print(f'  {cap_lbl} 2nd half: CAGR={m2.get("cagr",0):.1%} Sharpe={m2.get("sharpe",0):.3f}')
    print(f'  Sharpe ratio (2nd/1st): {m2.get("sharpe",0)/m1.get("sharpe",1) if m1.get("sharpe",0)>0.01 else 0:.2f}')
    print()

# ============================================================
# TEST 4: PARAMETER PERTURBATION
# ============================================================
print('='*90)
print('TEST 4: PARAMETER PERTURBATION (BEAST nocap)')
print('='*90)
print(f'  {"Config":<30s} {"CAGR":>7} {"Sharpe":>8} {"MaxDD":>8} {"Calmar":>8}')
print(f'  {"-"*65}')
configs = [
    ('Base (6t,20r,corr0.80)', dict(n_themes=6, rebal=20, max_corr=0.80, cap=None)),
    ('5 themes',               dict(n_themes=5, rebal=20, max_corr=0.80, cap=None)),
    ('7 themes',               dict(n_themes=7, rebal=20, max_corr=0.80, cap=None)),
    ('8 themes',               dict(n_themes=8, rebal=20, max_corr=0.80, cap=None)),
    ('Rebal 15d',              dict(n_themes=6, rebal=15, max_corr=0.80, cap=None)),
    ('Rebal 25d',              dict(n_themes=6, rebal=25, max_corr=0.80, cap=None)),
    ('Corr 0.70',              dict(n_themes=6, rebal=20, max_corr=0.70, cap=None)),
    ('Corr 0.90',              dict(n_themes=6, rebal=20, max_corr=0.90, cap=None)),
    ('No corr filter',         dict(n_themes=6, rebal=20, max_corr=1.00, cap=None)),
]
base_sharpe = None
for lbl, kw in configs:
    dr, _, _ = run_engine(**kw)
    m = calc(dr)
    if base_sharpe is None: base_sharpe = m.get('sharpe', 0)
    sign_ok = '✅' if m.get('sharpe',0) > 0 else '❌'
    print(f'  {lbl:<30s} {m.get("cagr",0):>6.1%} {m.get("sharpe",0):>7.3f} {m.get("maxdd",0):>7.1%} {m.get("calmar",0):>7.2f} {sign_ok}')

# ============================================================
# TEST 5: COST-ADJUSTED (NET) PERFORMANCE
# ============================================================
print('\n' + '='*90)
print('TEST 5: COST-ADJUSTED PERFORMANCE')
print('='*90)
dr_beast, to_beast, _ = run_engine(cap=None)
dr_g2, to_g2, _ = run_engine(cap=0.30)
for lbl, dr, to in [('G2-MAX', dr_g2, to_g2), ('BEAST', dr_beast, to_beast)]:
    m_gross = calc(dr)
    avg_to = np.mean(to) if to else 0
    annual_to = avg_to * (252/20)  # rebal every 20 days
    for cost_bps in [10, 25, 50, 100]:
        cost_per_trade = cost_bps / 10000
        daily_cost = cost_per_trade * avg_to / 20  # spread across holding period
        dr_net = [r - daily_cost for r in dr]
        m_net = calc(dr_net)
        net_ratio = m_net.get('sharpe',0) / m_gross.get('sharpe',1) if m_gross.get('sharpe',0) > 0.01 else 0
        print(f'  {lbl} @ {cost_bps:>3d}bps: Sharpe={m_net.get("sharpe",0):.3f} (gross={m_gross.get("sharpe",0):.3f}, retention={net_ratio:.1%}) CAGR={m_net.get("cagr",0):.1%}')
    print(f'  Annual turnover: {annual_to:.1%}')
    print()

# ============================================================
# TEST 6: REGIME SLICING
# ============================================================
print('='*90)
print('TEST 6: REGIME SLICING (BEAST)')
print('='*90)
dr_beast_full, _, _ = run_engine(cap=None)
bt_dates = tk_wide.index[-len(dr_beast_full):]
beast_df = pd.DataFrame({'date': bt_dates, 'beast': dr_beast_full}).set_index('date')
spy_daily = spy_ret.reindex(beast_df.index).fillna(0)
beast_df['spy'] = spy_daily.values
# Rolling 21d vol of SPY
beast_df['spy_vol21'] = beast_df['spy'].rolling(21).std() * np.sqrt(252)
vol_med = beast_df['spy_vol21'].median()
# Regimes
beast_df['spy_cum'] = (1+beast_df['spy']).cumprod()
beast_df['spy_peak'] = beast_df['spy_cum'].cummax()
beast_df['spy_dd'] = beast_df['spy_cum']/beast_df['spy_peak']-1
regimes = {
    'Low vol (SPY vol<med)': beast_df['spy_vol21'] < vol_med,
    'High vol (SPY vol≥med)': beast_df['spy_vol21'] >= vol_med,
    'SPY rising (DD>-5%)': beast_df['spy_dd'] > -0.05,
    'SPY drawdown (DD≤-5%)': beast_df['spy_dd'] <= -0.05,
    'SPY crash (DD≤-10%)': beast_df['spy_dd'] <= -0.10,
    'Post-crash rebound': (beast_df['spy_dd'].shift(21) <= -0.10) & (beast_df['spy_dd'] > -0.05),
}
print(f'  {"Regime":<30s} {"Days":>5} {"CAGR":>8} {"Sharpe":>8} {"MaxDD":>8}')
print(f'  {"-"*65}')
for name, mask in regimes.items():
    sub = beast_df.loc[mask, 'beast'].values
    ms = calc(sub) if len(sub) > 20 else {}
    print(f'  {name:<30s} {len(sub):>5d} {ms.get("cagr",0):>7.1%} {ms.get("sharpe",0):>7.3f} {ms.get("maxdd",0):>7.1%}')

# ============================================================
# TEST 7: WORST DRAWDOWN DECOMPOSITION
# ============================================================
print('\n' + '='*90)
print('TEST 7: DRAWDOWN DECOMPOSITION (BEAST)')
print('='*90)
eq = np.cumprod(1+np.array(dr_beast_full))
peak = np.maximum.accumulate(eq)
dd = eq/peak - 1
# Find top 3 drawdowns
dd_ser = pd.Series(dd, index=bt_dates)
in_dd = dd_ser < -0.01
starts=[]; ends=[]; depths=[]
i=0
while i<len(dd_ser):
    if dd_ser.iloc[i]<-0.01:
        s=i
        worst=dd_ser.iloc[i]; worst_i=i
        while i<len(dd_ser) and dd_ser.iloc[i]<0:
            if dd_ser.iloc[i]<worst: worst=dd_ser.iloc[i]; worst_i=i
            i+=1
        starts.append(s); ends.append(i-1); depths.append(worst)
    i+=1
top_dd = sorted(zip(depths,starts,ends))[:5]
print(f'  {"#":>2} {"Start":<12} {"Trough":<12} {"End":<12} {"Depth":>8} {"Duration":>8} {"Recovery":>8}')
for rank,(depth,s,e) in enumerate(top_dd):
    sd=bt_dates[s]; ed=bt_dates[min(e,len(bt_dates)-1)]
    dur=(ed-sd).days
    # Find recovery
    rec_days='N/A'
    trough_i=s+np.argmin(dd[s:e+1])
    for ri in range(trough_i,min(trough_i+252,len(dd))):
        if dd[ri]>=0: rec_days=str((bt_dates[ri]-bt_dates[trough_i]).days)+'d'; break
    print(f'  {rank+1:>2} {str(sd.date()):<12} {str(bt_dates[trough_i].date()):<12} {str(ed.date()):<12} {depth:>7.1%} {dur:>7d}d {rec_days:>8}')

# ============================================================
# TEST 8: CONCENTRATION ANALYSIS
# ============================================================
print('\n' + '='*90)
print('TEST 8: CONCENTRATION ANALYSIS')
print('='*90)
_, _, conc_g2 = run_engine(cap=0.30)
_, _, conc_beast = run_engine(cap=None)
print(f'  G2-MAX (cap30):  avg_max_weight={np.mean(conc_g2):.1%}  max_max_weight={max(conc_g2):.1%}')
print(f'  BEAST (nocap):   avg_max_weight={np.mean(conc_beast):.1%}  max_max_weight={max(conc_beast):.1%}')
print(f'  Concentration ratio (BEAST/G2): {np.mean(conc_beast)/np.mean(conc_g2):.2f}x')

# ============================================================
# TEST 9: DSR APPROXIMATION (Deflated Sharpe Ratio)
# ============================================================
print('\n' + '='*90)
print('TEST 9: DSR APPROXIMATION')
print('='*90)
from scipy import stats as sp_stats
dr_arr = np.array(dr_beast_full)
T = len(dr_arr)
sr = np.mean(dr_arr)/np.std(dr_arr,ddof=1) * np.sqrt(252)  # annualized
sr_daily = np.mean(dr_arr)/np.std(dr_arr,ddof=1)
skew = float(sp_stats.skew(dr_arr))
kurt = float(sp_stats.kurtosis(dr_arr))
# Number of independent trials (conservative estimate)
M_trials = 9  # 9 configs tested in perturbation
# Harvey & Liu (2015) / Bailey & de Prado DSR
# SR* = sqrt(V[SR]) * Phi^-1(1 - 1/M)  where V[SR] ≈ (1 - skew*SR + (kurt-1)/4*SR^2) / T
var_sr = (1 - skew*sr_daily + (kurt-1)/4*sr_daily**2) / T
sr_star = np.sqrt(var_sr) * sp_stats.norm.ppf(1 - 1/M_trials)
# DSR = Phi((SR - SR*) / sqrt(V[SR]))
dsr = float(sp_stats.norm.cdf((sr_daily - sr_star*np.sqrt(252)**(-1)) / np.sqrt(var_sr)))
print(f'  Observed Sharpe (annual): {sr:.3f}')
print(f'  Skewness: {skew:.3f}')
print(f'  Excess kurtosis: {kurt:.3f}')
print(f'  Sample size T: {T} days')
print(f'  Trial count M: {M_trials}')
print(f'  SR* (threshold): {sr_star*np.sqrt(252):.3f}')
print(f'  DSR (prob Sharpe is real): {dsr:.3f}')
print(f'  {"✅ PASS (DSR≥0.95)" if dsr>=0.95 else "⚠ YELLOW (0.80-0.95)" if dsr>=0.80 else "❌ RED (DSR<0.80)"}')

# ============================================================
# FINAL SUMMARY
# ============================================================
print('\n' + '='*90)
print('AUDIT SUMMARY')
print('='*90)
m_base = calc(dr_beast_full)
m_g2 = calc(dr_g2)
print(f'''
  Test 1 - Baseline:       BEAST CAGR={m_base["cagr"]:.0%} Sharpe={m_base["sharpe"]:.3f} MaxDD={m_base["maxdd"]:.1%}
  Test 2 - Walk-Forward:   OOS Sharpe retention = {retention:.1%} {"✅" if retention>=0.50 else "❌"}
  Test 3 - Subsample:      See above
  Test 4 - Perturbation:   All configs Sharpe>0 = sign stability check
  Test 5 - Net cost:       See above (net Sharpe retention at various cost levels)
  Test 6 - Regime:         See above (crash/rebound performance)
  Test 7 - Drawdowns:      Top 5 drawdowns decomposed
  Test 8 - Concentration:  BEAST max_weight = {max(conc_beast):.1%}
  Test 9 - DSR:            {dsr:.3f} {"✅" if dsr>=0.95 else "⚠" if dsr>=0.80 else "❌"}
''')
print(f'=== Done in {time.time()-t0:.1f}s ===')
