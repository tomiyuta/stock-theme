"""Emergency exit trigger BT: monthly rebalance ± emergency exit for PRISM-R/RQ/G2-MAX
B0: monthly only (20-day cadence, no emergency)
B1: monthly + emergency exit at rank>35 (cash until next rebal)
B2: monthly + emergency exit at rank>50 (looser threshold)
B3: monthly + emergency exit at rank>35 + immediate replacement
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

WARMUP=126; REBAL=20; MIN_M=4

def compute_theme_scores(panel, dates_all, day_idx):
    """Compute theme scores for a given day index."""
    dt63 = set(dates_all[max(0,day_idx-62):day_idx+1])
    dt126 = set(dates_all[max(0,day_idx-125):day_idx+1])
    sub = panel[panel['date'].isin(dt63)]
    sub126 = panel[panel['date'].isin(dt126)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm>=MIN_M].index.tolist()
    if not elig: return {}, sub
    tm_mom63, tm_mom126, tm_mom21 = {}, {}, {}
    for th in elig:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21: tm_mom21[th]=cumret(td[-21:])
        if len(td)>=63: tm_mom63[th]=cumret(td)
        td126 = sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126)>=63: tm_mom126[th]=cumret(td126)
    # G2-MAX score
    tdf = pd.DataFrame({'m63':pd.Series(tm_mom63),'m126':pd.Series(tm_mom126),'m21':pd.Series(tm_mom21)}).dropna(subset=['m63'])
    if len(tdf)==0: return {}, sub
    tdf['score'] = (0.50*tdf['m63'].rank(pct=True) +
                    0.30*tdf['m126'].rank(pct=True,na_option='bottom') +
                    0.20*tdf['m21'].rank(pct=True,na_option='bottom'))
    # Return rank (1=best)
    tdf['rank'] = tdf['score'].rank(ascending=False).astype(int)
    return dict(zip(tdf.index, tdf['rank'])), sub

def pick_stock(sub, theme):
    """Pick top raw alpha63 stock from a theme."""
    ths = sub[(sub['theme']==theme)&sub['ret'].notna()]
    tks = ths['ticker'].unique()
    if len(tks) < MIN_M: return None
    scores = {}
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        a = ols_alpha(tkd['ret'].values, tkd['theme_ex_self'].values)
        scores[tk] = a if np.isfinite(a) else -999
    best = max(scores, key=scores.get) if scores else None
    return best if best and scores[best] > -999 else None

# Precompute daily theme rankings for emergency checks (every 5 days to save time)
print('Precomputing theme rankings...')
CHECK_INTERVAL = 5  # check every 5 days between rebalances
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)

# All check days = rebalance days + every 5th day between
check_days = set(rebal_idx)
for i in range(len(rebal_idx)-1):
    for d in range(rebal_idx[i]+CHECK_INTERVAL, rebal_idx[i+1], CHECK_INTERVAL):
        check_days.add(d)
check_days = sorted(check_days)

# Precompute theme ranks at check days
theme_ranks_cache = {}
sub_cache = {}
for di, day_idx in enumerate(check_days):
    ranks, sub = compute_theme_scores(panel, dates_all, day_idx)
    theme_ranks_cache[day_idx] = ranks
    sub_cache[day_idx] = sub
    if di % 50 == 0: print(f'  {di}/{len(check_days)} ({dates_all[day_idx].strftime("%Y-%m-%d")})')
print(f'Precomputed {len(check_days)} check days')

def get_ranks_for_day(day_idx):
    """Get closest precomputed ranks for a given day."""
    # Find nearest check day <= day_idx
    best = None
    for cd in check_days:
        if cd <= day_idx: best = cd
        else: break
    return theme_ranks_cache.get(best, {}), sub_cache.get(best)

# Strategy configs
STRATS = {
    'B0_monthly':    {'emerg': False, 'rank_thr': None, 'replace': False},
    'B1_emerg35':    {'emerg': True,  'rank_thr': 35,   'replace': False},
    'B2_emerg50':    {'emerg': True,  'rank_thr': 50,   'replace': False},
    'B3_emerg35rep': {'emerg': True,  'rank_thr': 35,   'replace': True},
}
N_THEMES = 6  # G2-MAX

daily_ret = {s: [] for s in STRATS}
emerg_events = {s: 0 for s in STRATS}
whipsaw_events = {s: 0 for s in STRATS}

for pos in range(len(rebal_idx)-1):
    j_start = rebal_idx[pos]; j_end = rebal_idx[pos+1]
    hold_dates = tk_wide.index[j_start+1:j_end+1]
    if len(hold_dates) == 0: continue
    
    # Full rebalance: select themes + stocks
    ranks_at_rebal = theme_ranks_cache.get(j_start, {})
    sub_at_rebal = sub_cache.get(j_start)
    if not ranks_at_rebal or sub_at_rebal is None:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    ranked_themes = sorted(ranks_at_rebal.keys(), key=lambda th: ranks_at_rebal[th])
    sel_themes = corr_select(ranked_themes, sub_at_rebal, N_THEMES)
    
    # Pick stocks
    base_port = {}  # theme -> ticker
    for th in sel_themes:
        tk = pick_stock(sub_at_rebal, th)
        if tk: base_port[th] = tk
    
    # Simulate each strategy
    for sname, cfg in STRATS.items():
        port = dict(base_port)  # theme->ticker, copy per strategy
        exited_themes = set()   # themes that were emergency-exited
        
        for day_offset, hd in enumerate(hold_dates):
            day_idx = j_start + 1 + day_offset
            
            # Emergency check (if enabled)
            if cfg['emerg'] and day_offset > 0:
                ranks, sub_now = get_ranks_for_day(day_idx)
                if ranks:
                    to_exit = []
                    for th in list(port.keys()):
                        r = ranks.get(th, 999)
                        if r > cfg['rank_thr']:
                            to_exit.append(th)
                    for th in to_exit:
                        del port[th]
                        exited_themes.add(th)
                        emerg_events[sname] += 1
                    
                    # B3: immediate replacement
                    if cfg['replace'] and to_exit and sub_now is not None:
                        used_tickers = set(port.values())
                        for th_new in sorted(ranks.keys(), key=lambda t: ranks[t]):
                            if th_new in port or th_new in exited_themes: continue
                            if ranks[th_new] > cfg['rank_thr']: continue
                            tk = pick_stock(sub_now, th_new)
                            if tk and tk not in used_tickers:
                                port[th_new] = tk
                                used_tickers.add(tk)
                                if len(port) >= N_THEMES: break
            
            # Compute daily return
            if not port:
                daily_ret[sname].append(0.0)
            else:
                w = 1.0 / len(port) if port else 0
                tickers = list(port.values())
                day_rets = tk_wide.loc[hd].reindex(tickers).fillna(0)
                daily_ret[sname].append(float(day_rets.sum() * w))


# === Metrics ===
eq_dates = tk_wide.index[-len(daily_ret['B0_monthly']):]
def calc(dr):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    terminal=float(eq[-1])
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    # Worst drawdown episode
    wm=float(pd.Series(arr,index=eq_dates[:len(arr)]).resample('ME').apply(lambda x:float(np.expm1(np.log1p(x).sum()))).min())
    return {'cagr':cagr,'vol':vol,'sharpe':sharpe,'calmar':calmar,'maxdd':maxdd,'terminal':terminal,'worst_month':wm}

print("\n"+"="*100)
print(f"{'Strategy':<16} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} {'Calmar':>8} {'MaxDD':>8} {'Term$':>8} {'WorstM':>8} {'Emerg#':>7}")
print("="*100)
results = {}
for sname in STRATS:
    m = calc(daily_ret[sname])
    m['emergency_exits'] = emerg_events[sname]
    results[sname] = m
    print(f"  {sname:<14} {m['cagr']:>7.1%} {m['vol']:>7.1%} {m['sharpe']:>7.3f} {m['calmar']:>7.3f} {m['maxdd']:>7.1%} {m['terminal']:>7.1f}x {m['worst_month']:>7.1%}  {m['emergency_exits']:>5}")
print("="*100)

# Ablation: compare each emergency variant to B0
print("\n=== ABLATION vs B0 (monthly-only) ===")
b0 = results['B0_monthly']
for sname in ['B1_emerg35','B2_emerg50','B3_emerg35rep']:
    m = results[sname]
    print(f"  {sname:<16} ΔCAGR={m['cagr']-b0['cagr']:>+6.1%} ΔSharpe={m['sharpe']-b0['sharpe']:>+6.3f} ΔMaxDD={m['maxdd']-b0['maxdd']:>+6.1%} Emerg={m['emergency_exits']}")

# Annual
print("\n=== ANNUAL ===")
for sname in STRATS:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[sname])),index=eq_dates)
    results[sname]['annual']={str(d.year):round(float(r),3) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
header=f"{'Year':<6}"+"".join(f"{s:>16}" for s in STRATS)
print(header)
for yr in ['2021','2022','2023','2024','2025','2026']:
    row=f"  {yr:<4}"
    for s in STRATS: row+=f"  {results[s].get('annual',{}).get(yr,0):>+13.1%}"
    print(row)

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_emergency_results.json','w') as f:
    json.dump(results,f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
