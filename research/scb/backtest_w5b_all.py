"""W5b/BEAST test for PRISM-R / PRISM-RQ style strategies (10 themes)
W0: equal weight
W5b: R63/R126/R252ex1m consistency (30% cap)
BEAST: R63/R126/R252ex1m consistency (no cap)
"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

# Sector mapping for PRISM sector cap
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
tk_sector = dict(zip(meta['ticker'], meta['sector']))
# Theme → majority sector
theme_tickers = panel.groupby('theme')['ticker'].apply(set).to_dict()
theme_sector = {}
for th, tks in theme_tickers.items():
    secs = [tk_sector.get(tk,'Unk') for tk in tks if tk in tk_sector]
    theme_sector[th] = max(set(secs), key=secs.count) if secs else 'Unk'

def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
def ols_alpha(y, x):
    mask=np.isfinite(y)&np.isfinite(x); y,x=y[mask],x[mask]
    if len(y)<20: return np.nan, np.nan
    xm,ym=x.mean(),y.mean(); vx=np.var(x,ddof=1)
    if vx<1e-15: return np.nan, np.nan
    b=np.dot(x-xm,y-ym)/(len(y)-1)/vx; a=ym-b*xm
    resid=y-a-b*x; sigma=float(np.std(resid,ddof=2))
    alpha=a*len(y)
    return alpha, sigma
def corr_select(ranked, sub, max_n, max_corr=0.80, sector_cap=None):
    tdr={}
    for th in ranked:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        if len(td)>=20: tdr[th]=td
    if len(tdr)<2: return ranked[:max_n]
    cdf=pd.DataFrame(tdr).dropna().corr(); sel=[]; sec_count={}
    for th in ranked:
        if th not in cdf.index: continue
        ok=all(abs(cdf.loc[th,s])<max_corr for s in sel if s in cdf.columns)
        if not ok: continue
        if sector_cap is not None:
            s=theme_sector.get(th,'Unk')
            if sec_count.get(s,0)>=sector_cap: continue
            sec_count[s]=sec_count.get(s,0)+1
        sel.append(th)
        if len(sel)>=max_n: break
    return sel

def w5b_weights(port, cap=0.30):
    """Compute W5b consistency weights. cap=None for BEAST."""
    ws = []
    for p in port:
        vals = [p.get('r63',np.nan), p.get('r126',np.nan), p.get('r252ex1m',np.nan)]
        valid = [v for v in vals if np.isfinite(v)]
        if len(valid) < 2:
            ws.append(1.0)
        else:
            pc = sum(1 for v in valid if v > 0)
            ar = np.mean([max(v,0) for v in valid])
            ws.append(pc * (1 + ar))
    wa = np.array(ws, dtype=float)
    if wa.sum() <= 0: return np.ones(len(port)) / len(port)
    wa = wa / wa.sum()
    if cap is not None:
        for _ in range(5):
            exc = np.maximum(wa - cap, 0)
            if exc.sum() < 1e-6: break
            under = wa < cap; wa = np.minimum(wa, cap)
            if under.any(): wa[under] += exc.sum() * (wa[under] / wa[under].sum())
            wa = wa / wa.sum()
    return wa

WARMUP=126; REBAL=20; MIN_M=4; MAX_CORR=0.80
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance: {len(rebal_idx)-1} periods')

# Strategy definitions
STRATEGIES = {
    'PRISM':    {'n_themes': 10, 'stock_score': 'raw_alpha', 'sector_cap': 3},
    'PRISM-R':  {'n_themes': 10, 'stock_score': 'raw_alpha', 'sector_cap': None},
    'PRISM-RQ': {'n_themes': 10, 'stock_score': 'snrb',      'sector_cap': None},
}
WEIGHT_MODES = ['W0_equal', 'W5b_cap30', 'BEAST_nocap']

all_daily = {}
for strat_name, strat_cfg in STRATEGIES.items():
    for wmode in WEIGHT_MODES:
        key = f'{strat_name}_{wmode}'
        all_daily[key] = []

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    dt252=set(dates_all[max(0,j-251):j+1])
    sub=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    sub252=panel[panel['date'].isin(dt252)]
    tm=sub.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme momentum
    tm_m = {h:{} for h in [21,63,126,252]}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21: tm_m[21][th]=cumret(td[-21:])
        if len(td)>=63: tm_m[63][th]=cumret(td)
        td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126v)>=63: tm_m[126][th]=cumret(td126v)
        td252v=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td252v)>=126: tm_m[252][th]=cumret(td252v)
    tdf=pd.DataFrame({f'm{h}':pd.Series(tm_m[h]) for h in [21,63,126,252]}).dropna(subset=['m63'])
    if len(tdf)<3:
        for key in all_daily: all_daily[key].extend([0.0]*len(hold_dates))
        continue
    tdf['m252ex1m']=np.where(tdf['m252'].notna()&tdf['m21'].notna(),(1+tdf['m252'])/(1+tdf['m21'])-1,np.nan)
    tdf['score']=(0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom'))
    ranked=list(tdf.sort_values('score',ascending=False).index)
    for strat_name, strat_cfg in STRATEGIES.items():
        n_th = strat_cfg['n_themes']
        sec_cap = strat_cfg.get('sector_cap', None)
        sel = corr_select(ranked, sub, n_th, sector_cap=sec_cap)
        # Stock selection per theme
        port = []; used = set()
        for th in sel:
            ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
            if len(tks)<MIN_M: continue
            scores={}
            for tk in tks:
                tkd=ths[ths['ticker']==tk].sort_values('date')
                alpha, sigma = ols_alpha(tkd['ret'].values, tkd['theme_ex_self'].values)
                if not np.isfinite(alpha): continue
                if strat_cfg['stock_score'] == 'raw_alpha':
                    scores[tk] = alpha
                else:  # snrb
                    snrb = alpha / sigma if sigma > 1e-8 else 0
                    scores[tk] = snrb
            # Pick best not yet used
            for tk, sc in sorted(scores.items(), key=lambda x: -x[1]):
                if tk not in used:
                    r63=tdf.loc[th,'m63'] if th in tdf.index else np.nan
                    r126=tdf.loc[th,'m126'] if th in tdf.index and np.isfinite(tdf.loc[th,'m126']) else np.nan
                    r252ex1m=tdf.loc[th,'m252ex1m'] if th in tdf.index and np.isfinite(tdf.loc[th,'m252ex1m']) else np.nan
                    port.append({'tk':tk,'th':th,'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                    used.add(tk); break
        if not port:
            for wm in WEIGHT_MODES: all_daily[f'{strat_name}_{wm}'].extend([0.0]*len(hold_dates))
            continue
        n=len(port); tickers=[p['tk'] for p in port]
        weights = {
            'W0_equal': np.ones(n)/n,
            'W5b_cap30': w5b_weights(port, cap=0.30),
            'BEAST_nocap': w5b_weights(port, cap=None),
        }
        for wm in WEIGHT_MODES:
            ws=pd.Series(weights[wm], index=tickers)
            dr=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ws,axis=1).sum(axis=1)
            all_daily[f'{strat_name}_{wm}'].extend(dr.values.tolist())

# === Results ===
eq_dates=tk_wide.index[-len(all_daily[list(all_daily.keys())[0]]):]
def calc(dr):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    terminal=float(eq[-1])
    neg=arr[arr<0]; dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    return {'cagr':cagr,'vol':vol,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'terminal':terminal}

for strat_name in STRATEGIES:
    print(f"\n{'='*110}")
    print(f"  {strat_name} ({STRATEGIES[strat_name]['stock_score']})")
    print(f"{'='*110}")
    print(f"  {'Weight':<16} {'CAGR':>8} {'MaxDD':>8} {'Sharpe':>8} {'Sortino':>8} {'Calmar':>8} {'Vol':>8} {'Term':>8}")
    print(f"  {'-'*95}")
    results = {}
    for wm in WEIGHT_MODES:
        key=f'{strat_name}_{wm}'
        m=calc(all_daily[key]); results[wm]=m
        star=' ★' if wm=='W0_equal' else ''
        print(f"  {wm:<14}{star:2s} {m['cagr']:>7.1%} {m['maxdd']:>7.1%} {m['sharpe']:>7.3f} {m['sortino']:>7.3f} {m['calmar']:>7.3f} {m['vol']:>7.1%} {m['terminal']:>7.1f}x")
    w0=results['W0_equal']
    print(f"\n  vs W0:")
    for wm in ['W5b_cap30','BEAST_nocap']:
        m=results[wm]
        print(f"    {wm:<16} ΔCAGR={m['cagr']-w0['cagr']:>+7.1%} ΔSharpe={m['sharpe']-w0['sharpe']:>+6.3f} ΔMaxDD={m['maxdd']-w0['maxdd']:>+6.1%} ΔCalmar={m['calmar']-w0['calmar']:>+6.3f}")

    # Annual
    print(f"\n  Annual:")
    header=f"  {'Year':<6}"
    for wm in WEIGHT_MODES: header+=f"  {wm:>14}"
    print(header)
    for wm in WEIGHT_MODES:
        key=f'{strat_name}_{wm}'
        eq=pd.Series(np.cumprod(1+np.array(all_daily[key])),index=eq_dates)
        results[wm]['annual']={str(d.year):round(float(r),3) for d,r in eq.resample('YE').last().pct_change().dropna().items()}
    for yr in ['2020','2021','2022','2023','2024','2025','2026']:
        row=f"    {yr}"
        for wm in WEIGHT_MODES: row+=f"  {results[wm].get('annual',{}).get(yr,0):>+13.1%}"
        print(row)

# === Cross-strategy comparison ===
print(f"\n{'='*110}")
print("  CROSS-COMPARISON: W5b improvement by strategy type")
print(f"{'='*110}")
print(f"  {'Strategy':<12} {'W0 Sharpe':>10} {'W5b Sharpe':>11} {'BEAST Sharpe':>13} {'W5b ΔSharpe':>12} {'W5b ΔCAGR':>10} {'W5b ΔMaxDD':>11}")
print(f"  {'-'*80}")
for strat_name in STRATEGIES:
    w0=calc(all_daily[f'{strat_name}_W0_equal'])
    w5=calc(all_daily[f'{strat_name}_W5b_cap30'])
    be=calc(all_daily[f'{strat_name}_BEAST_nocap'])
    print(f"  {strat_name:<12} {w0['sharpe']:>9.3f} {w5['sharpe']:>10.3f} {be['sharpe']:>12.3f} {w5['sharpe']-w0['sharpe']:>+11.3f} {w5['cagr']-w0['cagr']:>+9.1%} {w5['maxdd']-w0['maxdd']:>+10.1%}")

# Save
all_results = {}
for key in all_daily:
    all_results[key] = calc(all_daily[key])
with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_w5b_all_results.json','w') as f:
    json.dump(all_results, f, indent=2, default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
