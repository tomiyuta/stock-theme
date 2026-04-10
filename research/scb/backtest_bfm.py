"""
BFM-v1 backtest — Breadth-Adjusted Theme Factor Momentum
Layer 1 only change. Layer 2 = A5-SNRb fixed.
"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes | {panel.ticker.nunique()} tickers')

panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)

psec = panel[['theme','ticker']].drop_duplicates().merge(meta[['ticker','sector']], on='ticker', how='left')
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')
ticker_sector = dict(zip(meta['ticker'], meta['sector']))
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

def ols_full(y, x):
    mask = np.isfinite(y) & np.isfinite(x); y, x = y[mask], x[mask]; n = len(y)
    if n < 20: return np.nan, np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean(); vx = np.var(x, ddof=1)
    if vx < 1e-15: return np.nan, np.nan, np.nan, np.nan
    b = np.dot(x-xm, y-ym)/(n-1)/vx; a = ym - b*xm
    resid = y - a - b*x
    ss_res = float(np.sum(resid**2)); ss_tot = float(np.sum((y-ym)**2))
    r2 = 1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n, b, r2, float(np.std(resid, ddof=1)*np.sqrt(n))

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def score_snrb(a63, rvol, r2):
    if not np.isfinite(a63) or not np.isfinite(rvol) or rvol < 1e-8: return -999
    shrk = shrink_r2(r2) if np.isfinite(r2) else 0
    return (a63 / rvol) * shrk

# === BFM-v1 Theme Scorer ===
def compute_bfm_features(sub, dates_j, theme, MIN_M=4):
    """Compute BFM features for a theme at rebalance date."""
    ths = sub[sub['theme']==theme]
    tks = ths['ticker'].unique()
    if len(tks) < MIN_M: return None
    
    # Theme daily returns (sorted)
    theme_daily = ths.groupby('date')['theme_ret'].first().sort_index()
    td = theme_daily.values
    n = len(td)
    if n < 63: return None
    
    # R63, R126
    R63 = cumret(td[-63:])
    R126 = cumret(td[-min(126,n):]) if n >= 63 else np.nan
    
    # decel (PRISM-compatible)
    r021 = cumret(td[-21:]); r2142 = cumret(td[-42:-21]); r4263 = cumret(td[-63:-42])
    decel = -(r021 - 0.5*(r2142+r4263)) if all(np.isfinite([r021,r2142,r4263])) else np.nan
    
    # breadth63: fraction of tickers with positive 63-day return
    ticker_rets_63 = {}
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        if len(tkd) >= 20:
            ticker_rets_63[tk] = cumret(tkd['ret'].values[-63:])
    if len(ticker_rets_63) < MIN_M: return None
    breadth63 = sum(1 for v in ticker_rets_63.values() if np.isfinite(v) and v > 0) / len(ticker_rets_63)
    
    # breadth_persist63: avg fraction of positive days per ticker over 63 days
    pos_ratios = []
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        rets63 = tkd['ret'].values[-63:]
        valid = rets63[np.isfinite(rets63)]
        if len(valid) >= 20:
            pos_ratios.append(np.sum(valid > 0) / len(valid))
    breadth_persist63 = np.mean(pos_ratios) if pos_ratios else np.nan
    
    # concentration63: Herfindahl of absolute return contributions
    abs_contribs = np.array([abs(v) for v in ticker_rets_63.values() if np.isfinite(v)])
    total_abs = abs_contribs.sum()
    concentration63 = float(np.sum((abs_contribs/total_abs)**2)) if total_abs > 1e-10 else 1.0
    
    # theme_vol63
    theme_vol63 = float(np.std(td[-63:], ddof=1) * np.sqrt(252)) if n >= 63 else np.nan
    
    return {
        'R63': R63, 'R126': R126, 'decel': decel,
        'breadth63': breadth63, 'breadth_persist63': breadth_persist63,
        'concentration63': concentration63, 'theme_vol63': theme_vol63
    }

WARMUP = 126; REBAL = 20; MIN_M = 4; TOP_T = 10; SEC_MAX = 3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

STRATS = ['base', 'bfm']  # both use A5-SNRb for Layer 2
daily_ret = {s: [] for s in STRATS}
detail_log = []

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]; dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1])
    dt126 = set(dates_all[max(0,j-125):j+1])
    dt21 = set(dates_all[max(0,j-20):j+1])
    sub = panel[panel['date'].isin(dt63)]
    sub126 = panel[panel['date'].isin(dt126)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm >= MIN_M].index.tolist()
    
    # === Layer 1: Current PRISM (Base) ===
    tm_mom = {}
    for th in elig:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        tm_mom[th] = cumret(td.values)
    ms = pd.Series(tm_mom).dropna().sort_values(ascending=False)
    dc = {}
    for th in ms.index:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)<63: continue
        r021=cumret(td[-21:]); r2142=cumret(td[-42:-21]); r4263=cumret(td[-63:-42])
        if all(np.isfinite([r021,r2142,r4263])): dc[th]=-(r021-0.5*(r2142+r4263))
    dcs=pd.Series(dc); common=list(set(ms.index)&set(dcs.index))
    hold_dates = tk_wide.index[j+1:j_next+1]
    if not common:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    
    # Base Layer 1
    ts_base = pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts_base['r_mom']=ts_base['mom63'].rank(pct=True)
    ts_base['r_dec']=ts_base['decel'].rank(pct=True)
    ts_base['score']=0.70*ts_base['r_mom']+0.30*ts_base['r_dec']
    ts_base = ts_base.sort_values('score', ascending=False)
    sel_base=[]; sc_cnt={}
    for th in ts_base.index:
        s2=theme_sector.get(th,'Unk')
        if sc_cnt.get(s2,0)>=SEC_MAX: continue
        sel_base.append(th); sc_cnt[s2]=sc_cnt.get(s2,0)+1
        if len(sel_base)>=TOP_T: break
    
    # === Layer 1: BFM-v1 ===
    bfm_features = {}
    for th in elig:
        feat = compute_bfm_features(sub126 if len(sub126)>0 else sub, dates_all[j], th, MIN_M)
        if feat and all(np.isfinite(v) for v in feat.values()):
            bfm_features[th] = feat
    if bfm_features:
        bfm_df = pd.DataFrame(bfm_features).T
        # Rank blocks
        trend = (bfm_df['R63'].rank(pct=True) + bfm_df['R126'].rank(pct=True) + bfm_df['decel'].rank(pct=True)) / 3
        breadth = (bfm_df['breadth63'].rank(pct=True) + bfm_df['breadth_persist63'].rank(pct=True)) / 2
        fragility = (bfm_df['concentration63'].rank(pct=True) + bfm_df['theme_vol63'].rank(pct=True)) / 2
        bfm_df['bfm_score'] = trend + breadth - fragility
        bfm_df = bfm_df.sort_values('bfm_score', ascending=False)
        sel_bfm=[]; sc_cnt2={}
        for th in bfm_df.index:
            s2=theme_sector.get(th,'Unk')
            if sc_cnt2.get(s2,0)>=SEC_MAX: continue
            sel_bfm.append(th); sc_cnt2[s2]=sc_cnt2.get(s2,0)+1
            if len(sel_bfm)>=TOP_T: break
    else:
        sel_bfm = sel_base  # fallback
    
    # === Layer 2: A5-SNRb for both ===
    def pick_snrb(sel_themes):
        ports = {}; used = set()
        for th in sel_themes:
            ths = sub[(sub['theme']==th)&sub['ret'].notna()]
            tks = ths['ticker'].unique()
            if len(tks) < MIN_M: continue
            scores = {}
            for tk in tks:
                tkd = ths[ths['ticker']==tk].sort_values('date')
                a63, b63, r2_63, rvol = ols_full(tkd['ret'].values, tkd['theme_ex_self'].values)
                scores[tk] = score_snrb(a63, rvol, r2_63)
            for tk, sc in sorted(scores.items(), key=lambda x:-x[1]):
                if tk not in used and sc > -999:
                    ports[tk] = 1.0; used.add(tk); break
        total = sum(ports.values())
        if total > 0:
            for k in ports: ports[k] /= total
        return ports
    
    ports_base = pick_snrb(sel_base)
    ports_bfm = pick_snrb(sel_bfm)
    
    # Compute returns
    for s, port in [('base', ports_base), ('bfm', ports_bfm)]:
        if not port:
            daily_ret[s].extend([0.0]*len(hold_dates)); continue
        ws = pd.Series(port)
        dr = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1).sum(axis=1)
        daily_ret[s].extend(dr.values.tolist())
    
    # BFM features for selected themes
    sel_breadth = [bfm_features[th]['breadth63'] for th in sel_bfm if th in bfm_features] if bfm_features else []
    sel_conc = [bfm_features[th]['concentration63'] for th in sel_bfm if th in bfm_features] if bfm_features else []
    
    theme_overlap = len(set(sel_base) & set(sel_bfm))
    stock_overlap = len(set(ports_base.keys()) & set(ports_bfm.keys()))
    detail_log.append({
        'date': str(dt.date()), 'n_elig': len(elig),
        'base_themes': len(sel_base), 'bfm_themes': len(sel_bfm),
        'theme_overlap': theme_overlap, 'stock_overlap': stock_overlap,
        'avg_breadth': round(np.mean(sel_breadth),3) if sel_breadth else None,
        'avg_conc': round(np.mean(sel_conc),3) if sel_conc else None,
    })

# === Metrics ===
eq_dates = tk_wide.index[-len(daily_ret['base']):]

def calc_metrics(dr, name):
    arr = np.array(dr); arr = arr[np.isfinite(arr)]; n = len(arr); yrs = n/252
    cum = float(np.expm1(np.log1p(arr).sum()))
    cagr = (1+cum)**(1/yrs)-1; vol = float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe = cagr/vol if vol>1e-8 else 0
    eq = np.cumprod(1+arr); peak = np.maximum.accumulate(eq)
    maxdd = float(((eq-peak)/peak).min())
    neg = arr[arr<0]; dd = np.sqrt(np.mean(neg**2))*np.sqrt(252) if len(neg)>0 else 1e-8
    sortino = cagr/dd; calmar = cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    return {'name':name,'cagr':cagr,'vol':vol,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd}

print("\n" + "="*70)
print(f"{'Metric':<18} {'Base(L1)+SNRb':>15} {'BFM-v1+SNRb':>15} {'差':>12}")
print("="*70)
m1 = calc_metrics(daily_ret['base'], 'Base')
m2 = calc_metrics(daily_ret['bfm'], 'BFM-v1')
for key, label in [('cagr','CAGR'),('vol','Vol'),('sharpe','Sharpe'),('sortino','Sortino'),('calmar','Calmar'),('maxdd','MaxDD')]:
    v1,v2 = m1[key],m2[key]; diff = v2-v1
    fmt = lambda v: f"{v:.1%}" if key in ['cagr','vol','maxdd'] else f"{v:.3f}"
    print(f"  {label:<16} {fmt(v1):>14} {fmt(v2):>14} {'+' if diff>=0 else ''}{fmt(diff):>11}")
print("="*70)

# === Direction Check ===
diff = np.array(daily_ret['bfm']) - np.array(daily_ret['base'])
df_diff = pd.Series(diff, index=eq_dates)
monthly = df_diff.resample('M').sum()
print(f"\n=== BFM-v1 vs Base DIRECTION CHECK ===")
print(f"  Median monthly diff: {monthly.median():+.4f} ({'↑' if monthly.median()>0 else '↓'})")
print(f"  Positive months: {(monthly>0).sum()}/{len(monthly)} ({(monthly>0).sum()/len(monthly):.0%})")

# === Theme/Stock Overlap ===
dl = pd.DataFrame(detail_log)
print(f"\n=== OVERLAP ===")
print(f"  Theme overlap: {dl['theme_overlap'].mean():.1f}/{TOP_T} ({dl['theme_overlap'].mean()/TOP_T:.0%})")
print(f"  Stock overlap: {dl['stock_overlap'].mean():.1f}/{TOP_T} ({dl['stock_overlap'].mean()/TOP_T:.0%})")

# === BFM Features ===
print(f"\n=== BFM SELECTED THEME FEATURES ===")
print(f"  Avg breadth63: {dl['avg_breadth'].mean():.3f}")
print(f"  Avg concentration63: {dl['avg_conc'].mean():.3f}")

# === Annual ===
print(f"\n=== ANNUAL ===")
for s in STRATS:
    eq = pd.Series(np.cumprod(1+np.array(daily_ret[s])), index=eq_dates)
    annual = eq.resample('YE').last().pct_change().dropna()
    print(f"  {s}:")
    for dt_y, r in annual.items(): print(f"    {dt_y.year}: {r:+.1%}")

# === Save ===
with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_bfm_results.json','w') as f:
    json.dump({'metrics':{'base':m1,'bfm':m2},'detail_log':detail_log}, f, indent=2, default=str)

print(f'\n=== Done in {time.time()-t0:.1f}s ===')
