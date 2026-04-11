"""GMAX-K3 Ablation Backtest — CAGR maximization ladder
G0: A5-lite baseline
G1: 10→5 themes + corr budget (no sector cap)
G2: raw α63 (no SNRb/shrink)
G3a: α/σ² Kelly ranking
G3b: 5→3 themes cluster-distinct
G4: Kelly-lite sizing (45/35/20)
G5: panic-state de-gearing
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
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

# Load SPY for panic detection
import yfinance as yf
spy = yf.download('SPY', start='2019-01-01', end='2027-01-01', progress=False)
spy_ret = spy['Close'].pct_change().dropna()
spy_ret.index = pd.to_datetime(spy_ret.index).tz_localize(None)

def ols_full(y, x):
    mask = np.isfinite(y)&np.isfinite(x); y,x = y[mask],x[mask]; n=len(y)
    if n<20: return np.nan, np.nan, np.nan, np.nan, np.nan
    xm,ym = x.mean(),y.mean(); vx = np.var(x,ddof=1)
    if vx<1e-15: return np.nan, np.nan, np.nan, np.nan, np.nan
    b = np.dot(x-xm,y-ym)/(n-1)/vx; a = ym-b*xm
    resid = y-a-b*x
    ss_res=float(np.sum(resid**2)); ss_tot=float(np.sum((y-ym)**2))
    r2 = 1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    resid_var = float(np.var(resid, ddof=1))
    return a*n, b, r2, float(np.std(resid,ddof=1)*np.sqrt(n)), resid_var*n
def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0
def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def theme_corr_matrix(sub, themes):
    """Build theme daily return correlation matrix."""
    tdr = {}
    for th in themes:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        if len(td) >= 20: tdr[th] = td
    if len(tdr) < 2: return pd.DataFrame()
    return pd.DataFrame(tdr).dropna().corr()

def select_cluster_distinct(scored_themes, sub, max_n, max_corr=0.80):
    """Greedy cluster-distinct selection."""
    corr_mat = theme_corr_matrix(sub, scored_themes)
    if corr_mat.empty: return scored_themes[:max_n]
    selected = []
    for th in scored_themes:
        if th not in corr_mat.index: continue
        conflict = False
        for sel_th in selected:
            if sel_th in corr_mat.columns and th in corr_mat.index:
                if abs(corr_mat.loc[th, sel_th]) > max_corr:
                    conflict = True; break
        if not conflict:
            selected.append(th)
            if len(selected) >= max_n: break
    return selected
def is_panic(dt, spy_ret_series):
    """Panic state: market down >10% in 63d AND vol high."""
    idx = spy_ret_series.index.get_indexer([dt], method='ffill')[0]
    if idx < 63: return False
    r63 = float(np.expm1(np.log1p(spy_ret_series.iloc[max(0,idx-62):idx+1].values).sum()))
    vol = float(spy_ret_series.iloc[max(0,idx-19):idx+1].std() * np.sqrt(252))
    return r63 < -0.10 and vol > 0.25

WARMUP=126; REBAL=20; MIN_M=4; SEC_MAX=3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

# Strategy configs
STRATS = {
    'G0_A5lite':   {'n_themes':10, 'l2':'a5lite',  'corr_budget':False, 'sec_cap':True,  'sizing':'equal', 'panic':False},
    'G1_conc5':    {'n_themes':5,  'l2':'a5lite',  'corr_budget':True,  'sec_cap':False, 'sizing':'equal', 'panic':False},
    'G2_rawA':     {'n_themes':5,  'l2':'raw_alpha','corr_budget':True,  'sec_cap':False, 'sizing':'equal', 'panic':False},
    'G3a_kelly':   {'n_themes':5,  'l2':'kelly',   'corr_budget':True,  'sec_cap':False, 'sizing':'equal', 'panic':False},
    'G3b_3theme':  {'n_themes':3,  'l2':'kelly',   'corr_budget':True,  'sec_cap':False, 'sizing':'equal', 'panic':False},
    'G4_klsize':   {'n_themes':3,  'l2':'kelly',   'corr_budget':True,  'sec_cap':False, 'sizing':'kelly', 'panic':False},
    'G5_panic':    {'n_themes':3,  'l2':'kelly',   'corr_budget':True,  'sec_cap':False, 'sizing':'kelly', 'panic':True},
}
daily_ret = {s: [] for s in STRATS}
for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]; dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1])
    dt126 = set(dates_all[max(0,j-125):j+1])
    sub = panel[panel['date'].isin(dt63)]
    sub126 = panel[panel['date'].isin(dt126)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm>=MIN_M].index.tolist()
    hold_dates = tk_wide.index[j+1:j_next+1]
    # Theme momentum
    tm_mom63, tm_mom126, tm_mom21 = {}, {}, {}
    for th in elig:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td) >= 21: tm_mom21[th] = cumret(td[-21:])
        if len(td) >= 63: tm_mom63[th] = cumret(td)
        td126v = sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126v) >= 63: tm_mom126[th] = cumret(td126v)
    # Decel
    dc = {}
    for th in tm_mom63:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)<63: continue
        r021=cumret(td[-21:]); r2142=cumret(td[-42:-21]); r4263=cumret(td[-63:-42])
        if all(np.isfinite([r021,r2142,r4263])): dc[th]=-(r021-0.5*(r2142+r4263))
    # Build theme scores
    common63 = [th for th in elig if th in tm_mom63 and th in dc]
    if not common63:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    tdf = pd.DataFrame({'mom63': pd.Series(tm_mom63), 'mom126': pd.Series(tm_mom126),
                         'mom21': pd.Series(tm_mom21), 'decel': pd.Series(dc)}).dropna(subset=['mom63'])
    # G0 score (PRISM standard)
    tdf['score_g0'] = 0.70*tdf['mom63'].rank(pct=True) + 0.30*tdf['decel'].rank(pct=True,na_option='bottom')
    # Growth score (R63/R126/R21 trend)
    tdf['score_growth'] = (0.50*tdf['mom63'].rank(pct=True) +
                           0.30*tdf['mom126'].rank(pct=True,na_option='bottom') +
                           0.20*tdf['mom21'].rank(pct=True,na_option='bottom'))
    panic = is_panic(dt, spy_ret)
    for sname, cfg in STRATS.items():
        n_th = cfg['n_themes']
        # Theme selection
        score_col = 'score_g0' if sname.startswith('G0') else 'score_growth'
        ranked = tdf.sort_values(score_col, ascending=False)
        if cfg['corr_budget']:
            sel = select_cluster_distinct(list(ranked.index), sub, n_th)
        elif cfg['sec_cap']:
            sel=[]; sc_cnt={}
            for th in ranked.index:
                s2=theme_sector.get(th,'Unk')
                if sc_cnt.get(s2,0)>=SEC_MAX: continue
                sel.append(th); sc_cnt[s2]=sc_cnt.get(s2,0)+1
                if len(sel)>=n_th: break
        else:
            sel = list(ranked.index[:n_th])
        # Stock selection per theme
        port = {}; used = set()
        theme_kelly_scores = {}
        for th in sel:
            ths = sub[(sub['theme']==th)&sub['ret'].notna()]
            tks = ths['ticker'].unique()
            if len(tks) < MIN_M: continue
            scores = {}
            for tk in tks:
                tkd = ths[ths['ticker']==tk].sort_values('date')
                a63,b63,r2_63,rvol,rvar = ols_full(tkd['ret'].values, tkd['theme_ex_self'].values)
                if cfg['l2'] == 'a5lite':
                    shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
                    scores[tk] = a63*shrk if np.isfinite(a63) else -999
                elif cfg['l2'] == 'raw_alpha':
                    scores[tk] = a63 if np.isfinite(a63) else -999
                elif cfg['l2'] == 'kelly':
                    # K = alpha_cum63 / resid_var_63
                    if np.isfinite(a63) and np.isfinite(rvar) and rvar > 1e-10:
                        scores[tk] = a63 / rvar
                    else:
                        scores[tk] = -999
            # Pick top 2 for G3b/G4/G5, top 1 for others
            n_picks = 2 if cfg['n_themes'] <= 3 else 1
            picked = []
            for tk, sc in sorted(scores.items(), key=lambda x:-x[1]):
                if tk not in used and sc > -999:
                    picked.append((tk, sc)); used.add(tk)
                    if len(picked) >= n_picks: break
            for tk, sc in picked:
                port[tk] = sc
            if picked:
                theme_kelly_scores[th] = sum(sc for _,sc in picked)
        # Weighting
        if not port:
            daily_ret[sname].extend([0.0]*len(hold_dates)); continue
        if cfg['sizing'] == 'kelly' and len(sel) >= 2:
            # Theme weights: rank proxy 45/35/20 for 3 themes, else proportional
            theme_weights = {}
            sorted_themes = sorted(theme_kelly_scores.items(), key=lambda x:-x[1])
            if len(sorted_themes) == 3:
                wts = [0.45, 0.35, 0.20]
            elif len(sorted_themes) == 2:
                wts = [0.60, 0.40]
            else:
                wts = [1.0/len(sorted_themes)] * len(sorted_themes)
            for idx_t, (th, _) in enumerate(sorted_themes):
                if idx_t < len(wts): theme_weights[th] = wts[idx_t]
            # Assign stock weights within theme by K score
            ws = {}
            for th in sel:
                th_stocks = [(tk, sc) for tk, sc in port.items()
                             if any(tk == p[0] for p in [(t,s) for t,s in port.items()])]
            # Simpler: just assign proportional to K scores
            total_k = sum(max(v,0) for v in port.values())
            if total_k > 0:
                ws = {tk: max(v,0)/total_k for tk, v in port.items()}
            else:
                ws = {tk: 1.0/len(port) for tk in port}
        else:
            ws = {tk: 1.0/len(port) for tk in port}
        # Panic de-gearing
        gross_mult = 0.5 if (cfg['panic'] and panic) else 1.0
        ws_series = pd.Series(ws) * gross_mult
        dr = tk_wide.loc[hold_dates].reindex(columns=ws_series.index).fillna(0).mul(ws_series, axis=1).sum(axis=1)
        daily_ret[sname].extend(dr.values.tolist())

# === Metrics ===
eq_dates = tk_wide.index[-len(daily_ret['G0_A5lite']):]
def calc_m(dr, name):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    log_growth = float(np.mean(np.log1p(arr))) * 252  # annualized
    terminal = float(eq[-1]) if len(eq)>0 else 1
    worst_m = float(pd.Series(arr, index=eq_dates[:len(arr)]).resample('M').apply(lambda x: float(np.expm1(np.log1p(x).sum()))).min())
    return {'name':name,'cagr':cagr,'vol':vol,'sharpe':sharpe,'maxdd':maxdd,
            'log_growth':log_growth,'terminal':terminal,'worst_month':worst_m}

print("\n" + "="*95)
print(f"{'Strategy':<16} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} {'MaxDD':>8} {'LogGr':>8} {'Term$':>8} {'WorstM':>8}")
print("="*95)
results = {}
for sname in STRATS:
    m = calc_m(daily_ret[sname], sname)
    results[sname] = m
    print(f"  {sname:<14} {m['cagr']:>7.1%} {m['vol']:>7.1%} {m['sharpe']:>7.3f} {m['maxdd']:>7.1%} {m['log_growth']:>7.3f} {m['terminal']:>7.1f}x {m['worst_month']:>7.1%}")
print("="*95)

# Annual
print("\n=== ANNUAL ===")
header = f"{'Year':<6}" + "".join(f"{s:>14}" for s in STRATS)
print(header)
for s in STRATS:
    eq = pd.Series(np.cumprod(1+np.array(daily_ret[s])), index=eq_dates)
    annual = eq.resample('YE').last().pct_change().dropna()
    results[s]['annual'] = {str(d.year): round(float(r),3) for d, r in annual.items()}
for yr in ['2021','2022','2023','2024','2025','2026']:
    row = f"  {yr:<4}"
    for s in STRATS:
        v = results[s].get('annual',{}).get(yr, 0)
        row += f"  {v:>+11.1%}"
    print(row)

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_gmax_results.json','w') as f:
    json.dump(results, f, indent=2, default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
