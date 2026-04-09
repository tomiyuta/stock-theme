"""P1: Feasibility Report + P2: A4 vs A5-lite comparison.
Runs in ~2 min on 244K-row panel (207 themes × 252 days).
"""
import pandas as pd, numpy as np
from pathlib import Path
import time, warnings
warnings.filterwarnings('ignore')

t0 = time.time()

# ---- Load ----
panel = pd.read_parquet('theme_daily_panel.parquet')
meta = pd.read_parquet('ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes | {panel.ticker.nunique()} tickers')

# ---- Returns ----
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)

# ---- Unique ticker returns wide ----
tk_ret = panel[['date','ticker','ret']].drop_duplicates(['date','ticker']).dropna(subset=['ret'])
tk_wide = tk_ret.pivot(index='date', columns='ticker', values='ret').sort_index()
dates_all = sorted(panel['date'].unique())
print(f'Trading days: {len(dates_all)}')

# ---- Sector mapping ----
meta_sec = meta[['ticker','sector']].drop_duplicates('ticker')
psec = panel[['theme','ticker']].drop_duplicates().merge(meta_sec, on='ticker', how='left')
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unknown')

# ---- Helpers ----
def ols_ab(y, x):
    mask = np.isfinite(y) & np.isfinite(x)
    y, x = y[mask], x[mask]
    n = len(y)
    if n < 10: return np.nan, np.nan, np.nan, n
    xm, ym = x.mean(), y.mean()
    xd = x - xm
    vx = np.dot(xd, xd)/(n-1)
    if vx < 1e-12: return np.nan, np.nan, np.nan, n
    b = np.dot(xd, y-ym)/(n-1) / vx
    a = ym - b*xm
    ss_res = float(np.sum((y - a - b*x)**2))
    ss_tot = float(np.sum((y - ym)**2))
    r2 = 1 - ss_res/ss_tot if ss_tot > 1e-12 else np.nan
    return a*n, b, r2, n

def cumret(arr):
    a = np.asarray(arr, dtype=float)
    a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v < 0: return 0.0
    if r2v < 0.10: return r2v * 2
    if r2v <= 0.50: return 0.20 + (r2v - 0.10) * 2.0
    return 1.0

# ---- Config ----
WARMUP = 126
REBAL = 20
MIN_MEMBERS = 4
TOP_THEMES = 10
SECTOR_MAX = 3

rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1:
    rebal_idx.append(len(dates_all)-1)
print(f'Holding periods: {len(rebal_idx)-1}')

# ---- Main loop ----
feas_log = []   # feasibility
a4_rets = []    # A4: raw 1M
a5_rets = []    # A5-lite: residual alpha + r2 shrinkage

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]
    j_next = rebal_idx[pos+1]
    dt = dates_all[j]
    dt_set_63 = set(dates_all[max(0,j-62):j+1])
    dt_set_21 = set(dates_all[max(0,j-20):j+1])

    # --- Theme features ---
    sub = panel[panel['date'].isin(dt_set_63)].copy()
    theme_members = sub.groupby('theme')['ticker'].nunique()
    eligible = theme_members[theme_members >= MIN_MEMBERS].index.tolist()

    # Theme 3M momentum
    theme_mom = {}
    for th in eligible:
        tdata = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        theme_mom[th] = cumret(tdata.values)

    mom_s = pd.Series(theme_mom).dropna().sort_values(ascending=False)
    # Deceleration: ret_0_21 vs avg(ret_21_42, ret_42_63)
    decel_s = {}
    for th in mom_s.index:
        tdata = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        vals = tdata.values
        if len(vals) < 63: continue
        r021 = cumret(vals[-21:])
        r2142 = cumret(vals[-42:-21])
        r4263 = cumret(vals[-63:-42])
        if np.isfinite(r021) and np.isfinite(r2142) and np.isfinite(r4263):
            decel_s[th] = -(r021 - 0.5*(r2142+r4263))  # higher = more decelerated
    decel_ser = pd.Series(decel_s)

    # Theme score: 0.60*rank(mom63) + 0.20*rank(mom126_proxy) + 0.20*rank(decel)
    # Simplify: use mom63 + decel only (mom126 needs 126d which may not be fully in 63d window)
    common = list(set(mom_s.index) & set(decel_ser.index))
    if not common:
        feas_log.append({'date':dt, 'eligible':len(eligible), 'scored':0, 'a4_names':0, 'a5_names':0})
        continue
    ts = pd.DataFrame({'mom63': mom_s[common], 'decel': decel_ser[common]})
    ts['r_mom'] = ts['mom63'].rank(pct=True)
    ts['r_dec'] = ts['decel'].rank(pct=True)
    ts['score'] = 0.70*ts['r_mom'] + 0.30*ts['r_dec']
    ts = ts.sort_values('score', ascending=False)

    # Sector cap: max SECTOR_MAX per sector
    selected_themes = []
    sec_count = {}
    for th in ts.index:
        sec = theme_sector.get(th, 'Unknown')
        if sec_count.get(sec, 0) >= SECTOR_MAX:
            continue
        selected_themes.append(th)
        sec_count[sec] = sec_count.get(sec, 0) + 1
        if len(selected_themes) >= TOP_THEMES:
            break

    # --- Stock scoring within selected themes ---
    a4_picks = {}  # raw 1M
    a5_picks = {}  # residual alpha
    stock_feas = {'pass_final': 0, 'total_candidates': 0, 'themes_with_pick': 0}

    for th in selected_themes:
        th_sub = sub[(sub['theme']==th) & sub['ret'].notna()].copy()
        tickers_in_theme = th_sub['ticker'].unique()
        if len(tickers_in_theme) < MIN_MEMBERS:
            continue

        stock_scores_a4 = {}
        stock_scores_a5 = {}

        for tk in tickers_in_theme:
            tk_data = th_sub[th_sub['ticker']==tk].sort_values('date')
            y63 = tk_data['ret'].values
            x63 = tk_data['theme_ex_self'].values

            # A4: raw 1M return
            r21_data = tk_data[tk_data['date'].isin(dt_set_21)]
            raw_1m = cumret(r21_data['ret'].values) if len(r21_data) >= 10 else np.nan
            stock_scores_a4[tk] = raw_1m if np.isfinite(raw_1m) else -999

            # A5-lite: residual alpha63 × shrink(r2)
            alpha63, beta63, r2_63, n63 = ols_ab(y63, x63)
            if np.isfinite(alpha63) and np.isfinite(r2_63):
                s = shrink_r2(r2_63)
                stock_scores_a5[tk] = alpha63 * s
                stock_feas['total_candidates'] += 1
                if alpha63 > 0:
                    stock_feas['pass_final'] += 1
            else:
                stock_scores_a5[tk] = -999

        # A4 pick: top raw 1M (skip if already picked)
        a4_sorted = sorted(stock_scores_a4.items(), key=lambda x: -x[1])
        for tk, sc in a4_sorted:
            if tk not in a4_picks and sc > -999:
                a4_picks[tk] = 1.0/TOP_THEMES
                break

        # A5 pick: top residual alpha*shrink (skip if already picked)
        a5_sorted = sorted(stock_scores_a5.items(), key=lambda x: -x[1])
        for tk, sc in a5_sorted:
            if tk not in a5_picks and sc > -999:
                a5_picks[tk] = 1.0/TOP_THEMES
                stock_feas['themes_with_pick'] += 1
                break

    # Normalize weights
    for d in [a4_picks, a5_picks]:
        total = sum(d.values())
        if total > 0:
            for k in d: d[k] /= total

    feas_log.append({
        'date': dt,
        'eligible': len(eligible),
        'scored': len(common),
        'selected_themes': len(selected_themes),
        'a4_names': len(a4_picks),
        'a5_names': len(a5_picks),
        'a5_candidates': stock_feas['total_candidates'],
        'a5_pass_alpha_pos': stock_feas['pass_final'],
        'a5_themes_with_pick': stock_feas['themes_with_pick'],
    })

    # Portfolio returns for this holding period
    hold_dates = tk_wide.index[j+1:j_next+1]
    for w_dict, ret_list in [(a4_picks, a4_rets), (a5_picks, a5_rets)]:
        if not w_dict:
            ret_list.extend([0.0]*len(hold_dates))
            continue
        ws = pd.Series(w_dict)
        daily_r = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws, axis=1).sum(axis=1)
        ret_list.extend(daily_r.values.tolist())

    if (pos+1) % 2 == 0:
        print(f'  Period {pos+1}/{len(rebal_idx)-1} done ({dt.date()}) | A4={len(a4_picks)} A5={len(a5_picks)} names')

print(f'\nMain loop done in {time.time()-t0:.1f}s')

# =========== P1: FEASIBILITY REPORT ===========
fl = pd.DataFrame(feas_log)
print('\n' + '='*60)
print('P1: FEASIBILITY REPORT')
print('='*60)
print(f'\nRebalance periods: {len(fl)}')
print(f'\n--- Theme eligibility (min_members={MIN_MEMBERS}) ---')
print(f'  Avg eligible themes/period:     {fl["eligible"].mean():.1f}')
print(f'  Min eligible themes:            {fl["eligible"].min()}')
print(f'  Avg scored themes:              {fl["scored"].mean():.1f}')
print(f'  Avg selected themes:            {fl["selected_themes"].mean():.1f}')

print(f'\n--- A5 Stock feasibility ---')
print(f'  Avg candidates/period:          {fl["a5_candidates"].mean():.1f}')
print(f'  Avg pass alpha>0:               {fl["a5_pass_alpha_pos"].mean():.1f}')
print(f'  Avg themes with A5 pick:        {fl["a5_themes_with_pick"].mean():.1f}')

print(f'\n--- Portfolio coverage ---')
print(f'  A4 avg names:                   {fl["a4_names"].mean():.1f}')
print(f'  A5 avg names:                   {fl["a5_names"].mean():.1f}')
print(f'  A4 zero-pick periods:           {(fl["a4_names"]==0).sum()}')
print(f'  A5 zero-pick periods:           {(fl["a5_names"]==0).sum()}')

# Fallback rate: periods where A5 < A4 names
fallback = (fl['a5_names'] < fl['a4_names']).sum()
total_periods = len(fl)
print(f'\n  A5 fallback rate (fewer names than A4): {fallback}/{total_periods} = {fallback/total_periods:.0%}')

# =========== P2: A4 vs A5-lite PERFORMANCE ===========
def calc_metrics(daily_rets, label):
    r = np.array(daily_rets, dtype=float)
    r = r[np.isfinite(r)]
    if len(r) < 10:
        return {}
    cum = float(np.prod(1+r) - 1)
    n_yr = len(r)/252
    cagr = (1+cum)**(1/n_yr)-1 if n_yr > 0 else np.nan
    vol = float(np.std(r, ddof=1)*np.sqrt(252))
    sharpe = float(np.mean(r)/np.std(r, ddof=1)*np.sqrt(252)) if np.std(r)>0 else np.nan
    down = r[r<0]
    sortino = float(np.mean(r)/np.std(down, ddof=1)*np.sqrt(252)) if len(down)>1 and np.std(down)>0 else np.nan
    wealth = np.cumprod(1+r)
    peak = np.maximum.accumulate(wealth)
    dd = wealth/peak - 1
    mdd = float(dd.min())
    calmar = cagr/abs(mdd) if mdd < 0 else np.nan
    win = float(np.mean(r>0))
    return {
        'strategy': label, 'CAGR': f'{cagr:.1%}', 'Vol': f'{vol:.1%}',
        'Sharpe': f'{sharpe:.2f}', 'Sortino': f'{sortino:.2f}',
        'MaxDD': f'{mdd:.1%}', 'Calmar': f'{calmar:.2f}',
        'CumRet': f'{cum:.1%}', 'WinRate': f'{win:.1%}', 'Days': len(r)
    }

m4 = calc_metrics(a4_rets, 'A4: raw 1M')
m5 = calc_metrics(a5_rets, 'A5-lite: α63×shrink(r²)')

print('\n' + '='*60)
print('P2: A4 vs A5-lite PERFORMANCE COMPARISON')
print('='*60)
comp = pd.DataFrame([m4, m5]).set_index('strategy')
print(comp.to_string())

# =========== P2 cont: Overlap / Worst-name / Monthly attribution ===========
# Monthly returns
a4_s = pd.Series(a4_rets, index=tk_wide.index[WARMUP+1:WARMUP+1+len(a4_rets)])
a5_s = pd.Series(a5_rets, index=tk_wide.index[WARMUP+1:WARMUP+1+len(a5_rets)])
a4_m = (1+a4_s).resample('M').prod()-1
a5_m = (1+a5_s).resample('M').prod()-1
diff_m = a5_m - a4_m

print('\n--- Monthly Returns ---')
monthly = pd.DataFrame({'A4': a4_m, 'A5': a5_m, 'Diff(A5-A4)': diff_m})
print(monthly.to_string(float_format='{:.2%}'.format))

print(f'\n--- A5 vs A4 Monthly Diff ---')
print(f'  Mean diff:     {diff_m.mean():.2%}')
print(f'  Months A5 > A4: {(diff_m>0).sum()}/{len(diff_m)}')
print(f'  Worst month A4: {a4_m.min():.2%}')
print(f'  Worst month A5: {a5_m.min():.2%}')
print(f'  Best month A4:  {a4_m.max():.2%}')
print(f'  Best month A5:  {a5_m.max():.2%}')

# Overlap: how many tickers are shared between A4 and A5 at each rebalance
print(f'\n--- Name Overlap A4∩A5 ---')
print('(computed within main loop - see feasibility log for details)')

# Save feasibility log
fl.to_csv('feasibility_report.csv', index=False)
print(f'\nSaved feasibility_report.csv')

print(f'\nTotal runtime: {time.time()-t0:.1f}s')
print('\n' + '='*60)
print('VERDICT')
print('='*60)
feas_ok = fl['a5_names'].mean() >= 7 and (fl['a5_names']==0).sum() == 0
print(f'  Feasibility gate (avg names>=7, no zero-pick): {"PASS" if feas_ok else "FAIL"}')
if not feas_ok:
    print(f'  → A5-lite may need looser filters or fallback logic')
print(f'  Direction (A5 avg monthly diff vs A4): {"POSITIVE" if diff_m.mean()>0 else "NEGATIVE"} ({diff_m.mean():+.2%}/mo)')
