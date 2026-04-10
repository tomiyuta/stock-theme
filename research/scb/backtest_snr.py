"""
A5-SNR / A5-Quality backtest — direction check only
Extends backtest_extended.py with 3 new scoring methods.
"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes | {panel.ticker.nunique()} tickers')
print(f'Date: {panel.date.min().date()} ~ {panel.date.max().date()} ({panel.date.nunique()} days)')

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

# === OLS with residual stats ===
def ols_full(y, x):
    """Returns (alpha_cum63, beta, r2, resid_vol63, noise_path, residuals)"""
    mask = np.isfinite(y) & np.isfinite(x)
    y, x = y[mask], x[mask]
    n = len(y)
    if n < 20:
        return np.nan, np.nan, np.nan, np.nan, np.nan, np.array([])
    xm, ym = x.mean(), y.mean()
    vx = np.var(x, ddof=1)
    if vx < 1e-15:
        return np.nan, np.nan, np.nan, np.nan, np.nan, np.array([])
    b = np.dot(x - xm, y - ym) / (n - 1) / vx
    a = ym - b * xm
    residuals = y - a - b * x
    ss_res = float(np.sum(residuals**2))
    ss_tot = float(np.sum((y - ym)**2))
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-12 else np.nan
    alpha_cum = a * n  # cumulative alpha (same as existing)
    resid_vol = float(np.std(residuals, ddof=1) * np.sqrt(n))  # 63-day scale
    noise_path = float(np.sqrt(np.sum(residuals**2)))  # path noise
    return alpha_cum, b, r2, resid_vol, noise_path, residuals

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v < 0: return 0.0
    if r2v < 0.10: return r2v * 2
    if r2v <= 0.50: return 0.20 + (r2v - 0.10) * 2.0
    return 1.0

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

# === Scoring functions ===
def score_a5lite(alpha_cum, r2):
    shrk = shrink_r2(r2) if np.isfinite(r2) else 0
    return alpha_cum * shrk if np.isfinite(alpha_cum) else -999

def score_snra(alpha_cum, resid_vol):
    if not np.isfinite(alpha_cum) or not np.isfinite(resid_vol) or resid_vol < 1e-8:
        return -999
    return alpha_cum / resid_vol

def score_snrb(alpha_cum, resid_vol, r2):
    if not np.isfinite(alpha_cum) or not np.isfinite(resid_vol) or resid_vol < 1e-8:
        return -999
    shrk = shrink_r2(r2) if np.isfinite(r2) else 0
    return (alpha_cum / resid_vol) * shrk

def score_quality(alpha_cum, noise_path):
    if not np.isfinite(alpha_cum) or not np.isfinite(noise_path) or noise_path < 1e-8:
        return -999
    quality = abs(alpha_cum) / (abs(alpha_cum) + noise_path)
    return alpha_cum * quality

WARMUP = 126; REBAL = 20; MIN_M = 4; TOP_T = 10; SEC_MAX = 3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all) - 1:
    rebal_idx.append(len(dates_all) - 1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

STRATS = ['a4', 'a5lite', 'snra', 'snrb', 'quality']
daily_ret = {s: [] for s in STRATS}
detail_log = []

for pos in range(len(rebal_idx) - 1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos + 1]
    dt = dates_all[j]
    dt63 = set(dates_all[max(0, j-62):j+1])
    dt21 = set(dates_all[max(0, j-20):j+1])
    sub = panel[panel['date'].isin(dt63)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm >= MIN_M].index.tolist()
    # Theme selection (Layer 1) — identical for all strategies
    tm_mom = {}
    for th in elig:
        td = sub[sub['theme'] == th].groupby('date')['theme_ret'].first().sort_index()
        tm_mom[th] = cumret(td.values)
    ms = pd.Series(tm_mom).dropna().sort_values(ascending=False)
    dc = {}
    for th in ms.index:
        td = sub[sub['theme'] == th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td) < 63: continue
        r021 = cumret(td[-21:]); r2142 = cumret(td[-42:-21]); r4263 = cumret(td[-63:-42])
        if all(np.isfinite([r021, r2142, r4263])):
            dc[th] = -(r021 - 0.5 * (r2142 + r4263))
    dcs = pd.Series(dc); common = list(set(ms.index) & set(dcs.index))
    if not common:
        hold_dates = tk_wide.index[j+1:j_next+1]
        for s in STRATS:
            daily_ret[s].extend([0.0] * len(hold_dates))
        continue
    ts = pd.DataFrame({'mom63': ms[common], 'decel': dcs[common]})
    ts['r_mom'] = ts['mom63'].rank(pct=True)
    ts['r_dec'] = ts['decel'].rank(pct=True)
    ts['score'] = 0.70 * ts['r_mom'] + 0.30 * ts['r_dec']
    ts = ts.sort_values('score', ascending=False)
    sel = []; sc_cnt = {}
    for th in ts.index:
        s = theme_sector.get(th, 'Unk')
        if sc_cnt.get(s, 0) >= SEC_MAX: continue
        sel.append(th); sc_cnt[s] = sc_cnt.get(s, 0) + 1
        if len(sel) >= TOP_T: break
    # Layer 2: stock selection per strategy
    ports = {s: {} for s in STRATS}
    used = {s: set() for s in STRATS}
    for th in sel:
        ths = sub[(sub['theme'] == th) & sub['ret'].notna()]
        tks = ths['ticker'].unique()
        if len(tks) < MIN_M: continue
        scores = {s: {} for s in STRATS}
        for tk in tks:
            tkd = ths[ths['ticker'] == tk].sort_values('date')
            # A4 score (raw 1M momentum)
            r21d = tkd[tkd['date'].isin(dt21)]
            raw_1m = cumret(r21d['ret'].values) if len(r21d) >= 10 else np.nan
            scores['a4'][tk] = raw_1m if np.isfinite(raw_1m) else -999
            # OLS-based scores
            a63, b63, r2_63, rvol, npath, resid = ols_full(
                tkd['ret'].values, tkd['theme_ex_self'].values)
            scores['a5lite'][tk] = score_a5lite(a63, r2_63)
            scores['snra'][tk] = score_snra(a63, rvol)
            scores['snrb'][tk] = score_snrb(a63, rvol, r2_63)
            scores['quality'][tk] = score_quality(a63, npath)
        # Pick top ticker per theme per strategy
        for s in STRATS:
            for tk, sc in sorted(scores[s].items(), key=lambda x: -x[1]):
                if tk not in used[s] and sc > -999:
                    ports[s][tk] = 1.0; used[s].add(tk); break
    # Equal weight
    for s in STRATS:
        total = sum(ports[s].values())
        if total > 0:
            for k in ports[s]: ports[s][k] /= total
    # Compute returns
    hold_dates = tk_wide.index[j+1:j_next+1]
    for s in STRATS:
        if not ports[s]:
            daily_ret[s].extend([0.0] * len(hold_dates)); continue
        ws = pd.Series(ports[s])
        dr = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws, axis=1).sum(axis=1)
        daily_ret[s].extend(dr.values.tolist())
    # Log detail
    overlap_a4_snra = set(ports['a4'].keys()) & set(ports['snra'].keys())
    overlap_a5_snra = set(ports['a5lite'].keys()) & set(ports['snra'].keys())
    detail_log.append({
        'date': str(dt.date()), 'n_themes': len(sel),
        'a4_n': len(ports['a4']), 'a5_n': len(ports['a5lite']),
        'snra_n': len(ports['snra']), 'snrb_n': len(ports['snrb']), 'q_n': len(ports['quality']),
        'a4_snra_overlap': len(overlap_a4_snra), 'a5_snra_overlap': len(overlap_a5_snra),
    })

# === Metrics ===
spy = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
spy_close = spy[spy['ticker']=='SPY'].drop_duplicates('date').set_index('date')['close'].sort_index()
spy_ret = spy_close.pct_change().dropna()

def calc_metrics(dr, name):
    arr = np.array(dr); arr = arr[np.isfinite(arr)]
    n = len(arr)
    cum = float(np.expm1(np.log1p(arr).sum()))
    yrs = n / 252
    cagr = (1 + cum)**(1/yrs) - 1 if yrs > 0 else 0
    vol = float(np.std(arr, ddof=1) * np.sqrt(252))
    sharpe = cagr / vol if vol > 1e-8 else 0
    # MaxDD
    eq = np.cumprod(1 + arr)
    peak = np.maximum.accumulate(eq)
    dd = (eq - peak) / peak
    maxdd = float(dd.min())
    return {'name': name, 'cagr': cagr, 'vol': vol, 'sharpe': sharpe, 'maxdd': maxdd, 'n_days': n}

results = {}
for s in STRATS:
    m = calc_metrics(daily_ret[s], s)
    results[s] = m
    print(f"{s:10s}: CAGR={m['cagr']:.1%} Vol={m['vol']:.1%} Sharpe={m['sharpe']:.3f} MaxDD={m['maxdd']:.1%}")

# === Direction Check: monthly active diff ===
print('\n=== DIRECTION CHECK (vs A5-lite) ===')
eq_dates = tk_wide.index[-len(daily_ret['a4']):]

for s in ['snra', 'snrb', 'quality']:
    diff = np.array(daily_ret[s]) - np.array(daily_ret['a5lite'])
    # Monthly aggregate
    df_diff = pd.Series(diff, index=eq_dates)
    monthly = df_diff.resample('M').sum()
    pos_months = (monthly > 0).sum()
    neg_months = (monthly < 0).sum()
    median_monthly = monthly.median()
    cum_diff = float(np.expm1(np.log1p(daily_ret[s]).sum()) - np.expm1(np.log1p(daily_ret['a5lite']).sum()))
    print(f"\n  {s} vs a5lite:")
    print(f"    Median monthly diff: {median_monthly:+.4f} ({'↑' if median_monthly > 0 else '↓'})")
    print(f"    Positive months: {pos_months}/{len(monthly)} ({pos_months/len(monthly):.0%})")
    print(f"    Cumulative diff: {cum_diff:+.1%}")

# === Overlap analysis ===
print('\n=== OVERLAP ANALYSIS ===')
dl = pd.DataFrame(detail_log)
for col in ['a4_snra_overlap', 'a5_snra_overlap']:
    vals = dl[col]
    print(f"  {col}: mean={vals.mean():.1f} / {dl['snra_n'].mean():.1f} ({vals.mean()/dl['snra_n'].mean():.0%})")

# === Tech concentration proxy ===
# Count how many unique tickers each strategy picks
print('\n=== TICKER DIVERSITY ===')
for s in STRATS:
    n_rebal = len([d for d in detail_log if d[f'{s}_n'] > 0]) if f'{s}_n' in detail_log[0] else 'N/A'
    print(f"  {s}: avg stocks/rebal = {dl[f'{s}_n'].mean():.1f}" if f'{s}_n' in dl.columns else f"  {s}: N/A")

# === Annual breakdown ===
print('\n=== ANNUAL CAGR BY STRATEGY ===')
for s in STRATS:
    eq = pd.Series(np.cumprod(1 + np.array(daily_ret[s])), index=eq_dates)
    annual = eq.resample('Y').last()
    annual_ret = annual.pct_change().dropna()
    print(f"\n  {s}:")
    for dt_y, r in annual_ret.items():
        print(f"    {dt_y.year}: {r:+.1%}")

# === Save results ===
output = {
    'metrics': results,
    'detail_log': detail_log,
    'direction_check': {},
}
for s in ['snra', 'snrb', 'quality']:
    diff = np.array(daily_ret[s]) - np.array(daily_ret['a5lite'])
    df_diff = pd.Series(diff, index=eq_dates)
    monthly = df_diff.resample('M').sum()
    output['direction_check'][s] = {
        'median_monthly_diff': float(monthly.median()),
        'positive_months_pct': float((monthly > 0).sum() / len(monthly)),
        'cumulative_diff': float(np.sum(diff)),
    }

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_snr_results.json', 'w') as f:
    json.dump(output, f, indent=2, default=str)

print(f'\n=== Done in {time.time()-t0:.1f}s ===')
print(f'Results saved to bt_snr_results.json')
