#!/usr/bin/env python3
"""Generate PRISM-R shadow comparison JSON from theme-details data.
Runs as part of daily_update.yml after build_for_vercel.py.
Input:  public/api/theme-details/*.json + public/api/stock_meta.json + public/api/theme_ranking.json
Output: public/api/prism-r/shadow_comparison.json + public/api/prism-r/meta.json
"""
import json, os, sys
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
API = ROOT / 'public' / 'api'
OUT = API / 'prism-r'
OUT.mkdir(exist_ok=True)

# === Load theme-details → long panel ===
rows = []
td_dir = API / 'theme-details'
if not td_dir.exists():
    print('ERROR: theme-details not found'); sys.exit(1)
for f in sorted(td_dir.glob('*.json')):
    d = json.load(open(f))
    slug = d['slug']; tickers = d.get('tickers', [])
    if not tickers or not d.get('prices'): continue
    for p in d['prices']:
        date = p.get('date')
        if not date: continue
        for tk in tickers:
            close = p.get(tk)
            if close is not None and close > 0:
                rows.append({'date': date, 'theme': slug, 'ticker': tk, 'close': float(close)})
panel = pd.DataFrame(rows)
if panel.empty:
    print('ERROR: no price data'); sys.exit(1)
panel['date'] = pd.to_datetime(panel['date'])
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes | {panel.ticker.nunique()} tickers')

# === Load metadata ===
meta_file = API / 'stock_meta.json'
meta_raw = json.load(open(meta_file)) if meta_file.exists() else {}
meta_d = {tk: v for tk, v in meta_raw.items()}

# Theme name mapping
tr_file = API / 'theme_ranking.json'
slug_to_name = {}
if tr_file.exists():
    tr = json.load(open(tr_file))
    for t in tr.get('all_themes', []):
        s = t.get('slug', '')
        if s: slug_to_name[s] = t.get('name', s)

# === Returns and theme aggregates ===
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(
    sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(
    panel['n_day']>1,
    (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
dates_all = sorted(panel['date'].unique())

# Sector mapping
psec = panel[['theme','ticker']].drop_duplicates()
psec['sector'] = psec['ticker'].map(lambda t: meta_d.get(t,{}).get('sector',''))
theme_sector = psec.groupby('theme')['sector'].agg(
    lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')

# === Helpers ===
def ols_ab(y, x):
    mask = np.isfinite(y) & np.isfinite(x); y, x = y[mask], x[mask]; n = len(y)
    if n < 10: return np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean(); xd = x - xm; vx = np.dot(xd, xd)/(n-1)
    if vx < 1e-12: return np.nan, np.nan, np.nan
    b = np.dot(xd, y-ym)/(n-1) / vx; a = ym - b*xm
    ss = float(np.sum((y-a-b*x)**2)); st = float(np.sum((y-ym)**2))
    r2 = 1-ss/st if st>1e-12 else np.nan
    return a*n, b, r2

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

# === Main logic ===
WARMUP = 126; MIN_M = 4; TOP_T = 10; SEC_MAX = 3
if len(dates_all) < WARMUP + 20:
    print(f'WARN: only {len(dates_all)} days, need {WARMUP+20}'); sys.exit(0)

j = len(dates_all) - 1  # latest date
dt = dates_all[j]
dt63 = set(dates_all[max(0,j-62):j+1])
dt21 = set(dates_all[max(0,j-20):j+1])
sub = panel[panel['date'].isin(dt63)]

# Theme scoring
tm = sub.groupby('theme')['ticker'].nunique()
elig = tm[tm >= MIN_M].index.tolist()
tm_mom = {}
for th in elig:
    td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
    tm_mom[th] = cumret(td.values)
ms = pd.Series(tm_mom).dropna().sort_values(ascending=False)
dc = {}
for th in ms.index:
    td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    if len(td) < 63: continue
    r021 = cumret(td[-21:]); r2142 = cumret(td[-42:-21]); r4263 = cumret(td[-63:-42])
    if all(np.isfinite([r021, r2142, r4263])):
        dc[th] = -(r021 - 0.5*(r2142 + r4263))
dcs = pd.Series(dc)
common = list(set(ms.index) & set(dcs.index))
if not common:
    print('WARN: no themes scored'); sys.exit(0)
ts = pd.DataFrame({'mom63': ms[common], 'decel': dcs[common]})
ts['score'] = 0.70*ts['mom63'].rank(pct=True) + 0.30*ts['decel'].rank(pct=True)
ts = ts.sort_values('score', ascending=False)
sel = []; sc_cnt = {}
for th in ts.index:
    s = theme_sector.get(th, 'Unk')
    if sc_cnt.get(s, 0) >= SEC_MAX: continue
    sel.append(th); sc_cnt[s] = sc_cnt.get(s, 0) + 1
    if len(sel) >= TOP_T: break

# Stock scoring + comparison
comparisons = []
used4 = set(); used5 = set()
for rank_i, th in enumerate(sel):
    ths = sub[(sub['theme']==th) & sub['ret'].notna()]
    tks = ths['ticker'].unique()
    if len(tks) < MIN_M: continue
    all_stocks = []
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        r21d = tkd[tkd['date'].isin(dt21)]
        raw_1m = cumret(r21d['ret'].values) if len(r21d)>=10 else np.nan
        a63, b63, r2_63 = ols_ab(tkd['ret'].values, tkd['theme_ex_self'].values)
        shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
        score5 = a63*shrk if np.isfinite(a63) else -999
        latest = panel[(panel['theme']==th)&(panel['ticker']==tk)&(panel['date']==dt)]
        price = float(latest['close'].iloc[0]) if len(latest)>0 else None
        mi = meta_d.get(tk, {})
        all_stocks.append({
            'ticker': tk,
            'raw_1m': round(raw_1m, 4) if np.isfinite(raw_1m) else None,
            'alpha63': round(a63, 4) if np.isfinite(a63) else None,
            'beta63': round(b63, 3) if np.isfinite(b63) else None,
            'r2_63': round(r2_63, 3) if np.isfinite(r2_63) else None,
            'shrink': round(shrk, 3),
            'score_a5': round(score5, 4) if score5 > -999 else None,
            'price': round(price, 2) if price else None,
            'sector': mi.get('sector', ''), 'mc': mi.get('mc', ''),
            'name': mi.get('name', ''), 'name_ja': mi.get('name_ja', '')
        })
    s4 = sorted(all_stocks, key=lambda x: -(x['raw_1m'] or -999))
    s5 = sorted(all_stocks, key=lambda x: -(x['score_a5'] or -999))
    a4_tk = None
    for s in s4:
        if s['ticker'] not in used4 and s['raw_1m'] is not None:
            a4_tk = s['ticker']; used4.add(a4_tk); break
    a5_tk = None
    for s in s5:
        if s['ticker'] not in used5 and s['score_a5'] is not None:
            a5_tk = s['ticker']; used5.add(a5_tk); break
    # Theme state per EXIT CONSTITUTION v2
    full_rank = int(ts['score'].rank(ascending=False).loc[th])
    theme_state = 'ENTRY' if full_rank <= 20 else 'WATCH' if full_rank <= 35 else 'EXIT'
    comparisons.append({
        'theme': th, 'theme_name': slug_to_name.get(th, th),
        'rank': rank_i + 1, 'full_rank': full_rank,
        'theme_state': theme_state,
        'sector': theme_sector.get(th, ''),
        'mom63': round(float(ts.loc[th, 'mom63']), 4),
        'decel': round(float(ts.loc[th, 'decel']), 4),
        'theme_score': round(float(ts.loc[th, 'score']), 3),
        'n_members': len(tks),
        'a4_pick': a4_tk, 'a5_pick': a5_tk, 'same': a4_tk == a5_tk,
        'stocks': all_stocks
    })

# === Output ===
overlap = sum(1 for c in comparisons if c['same'])
output = {
    'snapshot_date': str(dt.date()),
    'generated_at': datetime.now().isoformat(),
    'version': 'A5-lite_shadow_v1',
    'status': 'SHADOW',
    'frozen_params': {
        'alpha_window': 63,
        'theme_score': '0.70×rank(mom63)+0.30×rank(decel)',
        'stock_score': 'α63×shrink(r²_63)',
        'top_themes': 10, 'picks_per_theme': 1,
        'min_members': 4, 'sector_cap': 3, 'rebalance_days': 20
    },
    'summary': {
        'themes_selected': len(comparisons),
        'a4_names': len(used4), 'a5_names': len(used5),
        'overlap': overlap,
        'overlap_pct': round(overlap / max(len(comparisons), 1), 2),
        'diff_names': len(comparisons) - overlap
    },
    'comparisons': comparisons
}
with open(OUT / 'shadow_comparison.json', 'w') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

meta_out = {
    'snapshot_date': str(dt.date()),
    'generated_at': datetime.now().isoformat(),
    'version': 'A5-lite_shadow_v1',
    'status': 'SHADOW',
    'pit_safe': False,
    'forward_rebalances': 0,
    'target_rebalances': 18,
    'next_rebalance_est': '',
    'frozen_params': output['frozen_params'],
    'backtest_5yr': {
        'a4_sharpe': 1.29, 'a5_sharpe': 1.41, 'sharpe_diff': 0.12,
        'monthly_diff': '+1.13%', 'monthly_win_rate': '53%',
        'sign_test_p': 0.360, 'tech_share_net': '76%',
        'a5_maxdd': '-42.0%', 'a4_maxdd': '-39.7%'
    }
}
with open(OUT / 'meta.json', 'w') as f:
    json.dump(meta_out, f, ensure_ascii=False, indent=2)

comp_size = (OUT / 'shadow_comparison.json').stat().st_size / 1024
print(f'PRISM-R: {len(comparisons)} themes, overlap={overlap}/{len(comparisons)}, '
      f'snapshot={dt.date()}, size={comp_size:.0f}KB')
