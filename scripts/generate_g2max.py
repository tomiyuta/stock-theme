"""Generate G2-MAX snapshot: 6 themes (corr budget) × raw self-excluded α63.
Output: public/api/prism-g2/shadow_comparison.json + meta.json
"""
import json, os, sys, numpy as np, pandas as pd
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
API = ROOT / 'public' / 'api'
OUT = API / 'prism-g2'
OUT.mkdir(parents=True, exist_ok=True)

# Load theme details
td_dir = API / 'theme-details'
if not td_dir.exists(): print('WARN: no theme-details'); sys.exit(0)

ranking_path = SCRIPT_DIR / 'theme_ranking_raw.json'
ranking = json.load(open(ranking_path))
slug_to_name = {t['slug']: t['name'] for t in ranking['themes'] if t.get('slug')}

meta_path = API / 'stock_meta.json'
meta_d = json.load(open(meta_path)) if meta_path.exists() else {}

# Build panel from theme-details (prices format: [{date, tk1, tk2, ...}, ...])
rows = []
for f in sorted(td_dir.glob('*.json')):
    d = json.load(open(f))
    slug = d.get('slug', f.stem)
    tickers = d.get('tickers', [])
    for rec in d.get('prices', []):
        dt = rec.get('date')
        if not dt: continue
        for tk in tickers:
            if tk in rec and rec[tk] is not None:
                rows.append({'theme': slug, 'ticker': tk, 'date': pd.Timestamp(dt), 'close': float(rec[tk])})
if not rows: print('WARN: no data'); sys.exit(0)
panel = pd.DataFrame(rows).sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)

# Sector mapping
psec = panel[['theme','ticker']].drop_duplicates()
for tk in psec['ticker'].unique():
    mi = meta_d.get(tk, {})
    psec.loc[psec['ticker']==tk, 'sector'] = mi.get('sector', 'Unk')
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')

dates_all = sorted(panel['date'].unique())

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def ols_alpha(y, x):
    mask = np.isfinite(y) & np.isfinite(x); y, x = y[mask], x[mask]; n = len(y)
    if n < 20: return np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean(); vx = np.var(x, ddof=1)
    if vx < 1e-15: return np.nan, np.nan, np.nan
    b = np.dot(x-xm, y-ym)/(n-1)/vx; a = ym - b*xm
    resid = y - a - b*x
    ss_res, ss_tot = float(np.sum(resid**2)), float(np.sum((y-ym)**2))
    r2 = 1 - ss_res/ss_tot if ss_tot > 1e-12 else np.nan
    return a*n, b, r2  # alpha_cum63, beta, r2

# === Main Logic ===
WARMUP = 126; MIN_M = 4; TOP_T = 6; MAX_CORR = 0.80
j = len(dates_all) - 1; dt = dates_all[j]
dt63 = set(dates_all[max(0,j-62):j+1])
dt126 = set(dates_all[max(0,j-125):j+1])
sub = panel[panel['date'].isin(dt63)]
sub126 = panel[panel['date'].isin(dt126)]

# Theme momentum
tm = sub.groupby('theme')['ticker'].nunique()
elig = tm[tm >= MIN_M].index.tolist()
tm_mom63, tm_mom126, tm_mom21 = {}, {}, {}
for th in elig:
    td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    if len(td) >= 21: tm_mom21[th] = cumret(td[-21:])
    if len(td) >= 63: tm_mom63[th] = cumret(td)
    td126v = sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    if len(td126v) >= 63: tm_mom126[th] = cumret(td126v)

tdf = pd.DataFrame({'mom63': pd.Series(tm_mom63), 'mom126': pd.Series(tm_mom126),
                     'mom21': pd.Series(tm_mom21)}).dropna(subset=['mom63'])
tdf['score'] = (0.50*tdf['mom63'].rank(pct=True) +
                0.30*tdf['mom126'].rank(pct=True, na_option='bottom') +
                0.20*tdf['mom21'].rank(pct=True, na_option='bottom'))
tdf = tdf.sort_values('score', ascending=False)

# Cluster-distinct theme selection
theme_daily = {}
for th in tdf.index[:20]:
    td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
    if len(td) >= 20: theme_daily[th] = td
corr_mat = pd.DataFrame(theme_daily).dropna().corr() if len(theme_daily) >= 2 else pd.DataFrame()

sel = []
for th in tdf.index:
    if th not in corr_mat.index: continue
    conflict = False
    for s in sel:
        if s in corr_mat.columns and abs(corr_mat.loc[th, s]) > MAX_CORR:
            conflict = True; break
    if not conflict:
        sel.append(th)
        if len(sel) >= TOP_T: break

# Stock selection: raw self-excluded α63 (no shrink, no SNR)
comparisons = []; used = set()
for th in sel:
    ths = sub[(sub['theme']==th) & sub['ret'].notna()]
    tks = ths['ticker'].unique()
    if len(tks) < MIN_M: continue
    all_stocks = []
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        a63, b63, r2_63 = ols_alpha(tkd['ret'].values, tkd['theme_ex_self'].values)
        raw_1m = cumret(tkd['ret'].values[-21:])
        latest = panel[(panel['theme']==th)&(panel['ticker']==tk)&(panel['date']==dt)]
        price = float(latest['close'].iloc[0]) if len(latest)>0 else None
        mi = meta_d.get(tk, {})
        all_stocks.append({
            'ticker': tk, 'alpha63': round(a63, 4) if np.isfinite(a63) else None,
            'beta63': round(b63, 3) if np.isfinite(b63) else None,
            'r2_63': round(r2_63, 3) if np.isfinite(r2_63) else None,
            'raw_1m': round(raw_1m, 4) if np.isfinite(raw_1m) else None,
            'price': round(price, 2) if price else None,
            'sector': mi.get('sector',''), 'mc': mi.get('mc',''),
            'name': mi.get('name',''), 'name_ja': mi.get('name_ja',''),
        })
    s_sorted = sorted(all_stocks, key=lambda x: -(x['alpha63'] or -999))
    pick = None
    for s in s_sorted:
        if s['ticker'] not in used and s['alpha63'] is not None:
            pick = s['ticker']; used.add(pick); break
    th_name = slug_to_name.get(th, th)
    comparisons.append({
        'theme': th, 'theme_name': th_name,
        'theme_score': round(float(tdf.loc[th, 'score']), 4),
        'mom63': round(float(tdf.loc[th, 'mom63']), 4),
        'mom126': round(float(tdf.loc[th, 'mom126']), 4) if th in tdf.index and 'mom126' in tdf.columns else None,
        'pick': pick, 'stocks': all_stocks,
    })

# Correlation diagnostics for selected themes
sel_corr = {}
if len(sel) >= 2 and not corr_mat.empty:
    upper = corr_mat.loc[sel, sel].where(np.triu(np.ones((len(sel),len(sel)), dtype=bool), k=1))
    all_corrs = upper.stack().values
    sel_corr = {
        'n_themes': len(sel), 'avg_corr': round(float(np.mean(all_corrs)), 3),
        'max_corr': round(float(np.max(all_corrs)), 3),
    }

output = {
    'snapshot_date': str(dt.date()),
    'generated_at': datetime.now().isoformat(),
    'version': 'G2-MAX_v1',
    'status': 'SHADOW',
    'strategy': 'G2-MAX: 6-theme concentrated raw residual momentum',
    'frozen_params': {
        'theme_score': '0.50×rank(R63) + 0.30×rank(R126) + 0.20×rank(R21)',
        'stock_score': 'raw self-excluded α63 (no shrink, no SNR)',
        'top_themes': 5, 'max_corr': MAX_CORR,
        'picks_per_theme': 1, 'min_members': MIN_M,
    },
    'summary': {
        'themes_selected': len(comparisons),
        'picks': len(used),
        'correlation': sel_corr,
    },
    'comparisons': comparisons,
}

with open(OUT / 'shadow_comparison.json', 'w') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)
meta_out = {'status': 'SHADOW', 'version': 'G2-MAX_v1', 'snapshot_date': str(dt.date()),
            'forward_rebalances': 0, 'target_rebalances': 18}
with open(OUT / 'meta.json', 'w') as f:
    json.dump(meta_out, f, indent=2)
comp_size = os.path.getsize(OUT / 'shadow_comparison.json') / 1024
print(f'G2-MAX: {len(comparisons)} themes, {len(used)} picks, corr={sel_corr.get("max_corr","N/A")}, snapshot={dt.date()}, size={comp_size:.0f}KB')
