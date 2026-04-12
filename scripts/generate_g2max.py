"""Generate G2-MAX snapshot: 6 themes (corr budget) × raw self-excluded α63 × W5b consistency weighting.
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

def split_alpha(y_long, x_long, y_short, x_short):
    """Split-window: β from long window (126d), α from short window (63d)."""
    mask = np.isfinite(y_long) & np.isfinite(x_long)
    yl, xl = y_long[mask], x_long[mask]; nl = len(yl)
    if nl < 20: return np.nan, np.nan, np.nan
    xm, ym = xl.mean(), yl.mean(); vx = np.var(xl, ddof=1)
    if vx < 1e-15: return np.nan, np.nan, np.nan
    b_long = np.dot(xl-xm, yl-ym)/(nl-1)/vx
    a_long = ym - b_long*xm
    resid_long = yl - a_long - b_long*xl
    ss = float(np.sum(resid_long**2)); st = float(np.sum((yl-ym)**2))
    r2_long = 1-ss/st if st > 1e-12 else np.nan
    mask_s = np.isfinite(y_short) & np.isfinite(x_short)
    ys, xs = y_short[mask_s], x_short[mask_s]; ns = len(ys)
    if ns < 10: return np.nan, b_long, r2_long
    alpha_daily = np.mean(ys - b_long*xs)
    alpha_cum = alpha_daily * ns
    return alpha_cum, b_long, r2_long

# === Main Logic ===
WARMUP = 126; MIN_M = 4; TOP_T = 6; MAX_CORR = 0.80
j = len(dates_all) - 1; dt = dates_all[j]
dt63 = set(dates_all[max(0,j-62):j+1])
dt126 = set(dates_all[max(0,j-125):j+1])
dt252 = set(dates_all[max(0,j-251):j+1])
sub = panel[panel['date'].isin(dt63)]
sub126 = panel[panel['date'].isin(dt126)]
sub252 = panel[panel['date'].isin(dt252)]

# Theme momentum
tm = sub.groupby('theme')['ticker'].nunique()
elig = tm[tm >= MIN_M].index.tolist()
tm_mom63, tm_mom126, tm_mom21, tm_mom252 = {}, {}, {}, {}
for th in elig:
    td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    if len(td) >= 21: tm_mom21[th] = cumret(td[-21:])
    if len(td) >= 63: tm_mom63[th] = cumret(td)
    td126v = sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    if len(td126v) >= 63: tm_mom126[th] = cumret(td126v)
    td252v = sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    if len(td252v) >= 126: tm_mom252[th] = cumret(td252v)

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

# Stock selection: split-window raw α (β=126d, α=63d, no shrink, no SNR)
comparisons = []; used = set()
for th in sel:
    ths = sub[(sub['theme']==th) & sub['ret'].notna()]
    ths126 = sub126[(sub126['theme']==th) & sub126['ret'].notna()]
    tks = ths['ticker'].unique()
    if len(tks) < MIN_M: continue
    all_stocks = []
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        tkd126 = ths126[ths126['ticker']==tk].sort_values('date')
        # Split-window: β/R² from 126d, α from 63d
        if len(tkd126) >= 20:
            a63, b63, r2_63 = split_alpha(
                tkd126['ret'].values, tkd126['theme_ex_self'].values,
                tkd['ret'].values, tkd['theme_ex_self'].values)
        else:
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
        'mom252': round(float(tm_mom252.get(th, np.nan)), 4) if np.isfinite(tm_mom252.get(th, np.nan)) else None,
        'pick': pick, 'stocks': all_stocks,
    })

# === W5b Consistency Weighting (R63/R126/R252_ex1m) ===
w5b_data = {}
for c in comparisons:
    th = c['theme']
    r63 = tdf.loc[th, 'mom63'] if th in tdf.index else np.nan
    r126 = tdf.loc[th, 'mom126'] if th in tdf.index and np.isfinite(tdf.loc[th, 'mom126']) else np.nan
    r21 = tdf.loc[th, 'mom21'] if th in tdf.index and np.isfinite(tdf.loc[th, 'mom21']) else np.nan
    r252 = tm_mom252.get(th, np.nan)
    # R252_ex1m = (1+R252)/(1+R21) - 1
    r252ex1m = ((1+r252)/(1+r21) - 1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21) > 1e-8 else np.nan
    horizons = [r63, r126, r252ex1m]
    valid = [v for v in horizons if np.isfinite(v)]
    if len(valid) >= 2:
        pos_count = sum(1 for v in valid if v > 0)
        avg_ret = float(np.mean([max(v, 0) for v in valid]))
        raw_w = pos_count * (1 + avg_ret)
    else:
        pos_count = 0; avg_ret = 0; raw_w = 1.0  # fallback
    w5b_data[th] = {'r63': r63, 'r126': r126, 'r252ex1m': r252ex1m,
                     'pos_count': pos_count, 'avg_ret': round(avg_ret, 4), 'raw_weight': round(raw_w, 4)}

# Normalize + 30% cap
total_raw = sum(d['raw_weight'] for d in w5b_data.values())
if total_raw > 0:
    for th in w5b_data: w5b_data[th]['weight'] = w5b_data[th]['raw_weight'] / total_raw
else:
    for th in w5b_data: w5b_data[th]['weight'] = 1.0 / len(w5b_data)
for _ in range(5):
    ws = np.array([w5b_data[th]['weight'] for th in w5b_data])
    excess = np.maximum(ws - 0.30, 0)
    if excess.sum() < 1e-6: break
    under = ws < 0.30; ws = np.minimum(ws, 0.30)
    if under.any(): ws[under] += excess.sum() * (ws[under] / ws[under].sum())
    ws = ws / ws.sum()
    for i, th in enumerate(w5b_data): w5b_data[th]['weight'] = round(float(ws[i]), 4)

# Add W5b weights to comparisons
# BEAST = nocap normalized weights
beast_total = sum(d['raw_weight'] for d in w5b_data.values())
for c in comparisons:
    th = c['theme']
    wd = w5b_data.get(th, {})
    c['w5b_weight'] = round(wd.get('weight', 1.0/len(comparisons)), 4)
    c['beast_weight'] = round(wd.get('raw_weight', 1.0) / beast_total, 4) if beast_total > 0 else round(1.0/len(comparisons), 4)
    c['w5b_pos_count'] = wd.get('pos_count', 0)
    c['w5b_r252ex1m'] = round(wd.get('r252ex1m', 0), 4) if np.isfinite(wd.get('r252ex1m', np.nan)) else None

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
    'strategy': 'G2-MAX: 6-theme W5b consistency-weighted raw residual momentum',
    'frozen_params': {
        'theme_score': '0.50×rank(R63) + 0.30×rank(R126) + 0.20×rank(R21)',
        'stock_score': 'split-window raw α (β=126d, α=63d, no shrink, no SNR)',
        'weighting': 'W5b consistency: pos_count(R63,R126,R252ex1m) × (1+avg_positive_ret), 30% cap',
        'top_themes': TOP_T, 'max_corr': MAX_CORR,
        'picks_per_theme': 1, 'min_members': MIN_M,
    },
    'summary': {
        'themes_selected': len(comparisons),
        'picks': len(used),
        'correlation': sel_corr,
        'w5b': {
            'avg_pos_count': round(np.mean([w5b_data[th]['pos_count'] for th in w5b_data]), 2),
            'frac_3of3': round(np.mean([1 for th in w5b_data if w5b_data[th]['pos_count']==3]) / max(len(w5b_data),1), 2),
            'weight_top1': round(max(w5b_data[th]['weight'] for th in w5b_data), 4),
            'weight_min': round(min(w5b_data[th]['weight'] for th in w5b_data), 4),
        },
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
