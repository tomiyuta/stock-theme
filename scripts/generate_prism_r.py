#!/usr/bin/env python3
"""Generate PRISM-RQ shadow comparison JSON from theme-details data.
PRISM-RQ = Residual-SNR (Layer 2: A5-SNRb) + Quality filter (Layer 1: BFM-v2)
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

# Load stock-themes beta_alpha for CRA-v1 confirmation
BA_PATH = ROOT / 'data' / 'stock-themes-api' / 'beta_alpha_all.json'
st_beta_alpha = {}
if BA_PATH.exists():
    try: st_beta_alpha = json.load(open(BA_PATH))
    except: pass
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
    if n < 10: return np.nan, np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean(); xd = x - xm; vx = np.dot(xd, xd)/(n-1)
    if vx < 1e-12: return np.nan, np.nan, np.nan, np.nan
    b = np.dot(xd, y-ym)/(n-1) / vx; a = ym - b*xm
    resid = y - a - b*x
    ss = float(np.sum(resid**2)); st = float(np.sum((y-ym)**2))
    r2 = 1-ss/st if st>1e-12 else np.nan
    resid_vol = float(np.std(resid, ddof=1)*np.sqrt(n)) if n>2 else np.nan
    return a*n, b, r2, resid_vol

def split_alpha(y_long, x_long, y_short, x_short):
    """Split-window: β/R² from long window (126d), α from short window (63d)."""
    mask = np.isfinite(y_long) & np.isfinite(x_long)
    yl, xl = y_long[mask], x_long[mask]; nl = len(yl)
    if nl < 20: return np.nan, np.nan, np.nan, np.nan
    xm, ym = xl.mean(), yl.mean(); xd = xl - xm; vx = np.dot(xd, xd)/(nl-1)
    if vx < 1e-12: return np.nan, np.nan, np.nan, np.nan
    b_long = np.dot(xd, yl-ym)/(nl-1) / vx
    a_long = ym - b_long*xm
    resid_long = yl - a_long - b_long*xl
    ss = float(np.sum(resid_long**2)); st = float(np.sum((yl-ym)**2))
    r2_long = 1-ss/st if st>1e-12 else np.nan
    mask_s = np.isfinite(y_short) & np.isfinite(x_short)
    ys, xs = y_short[mask_s], x_short[mask_s]; ns = len(ys)
    if ns < 10: return np.nan, b_long, r2_long, np.nan
    resid_short = ys - b_long*xs
    alpha_daily = np.mean(resid_short) - a_long  # remove long-window intercept bias
    alpha_daily = np.mean(ys - b_long*xs)  # simpler: just mean of residuals using long β
    alpha_cum = alpha_daily * ns
    resid_vol = float(np.std(resid_short, ddof=1)*np.sqrt(ns)) if ns > 2 else np.nan
    return alpha_cum, b_long, r2_long, resid_vol

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

def cra_audit(theme_slug, ticker, self_alpha):
    """CRA-v1: Consensus Residual Alpha confirmation from stock-themes."""
    ba_theme = st_beta_alpha.get(theme_slug, {}).get('data', {})
    ba_tk = ba_theme.get(ticker, {})
    if not ba_tk: return 0.0, {}
    a3m = ba_tk.get('3M', {}); a6m = ba_tk.get('6M', {})
    st_a3 = a3m.get('alpha', 0); st_a6 = a6m.get('alpha', 0)
    st_t3 = a3m.get('alpha_tval', 0); st_t6 = a6m.get('alpha_tval', 0)
    st_p3 = a3m.get('alpha_pval', 1); st_p6 = a6m.get('alpha_pval', 1)
    sa = 1 if self_alpha > 0 else (-1 if self_alpha < 0 else 0)
    c1 = 1.0 if sa != 0 and (1 if st_a3 > 0 else -1) == sa else 0.0
    c2 = 1.0 if sa != 0 and (1 if st_a6 > 0 else -1) == sa else 0.0
    c3 = 1.0 if (st_a3 > 0) == (st_a6 > 0) else 0.0
    c4 = 1.0 if abs(st_t3) > 1 or st_p3 < 0.10 else 0.0
    c5 = 1.0 if abs(st_t6) > 1 or st_p6 < 0.10 else 0.0
    audit = 0.30*c1 + 0.30*c2 + 0.15*c3 + 0.125*c4 + 0.125*c5
    detail = {'c1_sign3m':c1,'c2_sign6m':c2,'c3_st_stable':c3,'c4_tval3m':c4,'c5_tval6m':c5,
              'audit_score':round(audit,3),'st_alpha_3m':round(st_a3,6),'st_alpha_6m':round(st_a6,6),
              'st_tval_3m':round(st_t3,3),'st_tval_6m':round(st_t6,3)}
    return audit, detail

# === Main logic ===
WARMUP = 126; MIN_M = 4; TOP_T = 10; SEC_MAX = 3
if len(dates_all) < WARMUP + 20:
    print(f'WARN: only {len(dates_all)} days, need {WARMUP+20}'); sys.exit(0)

j = len(dates_all) - 1  # latest date
dt = dates_all[j]
dt63 = set(dates_all[max(0,j-62):j+1])
dt21 = set(dates_all[max(0,j-20):j+1])
dt126 = set(dates_all[max(0,j-125):j+1])
dt252 = set(dates_all[max(0,j-251):j+1])
sub = panel[panel['date'].isin(dt63)]
sub126 = panel[panel['date'].isin(dt126)]
sub252 = panel[panel['date'].isin(dt252)]

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

# === BFM-v2: Quality Filter on top-25 candidates ===
CAND_N = 25
candidates = list(ts.index[:CAND_N])
cand_feat = {}
for th in candidates:
    ths = sub[sub['theme']==th]
    tks = ths['ticker'].unique()
    if len(tks) < MIN_M: continue
    tk_r63 = {}
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        if len(tkd) >= 20: tk_r63[tk] = cumret(tkd['ret'].values[-63:])
    if len(tk_r63) < MIN_M: continue
    breadth63 = sum(1 for v in tk_r63.values() if np.isfinite(v) and v > 0) / len(tk_r63)
    abs_c = np.array([abs(v) for v in tk_r63.values() if np.isfinite(v)])
    tot_abs = abs_c.sum()
    conc63 = float(np.sum((abs_c/tot_abs)**2)) if tot_abs > 1e-10 else 1.0
    td_vals = ths.groupby('date')['theme_ret'].first().sort_index().values
    tvol = float(np.std(td_vals[-63:], ddof=1)*np.sqrt(252)) if len(td_vals)>=63 else np.nan
    if np.isfinite(breadth63) and np.isfinite(conc63) and np.isfinite(tvol):
        cand_feat[th] = {'breadth63': breadth63, 'concentration63': conc63, 'theme_vol63': tvol}

sel_bfm2 = sel  # fallback
bfm2_vetoed = set()
if len(cand_feat) >= 5:
    cf = pd.DataFrame(cand_feat).T
    b_thresh = cf['breadth63'].quantile(0.30)
    c_thresh = cf['concentration63'].quantile(0.80)
    v_thresh = cf['theme_vol63'].quantile(0.80)
    for th in cf.index:
        if cf.loc[th,'breadth63'] < b_thresh: bfm2_vetoed.add(th)
        if cf.loc[th,'concentration63'] > c_thresh: bfm2_vetoed.add(th)
        if cf.loc[th,'theme_vol63'] > v_thresh: bfm2_vetoed.add(th)
    survivors = [th for th in ts.index if th in candidates and th not in bfm2_vetoed]
    sel_bfm2 = []; sc_cnt3 = {}
    for th in survivors:
        s2 = theme_sector.get(th, 'Unk')
        if sc_cnt3.get(s2, 0) >= SEC_MAX: continue
        sel_bfm2.append(th); sc_cnt3[s2] = sc_cnt3.get(s2, 0) + 1
        if len(sel_bfm2) >= TOP_T: break

# === Theme Correlation Budget Diagnostics ===
theme_daily_rets = {}
for th in sel:
    td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
    if len(td) >= 20: theme_daily_rets[th] = td
corr_diag = {}
if len(theme_daily_rets) >= 2:
    tdr = pd.DataFrame(theme_daily_rets).dropna()
    if len(tdr) >= 20:
        corr_mat = tdr.corr()
        n_th = len(corr_mat)
        upper = corr_mat.where(np.triu(np.ones((n_th,n_th), dtype=bool), k=1))
        all_corrs = upper.stack().values
        corr_diag = {
            'n_themes': n_th,
            'avg_pairwise_corr': round(float(np.mean(all_corrs)), 3),
            'max_pairwise_corr': round(float(np.max(all_corrs)), 3),
            'min_pairwise_corr': round(float(np.min(all_corrs)), 3),
            'n_high_corr_pairs': int(np.sum(all_corrs > 0.7)),
            'effective_n_themes': round(float(1.0 / np.mean((1.0/n_th + (1-1.0/n_th)*np.mean(all_corrs)))), 1) if np.mean(all_corrs) < 1.0 else 1.0,
        }

# Stock scoring + comparison
comparisons = []
used4 = set(); used5 = set(); used_snrb = set(); used_bfm2 = set(); used_cra = set()
for rank_i, th in enumerate(sel):
    ths = sub[(sub['theme']==th) & sub['ret'].notna()]
    ths126 = sub126[(sub126['theme']==th) & sub126['ret'].notna()]
    tks = ths['ticker'].unique()
    if len(tks) < MIN_M: continue
    all_stocks = []
    for tk in tks:
        tkd = ths[ths['ticker']==tk].sort_values('date')
        tkd126 = ths126[ths126['ticker']==tk].sort_values('date')
        r21d = tkd[tkd['date'].isin(dt21)]
        raw_1m = cumret(r21d['ret'].values) if len(r21d)>=10 else np.nan
        # Split-window: β/R² from 126d, α from 63d
        if len(tkd126) >= 20:
            a63, b63, r2_63, rvol63 = split_alpha(
                tkd126['ret'].values, tkd126['theme_ex_self'].values,
                tkd['ret'].values, tkd['theme_ex_self'].values)
        else:
            a63, b63, r2_63, rvol63 = ols_ab(tkd['ret'].values, tkd['theme_ex_self'].values)
        shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
        score5 = a63*shrk if np.isfinite(a63) else -999
        # A5-SNRb score: (alpha_cum / resid_vol) × shrink(r²)
        if np.isfinite(a63) and np.isfinite(rvol63) and rvol63 > 1e-8:
            score_snrb = (a63 / rvol63) * shrk
        else:
            score_snrb = -999
        # CRA-v1: confirmation overlay on SNRb
        cra_audit_score, cra_detail = cra_audit(th, tk, a63)
        score_cra = score_snrb * (0.70 + 0.30 * cra_audit_score) if score_snrb > -999 else -999
        latest = panel[(panel['theme']==th)&(panel['ticker']==tk)&(panel['date']==dt)]
        price = float(latest['close'].iloc[0]) if len(latest)>0 else None
        mi = meta_d.get(tk, {})
        all_stocks.append({
            'ticker': tk,
            'raw_1m': round(raw_1m, 4) if np.isfinite(raw_1m) else None,
            'alpha63': round(a63, 4) if np.isfinite(a63) else None,
            'beta63': round(b63, 3) if np.isfinite(b63) else None,
            'r2_63': round(r2_63, 3) if np.isfinite(r2_63) else None,
            'resid_vol63': round(rvol63, 4) if np.isfinite(rvol63) else None,
            'shrink': round(shrk, 3),
            'score_a5': round(score5, 4) if score5 > -999 else None,
            'score_snrb': round(score_snrb, 4) if score_snrb > -999 else None,
            'score_cra': round(score_cra, 4) if score_cra > -999 else None,
            'cra_audit': cra_detail if cra_detail else None,
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
    # SNRb pick
    s_snrb = sorted(all_stocks, key=lambda x: -(x['score_snrb'] or -999))
    snrb_tk = None
    for s in s_snrb:
        if s['ticker'] not in used_snrb and s['score_snrb'] is not None:
            snrb_tk = s['ticker']; used_snrb.add(snrb_tk); break
    # BFM-v2 pick (only if theme is in sel_bfm2)
    bfm2_tk = None
    if th in sel_bfm2:
        for s in s_snrb:  # same Layer 2 scorer as SNRb
            if s['ticker'] not in used_bfm2 and s['score_snrb'] is not None:
                bfm2_tk = s['ticker']; used_bfm2.add(bfm2_tk); break
    # CRA-v1 pick
    s_cra = sorted(all_stocks, key=lambda x: -(x['score_cra'] or -999))
    cra_tk = None
    for s in s_cra:
        if s['ticker'] not in used_cra and s['score_cra'] is not None:
            cra_tk = s['ticker']; used_cra.add(cra_tk); break
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
        'a4_pick': a4_tk, 'a5_pick': a5_tk, 'snrb_pick': snrb_tk, 'bfm2_pick': bfm2_tk, 'cra_pick': cra_tk,
        'same': a4_tk == a5_tk, 'a5_snrb_same': a5_tk == snrb_tk, 'snrb_cra_same': snrb_tk == cra_tk,
        'in_bfm2': th in sel_bfm2,
        'stocks': all_stocks
    })

# === Load previous snapshot for exit detection ===
prev_picks = {}
prev_file = OUT / 'shadow_comparison.json'
if prev_file.exists():
    try:
        prev = json.load(open(prev_file))
        for c in prev.get('comparisons', []):
            prev_picks[c['a5_pick']] = {'theme': c.get('theme_name',''), 'rank': c.get('rank',0), 'full_rank': c.get('full_rank',0)}
    except: pass

curr_picks = {c['a5_pick']: c['theme_name'] for c in comparisons}
# Virtual exits: in previous but not in current
virtual_exits = []
for tk, info in prev_picks.items():
    if tk not in curr_picks:
        virtual_exits.append({'ticker': tk, 'prev_theme': info['theme'], 'prev_rank': info['rank'], 'reason': 'removed_from_target'})
# Virtual entries: in current but not in previous
virtual_entries = []
for tk, th in curr_picks.items():
    if tk not in prev_picks:
        virtual_entries.append({'ticker': tk, 'theme': th})

# === Output ===
overlap = sum(1 for c in comparisons if c['same'])
snrb_overlap_a5 = sum(1 for c in comparisons if c['a5_snrb_same'])
bfm2_themes = [c for c in comparisons if c['in_bfm2']]
bfm2_overlap_base = sum(1 for c in comparisons if c['in_bfm2'])
cra_overlap_snrb = sum(1 for c in comparisons if c['snrb_cra_same'])

# === W5b Consistency Weighting for PRISM-R (a5_pick) ===
w5b_data = {}
for c in comparisons:
    th = c['theme']
    # Compute R63/R126/R252/R21 per theme
    td63v = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    r63 = cumret(td63v) if len(td63v) >= 63 else np.nan
    r21 = cumret(td63v[-21:]) if len(td63v) >= 21 else np.nan
    td126v = sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    r126 = cumret(td126v) if len(td126v) >= 63 else np.nan
    td252v = sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
    r252 = cumret(td252v) if len(td252v) >= 126 else np.nan
    r252ex1m = ((1+r252)/(1+r21)-1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21)>1e-8 else np.nan
    horizons = [r63, r126, r252ex1m]
    valid = [v for v in horizons if np.isfinite(v)]
    if len(valid) >= 2:
        pos_count = sum(1 for v in valid if v > 0)
        avg_ret = float(np.mean([max(v,0) for v in valid]))
        raw_w = pos_count * (1 + avg_ret)
    else:
        pos_count = 0; avg_ret = 0; raw_w = 1.0
    w5b_data[th] = {'raw_weight': raw_w, 'pos_count': pos_count, 'r252ex1m': r252ex1m}
# Normalize + 30% cap
total_raw = sum(d['raw_weight'] for d in w5b_data.values())
beast_total = total_raw
if total_raw > 0:
    for th in w5b_data: w5b_data[th]['weight'] = w5b_data[th]['raw_weight'] / total_raw
else:
    for th in w5b_data: w5b_data[th]['weight'] = 1.0 / max(len(w5b_data), 1)
for _ in range(5):
    ws = np.array([w5b_data[th]['weight'] for th in w5b_data])
    excess = np.maximum(ws - 0.30, 0)
    if excess.sum() < 1e-6: break
    under = ws < 0.30; ws = np.minimum(ws, 0.30)
    if under.any(): ws[under] += excess.sum() * (ws[under] / ws[under].sum())
    ws = ws / ws.sum()
    for i, th in enumerate(w5b_data): w5b_data[th]['weight'] = round(float(ws[i]), 4)
# Add to comparisons
for c in comparisons:
    th = c['theme']
    wd = w5b_data.get(th, {})
    c['w5b_weight'] = round(wd.get('weight', 1.0/max(len(comparisons),1)), 4)
    c['beast_weight'] = round(wd.get('raw_weight', 1.0) / beast_total, 4) if beast_total > 0 else round(1.0/max(len(comparisons),1), 4)
    c['w5b_pos_count'] = wd.get('pos_count', 0)
    c['w5b_r252ex1m'] = round(wd.get('r252ex1m', 0), 4) if np.isfinite(wd.get('r252ex1m', np.nan)) else None
# === Dip Sleeve Diagnostics ===
DIP_PATH = ROOT / 'data' / 'stock-themes-api' / 'dip_alerts.json'
dip_sleeve = []
if DIP_PATH.exists():
    try:
        dip_data = json.load(open(DIP_PATH))
        # Build slug lookup for theme names
        name_to_slug = {}
        for th_slug in ts.index:
            name_to_slug[slug_to_name.get(th_slug, th_slug)] = th_slug
        for alert in dip_data.get('alerts', []):
            th_name = alert.get('theme_name', '')
            th_slug = name_to_slug.get(th_name)
            # Check trend qualification
            in_trend = False; trend_rank = None; trend_state = None
            if th_slug and th_slug in ts.index:
                trend_rank = int(ts['score'].rank(ascending=False).get(th_slug, 999))
                trend_state = 'ENTRY' if trend_rank <= 20 else 'WATCH' if trend_rank <= 35 else 'EXIT'
                in_trend = trend_state in ('ENTRY', 'WATCH')
            # Apply qualification filters
            win_rate = alert.get('win_rate', 0)
            sample_n = alert.get('sample_n', 0)
            window = alert.get('window', '')
            days_since = alert.get('days_since', 999)
            qualified = (in_trend and win_rate >= 0.60 and sample_n >= 20
                        and window in ('1M', '1-2M', '2-3M') and days_since <= 60)
            dip_sleeve.append({
                'theme': th_name, 'theme_slug': th_slug,
                'dip': round(alert.get('dip_actual', 0), 3),
                'band': alert.get('dip_band', ''),
                'win_rate': round(win_rate, 3), 'sample_n': sample_n,
                'window': window, 'days_since': days_since,
                'pattern': alert.get('pattern_label', ''),
                'trend_rank': trend_rank, 'trend_state': trend_state,
                'qualified': qualified
            })
    except Exception as e:
        print(f'WARN: dip_alerts load failed: {e}')
n_qualified = sum(1 for d in dip_sleeve if d['qualified'])

# === Continuity Filter Diagnostics (DIAGNOSTIC ONLY — does not affect selection) ===
ENABLE_CONTINUITY_ACTIVE = False  # True = affects selection; False = log only
SPARK_PATH = ROOT / 'data' / 'stock-themes-api' / 'sparklines_all.json'
BA_PATH2 = ROOT / 'data' / 'stock-themes-api' / 'beta_alpha_all.json'
continuity_diag = []
try:
    spark_data = json.load(open(SPARK_PATH)) if SPARK_PATH.exists() else {}
    ba_data = json.load(open(BA_PATH2)) if BA_PATH2.exists() else {}
    sparklines = spark_data.get('sparklines', {})
    for c in comparisons:
        th_slug = c['theme']; tk = c['a5_pick']
        th_name = slug_to_name.get(th_slug, th_slug)
        cd_entry = {'theme': th_slug, 'ticker': tk}
        # Sparkline smoothness
        vals = sparklines.get(th_name, [])
        if vals and len(vals) >= 10:
            arr = np.array(vals, dtype=float)
            diffs = np.diff(arr)
            sign_ch = np.sum(np.abs(np.diff(np.sign(diffs))) > 0)
            cd_entry['sign_consistency'] = round(1.0 - sign_ch / max(len(diffs)-1, 1), 3)
            cd_entry['monotonic_ratio'] = round(float(np.sum(diffs > 0) / max(len(diffs), 1)), 3)
            cd_entry['jumpiness'] = round(float(np.std(diffs) / (np.abs(np.mean(diffs)) + 1e-8)), 2)
        # Multi-horizon alpha sign consistency
        ba_tk = ba_data.get(th_slug, {}).get('data', {}).get(tk, {})
        if ba_tk:
            n_pos = sum(1 for p in ['5D','10D','1M','2M','3M','6M','12M']
                       if ba_tk.get(p, {}).get('alpha', 0) > 0)
            cd_entry['alpha_sign_positive'] = n_pos
            cd_entry['alpha_sign_total'] = 7
        continuity_diag.append(cd_entry)
except Exception as e:
    print(f'WARN: continuity filter failed: {e}')

# === Vol Overlay Diagnostics (DIAGNOSTIC ONLY — does not affect gross) ===
ENABLE_VOL_ACTIVE = False  # True = adjusts gross; False = log only
vol_diag = {}
try:
    # Portfolio realized vol (20-day and 63-day)
    port_daily = []
    for c in comparisons:
        tk = c['a5_pick']
        if tk:
            tk_data = panel[(panel['ticker']==tk)&(panel['date']<=dt)].drop_duplicates('date').sort_values('date').tail(63)
            if len(tk_data) >= 20:
                s = tk_data.set_index('date')['ret']
                s = s[~s.index.duplicated(keep='first')]
                port_daily.append(s)
    if port_daily:
        port_df = pd.DataFrame({i: s for i, s in enumerate(port_daily)}).dropna()
        eq_wt = port_df.mean(axis=1)  # equal weight
        vol20 = float(eq_wt.tail(20).std() * np.sqrt(252))
        vol63 = float(eq_wt.tail(63).std() * np.sqrt(252))
        target_vol = 0.25  # 25% target
        gross_scale_20 = min(1.0, target_vol / vol20) if vol20 > 0 else 1.0
        gross_scale_63 = min(1.0, target_vol / vol63) if vol63 > 0 else 1.0
        vol_diag = {
            'realized_vol_20d': round(vol20, 3),
            'realized_vol_63d': round(vol63, 3),
            'target_vol': target_vol,
            'gross_scale_20d': round(gross_scale_20, 3),
            'gross_scale_63d': round(gross_scale_63, 3),
            'would_reduce': gross_scale_20 < 1.0 or gross_scale_63 < 1.0,
        }
except Exception as e:
    print(f'WARN: vol overlay failed: {e}')

output = {
    'snapshot_date': str(dt.date()),
    'generated_at': datetime.now().isoformat(),
    'version': 'PRISM-RQ_v1',
    'status': 'SHADOW',
    'frozen_params': {
        'alpha_window': 63,
        'theme_score': '0.70×rank(mom63)+0.30×rank(decel)',
        'stock_score': 'split-window: β/R²=126d, α=63d → α_cum×shrink(R²_126)',
        'stock_score_snrb': '(α_split63|126/resid_vol)×shrink(R²_126)',
        'bfm2_filter': 'veto: breadth<30pct, conc>80pct, vol>80pct from top25',
        'cra_v1': 'SNRb × (0.70 + 0.30 × AuditScore), AuditScore from st sign/tval',
        'top_themes': 10, 'picks_per_theme': 1,
        'min_members': 4, 'sector_cap': 3, 'rebalance_days': 20
    },
    'summary': {
        'themes_selected': len(comparisons),
        'a4_names': len(used4), 'a5_names': len(used5), 'snrb_names': len(used_snrb),
        'cra_names': len(used_cra), 'snrb_cra_overlap': cra_overlap_snrb,
        'bfm2_themes': bfm2_overlap_base,
        'bfm2_vetoed': len(bfm2_vetoed),
        'overlap': overlap,
        'overlap_pct': round(overlap / max(len(comparisons), 1), 2),
        'a5_snrb_overlap': snrb_overlap_a5,
        'a5_snrb_overlap_pct': round(snrb_overlap_a5 / max(len(comparisons), 1), 2),
        'diff_names': len(comparisons) - overlap
    },
    'correlation_diagnostics': corr_diag,
    'bfm2_quality_features': {th: cand_feat.get(th) for th in sel if th in cand_feat},
    'dip_sleeve_diagnostics': dip_sleeve,
    'dip_sleeve_summary': {'total_alerts': len(dip_sleeve), 'qualified': n_qualified},
    'continuity_diagnostics': continuity_diag,
    'continuity_active': ENABLE_CONTINUITY_ACTIVE,
    'vol_overlay_diagnostics': vol_diag,
    'vol_overlay_active': ENABLE_VOL_ACTIVE,
    'comparisons': comparisons,
    'virtual_exits': virtual_exits,
    'virtual_entries': virtual_entries
}
with open(OUT / 'shadow_comparison.json', 'w') as f:
    json.dump(output, f, ensure_ascii=False, indent=2)

meta_out = {
    'snapshot_date': str(dt.date()),
    'generated_at': datetime.now().isoformat(),
    'version': 'PRISM-RQ_v1',
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
    },
    'snrb_backtest_5yr': {
        'snrb_sharpe': 1.46, 'snrb_cagr': '57.5%', 'snrb_vol': '39.3%',
        'snrb_maxdd': '-37.1%', 'snrb_calmar': 1.55,
        'snrb_top5_share': '34.1%',
        'status': 'research-shadow (independent forward clock)'
    }
}
with open(OUT / 'meta.json', 'w') as f:
    json.dump(meta_out, f, ensure_ascii=False, indent=2)

comp_size = (OUT / 'shadow_comparison.json').stat().st_size / 1024
print(f'PRISM-RQ: {len(comparisons)} themes, overlap={overlap}/{len(comparisons)}, '
      f'snrb_overlap={snrb_overlap_a5}/{len(comparisons)}, '
      f'cra_overlap_snrb={cra_overlap_snrb}/{len(comparisons)}, '
      f'bfm2={bfm2_overlap_base}/10 (vetoed={len(bfm2_vetoed)}), '
      f'corr={corr_diag.get("avg_pairwise_corr","N/A")}, '
      f'dip={n_qualified}/{len(dip_sleeve)} qualified, '
      f'snapshot={dt.date()}, size={comp_size:.0f}KB')
