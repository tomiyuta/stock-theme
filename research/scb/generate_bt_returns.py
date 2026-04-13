#!/usr/bin/env python3
"""Generate cumulative return JSON for PRISM/PRISM-R charts (opengrail format)."""
import pandas as pd, numpy as np, json, time, warnings, sys
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel_v2.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_us_metadata.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes | {panel.ticker.nunique()} tickers')

# Data quality filters: exclude OTC periods + clamp extreme returns
if 'on_major_exchange' in panel.columns:
    n_before = len(panel)
    panel = panel[panel.on_major_exchange == 1].copy()
    print(f'  OTC filter: {n_before:,} → {len(panel):,} rows ({len(panel)/n_before*100:.1f}%)')

panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
panel['ret'] = panel['ret'].clip(-0.50, 2.00)  # Clamp: -50% to +200% daily
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
dates_all = sorted(panel['date'].unique())
tk_ret = panel[['date','ticker','ret']].drop_duplicates(['date','ticker']).dropna(subset=['ret'])
tk_wide = tk_ret.pivot(index='date', columns='ticker', values='ret').sort_index()
meta_sec = meta.set_index('ticker')['gics_sector'].to_dict()
psec = panel[['theme','ticker']].drop_duplicates()
psec['sector'] = psec['ticker'].map(meta_sec)
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')

def ols_ab(y, x):
    mask = np.isfinite(y) & np.isfinite(x); y, x = y[mask], x[mask]; n = len(y)
    if n < 10: return np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean(); xd = x - xm; vx = np.dot(xd, xd)/(n-1)
    if vx < 1e-12: return np.nan, np.nan, np.nan
    b = np.dot(xd, y-ym)/(n-1) / vx; a = ym - b*xm
    ss_res = float(np.sum((y-a-b*x)**2)); ss_tot = float(np.sum((y-ym)**2))
    r2 = 1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n, b, r2

def split_alpha(y_long, x_long, y_short, x_short):
    mask=np.isfinite(y_long)&np.isfinite(x_long);yl,xl=y_long[mask],x_long[mask];nl=len(yl)
    if nl<20:return np.nan,np.nan,np.nan
    xm,ym=xl.mean(),yl.mean();xd=xl-xm;vx=np.dot(xd,xd)/(nl-1)
    if vx<1e-12:return np.nan,np.nan,np.nan
    b=np.dot(xd,yl-ym)/(nl-1)/vx;a=ym-b*xm
    ss=float(np.sum((yl-a-b*xl)**2));st=float(np.sum((yl-ym)**2))
    r2=1-ss/st if st>1e-12 else np.nan
    ms=np.isfinite(y_short)&np.isfinite(x_short);ys,xs=y_short[ms],x_short[ms];ns=len(ys)
    if ns<10:return np.nan,b,r2
    return float(np.mean(ys-b*xs)*ns),b,r2

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}, prep {time.time()-t0:.1f}s')

a4_daily = []; a5_daily = []; a5w5b_daily = []; a5beast_daily = []; a5def_daily = []; hold_dates_all = []

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]
    dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1])
    dt21 = set(dates_all[max(0,j-20):j+1])
    dt126 = set(dates_all[max(0,j-125):j+1])
    dt252 = set(dates_all[max(0,j-251):j+1])
    dt_7_12 = set(dates_all[max(0,j-251):max(0,j-146)])  # months 7-12 ago
    sub = panel[panel['date'].isin(dt63)]
    sub126 = panel[panel['date'].isin(dt126)]
    sub252 = panel[panel['date'].isin(dt252)]
    sub_7_12 = panel[panel['date'].isin(dt_7_12)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm>=MIN_M].index.tolist()
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
    dcs = pd.Series(dc); common = list(set(ms.index)&set(dcs.index))
    hold_dates = tk_wide.index[j+1:j_next+1]
    hold_dates_all.extend(hold_dates.tolist())
    if not common:
        a4_daily.extend([0.0]*len(hold_dates)); a5_daily.extend([0.0]*len(hold_dates))
        continue
    ts = pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['r_mom']=ts['mom63'].rank(pct=True); ts['r_dec']=ts['decel'].rank(pct=True)
    ts['score']=0.70*ts['r_mom']+0.30*ts['r_dec']
    ts = ts.sort_values('score', ascending=False)
    sel=[]; sc_cnt={}
    for th in ts.index:
        s=theme_sector.get(th,'Unk')
        if sc_cnt.get(s,0)>=SEC_MAX: continue
        sel.append(th); sc_cnt[s]=sc_cnt.get(s,0)+1
        if len(sel)>=TOP_T: break
    a4p={}; a5p={}; a5def={}; used4=set(); used5=set(); used_def=set()
    for th in sel:
        ths = sub[(sub['theme']==th)&sub['ret'].notna()]
        tks = ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        s4={}; s5={}; sdef={}
        for tk in tks:
            tkd = ths[ths['ticker']==tk].sort_values('date')
            r21d = tkd[tkd['date'].isin(dt21)]
            raw_1m = cumret(r21d['ret'].values) if len(r21d)>=10 else np.nan
            s4[tk] = raw_1m if np.isfinite(raw_1m) else -999
            # Split-window: β/R² from 126d, α from 63d
            tkd126 = sub126[(sub126['theme']==th)&(sub126['ticker']==tk)].sort_values('date')
            if len(tkd126) >= 20:
                a63,b63,r2_63 = split_alpha(tkd126['ret'].values, tkd126['theme_ex_self'].values,
                                            tkd['ret'].values, tkd['theme_ex_self'].values)
            else:
                a63,b63,r2_63 = ols_ab(tkd['ret'].values, tkd['theme_ex_self'].values)
            shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
            s5[tk] = a63*shrk if np.isfinite(a63) else -999
            # DEF: 12-7 month alpha
            tkd_7_12 = sub_7_12[(sub_7_12['theme']==th)&(sub_7_12['ticker']==tk)].sort_values('date')
            if len(tkd_7_12) >= 10:
                a_def,_,r2_def = ols_ab(tkd_7_12['ret'].values, tkd_7_12['theme_ex_self'].values)
                shrk_def = shrink_r2(r2_def) if np.isfinite(r2_def) else 0
                sdef[tk] = a_def*shrk_def if np.isfinite(a_def) else -999
            else:
                sdef[tk] = -999
        for tk,sc in sorted(s4.items(), key=lambda x:-x[1]):
            if tk not in used4 and sc>-999: a4p[tk]=1.0; used4.add(tk); break
        for tk,sc in sorted(s5.items(), key=lambda x:-x[1]):
            if tk not in used5 and sc>-999: a5p[tk]=1.0; used5.add(tk); break
        for tk,sc in sorted(sdef.items(), key=lambda x:-x[1]):
            if tk not in used_def and sc>-999: a5def[tk]=1.0; used_def.add(tk); break
    for d in [a4p, a5p, a5def]:
        total=sum(d.values())
        if total>0:
            for k in d: d[k]/=total
    # W5b weights for a5 picks
    a5w5b = {}; a5beast = {}
    if a5p:
        a5_theme_of = {}  # tk -> theme
        for th in sel:
            for tk in a5p:
                if tk in used5:
                    ths_sub = sub[(sub['theme']==th) & (sub['ticker']==tk)]
                    if len(ths_sub) > 0:
                        a5_theme_of[tk] = th
        w5b_raw = {}
        for tk in a5p:
            th = a5_theme_of.get(tk)
            if not th:
                w5b_raw[tk] = 1.0; continue
            td63v = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            r63 = cumret(td63v) if len(td63v)>=63 else np.nan
            r21 = cumret(td63v[-21:]) if len(td63v)>=21 else np.nan
            td126v = sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            r126 = cumret(td126v) if len(td126v)>=63 else np.nan
            td252v = sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
            r252 = cumret(td252v) if len(td252v)>=126 else np.nan
            r252ex1m = ((1+r252)/(1+r21)-1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21)>1e-8 else np.nan
            horizons = [r63, r126, r252ex1m]
            valid = [v for v in horizons if np.isfinite(v)]
            if len(valid) >= 2:
                pc = sum(1 for v in valid if v > 0)
                ar = np.mean([max(v,0) for v in valid])
                w5b_raw[tk] = pc * (1 + ar)
            else:
                w5b_raw[tk] = 1.0
        raw_total = sum(w5b_raw.values())
        # BEAST (nocap)
        if raw_total > 0:
            for tk in a5p: a5beast[tk] = w5b_raw[tk] / raw_total
        else:
            for tk in a5p: a5beast[tk] = 1.0 / len(a5p)
        # W5b (30% cap)
        a5w5b = dict(a5beast)
        for _ in range(5):
            ws = np.array([a5w5b[tk] for tk in a5w5b])
            exc = np.maximum(ws - 0.30, 0)
            if exc.sum() < 1e-6: break
            under = ws < 0.30; ws = np.minimum(ws, 0.30)
            if under.any(): ws[under] += exc.sum() * (ws[under] / ws[under].sum())
            ws = ws / ws.sum()
            for i, tk in enumerate(a5w5b): a5w5b[tk] = float(ws[i])
    for w_dict, ret_list in [(a4p, a4_daily), (a5p, a5_daily), (a5w5b, a5w5b_daily), (a5beast, a5beast_daily), (a5def, a5def_daily)]:
        if not w_dict:
            ret_list.extend([0.0]*len(hold_dates)); continue
        ws = pd.Series(w_dict)
        dr = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws, axis=1).sum(axis=1)
        ret_list.extend(dr.values.tolist())
    if (pos+1) % 20 == 0:
        print(f'  [{pos+1}/{len(rebal_idx)-1}] {dt.date()}')

print(f'BT done in {time.time()-t0:.1f}s | {len(a4_daily)} daily returns')

# === Convert daily to monthly and build cumulative_returns.json ===
import yfinance as yf

dates_ser = pd.Series(hold_dates_all)
df = pd.DataFrame({'date': dates_ser, 'a4': a4_daily, 'a5': a5_daily, 'a5w5b': a5w5b_daily, 'a5beast': a5beast_daily, 'a5def': a5def_daily})
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date').sort_index()

# SPY from yfinance
spy_start = df.index[0] - pd.Timedelta(days=5)
spy_end = df.index[-1] + pd.Timedelta(days=5)
spy = yf.download('SPY', start=spy_start.strftime('%Y-%m-%d'), end=spy_end.strftime('%Y-%m-%d'), progress=False)
if 'Adj Close' in spy.columns:
    spy_close = spy['Adj Close'].squeeze()
else:
    spy_close = spy['Close'].squeeze()
spy_ret = spy_close.pct_change().dropna()
spy_ret.index = spy_ret.index.tz_localize(None)
df['SPY'] = spy_ret.reindex(df.index).fillna(0)

# Monthly returns
df['ym'] = df.index.to_period('M')
monthly = df.groupby('ym').apply(lambda g: pd.Series({
    'a4': float((1+g['a4']).prod()-1),
    'a5': float((1+g['a5']).prod()-1),
    'a5w5b': float((1+g['a5w5b']).prod()-1),
    'a5beast': float((1+g['a5beast']).prod()-1),
    'a5def': float((1+g['a5def']).prod()-1),
    'SPY': float((1+g['SPY']).prod()-1),
})).reset_index()
monthly['date'] = monthly['ym'].dt.to_timestamp('M')
monthly = monthly.sort_values('date').reset_index(drop=True)

# Cumulative growth of $1
for col in ['a4','a5','a5w5b','a5beast','a5def','SPY']:
    monthly[f'cum_{col}'] = (1+monthly[col]).cumprod()

# Annual returns
monthly['year'] = monthly['date'].dt.year
annual = {}
for col in ['a4','a5','a5w5b','a5beast','a5def','SPY']:
    yr = monthly.groupby('year')[col].apply(lambda g: float((1+g).prod()-1))
    annual[col] = {str(y): round(v,4) for y,v in yr.items()}

# Stats
def calc_stats(monthly_rets):
    r = np.array(monthly_rets, dtype=float)
    n = len(r); n_yr = n/12
    cum = float(np.prod(1+r)-1)
    cagr = (1+cum)**(1/n_yr)-1 if n_yr>0 else 0
    vol = float(np.std(r,ddof=1)*np.sqrt(12))
    sharpe = float(np.mean(r)/np.std(r,ddof=1)*np.sqrt(12)) if np.std(r)>0 else 0
    down = r[r<0]
    dd_vol = float(np.std(down,ddof=1)*np.sqrt(12)) if len(down)>1 else 1
    sortino = float(np.mean(r)/np.std(down,ddof=1)*np.sqrt(12)) if len(down)>1 and np.std(down)>0 else 0
    wealth = np.cumprod(1+r); peak = np.maximum.accumulate(wealth)
    dd = wealth/peak-1; mdd = float(dd.min())
    return {'cagr':round(cagr,4),'sharpe':round(sharpe,4),'sortino':round(sortino,4),
            'maxdd':round(mdd,4),'n_months':n}

stats = {col: calc_stats(monthly[col].values) for col in ['a4','a5','a5w5b','a5beast','a5def','SPY']}

# Output JSON (opengrail format)
output = {
    'dates': [d.strftime('%Y-%m-%d') for d in monthly['date']],
    'a4': [round(v,6) for v in monthly['cum_a4']],
    'ret_a4': [round(v,6) for v in monthly['a4']],
    'a5': [round(v,6) for v in monthly['cum_a5']],
    'ret_a5': [round(v,6) for v in monthly['a5']],
    'a5w5b': [round(v,6) for v in monthly['cum_a5w5b']],
    'ret_a5w5b': [round(v,6) for v in monthly['a5w5b']],
    'beast': [round(v,6) for v in monthly['cum_a5beast']],
    'ret_beast': [round(v,6) for v in monthly['a5beast']],
    'def': [round(v,6) for v in monthly['cum_a5def']],
    'ret_def': [round(v,6) for v in monthly['a5def']],
    'SPY': [round(v,6) for v in monthly['cum_SPY']],
    'ret_SPY': [round(v,6) for v in monthly['SPY']],
    'annual': annual,
    'stats': stats,
    'meta': {
        'source': 'norgate_5yr_panel',
        'period': f'{monthly["date"].iloc[0].strftime("%Y-%m")} ~ {monthly["date"].iloc[-1].strftime("%Y-%m")}',
        'n_months': len(monthly),
        'note': 'PIT汚染あり。BTは参考値。forward実績はforward_overlay配列で追加予定。',
        'bt_boundary': monthly['date'].iloc[-1].strftime('%Y-%m-%d'),
    },
    'forward_overlay': {'dates': [], 'a4': [], 'a5': [], 'a5w5b': [], 'beast': [], 'def': [], 'SPY': []},
}

from pathlib import Path
out_prism = Path('/Users/yutatomi/Downloads/stock-theme/public/api/prism')
out_prismr = Path('/Users/yutatomi/Downloads/stock-theme/public/api/prism-r')
out_prism.mkdir(parents=True, exist_ok=True)
out_prismr.mkdir(parents=True, exist_ok=True)

# Preserve existing forward_overlay from both files
for out_path in [out_prism, out_prismr]:
    cum_file = out_path / 'cumulative_returns.json'
    if cum_file.exists():
        try:
            existing = json.load(open(cum_file))
            existing_fwd = existing.get('forward_overlay', {})
            if existing_fwd.get('dates'):
                output['forward_overlay'] = existing_fwd
                print(f'Preserved forward_overlay: {len(existing_fwd["dates"])} entries from {out_path.name}')
                break
        except: pass

with open(out_prism / 'cumulative_returns.json', 'w') as f:
    json.dump(output, f, ensure_ascii=False)
with open(out_prismr / 'cumulative_returns.json', 'w') as f:
    json.dump(output, f, ensure_ascii=False)

print(f'\nOutput: {len(monthly)} months ({monthly["date"].iloc[0].date()} ~ {monthly["date"].iloc[-1].date()})')
print(f'Stats A4: CAGR={stats["a4"]["cagr"]:.1%} Sharpe={stats["a4"]["sharpe"]:.2f} MaxDD={stats["a4"]["maxdd"]:.1%}')
print(f'Stats A5: CAGR={stats["a5"]["cagr"]:.1%} Sharpe={stats["a5"]["sharpe"]:.2f} MaxDD={stats["a5"]["maxdd"]:.1%}')
print(f'Stats W5b: CAGR={stats["a5w5b"]["cagr"]:.1%} Sharpe={stats["a5w5b"]["sharpe"]:.2f} MaxDD={stats["a5w5b"]["maxdd"]:.1%}')
print(f'Stats BEAST: CAGR={stats["a5beast"]["cagr"]:.1%} Sharpe={stats["a5beast"]["sharpe"]:.2f} MaxDD={stats["a5beast"]["maxdd"]:.1%}')
print(f'Stats DEF: CAGR={stats["a5def"]["cagr"]:.1%} Sharpe={stats["a5def"]["sharpe"]:.2f} MaxDD={stats["a5def"]["maxdd"]:.1%}')
print(f'Stats SPY: CAGR={stats["SPY"]["cagr"]:.1%} Sharpe={stats["SPY"]["sharpe"]:.2f} MaxDD={stats["SPY"]["maxdd"]:.1%}')
print(f'Saved to {out_prism}/cumulative_returns.json + {out_prismr}/')
print(f'Total time: {time.time()-t0:.1f}s')
