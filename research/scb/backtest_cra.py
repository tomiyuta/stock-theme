"""CRA-v1 backtest — Consensus Residual Alpha confirmation overlay on A5-SNRb"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

ba_all = json.load(open('/Users/yutatomi/Downloads/stock-theme/data/stock-themes-api/beta_alpha_all.json'))
panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes')
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

def ols_full(y, x):
    mask = np.isfinite(y) & np.isfinite(x); y, x = y[mask], x[mask]; n = len(y)
    if n < 20: return np.nan, np.nan, np.nan, np.nan
    xm, ym = x.mean(), y.mean(); vx = np.var(x, ddof=1)
    if vx < 1e-15: return np.nan, np.nan, np.nan, np.nan
    b = np.dot(x-xm, y-ym)/(n-1)/vx; a = ym - b*xm
    resid = y - a - b*x
    ss_res = float(np.sum(resid**2)); ss_tot = float(np.sum((y-ym)**2))
    r2 = 1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n, b, r2, float(np.std(resid,ddof=1)*np.sqrt(n))

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def cra_audit(theme, ticker, self_alpha):
    ba_tk = ba_all.get(theme, {}).get('data', {}).get(ticker, {})
    if not ba_tk: return 0.0
    a3m = ba_tk.get('3M', {}); a6m = ba_tk.get('6M', {})
    st_a3 = a3m.get('alpha', 0); st_a6 = a6m.get('alpha', 0)
    st_t3 = a3m.get('alpha_tval', 0); st_p3 = a3m.get('alpha_pval', 1)
    st_t6 = a6m.get('alpha_tval', 0); st_p6 = a6m.get('alpha_pval', 1)
    sa = 1 if self_alpha > 0 else (-1 if self_alpha < 0 else 0)
    c1 = 1.0 if sa != 0 and (1 if st_a3>0 else -1)==sa else 0.0
    c2 = 1.0 if sa != 0 and (1 if st_a6>0 else -1)==sa else 0.0
    c3 = 1.0 if (st_a3>0)==(st_a6>0) else 0.0
    c4 = 1.0 if abs(st_t3)>1 or st_p3<0.10 else 0.0
    c5 = 1.0 if abs(st_t6)>1 or st_p6<0.10 else 0.0
    return 0.30*c1 + 0.30*c2 + 0.15*c3 + 0.125*c4 + 0.125*c5

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

STRATS = ['snrb', 'cra']
daily_ret = {s: [] for s in STRATS}
detail_log = []

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]; dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1])
    sub = panel[panel['date'].isin(dt63)]
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
    dcs=pd.Series(dc); common=list(set(ms.index)&set(dcs.index))
    hold_dates = tk_wide.index[j+1:j_next+1]
    if not common:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    ts = pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['r_mom']=ts['mom63'].rank(pct=True); ts['r_dec']=ts['decel'].rank(pct=True)
    ts['score']=0.70*ts['r_mom']+0.30*ts['r_dec']
    ts = ts.sort_values('score', ascending=False)
    sel=[]; sc_cnt={}
    for th in ts.index:
        s2=theme_sector.get(th,'Unk')
        if sc_cnt.get(s2,0)>=SEC_MAX: continue
        sel.append(th); sc_cnt[s2]=sc_cnt.get(s2,0)+1
        if len(sel)>=TOP_T: break
    # Layer 2: SNRb vs CRA
    ports = {s:{} for s in STRATS}; used = {s:set() for s in STRATS}
    for th in sel:
        ths = sub[(sub['theme']==th)&sub['ret'].notna()]
        tks = ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        scores_snrb = {}; scores_cra = {}
        for tk in tks:
            tkd = ths[ths['ticker']==tk].sort_values('date')
            a63, b63, r2_63, rvol = ols_full(tkd['ret'].values, tkd['theme_ex_self'].values)
            if not np.isfinite(a63) or not np.isfinite(rvol) or rvol<1e-8:
                scores_snrb[tk] = -999; scores_cra[tk] = -999; continue
            shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
            sc_snrb = (a63/rvol)*shrk
            audit = cra_audit(th, tk, a63)
            sc_cra = sc_snrb * (0.70 + 0.30*audit)
            scores_snrb[tk] = sc_snrb; scores_cra[tk] = sc_cra
        for s, sc_dict in [('snrb',scores_snrb),('cra',scores_cra)]:
            for tk, sc in sorted(sc_dict.items(), key=lambda x:-x[1]):
                if tk not in used[s] and sc>-999:
                    ports[s][tk]=1.0; used[s].add(tk); break
    for s in STRATS:
        total=sum(ports[s].values())
        if total>0:
            for k in ports[s]: ports[s][k]/=total
    for s in STRATS:
        if not ports[s]: daily_ret[s].extend([0.0]*len(hold_dates)); continue
        ws=pd.Series(ports[s])
        dr=tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1).sum(axis=1)
        daily_ret[s].extend(dr.values.tolist())
    overlap = len(set(ports['snrb'].keys())&set(ports['cra'].keys()))
    detail_log.append({'date':str(dt.date()),'overlap':overlap,'n':len(sel)})

# === Metrics ===
eq_dates = tk_wide.index[-len(daily_ret['snrb']):]
def calc_metrics(dr, name):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    neg=arr[arr<0]; dd=np.sqrt(np.mean(neg**2))*np.sqrt(252) if len(neg)>0 else 1e-8
    sortino=cagr/dd; calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    return {'name':name,'cagr':cagr,'vol':vol,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd}

print("\n"+"="*70)
print(f"{'Metric':<18} {'SNRb':>14} {'CRA-v1':>14} {'差':>12}")
print("="*70)
m1=calc_metrics(daily_ret['snrb'],'SNRb'); m2=calc_metrics(daily_ret['cra'],'CRA')
for key,label in [('cagr','CAGR'),('vol','Vol'),('sharpe','Sharpe'),('sortino','Sortino'),('calmar','Calmar'),('maxdd','MaxDD')]:
    v1,v2=m1[key],m2[key]; diff=v2-v1
    fmt=lambda v: f"{v:.1%}" if key in ['cagr','vol','maxdd'] else f"{v:.3f}"
    print(f"  {label:<16} {fmt(v1):>13} {fmt(v2):>13} {'+' if diff>=0 else ''}{fmt(diff):>11}")
print("="*70)

diff_arr = np.array(daily_ret['cra'])-np.array(daily_ret['snrb'])
monthly = pd.Series(diff_arr, index=eq_dates).resample('M').sum()
print(f"\n=== CRA-v1 vs SNRb ===")
print(f"  Median monthly diff: {monthly.median():+.4f} ({'↑' if monthly.median()>0 else '↓'})")
print(f"  Positive months: {(monthly>0).sum()}/{len(monthly)} ({(monthly>0).sum()/len(monthly):.0%})")

dl=pd.DataFrame(detail_log)
print(f"  Avg stock overlap: {dl['overlap'].mean():.1f}/{TOP_T} ({dl['overlap'].mean()/TOP_T:.0%})")

# Annual
print(f"\n=== ANNUAL ===")
for s in STRATS:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[s])),index=eq_dates)
    annual=eq.resample('YE').last().pct_change().dropna()
    print(f"  {s}:")
    for dt_y,r in annual.items(): print(f"    {dt_y.year}: {r:+.1%}")

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_cra_results.json','w') as f:
    json.dump({'metrics':{'snrb':m1,'cra':m2},'detail_log':detail_log},f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
