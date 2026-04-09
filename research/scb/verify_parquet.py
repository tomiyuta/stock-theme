"""Full verification from parquet files: P1 Feasibility + P2 A4 vs A5-lite + Diagnostics."""
import pandas as pd, numpy as np, time, warnings
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('theme_daily_panel.parquet')
meta = pd.read_parquet('ticker_meta.parquet')
print(f'Panel: {len(panel):,} rows | {panel.theme.nunique()} themes | {panel.ticker.nunique()} tickers')
print(f'Date: {panel.date.min().date()} ~ {panel.date.max().date()}')

# Returns
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
dates_all = sorted(panel['date'].unique())
tk_ret = panel[['date','ticker','ret']].drop_duplicates(['date','ticker']).dropna(subset=['ret'])
tk_wide = tk_ret.pivot(index='date', columns='ticker', values='ret').sort_index()
meta_sec = meta.set_index('ticker')['sector'].to_dict()
meta_mc = meta.set_index('ticker')['mc'].to_dict()
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

def cumret(arr):
    a = np.asarray(arr, dtype=float); a = a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0

print(f'Prep done in {time.time()-t0:.1f}s')

# === MAIN BACKTEST ===
WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Holding periods: {len(rebal_idx)-1}')

feas_log = []
a4_daily = []; a5_daily = []
period_detail = []  # for attribution

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]
    dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1])
    dt21 = set(dates_all[max(0,j-20):j+1])
    sub = panel[panel['date'].isin(dt63)]
    tm = sub.groupby('theme')['ticker'].nunique()
    elig = tm[tm>=MIN_M].index.tolist()
    # Theme momentum
    tm_mom = {}
    for th in elig:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        tm_mom[th] = cumret(td.values)
    ms = pd.Series(tm_mom).dropna().sort_values(ascending=False)
    # Deceleration
    dc = {}
    for th in ms.index:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)<63: continue
        r021=cumret(td[-21:]); r2142=cumret(td[-42:-21]); r4263=cumret(td[-63:-42])
        if all(np.isfinite([r021,r2142,r4263])): dc[th]=-(r021-0.5*(r2142+r4263))
    dcs = pd.Series(dc)
    common = list(set(ms.index)&set(dcs.index))
    if not common:
        feas_log.append({'date':dt,'eligible':len(elig),'scored':0,'sel':0,'a4_n':0,'a5_n':0,'a5_cand':0,'a5_alpha_pos':0})
        hold_dates = tk_wide.index[j+1:j_next+1]
        a4_daily.extend([0.0]*len(hold_dates)); a5_daily.extend([0.0]*len(hold_dates))
        continue
    ts = pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['r_mom']=ts['mom63'].rank(pct=True); ts['r_dec']=ts['decel'].rank(pct=True)
    ts['score']=0.70*ts['r_mom']+0.30*ts['r_dec']
    ts = ts.sort_values('score', ascending=False)
    # Sector cap
    sel=[]; sc_cnt={}
    for th in ts.index:
        s=theme_sector.get(th,'Unk')
        if sc_cnt.get(s,0)>=SEC_MAX: continue
        sel.append(th); sc_cnt[s]=sc_cnt.get(s,0)+1
        if len(sel)>=TOP_T: break
    # Stock scoring
    a4p={}; a5p={}; used4=set(); used5=set()
    a5_cand_total=0; a5_alpha_pos_total=0
    for th in sel:
        ths = sub[(sub['theme']==th)&sub['ret'].notna()]
        tks = ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        s4={}; s5={}; alpha_info={}
        for tk in tks:
            tkd = ths[ths['ticker']==tk].sort_values('date')
            r21d = tkd[tkd['date'].isin(dt21)]
            raw_1m = cumret(r21d['ret'].values) if len(r21d)>=10 else np.nan
            s4[tk] = raw_1m if np.isfinite(raw_1m) else -999
            a63,b63,r2_63 = ols_ab(tkd['ret'].values, tkd['theme_ex_self'].values)
            shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
            score5 = a63*shrk if np.isfinite(a63) else -999
            s5[tk] = score5
            alpha_info[tk] = {'a63':a63,'r2':r2_63,'shrk':shrk,'raw1m':raw_1m,'s5':score5}
            if np.isfinite(a63): a5_cand_total += 1
            if np.isfinite(a63) and a63>0: a5_alpha_pos_total += 1
        # A4 pick
        for tk,sc in sorted(s4.items(), key=lambda x:-x[1]):
            if tk not in used4 and sc>-999: a4p[tk]=1.0; used4.add(tk); break
        # A5 pick
        for tk,sc in sorted(s5.items(), key=lambda x:-x[1]):
            if tk not in used5 and sc>-999: a5p[tk]=1.0; used5.add(tk); break
    # Normalize
    for d in [a4p, a5p]:
        total=sum(d.values())
        if total>0:
            for k in d: d[k]/=total
    # Portfolio returns
    hold_dates = tk_wide.index[j+1:j_next+1]
    for w_dict, ret_list in [(a4p, a4_daily), (a5p, a5_daily)]:
        if not w_dict:
            ret_list.extend([0.0]*len(hold_dates)); continue
        ws = pd.Series(w_dict)
        dr = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws, axis=1).sum(axis=1)
        ret_list.extend(dr.values.tolist())
    # Detail for attribution
    overlap = set(a4p.keys()) & set(a5p.keys())
    for w_dict, label in [(a4p,'A4'),(a5p,'A5')]:
        for tk, w in w_dict.items():
            tk_r = tk_wide.loc[hold_dates, tk].fillna(0) if tk in tk_wide.columns else pd.Series(0,index=hold_dates)
            period_detail.append({'period':dt.date(),'strat':label,'theme':'','ticker':tk,
                'sector':meta_sec.get(tk,'?'),'mc':meta_mc.get(tk,'?'),'w':w,
                'hold_ret':float((1+tk_r).prod()-1),'contrib':float(tk_r.sum()*w)})
    feas_log.append({'date':dt,'eligible':len(elig),'scored':len(common),'sel':len(sel),
        'a4_n':len(a4p),'a5_n':len(a5p),'a5_cand':a5_cand_total,'a5_alpha_pos':a5_alpha_pos_total,
        'overlap':len(overlap),'overlap_pct':len(overlap)/max(len(a4p),1)})
    print(f'  {dt.date()} A4={len(a4p)} A5={len(a5p)} overlap={len(overlap)} ({len(overlap)/max(len(a4p),1):.0%})')

print(f'\nBacktest done in {time.time()-t0:.1f}s')

# === P1: FEASIBILITY ===
fl = pd.DataFrame(feas_log)
print('\n' + '='*70)
print('P1: FEASIBILITY REPORT (from parquet)')
print('='*70)
print(f'Rebalance periods:          {len(fl)}')
print(f'Avg eligible themes:        {fl["eligible"].mean():.1f}')
print(f'Avg selected themes:        {fl["sel"].mean():.1f}')
print(f'A4 avg names:               {fl["a4_n"].mean():.1f}')
print(f'A5 avg names:               {fl["a5_n"].mean():.1f}')
print(f'A5 zero-pick periods:       {(fl["a5_n"]==0).sum()}')
print(f'A5 avg candidates/period:   {fl["a5_cand"].mean():.1f}')
print(f'A5 avg alpha>0/period:      {fl["a5_alpha_pos"].mean():.1f}')
print(f'Avg overlap A4∩A5:          {fl["overlap_pct"].mean():.0%}')
fallback = (fl['a5_n'] < fl['a4_n']).sum()
print(f'A5 fallback rate:           {fallback}/{len(fl)} = {fallback/len(fl):.0%}')

# === P2: PERFORMANCE ===
def metrics(daily_rets, label):
    r = np.array(daily_rets, dtype=float); r = r[np.isfinite(r)]
    if len(r)<10: return {}
    cum=float(np.prod(1+r)-1); n_yr=len(r)/252
    cagr=(1+cum)**(1/n_yr)-1 if n_yr>0 else np.nan
    vol=float(np.std(r,ddof=1)*np.sqrt(252))
    sharpe=float(np.mean(r)/np.std(r,ddof=1)*np.sqrt(252)) if np.std(r)>0 else np.nan
    down=r[r<0]
    sortino=float(np.mean(r)/np.std(down,ddof=1)*np.sqrt(252)) if len(down)>1 and np.std(down)>0 else np.nan
    wealth=np.cumprod(1+r); peak=np.maximum.accumulate(wealth); dd=wealth/peak-1; mdd=float(dd.min())
    calmar=cagr/abs(mdd) if mdd<0 else np.nan
    return {'strategy':label,'CAGR':f'{cagr:.1%}','Vol':f'{vol:.1%}','Sharpe':f'{sharpe:.2f}',
            'Sortino':f'{sortino:.2f}','MaxDD':f'{mdd:.1%}','Calmar':f'{calmar:.2f}',
            'CumRet':f'{cum:.1%}','WinRate':f'{np.mean(r>0):.1%}','Days':len(r)}

m4 = metrics(a4_daily, 'A4: raw 1M')
m5 = metrics(a5_daily, 'A5-lite: α63×shrink(r²)')
print('\n' + '='*70)
print('P2: A4 vs A5-lite PERFORMANCE (from parquet)')
print('='*70)
comp = pd.DataFrame([m4, m5]).set_index('strategy')
print(comp.to_string())

# Monthly returns
idx4 = tk_wide.index[WARMUP+1:WARMUP+1+len(a4_daily)]
a4_s = pd.Series(a4_daily, index=idx4)
a5_s = pd.Series(a5_daily, index=idx4)
a4_m = (1+a4_s).resample('M').prod()-1
a5_m = (1+a5_s).resample('M').prod()-1
diff_m = a5_m - a4_m
print('\n--- Monthly Returns ---')
monthly = pd.DataFrame({'A4': a4_m, 'A5': a5_m, 'Diff': diff_m})
print(monthly.to_string(float_format='{:.2%}'.format))
print(f'\nMean monthly diff: {diff_m.mean():+.2%}')
print(f'Months A5>A4: {(diff_m>0).sum()}/{len(diff_m)} = {(diff_m>0).mean():.0%}')

# Cost sensitivity
print('\n--- Cost Sensitivity ---')
avg_turn = 1 - fl['overlap_pct'].mean()
n_reb = len(fl)
for bps in [10, 25, 50]:
    cost_total = avg_turn * bps/10000 * 2 * n_reb
    for label, rets in [('A4',a4_daily),('A5',a5_daily)]:
        cum=float(np.prod(1+np.array(rets))-1)
        net=cum-cost_total; n_yr=len(rets)/252
        net_cagr=(1+net)**(1/n_yr)-1 if n_yr>0 else 0
        print(f'  {label} @{bps:2d}bps: gross={cum:.1%} cost={cost_total:.1%} net={net:.1%} CAGR≈{net_cagr:.1%}')

# Best-month dependence
print('\n--- Best-Month Dependence ---')
ds = diff_m.sort_values(ascending=False)
for ex in [0,1,2]:
    d = ds.iloc[ex:] if ex>0 else diff_m
    print(f'  Excl best {ex}: mean={d.mean():+.2%}/mo wins={int((d>0).sum())}/{len(d)}')

# Sector attribution
print('\n--- Sector Attribution (A5 - A4) ---')
det = pd.DataFrame(period_detail)
a4_sec = det[det['strat']=='A4'].groupby('sector')['contrib'].sum()
a5_sec = det[det['strat']=='A5'].groupby('sector')['contrib'].sum()
all_sec = sorted(set(a4_sec.index)|set(a5_sec.index))
sec_diff = pd.Series({s: a5_sec.get(s,0)-a4_sec.get(s,0) for s in all_sec}).sort_values(ascending=False)
for s,v in sec_diff.items():
    print(f'  {s:30s} {v:+.2%}')

# Top contributor concentration
a5_tk = det[det['strat']=='A5'].groupby('ticker')['contrib'].sum().sort_values(ascending=False)
a4_tk = det[det['strat']=='A4'].groupby('ticker')['contrib'].sum().sort_values(ascending=False)
a5_tot=a5_tk.sum(); a4_tot=a4_tk.sum()
print(f'\n--- Concentration ---')
print(f'  A4 top3: {a4_tk.head(3).sum()/a4_tot:.0%} of total  ({", ".join(a4_tk.head(3).index)})')
print(f'  A5 top3: {a5_tk.head(3).sum()/a5_tot:.0%} of total  ({", ".join(a5_tk.head(3).index)})')

print(f'\n{"="*70}')
print(f'VERDICT')
print(f'{"="*70}')
feas_ok = fl['a5_n'].mean()>=7 and (fl['a5_n']==0).sum()==0
print(f'  P1 Feasibility:  {"PASS ✓" if feas_ok else "FAIL ✗"}')
print(f'  P2 Direction:    {"POSITIVE ✓" if diff_m.mean()>0 else "NEGATIVE ✗"} ({diff_m.mean():+.2%}/mo)')
print(f'  P2 Sharpe:       A4={float(m4["Sharpe"]):.2f} → A5={float(m5["Sharpe"]):.2f}')
print(f'  P2 MaxDD:        A4={m4["MaxDD"]} → A5={m5["MaxDD"]}')
print(f'  Total runtime:   {time.time()-t0:.1f}s')
