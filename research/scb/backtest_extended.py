"""Extended backtest: A4 vs A5-lite on Norgate 5-year panel (72 rebalances)."""
import pandas as pd, numpy as np, time, warnings
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
dates_all = sorted(panel['date'].unique())
tk_ret = panel[['date','ticker','ret']].drop_duplicates(['date','ticker']).dropna(subset=['ret'])
tk_wide = tk_ret.pivot(index='date', columns='ticker', values='ret').sort_index()
meta_sec = meta.set_index('ticker')['sector'].to_dict()
psec = panel[['theme','ticker']].drop_duplicates()
psec['sector'] = psec['ticker'].map(meta_sec)
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')
print(f'Prep done in {time.time()-t0:.1f}s')

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

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1] != len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

feas_log = []
a4_daily = []; a5_daily = []
detail_log = []

for pos in range(len(rebal_idx)-1):
    j = rebal_idx[pos]; j_next = rebal_idx[pos+1]
    dt = dates_all[j]
    dt63 = set(dates_all[max(0,j-62):j+1])
    dt21 = set(dates_all[max(0,j-20):j+1])
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
    dcs = pd.Series(dc); common = list(set(ms.index)&set(dcs.index))
    if not common:
        hold_dates = tk_wide.index[j+1:j_next+1]
        a4_daily.extend([0.0]*len(hold_dates)); a5_daily.extend([0.0]*len(hold_dates))
        feas_log.append({'date':dt,'elig':len(elig),'sel':0,'a4_n':0,'a5_n':0,'overlap':0})
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
    a4p={}; a5p={}; used4=set(); used5=set()
    for th in sel:
        ths = sub[(sub['theme']==th)&sub['ret'].notna()]
        tks = ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        s4={}; s5={}
        for tk in tks:
            tkd = ths[ths['ticker']==tk].sort_values('date')
            r21d = tkd[tkd['date'].isin(dt21)]
            raw_1m = cumret(r21d['ret'].values) if len(r21d)>=10 else np.nan
            s4[tk] = raw_1m if np.isfinite(raw_1m) else -999
            a63,b63,r2_63 = ols_ab(tkd['ret'].values, tkd['theme_ex_self'].values)
            shrk = shrink_r2(r2_63) if np.isfinite(r2_63) else 0
            s5[tk] = a63*shrk if np.isfinite(a63) else -999
        for tk,sc in sorted(s4.items(), key=lambda x:-x[1]):
            if tk not in used4 and sc>-999: a4p[tk]=1.0; used4.add(tk); break
        for tk,sc in sorted(s5.items(), key=lambda x:-x[1]):
            if tk not in used5 and sc>-999: a5p[tk]=1.0; used5.add(tk); break
    for d in [a4p, a5p]:
        total=sum(d.values())
        if total>0:
            for k in d: d[k]/=total
    hold_dates = tk_wide.index[j+1:j_next+1]
    for w_dict, ret_list in [(a4p, a4_daily), (a5p, a5_daily)]:
        if not w_dict:
            ret_list.extend([0.0]*len(hold_dates)); continue
        ws = pd.Series(w_dict)
        dr = tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws, axis=1).sum(axis=1)
        ret_list.extend(dr.values.tolist())
    overlap = set(a4p.keys()) & set(a5p.keys())
    feas_log.append({'date':dt,'elig':len(elig),'sel':len(sel),'a4_n':len(a4p),
        'a5_n':len(a5p),'overlap':len(overlap),'overlap_pct':len(overlap)/max(len(a4p),1)})
    # Detail for attribution
    for w_dict, label in [(a4p,'A4'),(a5p,'A5')]:
        for tk, w in w_dict.items():
            tk_r = tk_wide.loc[hold_dates, tk].fillna(0) if tk in tk_wide.columns else pd.Series(0,index=hold_dates)
            detail_log.append({'period':dt.date(),'strat':label,'ticker':tk,
                'sector':meta_sec.get(tk,'?'),'w':w,'hold_ret':float((1+tk_r).prod()-1),
                'contrib':float(tk_r.sum()*w)})
    if (pos+1) % 10 == 0:
        print(f'  [{pos+1}/{len(rebal_idx)-1}] {dt.date()} A4={len(a4p)} A5={len(a5p)} ov={len(overlap)}')

print(f'\nBacktest done in {time.time()-t0:.1f}s')

# ============ RESULTS ============
fl = pd.DataFrame(feas_log)
def calc_metrics(rets, label):
    r = np.array(rets, dtype=float); r = r[np.isfinite(r)]
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

print('\n' + '='*70)
print('P1: FEASIBILITY (Extended)')
print('='*70)
print(f'Periods:         {len(fl)}')
print(f'Avg eligible:    {fl["elig"].mean():.0f}')
print(f'Avg selected:    {fl["sel"].mean():.1f}')
print(f'A4 avg names:    {fl["a4_n"].mean():.1f}')
print(f'A5 avg names:    {fl["a5_n"].mean():.1f}')
print(f'Zero-pick (A5):  {(fl["a5_n"]==0).sum()}')
print(f'Avg overlap:     {fl["overlap_pct"].mean():.0%}')
fb = (fl['a5_n'] < fl['a4_n']).sum()
print(f'Fallback rate:   {fb}/{len(fl)} = {fb/len(fl):.0%}')

m4 = calc_metrics(a4_daily, 'A4: raw 1M')
m5 = calc_metrics(a5_daily, 'A5-lite: α63×shrink(r²)')
print('\n' + '='*70)
print('P2: PERFORMANCE (Extended)')
print('='*70)
print(pd.DataFrame([m4, m5]).set_index('strategy').to_string())

# Monthly diff
idx = tk_wide.index[WARMUP+1:WARMUP+1+len(a4_daily)]
a4_s = pd.Series(a4_daily, index=idx)
a5_s = pd.Series(a5_daily, index=idx)
a4_m = (1+a4_s).resample('ME').prod()-1
a5_m = (1+a5_s).resample('ME').prod()-1
diff_m = a5_m - a4_m

print(f'\n--- Monthly Diff Summary ---')
print(f'Total months:        {len(diff_m)}')
print(f'A5 > A4:             {(diff_m>0).sum()}/{len(diff_m)} = {(diff_m>0).mean():.0%}')
print(f'Mean diff:           {diff_m.mean():+.2%}/mo')
print(f'Median diff:         {diff_m.median():+.2%}/mo')
print(f'Std diff:            {diff_m.std():.2%}')

# Sign test
n_pos = int((diff_m>0).sum()); n_total = len(diff_m)
try:
    from scipy.stats import binomtest
    p_val = binomtest(n_pos, n_total, 0.5, alternative='greater').pvalue
except:
    from math import comb
    p_val = sum(comb(n_total,k)*0.5**n_total for k in range(n_pos, n_total+1))
print(f'Sign test (1-sided): p={p_val:.4f}')

# Yearly breakdown
print(f'\n--- Yearly Breakdown ---')
a4_y = (1+a4_s).resample('YE').prod()-1
a5_y = (1+a5_s).resample('YE').prod()-1
diff_y = a5_y - a4_y
yearly = pd.DataFrame({'A4':a4_y,'A5':a5_y,'Diff':diff_y})
print(yearly.to_string(float_format='{:.1%}'.format))

# First half vs second half
mid = len(diff_m)//2
h1 = diff_m.iloc[:mid]; h2 = diff_m.iloc[mid:]
print(f'\n--- Half-split ---')
print(f'First half:  mean={h1.mean():+.2%} wins={int((h1>0).sum())}/{len(h1)}')
print(f'Second half: mean={h2.mean():+.2%} wins={int((h2>0).sum())}/{len(h2)}')

# Cost sensitivity
print(f'\n--- Cost Sensitivity ---')
avg_turn = 1 - fl['overlap_pct'].mean()
n_reb = len(fl)
for bps in [10, 25, 50]:
    cost_total = avg_turn * bps/10000 * 2 * n_reb
    for label, rets in [('A4',a4_daily),('A5',a5_daily)]:
        cum=float(np.prod(1+np.array(rets))-1)
        net=cum-cost_total; n_yr=len(rets)/252
        net_cagr=(1+net)**(1/n_yr)-1 if n_yr>0 else 0
        print(f'  {label} @{bps:2d}bps: gross={cum:.1%} net_cagr={net_cagr:.1%}')

# Sector attribution
det = pd.DataFrame(detail_log)
a4_sec = det[det['strat']=='A4'].groupby('sector')['contrib'].sum()
a5_sec = det[det['strat']=='A5'].groupby('sector')['contrib'].sum()
all_sec = sorted(set(a4_sec.index)|set(a5_sec.index))
sec_diff = pd.Series({s: a5_sec.get(s,0)-a4_sec.get(s,0) for s in all_sec}).sort_values(ascending=False)
print(f'\n--- Sector Attribution (A5-A4) ---')
for s,v in sec_diff.items():
    print(f'  {s:30s} {v:+.2%}')

# Concentration
a5_tk = det[det['strat']=='A5'].groupby('ticker')['contrib'].sum().sort_values(ascending=False)
a4_tk = det[det['strat']=='A4'].groupby('ticker')['contrib'].sum().sort_values(ascending=False)
a5_tot=a5_tk.sum(); a4_tot=a4_tk.sum()
print(f'\n--- Concentration ---')
print(f'  A4 top5: {a4_tk.head(5).sum()/a4_tot:.0%} ({", ".join(a4_tk.head(5).index)})')
print(f'  A5 top5: {a5_tk.head(5).sum()/a5_tot:.0%} ({", ".join(a5_tk.head(5).index)})')

# Best/worst month dependence
ds = diff_m.sort_values(ascending=False)
print(f'\n--- Best-Month Dependence ---')
for ex in [0,1,2,3]:
    d = ds.iloc[ex:] if ex>0 else diff_m
    print(f'  Excl best {ex}: mean={d.mean():+.2%} wins={int((d>0).sum())}/{len(d)}')

# Drawdown comparison
w4 = np.cumprod(1+np.array(a4_daily)); p4 = np.maximum.accumulate(w4); dd4 = w4/p4-1
w5 = np.cumprod(1+np.array(a5_daily)); p5 = np.maximum.accumulate(w5); dd5 = w5/p5-1
print(f'\n--- Drawdown Comparison ---')
print(f'  A4 MaxDD: {dd4.min():.1%}  A5 MaxDD: {dd5.min():.1%}')
print(f'  A4 avg DD: {dd4.mean():.1%}  A5 avg DD: {dd5.mean():.1%}')

# Non-Tech diff
non_tech_det = det[det['sector']!='Technology']
a4_nt = non_tech_det[non_tech_det['strat']=='A4'].groupby('period')['contrib'].sum()
a5_nt = non_tech_det[non_tech_det['strat']=='A5'].groupby('period')['contrib'].sum()
nt_diff = a5_nt - a4_nt
print(f'\n--- Non-Tech Only ---')
print(f'  Mean period diff: {nt_diff.mean():+.2%}')
print(f'  Positive: {(nt_diff>0).sum()}/{len(nt_diff)} = {(nt_diff>0).mean():.0%}')

print(f'\n{"="*70}')
print(f'VERDICT (Extended: {len(fl)} rebalances)')
print(f'{"="*70}')
feas_ok = fl['a5_n'].mean()>=7 and (fl['a5_n']==0).sum()==0
print(f'  P1 Feasibility:       {"PASS" if feas_ok else "FAIL"}')
print(f'  P2 Direction:          {"POSITIVE" if diff_m.mean()>0 else "NEGATIVE"} ({diff_m.mean():+.2%}/mo)')
print(f'  P2 Sign test:          p={p_val:.4f} {"***" if p_val<0.01 else "**" if p_val<0.05 else "*" if p_val<0.10 else "ns"}')
print(f'  P2 Half-split:         H1={h1.mean():+.2%} H2={h2.mean():+.2%} {"BOTH+" if h1.mean()>0 and h2.mean()>0 else "MIXED"}')
print(f'  Non-Tech diff:         {nt_diff.mean():+.2%} ({(nt_diff>0).mean():.0%} positive)')
print(f'  Runtime:               {time.time()-t0:.1f}s')
