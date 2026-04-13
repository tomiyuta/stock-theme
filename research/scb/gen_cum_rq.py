"""Generate cumulative_returns for PRISM-RQ (BFM-v2 + SNRb) from BT daily returns."""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

# Re-run BFM-v2 backtest to capture daily returns
panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel_v2.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_us_metadata.parquet')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
psec = panel[['theme','ticker']].drop_duplicates().merge(meta[['ticker','gics_sector']].rename(columns={'gics_sector':'sector'}), on='ticker', how='left')
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')

# Load SPY for benchmark
import yfinance as yf
spy = yf.download('SPY', start='2020-01-01', end='2026-12-31', progress=False)
spy_ret = spy['Close'].pct_change().dropna()
spy_ret.index = pd.to_datetime(spy_ret.index).tz_localize(None)

def ols_full(y, x):
    mask = np.isfinite(y)&np.isfinite(x); y,x = y[mask],x[mask]; n=len(y)
    if n<20: return np.nan,np.nan,np.nan,np.nan
    xm,ym=x.mean(),y.mean(); vx=np.var(x,ddof=1)
    if vx<1e-15: return np.nan,np.nan,np.nan,np.nan
    b=np.dot(x-xm,y-ym)/(n-1)/vx; a=ym-b*xm; resid=y-a-b*x
    ss_res=float(np.sum(resid**2)); ss_tot=float(np.sum((y-ym)**2))
    r2=1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n,b,r2,float(np.std(resid,ddof=1)*np.sqrt(n))
def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0
def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=10; SEC_MAX=3; CAND_N=25
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
daily_ret_rq=[]; daily_dates=[]
for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]; dt=dates_all[j]
    dt63=set(dates_all[max(0,j-62):j+1])
    sub=panel[panel['date'].isin(dt63)]
    tm=sub.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    tm_mom={}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        tm_mom[th]=cumret(td.values)
    ms=pd.Series(tm_mom).dropna().sort_values(ascending=False)
    dc={}
    for th in ms.index:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)<63: continue
        r021=cumret(td[-21:]); r2142=cumret(td[-42:-21]); r4263=cumret(td[-63:-42])
        if all(np.isfinite([r021,r2142,r4263])): dc[th]=-(r021-0.5*(r2142+r4263))
    dcs=pd.Series(dc); common=list(set(ms.index)&set(dcs.index))
    hold_dates=tk_wide.index[j+1:j_next+1]
    if not common:
        daily_ret_rq.extend([0.0]*len(hold_dates)); daily_dates.extend(hold_dates); continue
    ts=pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['r_mom']=ts['mom63'].rank(pct=True); ts['r_dec']=ts['decel'].rank(pct=True)
    ts['score']=0.70*ts['r_mom']+0.30*ts['r_dec']
    ts=ts.sort_values('score',ascending=False)
    # BFM-v2: top25 → veto → top10
    candidates=list(ts.index[:CAND_N])
    cand_feat={}
    for th in candidates:
        ths=sub[sub['theme']==th]; tks=ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        tk_r63={}
        for tk in tks:
            tkd=ths[ths['ticker']==tk].sort_values('date')
            if len(tkd)>=20: tk_r63[tk]=cumret(tkd['ret'].values[-63:])
        if len(tk_r63)<MIN_M: continue
        breadth63=sum(1 for v in tk_r63.values() if np.isfinite(v) and v>0)/len(tk_r63)
        abs_c=np.array([abs(v) for v in tk_r63.values() if np.isfinite(v)])
        tot=abs_c.sum(); conc=float(np.sum((abs_c/tot)**2)) if tot>1e-10 else 1.0
        td_v=ths.groupby('date')['theme_ret'].first().sort_index().values
        tvol=float(np.std(td_v[-63:],ddof=1)*np.sqrt(252)) if len(td_v)>=63 else np.nan
        if np.isfinite(breadth63) and np.isfinite(conc) and np.isfinite(tvol):
            cand_feat[th]={'b':breadth63,'c':conc,'v':tvol}
    vetoed=set()
    if len(cand_feat)>=5:
        cf=pd.DataFrame(cand_feat).T
        bt=cf['b'].quantile(0.30); ct=cf['c'].quantile(0.80); vt=cf['v'].quantile(0.80)
        for th in cf.index:
            if cf.loc[th,'b']<bt: vetoed.add(th)
            if cf.loc[th,'c']>ct: vetoed.add(th)
            if cf.loc[th,'v']>vt: vetoed.add(th)
    survivors=[th for th in ts.index if th in candidates and th not in vetoed]
    sel=[]; sc_cnt={}
    for th in survivors:
        s2=theme_sector.get(th,'Unk')
        if sc_cnt.get(s2,0)>=SEC_MAX: continue
        sel.append(th); sc_cnt[s2]=sc_cnt.get(s2,0)+1
        if len(sel)>=TOP_T: break
    # SNRb stock selection
    port={}; used=set()
    for th in sel:
        ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        scores={}
        for tk in tks:
            tkd=ths[ths['ticker']==tk].sort_values('date')
            a63,b63,r2_63,rvol=ols_full(tkd['ret'].values,tkd['theme_ex_self'].values)
            if np.isfinite(a63) and np.isfinite(rvol) and rvol>1e-8:
                scores[tk]=(a63/rvol)*(shrink_r2(r2_63) if np.isfinite(r2_63) else 0)
            else: scores[tk]=-999
        for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
            if tk not in used and sc>-999: port[tk]=1.0; used.add(tk); break
    total=sum(port.values())
    if total>0:
        for k in port: port[k]/=total
    if not port:
        daily_ret_rq.extend([0.0]*len(hold_dates)); daily_dates.extend(hold_dates); continue
    ws=pd.Series(port)
    dr=tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1).sum(axis=1)
    daily_ret_rq.extend(dr.values.tolist()); daily_dates.extend(hold_dates)

# === Convert to monthly cumulative returns ===
df = pd.DataFrame({'date': daily_dates, 'rq': daily_ret_rq})
df['date'] = pd.to_datetime(df['date'])
df = df.set_index('date').sort_index()
# Monthly returns
monthly_rq = df['rq'].resample('M').apply(lambda x: float(np.expm1(np.log1p(x).sum())))
# SPY monthly
spy_monthly = spy_ret.reindex(df.index).fillna(0).resample('M').apply(lambda x: float(np.expm1(np.log1p(x).sum())))
# Align
common_dates = sorted(set(monthly_rq.index) & set(spy_monthly.index))
monthly_rq = monthly_rq.loc[common_dates]
spy_monthly = spy_monthly.loc[common_dates]
# Cumulative growth
cum_rq = np.cumprod(1 + monthly_rq.values)
cum_spy = np.cumprod(1 + spy_monthly.values)
dates_str = [d.strftime('%Y-%m-%d') for d in common_dates]
# Annual returns
annual_rq = {}; annual_spy = {}
for d, r_rq, r_spy in zip(common_dates, monthly_rq.values, spy_monthly.values):
    y = str(d.year)
    annual_rq[y] = annual_rq.get(y, 1.0) * (1 + r_rq)
    annual_spy[y] = annual_spy.get(y, 1.0) * (1 + r_spy)
for y in annual_rq: annual_rq[y] = round(float(annual_rq[y]) - 1, 4)
for y in annual_spy: annual_spy[y] = round(float(annual_spy[y]) - 1, 4)

# Stats
arr = np.array(daily_ret_rq); arr = arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
cagr_rq = (1+float(np.expm1(np.log1p(arr).sum())))**(1/yrs)-1
vol_rq = float(np.std(arr,ddof=1)*np.sqrt(252))
sharpe_rq = cagr_rq/vol_rq if vol_rq>1e-8 else 0
eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
maxdd_rq = float(((eq-peak)/peak).min())

spy_arr = spy_ret.reindex(df.index).fillna(0).values; spy_arr=spy_arr[np.isfinite(spy_arr)]
n_s=len(spy_arr); yrs_s=n_s/252
cagr_spy=(1+float(np.expm1(np.log1p(spy_arr).sum())))**(1/yrs_s)-1
vol_spy=float(np.std(spy_arr,ddof=1)*np.sqrt(252))
sharpe_spy=cagr_spy/vol_spy if vol_spy>1e-8 else 0
eq_s=np.cumprod(1+spy_arr); pk_s=np.maximum.accumulate(eq_s)
maxdd_spy=float(((eq_s-pk_s)/pk_s).min())

output = {
    'dates': dates_str,
    'a5': [round(float(v),4) for v in cum_rq],
    'ret_a5': [round(float(v),4) for v in monthly_rq.values],
    'SPY': [round(float(v),4) for v in cum_spy],
    'ret_SPY': [round(float(v),4) for v in spy_monthly.values],
    'annual': {'a5': annual_rq, 'SPY': annual_spy},
    'stats': {
        'a5': {'cagr':round(cagr_rq,4),'sharpe':round(sharpe_rq,4),'maxdd':round(maxdd_rq,4),'n_months':len(dates_str)},
        'SPY': {'cagr':round(cagr_spy,4),'sharpe':round(sharpe_spy,4),'maxdd':round(maxdd_spy,4),'n_months':len(dates_str)},
    },
    'meta': {'strategy':'PRISM-RQ (BFM-v2 + A5-SNRb)','source':'Norgate BT','pit_warning':True},
    'forward_overlay': {'dates':[],'a5':[],'SPY':[]}
}
out_path = '/Users/yutatomi/Downloads/stock-theme/public/api/prism-rq/cumulative_returns.json'
import os; os.makedirs(os.path.dirname(out_path), exist_ok=True)
# Preserve existing forward_overlay
if os.path.exists(out_path):
    try:
        existing=json.load(open(out_path))
        efwd=existing.get('forward_overlay',{})
        if efwd.get('dates'):
            output['forward_overlay']=efwd
            print(f'Preserved forward_overlay: {len(efwd["dates"])} entries')
    except: pass
with open(out_path, 'w') as f: json.dump(output, f, indent=2)
print(f'Done in {time.time()-t0:.1f}s')
print(f'PRISM-RQ: CAGR={cagr_rq:.1%} Sharpe={sharpe_rq:.3f} MaxDD={maxdd_rq:.1%}')
print(f'SPY:      CAGR={cagr_spy:.1%} Sharpe={sharpe_spy:.3f} MaxDD={maxdd_spy:.1%}')
print(f'Annual RQ: {annual_rq}')
print(f'Annual SPY: {annual_spy}')
