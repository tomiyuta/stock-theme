"""Generate cumulative_returns.json for G2-MAX from BT daily returns."""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum', n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret'] / panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1, (panel['sum_ret']-panel['ret'])/(panel['n_day']-1), np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')
import yfinance as yf
spy = yf.download('SPY', start='2020-01-01', end='2027-01-01', progress=False)
spy_ret = spy['Close'].pct_change().dropna()
spy_ret.index = pd.to_datetime(spy_ret.index).tz_localize(None)

def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
def ols_alpha(y, x):
    mask=np.isfinite(y)&np.isfinite(x); y,x=y[mask],x[mask]; n=len(y)
    if n<20: return np.nan
    xm,ym=x.mean(),y.mean(); vx=np.var(x,ddof=1)
    if vx<1e-15: return np.nan
    b=np.dot(x-xm,y-ym)/(n-1)/vx; a=ym-b*xm; return a*n

WARMUP=126; REBAL=20; MIN_M=4; TOP_T=6; MAX_CORR=0.80
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
daily_ret_g2=[]; daily_dates=[]

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]; dt=dates_all[j]
    dt63=set(dates_all[max(0,j-62):j+1]); dt126=set(dates_all[max(0,j-125):j+1])
    sub=panel[panel['date'].isin(dt63)]; sub126=panel[panel['date'].isin(dt126)]
    tm=sub.groupby('theme')['ticker'].nunique(); elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    tm_mom63,tm_mom126,tm_mom21={},{},{}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=21: tm_mom21[th]=cumret(td[-21:])
        if len(td)>=63: tm_mom63[th]=cumret(td)
        td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td126v)>=63: tm_mom126[th]=cumret(td126v)
    tdf=pd.DataFrame({'m63':pd.Series(tm_mom63),'m126':pd.Series(tm_mom126),'m21':pd.Series(tm_mom21)}).dropna(subset=['m63'])
    if tdf.empty: daily_ret_g2.extend([0.0]*len(hold_dates)); daily_dates.extend(hold_dates); continue
    tdf['score']=0.50*tdf['m63'].rank(pct=True)+0.30*tdf['m126'].rank(pct=True,na_option='bottom')+0.20*tdf['m21'].rank(pct=True,na_option='bottom')
    tdf=tdf.sort_values('score',ascending=False)
    # Cluster-distinct selection
    tdr={}
    for th in tdf.index[:20]:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        if len(td)>=20: tdr[th]=td
    cm=pd.DataFrame(tdr).dropna().corr() if len(tdr)>=2 else pd.DataFrame()
    sel=[]
    for th in tdf.index:
        if th not in cm.index: continue
        ok=True
        for s in sel:
            if s in cm.columns and abs(cm.loc[th,s])>MAX_CORR: ok=False; break
        if ok: sel.append(th)
        if len(sel)>=TOP_T: break
    # Stock selection: raw α63
    port={}; used=set()
    for th in sel:
        ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        scores={}
        for tk in tks:
            tkd=ths[ths['ticker']==tk].sort_values('date')
            a63=ols_alpha(tkd['ret'].values, tkd['theme_ex_self'].values)
            scores[tk]=a63 if np.isfinite(a63) else -999
        for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
            if tk not in used and sc>-999: port[tk]=1.0; used.add(tk); break
    total=sum(port.values())
    if total>0:
        for k in port: port[k]/=total
    if not port:
        daily_ret_g2.extend([0.0]*len(hold_dates)); daily_dates.extend(hold_dates); continue
    ws=pd.Series(port)
    dr=tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1).sum(axis=1)
    daily_ret_g2.extend(dr.values.tolist()); daily_dates.extend(hold_dates)

# Monthly cumulative
df=pd.DataFrame({'date':daily_dates,'g2':daily_ret_g2}); df['date']=pd.to_datetime(df['date']); df=df.set_index('date').sort_index()
monthly_g2=df['g2'].resample('M').apply(lambda x:float(np.expm1(np.log1p(x).sum())))
spy_m=spy_ret.reindex(df.index).fillna(0).resample('M').apply(lambda x:float(np.expm1(np.log1p(x).sum())))
common=sorted(set(monthly_g2.index)&set(spy_m.index))
# Cut at 2026-03-31 (same as other pages)
common=[d for d in common if d<=pd.Timestamp('2026-03-31')]
mg2=monthly_g2.loc[common]; ms=spy_m.loc[common]
cum_g2=np.cumprod(1+mg2.values); cum_spy=np.cumprod(1+ms.values)
dates_str=[d.strftime('%Y-%m-%d') for d in common]
ann_g2,ann_spy={},{}
for d,rg,rs in zip(common,mg2.values,ms.values):
    y=str(d.year); ann_g2[y]=ann_g2.get(y,1.0)*(1+rg); ann_spy[y]=ann_spy.get(y,1.0)*(1+rs)
for y in ann_g2: ann_g2[y]=round(float(ann_g2[y])-1,4)
for y in ann_spy: ann_spy[y]=round(float(ann_spy[y])-1,4)
# Stats
arr=np.array(daily_ret_g2); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
cagr=(1+float(np.expm1(np.log1p(arr).sum())))**(1/yrs)-1
vol=float(np.std(arr,ddof=1)*np.sqrt(252)); sharpe=cagr/vol if vol>1e-8 else 0
eq=np.cumprod(1+arr); pk=np.maximum.accumulate(eq); maxdd=float(((eq-pk)/pk).min())
sa=spy_ret.reindex(df.index).fillna(0).values; sa=sa[np.isfinite(sa)]
ns=len(sa); ys=ns/252; cagr_s=(1+float(np.expm1(np.log1p(sa).sum())))**(1/ys)-1
vol_s=float(np.std(sa,ddof=1)*np.sqrt(252)); sh_s=cagr_s/vol_s if vol_s>1e-8 else 0
eq_s=np.cumprod(1+sa); pk_s=np.maximum.accumulate(eq_s); maxdd_s=float(((eq_s-pk_s)/pk_s).min())

output={
    'dates':dates_str,
    'a5':[round(float(v),4) for v in cum_g2],
    'ret_a5':[round(float(v),4) for v in mg2.values],
    'SPY':[round(float(v),4) for v in cum_spy],
    'ret_SPY':[round(float(v),4) for v in ms.values],
    'annual':{'a5':ann_g2,'SPY':ann_spy},
    'stats':{
        'a5':{'cagr':round(cagr,4),'sharpe':round(sharpe,4),'maxdd':round(maxdd,4),'n_months':len(dates_str)},
        'SPY':{'cagr':round(cagr_s,4),'sharpe':round(sh_s,4),'maxdd':round(maxdd_s,4),'n_months':len(dates_str)},
    },
    'meta':{'strategy':'G2-MAX (6-theme concentrated raw α)','source':'Norgate BT','pit_warning':True},
    'forward_overlay':{'dates':[],'a5':[],'SPY':[]}
}
import os; os.makedirs('/Users/yutatomi/Downloads/stock-theme/public/api/prism-g2', exist_ok=True)
with open('/Users/yutatomi/Downloads/stock-theme/public/api/prism-g2/cumulative_returns.json','w') as f:
    json.dump(output,f,indent=2)
print(f'Done in {time.time()-t0:.1f}s | G2-MAX: CAGR={cagr:.1%} Sharpe={sharpe:.3f} MaxDD={maxdd:.1%}')
print(f'Annual G2: {ann_g2}')
