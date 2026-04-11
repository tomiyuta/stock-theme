"""Theme concentration ablation: 10 vs 5 themes for PRISM/PRISM-R/PRISM-RQ"""
import pandas as pd, numpy as np, time, warnings, json
warnings.filterwarnings('ignore')
t0 = time.time()

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
    mask = np.isfinite(y)&np.isfinite(x); y,x = y[mask],x[mask]; n=len(y)
    if n<20: return np.nan, np.nan, np.nan, np.nan
    xm,ym = x.mean(),y.mean(); vx = np.var(x,ddof=1)
    if vx<1e-15: return np.nan, np.nan, np.nan, np.nan
    b = np.dot(x-xm,y-ym)/(n-1)/vx; a = ym-b*xm; resid = y-a-b*x
    ss_res=float(np.sum(resid**2)); ss_tot=float(np.sum((y-ym)**2))
    r2 = 1-ss_res/ss_tot if ss_tot>1e-12 else np.nan
    return a*n, b, r2, float(np.std(resid,ddof=1)*np.sqrt(n))
def shrink_r2(r2v):
    if np.isnan(r2v) or r2v<0: return 0.0
    if r2v<0.10: return r2v*2
    if r2v<=0.50: return 0.20+(r2v-0.10)*2.0
    return 1.0
def cumret(arr):
    a=np.asarray(arr,dtype=float); a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan

def corr_budget_select(ranked_themes, sub, max_n, max_corr=0.80):
    tdr = {}
    for th in ranked_themes:
        td = sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index()
        if len(td)>=20: tdr[th]=td
    if len(tdr)<2: return ranked_themes[:max_n]
    cdf = pd.DataFrame(tdr).dropna().corr()
    sel = []
    for th in ranked_themes:
        if th not in cdf.index: continue
        ok = True
        for s in sel:
            if s in cdf.columns and abs(cdf.loc[th,s])>max_corr: ok=False; break
        if ok: sel.append(th)
        if len(sel)>=max_n: break
    return sel

WARMUP=126; REBAL=20; MIN_M=4; SEC_MAX=3; CAND_N=25
rebal_idx = list(range(WARMUP, len(dates_all), REBAL))
if rebal_idx[-1]!=len(dates_all)-1: rebal_idx.append(len(dates_all)-1)
print(f'Rebalance periods: {len(rebal_idx)-1}')

# 6 strategies: 3 Layer2 types × 2 theme counts
STRATS = {
    'A4_10':  {'n':10,'l2':'raw1m','bfm':False,'corr':False},
    'A4_5':   {'n':5, 'l2':'raw1m','bfm':False,'corr':True},
    'A5_10':  {'n':10,'l2':'a5lite','bfm':False,'corr':False},
    'A5_5':   {'n':5, 'l2':'a5lite','bfm':False,'corr':True},
    'RQ_10':  {'n':10,'l2':'snrb','bfm':True,'corr':False},
    'RQ_5':   {'n':5, 'l2':'snrb','bfm':True,'corr':True},
}
daily_ret = {s:[] for s in STRATS}

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos]; j_next=rebal_idx[pos+1]; dt=dates_all[j]
    dt63=set(dates_all[max(0,j-62):j+1])
    sub=panel[panel['date'].isin(dt63)]
    tm=sub.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme scoring (current PRISM L1)
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
    if not common:
        for s in STRATS: daily_ret[s].extend([0.0]*len(hold_dates))
        continue
    ts=pd.DataFrame({'mom63':ms[common],'decel':dcs[common]})
    ts['score']=0.70*ts['mom63'].rank(pct=True)+0.30*ts['decel'].rank(pct=True)
    ts=ts.sort_values('score',ascending=False)
    # BFM-v2 quality features for RQ variants
    cand_feat={}
    for th in list(ts.index[:CAND_N]):
        ths=sub[sub['theme']==th]; tks=ths['ticker'].unique()
        if len(tks)<MIN_M: continue
        tk_r63={}
        for tk in tks:
            tkd=ths[ths['ticker']==tk].sort_values('date')
            if len(tkd)>=20: tk_r63[tk]=cumret(tkd['ret'].values[-63:])
        if len(tk_r63)<MIN_M: continue
        b63=sum(1 for v in tk_r63.values() if np.isfinite(v) and v>0)/len(tk_r63)
        ac=np.array([abs(v) for v in tk_r63.values() if np.isfinite(v)])
        tot=ac.sum(); c63=float(np.sum((ac/tot)**2)) if tot>1e-10 else 1.0
        tdv=ths.groupby('date')['theme_ret'].first().sort_index().values
        tv=float(np.std(tdv[-63:],ddof=1)*np.sqrt(252)) if len(tdv)>=63 else np.nan
        if np.isfinite(b63) and np.isfinite(c63) and np.isfinite(tv):
            cand_feat[th]={'b':b63,'c':c63,'v':tv}
    # BFM-v2 veto set
    bfm_vetoed=set()
    if len(cand_feat)>=5:
        cf=pd.DataFrame(cand_feat).T
        bt=cf['b'].quantile(0.30); ct=cf['c'].quantile(0.80); vt=cf['v'].quantile(0.80)
        for th in cf.index:
            if cf.loc[th,'b']<bt or cf.loc[th,'c']>ct or cf.loc[th,'v']>vt: bfm_vetoed.add(th)
    for sname, cfg in STRATS.items():
        n_th=cfg['n']
        # Theme selection
        if cfg['bfm']:
            # BFM-v2: top25 → veto → top n
            survivors=[th for th in ts.index[:CAND_N] if th not in bfm_vetoed]
        else:
            survivors=list(ts.index)
        if cfg['corr']:
            sel=corr_budget_select(survivors, sub, n_th)
        else:
            sel=[]; sc_cnt={}
            for th in survivors:
                s2=theme_sector.get(th,'Unk')
                if sc_cnt.get(s2,0)>=SEC_MAX: continue
                sel.append(th); sc_cnt[s2]=sc_cnt.get(s2,0)+1
                if len(sel)>=n_th: break
        # Stock selection
        port={}; used=set()
        for th in sel:
            ths=sub[(sub['theme']==th)&sub['ret'].notna()]; tks=ths['ticker'].unique()
            if len(tks)<MIN_M: continue
            scores={}
            for tk in tks:
                tkd=ths[ths['ticker']==tk].sort_values('date')
                if cfg['l2']=='raw1m':
                    scores[tk]=cumret(tkd['ret'].values[-21:]) if len(tkd)>=21 else -999
                elif cfg['l2']=='a5lite':
                    a63,b63,r2,rvol=ols_full(tkd['ret'].values,tkd['theme_ex_self'].values)
                    shrk=shrink_r2(r2) if np.isfinite(r2) else 0
                    scores[tk]=a63*shrk if np.isfinite(a63) else -999
                elif cfg['l2']=='snrb':
                    a63,b63,r2,rvol=ols_full(tkd['ret'].values,tkd['theme_ex_self'].values)
                    shrk=shrink_r2(r2) if np.isfinite(r2) else 0
                    scores[tk]=(a63/rvol)*shrk if np.isfinite(a63) and np.isfinite(rvol) and rvol>1e-8 else -999
            for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
                if tk not in used and sc>-999: port[tk]=1.0; used.add(tk); break
        total=sum(port.values())
        if total>0:
            for k in port: port[k]/=total
        if not port:
            daily_ret[sname].extend([0.0]*len(hold_dates)); continue
        ws=pd.Series(port)
        dr=tk_wide.loc[hold_dates].reindex(columns=ws.index).fillna(0).mul(ws,axis=1).sum(axis=1)
        daily_ret[sname].extend(dr.values.tolist())

# === Results ===
eq_dates=tk_wide.index[-len(daily_ret['A4_10']):]
def calc(dr,name):
    arr=np.array(dr); arr=arr[np.isfinite(arr)]; n=len(arr); yrs=n/252
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1; vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr); peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    neg=arr[arr<0]; dd=np.sqrt(np.mean(neg**2))*np.sqrt(252) if len(neg)>0 else 1e-8
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    return {'name':name,'cagr':cagr,'vol':vol,'sharpe':sharpe,'calmar':calmar,'maxdd':maxdd}

print("\n"+"="*90)
print(f"{'Strategy':<12} {'CAGR':>8} {'Vol':>8} {'Sharpe':>8} {'Calmar':>8} {'MaxDD':>8}   10→5差(CAGR)")
print("="*90)
results={}
for sname in STRATS:
    m=calc(daily_ret[sname],sname); results[sname]=m
for base_name, pairs in [('PRISM(A4)',('A4_10','A4_5')),('PRISM-R(A5)',('A5_10','A5_5')),('PRISM-RQ',('RQ_10','RQ_5'))]:
    m10=results[pairs[0]]; m5=results[pairs[1]]
    diff=m5['cagr']-m10['cagr']
    for sn,m in [(pairs[0],m10),(pairs[1],m5)]:
        tag = '     ' if sn.endswith('10') else f" {diff:>+7.1%}"
        print(f"  {sn:<10} {m['cagr']:>7.1%} {m['vol']:>7.1%} {m['sharpe']:>7.3f} {m['calmar']:>7.3f} {m['maxdd']:>7.1%}  {tag}")
    print("-"*90)
print("="*90)

# Annual
print("\n=== ANNUAL ===")
for sname in STRATS:
    eq=pd.Series(np.cumprod(1+np.array(daily_ret[sname])),index=eq_dates)
    annual=eq.resample('YE').last().pct_change().dropna()
    results[sname]['annual']={str(d.year):round(float(r),3) for d,r in annual.items()}
header=f"{'Year':<6}"+"".join(f"{s:>10}" for s in STRATS)
print(header)
for yr in ['2021','2022','2023','2024','2025','2026']:
    row=f"  {yr:<4}"
    for s in STRATS: row+=f"  {results[s].get('annual',{}).get(yr,0):>+7.1%}"
    print(row)

with open('/Users/yutatomi/Downloads/stock-theme/research/scb/bt_conc_results.json','w') as f:
    json.dump(results,f,indent=2,default=str)
print(f'\n=== Done in {time.time()-t0:.1f}s ===')
