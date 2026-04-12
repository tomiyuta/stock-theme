#!/usr/bin/env python3
"""Vol Scaling Optimization — cap25 baseline
Phase 1: Total vol target sweep (20-60%)
Phase 2: Downside vol scaling comparison
Phase 3: Fixed-weight blend (raw + volscaled)
"""
import pandas as pd, numpy as np, time, warnings, json
from scipy import stats as scipy_stats
warnings.filterwarnings('ignore')
t0 = time.time()
panel = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet')
meta = pd.read_parquet('/Users/yutatomi/Downloads/stock-theme/research/scb/ticker_meta.parquet')
panel = panel.sort_values(['theme','ticker','date']).reset_index(drop=True)
panel['ret'] = panel.groupby(['theme','ticker'])['close'].pct_change()
agg = panel.dropna(subset=['ret']).groupby(['date','theme'])['ret'].agg(sum_ret='sum',n_day='count').reset_index()
panel = panel.merge(agg, on=['date','theme'], how='left')
panel['theme_ret'] = panel['sum_ret']/panel['n_day']
panel['theme_ex_self'] = np.where(panel['n_day']>1,(panel['sum_ret']-panel['ret'])/(panel['n_day']-1),np.nan)
dates_all = sorted(panel['date'].unique())
tk_wide = panel.pivot_table(index='date', columns='ticker', values='ret', aggfunc='first')
meta_sec = meta.set_index('ticker')['sector'].to_dict()
psec = panel[['theme','ticker']].drop_duplicates()
psec['sector'] = psec['ticker'].map(meta_sec)
theme_sector = psec.groupby('theme')['sector'].agg(lambda x: x.mode().iloc[0] if x.notna().any() else 'Unk')
import yfinance as yf
spy=yf.download('SPY',start='2018-01-01',end='2027-01-01',progress=False)
spy_close=(spy['Adj Close'] if 'Adj Close' in spy.columns else spy['Close']).squeeze()
spy_close.index=spy_close.index.tz_localize(None)
spy_ret=spy_close.pct_change().dropna()
print(f'Loaded: {len(panel):,} rows | {time.time()-t0:.1f}s')

def cumret(a):
    a=np.asarray(a,dtype=float);a=a[np.isfinite(a)]
    return float(np.expm1(np.log1p(a).sum())) if len(a) else np.nan
def ols_ab(y,x):
    mask=np.isfinite(y)&np.isfinite(x);y,x=y[mask],x[mask];n=len(y)
    if n<10:return np.nan,np.nan,np.nan
    xm,ym=x.mean(),y.mean();xd=x-xm;vx=np.dot(xd,xd)/(n-1)
    if vx<1e-12:return np.nan,np.nan,np.nan
    b=np.dot(xd,y-ym)/(n-1)/vx;a=ym-b*xm
    ss=float(np.sum((y-a-b*x)**2));st=float(np.sum((y-ym)**2))
    return a*n,b,(1-ss/st if st>1e-12 else np.nan)
def shrink_r2(v):
    if np.isnan(v) or v<0:return 0.0
    if v<0.10:return v*2
    if v<=0.50:return 0.20+(v-0.10)*2.0
    return 1.0
def w5b_w(port,cap=0.25):
    ws=[]
    for p in port:
        vals=[p.get('r63',np.nan),p.get('r126',np.nan),p.get('r252ex1m',np.nan)]
        valid=[v for v in vals if np.isfinite(v)]
        if len(valid)<2:ws.append(1.0)
        else:pc=sum(1 for v in valid if v>0);ar=np.mean([max(v,0) for v in valid]);ws.append(pc*(1+ar))
    wa=np.array(ws,dtype=float)
    if wa.sum()<=0:return np.ones(len(port))/len(port)
    wa=wa/wa.sum()
    if cap:
        for _ in range(5):
            exc=np.maximum(wa-cap,0)
            if exc.sum()<1e-6:break
            under=wa<cap;wa=np.minimum(wa,cap)
            if under.any():wa[under]+=exc.sum()*(wa[under]/wa[under].sum())
            wa=wa/wa.sum()
    return wa
def calc_stats(arr):
    arr=np.array(arr);arr=arr[np.isfinite(arr)];n=len(arr);yrs=n/252
    if n<20:return {}
    cum=float(np.expm1(np.log1p(arr).sum()))
    cagr=(1+cum)**(1/yrs)-1;vol=float(np.std(arr,ddof=1)*np.sqrt(252))
    sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr);peak=np.maximum.accumulate(eq)
    maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    neg=arr[arr<0];dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    return {'cagr':cagr,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'terminal':float(eq[-1]),'n':n}

# === Generate raw daily returns (cap25 baseline) ===
WARMUP=126;REBAL=20;MIN_M=4;TOP_T=10;SEC_MAX=3
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1:rebal_idx.append(len(dates_all)-1)
raw_daily=[]
for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos];j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1]);dt21=set(dates_all[max(0,j-20):j+1])
    dt126=set(dates_all[max(0,j-125):j+1]);dt252=set(dates_all[max(0,j-251):j+1])
    sub=panel[panel['date'].isin(dt63)];sub126=panel[panel['date'].isin(dt126)];sub252=panel[panel['date'].isin(dt252)]
    tm=sub.groupby('theme')['ticker'].nunique();elig=tm[tm>=MIN_M].index.tolist()
    hold_dates=tk_wide.index[j+1:j_next+1]
    tm_mom={};dc={}
    for th in elig:
        td=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td)>=63:tm_mom[th]=cumret(td)
        if len(td)>=63:
            r021=cumret(td[-21:]);r2142=cumret(td[-42:-21]);r4263=cumret(td[-63:-42])
            if all(np.isfinite([r021,r2142,r4263])):dc[th]=-(r021-0.5*(r2142+r4263))
    ms_s=pd.Series(tm_mom).dropna().sort_values(ascending=False)
    dcs=pd.Series(dc);common=list(set(ms_s.index)&set(dcs.index))
    if not common:raw_daily.extend([0.0]*len(hold_dates));continue
    ts=pd.DataFrame({'mom':ms_s[common],'dec':dcs[common]})
    ts['score']=0.70*ts['mom'].rank(pct=True)+0.30*ts['dec'].rank(pct=True)
    ts=ts.sort_values('score',ascending=False)
    sel=[];sc_cnt={}
    for th in ts.index:
        s=theme_sector.get(th,'Unk')
        if sc_cnt.get(s,0)>=SEC_MAX:continue
        sel.append(th);sc_cnt[s]=sc_cnt.get(s,0)+1
        if len(sel)>=TOP_T:break
    port=[];used=set()
    for th in sel:
        ths=sub[(sub['theme']==th)&sub['ret'].notna()];tks=ths['ticker'].unique()
        if len(tks)<MIN_M:continue
        scores={}
        for tk in tks:
            tkd=ths[ths['ticker']==tk].sort_values('date')
            a63,b63,r2_63=ols_ab(tkd['ret'].values,tkd['theme_ex_self'].values)
            shrk=shrink_r2(r2_63) if np.isfinite(r2_63) else 0
            scores[tk]=a63*shrk if np.isfinite(a63) else -999
        for tk,sc in sorted(scores.items(),key=lambda x:-x[1]):
            if tk not in used and sc>-999:
                r63=tm_mom.get(th,np.nan)
                td126v=sub126[sub126['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                r126=cumret(td126v) if len(td126v)>=63 else np.nan
                td252v=sub252[sub252['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                r252=cumret(td252v) if len(td252v)>=126 else np.nan
                r21v=sub[sub['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
                r21=cumret(r21v[-21:]) if len(r21v)>=21 else np.nan
                r252ex1m=((1+r252)/(1+r21)-1) if np.isfinite(r252) and np.isfinite(r21) and abs(1+r21)>1e-8 else np.nan
                port.append({'tk':tk,'th':th,'r63':r63,'r126':r126,'r252ex1m':r252ex1m})
                used.add(tk);break
    if not port:raw_daily.extend([0.0]*len(hold_dates));continue
    tickers=[p['tk'] for p in port]
    ws=w5b_w(port,cap=0.25)
    ww=pd.Series(ws,index=tickers)
    d=tk_wide.loc[hold_dates].reindex(columns=tickers).fillna(0).mul(ww,axis=1).sum(axis=1)
    raw_daily.extend(d.values.tolist())
print(f'Raw BT done: {len(raw_daily)} days | {time.time()-t0:.1f}s')
eq_dates=tk_wide.index[-len(raw_daily):]
raw=np.array(raw_daily)

# === Apply vol scaling variants on raw returns ===
spy_m63=spy_close.pct_change(63)
spy_aligned=spy_ret.reindex(eq_dates).fillna(0)
bear_mask=(spy_aligned.rolling(63).apply(lambda x:np.expm1(np.log1p(x).sum()),raw=True)<=0).fillna(False)

def apply_vol_scale(raw, target, lookback=20, mode='total', lev_cap=1.5):
    """Apply vol scaling. mode='total' or 'downside'."""
    scaled=np.zeros(len(raw))
    for i in range(len(raw)):
        if i<lookback:
            scaled[i]=raw[i]; continue
        window=raw[i-lookback:i]
        if mode=='total':
            rv=float(np.std(window,ddof=1)*np.sqrt(252))
        else:  # downside
            neg=window[window<0]
            rv=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>2 else float(np.std(window,ddof=1)*np.sqrt(252))
        if rv>0.01:
            scale=min(target/rv, lev_cap)
        else:
            scale=1.0
        scaled[i]=raw[i]*scale
    return scaled

# Phase 1: Total vol target sweep
print(f'\n{"="*120}')
print('  PHASE 1: TOTAL VOL TARGET SWEEP (cap25 baseline)')
print(f'{"="*120}')
base_s=calc_stats(raw)
base_bear=calc_stats(raw[bear_mask.values[:len(raw)]])
print(f'  {"Variant":<18} {"CAGR":>7} {"Shrp":>6} {"Sort":>5} {"Cal":>5} {"MaxDD":>7} {"BearS":>7} {"CAGR%":>6}')
print(f'  {"-"*70}')
print(f'  {"cap25_raw":<18} {base_s["cagr"]:>6.1%} {base_s["sharpe"]:>5.3f} {base_s["sortino"]:>4.2f} {base_s["calmar"]:>4.2f} {base_s["maxdd"]:>6.1%} {base_bear.get("sharpe",0):>+6.3f} {"100%":>5}')

phase1={}
for target in [0.20,0.25,0.30,0.35,0.40,0.45,0.50,0.60]:
    sc=apply_vol_scale(raw,target,mode='total')
    s=calc_stats(sc); bs=calc_stats(sc[bear_mask.values[:len(sc)]])
    cagr_ret=s['cagr']/base_s['cagr'] if base_s['cagr']>0 else 0
    label=f'total_vol_{int(target*100)}'
    phase1[label]={**s,'bear_sharpe':bs.get('sharpe',0),'cagr_retention':cagr_ret}
    print(f'  {label:<18} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} {s["maxdd"]:>6.1%} {bs.get("sharpe",0):>+6.3f} {cagr_ret:>5.1%}')

# Phase 2: Downside vol scaling
print(f'\n{"="*120}')
print('  PHASE 2: DOWNSIDE VOL SCALING')
print(f'{"="*120}')
print(f'  {"Variant":<18} {"CAGR":>7} {"Shrp":>6} {"Sort":>5} {"Cal":>5} {"MaxDD":>7} {"BearS":>7} {"CAGR%":>6}')
print(f'  {"-"*70}')
phase2={}
for target in [0.20,0.25,0.30,0.35,0.40,0.45,0.50,0.60]:
    sc=apply_vol_scale(raw,target,mode='downside')
    s=calc_stats(sc); bs=calc_stats(sc[bear_mask.values[:len(sc)]])
    cagr_ret=s['cagr']/base_s['cagr'] if base_s['cagr']>0 else 0
    label=f'down_vol_{int(target*100)}'
    phase2[label]={**s,'bear_sharpe':bs.get('sharpe',0),'cagr_retention':cagr_ret}
    print(f'  {label:<18} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} {s["maxdd"]:>6.1%} {bs.get("sharpe",0):>+6.3f} {cagr_ret:>5.1%}')

# Phase 3: Fixed-weight blend (raw + volscaled)
print(f'\n{"="*120}')
print('  PHASE 3: FIXED-WEIGHT BLEND (raw + total_vol_40)')
print(f'{"="*120}')
sc40=apply_vol_scale(raw,0.40,mode='total')
print(f'  {"Variant":<18} {"CAGR":>7} {"Shrp":>6} {"Sort":>5} {"Cal":>5} {"MaxDD":>7} {"BearS":>7} {"CAGR%":>6}')
print(f'  {"-"*70}')
phase3={}
for raw_pct in [1.00, 0.75, 0.50, 0.25, 0.00]:
    blend = raw * raw_pct + sc40 * (1-raw_pct)
    s=calc_stats(blend); bs=calc_stats(blend[bear_mask.values[:len(blend)]])
    cagr_ret=s['cagr']/base_s['cagr'] if base_s['cagr']>0 else 0
    label=f'blend_{int(raw_pct*100)}raw_{int((1-raw_pct)*100)}vs'
    phase3[label]={**s,'bear_sharpe':bs.get('sharpe',0),'cagr_retention':cagr_ret}
    print(f'  {label:<18} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} {s["maxdd"]:>6.1%} {bs.get("sharpe",0):>+6.3f} {cagr_ret:>5.1%}')

# Also blend with best downside vol
print(f'\n  Blend with downside_vol_40:')
dsc40=apply_vol_scale(raw,0.40,mode='downside')
for raw_pct in [0.75, 0.50, 0.25]:
    blend = raw * raw_pct + dsc40 * (1-raw_pct)
    s=calc_stats(blend); bs=calc_stats(blend[bear_mask.values[:len(blend)]])
    cagr_ret=s['cagr']/base_s['cagr'] if base_s['cagr']>0 else 0
    label=f'dblend_{int(raw_pct*100)}r_{int((1-raw_pct)*100)}d'
    phase3[label]={**s,'bear_sharpe':bs.get('sharpe',0),'cagr_retention':cagr_ret}
    print(f'  {label:<18} {s["cagr"]:>6.1%} {s["sharpe"]:>5.3f} {s["sortino"]:>4.2f} {s["calmar"]:>4.2f} {s["maxdd"]:>6.1%} {bs.get("sharpe",0):>+6.3f} {cagr_ret:>5.1%}')

# === FINAL COMPARISON (ChatGPT hard filter) ===
print(f'\n{"="*120}')
print('  FINAL: HARD FILTER + WINNER SELECTION')
print(f'{"="*120}')
print(f'  Hard filter: CAGR_ret≥85% AND Bear>-0.20 AND MaxDD≤cap25+1pt')
all_candidates={**phase1,**phase2,**phase3}
all_candidates['cap25_raw']={**base_s,'bear_sharpe':base_bear.get('sharpe',0),'cagr_retention':1.0}
survivors=[]
for label,s in all_candidates.items():
    cagr_ret=s.get('cagr_retention',0)
    bear_s=s.get('bear_sharpe',0)
    maxdd=s.get('maxdd',0)
    passes_cagr=cagr_ret>=0.85
    passes_bear=bear_s>-0.20
    passes_maxdd=maxdd>=base_s['maxdd']-0.01  # allow 1pt worse
    if passes_cagr and passes_bear:
        survivors.append((label,s))
        tag='✅' if passes_maxdd else '⚠MaxDD'
        print(f'  {tag} {label:<20} CAGR={s["cagr"]:.1%} Shrp={s["sharpe"]:.3f} MaxDD={s["maxdd"]:.1%} Bear={bear_s:+.3f} CAGR_ret={cagr_ret:.1%}')
if not survivors:
    print('  No survivors. Relaxing Bear filter to >-0.30...')
    for label,s in all_candidates.items():
        bear_s=s.get('bear_sharpe',0)
        cagr_ret=s.get('cagr_retention',0)
        if cagr_ret>=0.85 and bear_s>-0.30:
            survivors.append((label,s))
            print(f'  ⚠ {label:<20} CAGR={s["cagr"]:.1%} Shrp={s["sharpe"]:.3f} MaxDD={s["maxdd"]:.1%} Bear={bear_s:+.3f} CAGR_ret={cagr_ret:.1%}')
# Winner by Bear Sharpe (primary), then MaxDD, then Sharpe
if survivors:
    survivors.sort(key=lambda x:(-x[1].get('bear_sharpe',0), x[1].get('maxdd',0), -x[1].get('sharpe',0)))
    w=survivors[0]
    print(f'\n  WINNER: {w[0]}')
    print(f'    CAGR={w[1]["cagr"]:.1%} Sharpe={w[1]["sharpe"]:.3f} MaxDD={w[1]["maxdd"]:.1%} Bear={w[1]["bear_sharpe"]:+.3f} CAGR_ret={w[1]["cagr_retention"]:.1%}')
else:
    print('  No candidates survived filters.')

# Save
with open('/Users/yutatomi/Downloads/stock-theme/research/scb/vol_scale_results.json','w') as f:
    json.dump({k:{kk:round(vv,6) if isinstance(vv,float) else vv for kk,vv in v.items()} for k,v in all_candidates.items()},f,indent=2)
print(f'\n  Total elapsed: {time.time()-t0:.1f}s')
print('  === VOL SCALE OPTIMIZATION COMPLETE ===')
