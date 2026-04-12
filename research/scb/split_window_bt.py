#!/usr/bin/env python3
"""Split-Window 2×2 Factorial BT
Tests OLS estimation window (63d vs 126d) × formation horizon (current vs 6-2mo)
Design from ChatGPT consensus:
  H0/O0: current formation × current OLS (baseline)
  H0/O1: current formation × split126_63 (β=126d, α=63d)
  H1/O0: 6-2mo formation × current OLS (= F_stk6_2)
  H1/O1: 6-2mo formation × split126_63 (combination)
"""
import pandas as pd, numpy as np, time, warnings
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
def split_alpha(y_long, x_long, y_short, x_short):
    """Split-window: β from long window, α from short window."""
    # Step 1: estimate β from long window
    mask=np.isfinite(y_long)&np.isfinite(x_long)
    yl,xl=y_long[mask],x_long[mask];nl=len(yl)
    if nl<20:return np.nan,np.nan,np.nan
    xm,ym=xl.mean(),yl.mean();xd=xl-xm;vx=np.dot(xd,xd)/(nl-1)
    if vx<1e-12:return np.nan,np.nan,np.nan
    b_long=np.dot(xd,yl-ym)/(nl-1)/vx
    # R² from long window
    resid_long=yl-ym-b_long*(xl-xm)+ym-b_long*xm
    resid_long=yl-(ym-b_long*xm)-b_long*xl
    ss=float(np.sum(resid_long**2));st=float(np.sum((yl-ym)**2))
    r2_long=1-ss/st if st>1e-12 else np.nan
    # Step 2: α from short window using long β
    mask_s=np.isfinite(y_short)&np.isfinite(x_short)
    ys,xs=y_short[mask_s],x_short[mask_s];ns=len(ys)
    if ns<10:return np.nan,b_long,r2_long
    alpha_daily=np.mean(ys - b_long*xs)
    alpha_cum=alpha_daily*ns
    return alpha_cum,b_long,r2_long
def shrink_r2(v):
    if np.isnan(v) or v<0:return 0.0
    if v<0.10:return v*2
    if v<=0.50:return 0.20+(v-0.10)*2.0
    return 1.0
def w5b_w(port,cap=0.30):
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
    cum=float(np.expm1(np.log1p(arr).sum()));cagr=(1+cum)**(1/yrs)-1
    vol=float(np.std(arr,ddof=1)*np.sqrt(252));sharpe=cagr/vol if vol>1e-8 else 0
    eq=np.cumprod(1+arr);peak=np.maximum.accumulate(eq);maxdd=float(((eq-peak)/peak).min())
    calmar=cagr/abs(maxdd) if abs(maxdd)>1e-8 else 0
    neg=arr[arr<0];dd=float(np.sqrt(np.mean(neg**2))*np.sqrt(252)) if len(neg)>0 else 0.001
    sortino=cagr/dd if dd>1e-8 else 0
    return {'cagr':cagr,'sharpe':sharpe,'sortino':sortino,'calmar':calmar,'maxdd':maxdd,'terminal':float(eq[-1]),'n':n}

# === 2×2 Factorial Variants ===
VARIANTS = {
    'H0_O0': {'theme':'current', 'stock':'alpha63',        'ols_window':'63'},
    'H0_O1': {'theme':'current', 'stock':'split126_alpha63','ols_window':'126'},
    'H1_O0': {'theme':'current', 'stock':'alpha_6_2',       'ols_window':'63'},
    'H1_O1': {'theme':'current', 'stock':'split126_alpha_6_2','ols_window':'126'},
}
WARMUP=252;REBAL=20;MIN_M=4;TOP_T=10;SEC_MAX=3
rebal_idx=list(range(WARMUP,len(dates_all),REBAL))
if rebal_idx[-1]!=len(dates_all)-1:rebal_idx.append(len(dates_all)-1)
results={k:[] for k in VARIANTS}
eq_dates=[]
print(f'Rebalance: {len(rebal_idx)-1} periods')

for pos in range(len(rebal_idx)-1):
    j=rebal_idx[pos];j_next=rebal_idx[pos+1]
    dt63=set(dates_all[max(0,j-62):j+1])
    dt126=set(dates_all[max(0,j-125):j+1])
    dt252=set(dates_all[max(0,j-251):j+1])
    dt21=set(dates_all[max(0,j-20):j+1])
    dt_2_6=set(dates_all[max(0,j-125):max(0,j-41)])  # months 2-6 ago
    sub63=panel[panel['date'].isin(dt63)]
    sub126=panel[panel['date'].isin(dt126)]
    sub_2_6=panel[panel['date'].isin(dt_2_6)]
    hold_dates=tk_wide.index[j+1:j_next+1]
    # Theme scoring (current = mom63 + decel)
    tm=sub63.groupby('theme')['ticker'].nunique()
    elig=tm[tm>=MIN_M].index.tolist()
    tm_mom63={};dc={}
    for th in elig:
        td63v=sub63[sub63['theme']==th].groupby('date')['theme_ret'].first().sort_index().values
        if len(td63v)>=42:tm_mom63[th]=cumret(td63v)
        if len(td63v)>=63:
            r021=cumret(td63v[-21:]);r2142=cumret(td63v[-42:-21]);r4263=cumret(td63v[-63:-42])
            if all(np.isfinite([r021,r2142,r4263])):dc[th]=-(r021-0.5*(r2142+r4263))
    ms=pd.Series(tm_mom63).dropna();dcs=pd.Series(dc)
    common=list(set(ms.index)&set(dcs.index))
    if not common:
        for vname in VARIANTS:results[vname].extend([0.0]*len(hold_dates))
        eq_dates.extend(hold_dates.tolist());continue
    ts=pd.DataFrame({'mom':ms[common],'dec':dcs[common]})
    ts['score']=0.70*ts['mom'].rank(pct=True)+0.30*ts['dec'].rank(pct=True)
    theme_ranked=ts['score'].sort_values(ascending=False)
    sel=[];sc_cnt={}
    for th in theme_ranked.index:
        if th not in elig:continue
        s=theme_sector.get(th,'Unk')
        if sc_cnt.get(s,0)>=SEC_MAX:continue
        sel.append(th);sc_cnt[s]=sc_cnt.get(s,0)+1
        if len(sel)>=TOP_T:break
    # Score stocks for all variants
    var_picks = {v:{} for v in VARIANTS}
    for th in sel:
        ths63=sub63[(sub63['theme']==th)&sub63['ret'].notna()]
        ths126=sub126[(sub126['theme']==th)&sub126['ret'].notna()]
        ths_2_6=sub_2_6[(sub_2_6['theme']==th)&sub_2_6['ret'].notna()]
        tks=ths63['ticker'].unique()
        if len(tks)<MIN_M:continue
        scores={v:{} for v in VARIANTS}
        for tk in tks:
            # Current OLS (63d)
            tkd63=ths63[ths63['ticker']==tk].sort_values('date')
            if len(tkd63)<10:continue
            a63,b63,r2_63=ols_ab(tkd63['ret'].values,tkd63['theme_ex_self'].values)
            s63=a63*shrink_r2(r2_63) if np.isfinite(a63) else -999
            scores['H0_O0'][tk]=s63
            # Split126 OLS (β=126d, α=63d)
            tkd126=ths126[ths126['ticker']==tk].sort_values('date')
            if len(tkd126)>=20:
                a_split,b_split,r2_split=split_alpha(
                    tkd126['ret'].values,tkd126['theme_ex_self'].values,
                    tkd63['ret'].values,tkd63['theme_ex_self'].values)
                scores['H0_O1'][tk]=a_split*shrink_r2(r2_split) if np.isfinite(a_split) else -999
            else:
                scores['H0_O1'][tk]=-999
            # 6-2mo formation × current OLS
            tkd_2_6=ths_2_6[ths_2_6['ticker']==tk].sort_values('date')
            if len(tkd_2_6)>=10:
                a_6_2,b_6_2,r2_6_2=ols_ab(tkd_2_6['ret'].values,tkd_2_6['theme_ex_self'].values)
                scores['H1_O0'][tk]=a_6_2*shrink_r2(r2_6_2) if np.isfinite(a_6_2) else -999
            else:
                scores['H1_O0'][tk]=-999
            # 6-2mo formation × split126 OLS (β=126d, α from 6-2mo)
            if len(tkd126)>=20 and len(tkd_2_6)>=10:
                a_combo,b_combo,r2_combo=split_alpha(
                    tkd126['ret'].values,tkd126['theme_ex_self'].values,
                    tkd_2_6['ret'].values,tkd_2_6['theme_ex_self'].values)
                scores['H1_O1'][tk]=a_combo*shrink_r2(r2_combo) if np.isfinite(a_combo) else -999
            else:
                scores['H1_O1'][tk]=-999
        # Select best stock per theme per variant (no reuse)
        for vname in VARIANTS:
            for tk,sc in sorted(scores[vname].items(),key=lambda x:-x[1]):
                if tk not in var_picks[vname] and sc>-999:
                    var_picks[vname][tk]=1.0;break
    # Normalize weights (equal weight)
    for vname in VARIANTS:
        total=sum(var_picks[vname].values())
        if total>0:
            for k in var_picks[vname]:var_picks[vname][k]/=total
    # Hold period returns
    for d in hold_dates:
        eq_dates.append(d)
        day_ret = tk_wide.loc[d] if d in tk_wide.index else pd.Series(dtype=float)
        for vname in VARIANTS:
            port_ret = sum(w * day_ret.get(tk, 0) for tk, w in var_picks[vname].items())
            results[vname].append(float(port_ret) if np.isfinite(port_ret) else 0.0)
    if (pos+1) % 20 == 0:
        print(f'  [{pos+1}/{len(rebal_idx)-1}] {dates_all[j].strftime("%Y-%m-%d")}')

print(f'\nBT done in {time.time()-t0:.1f}s')

# === Results ===
eq_dates_pd = pd.to_datetime(eq_dates)
spy_daily = spy_ret.reindex(eq_dates_pd).fillna(0).values
bear_mask = spy_daily < -0.005  # down days

print(f'\n{"="*100}')
print(f'  SPLIT-WINDOW 2×2 FACTORIAL RESULTS')
print(f'{"="*100}')
print(f'  {"Variant":<20} {"CAGR":>7} {"Shrp":>6} {"Sort":>6} {"Cal":>6} {"MaxDD":>7} {"BearS":>7}  {"Term":>8}')
print(f'  {"-"*75}')
for vname in VARIANTS:
    dr=np.array(results[vname])
    st=calc_stats(dr)
    if not st:continue
    # Bear Sharpe
    bear_ret = dr[bear_mask[:len(dr)]] if len(bear_mask)>=len(dr) else dr[dr<0]
    bear_s = calc_stats(bear_ret)
    bs = bear_s.get('sharpe',0) if bear_s else 0
    print(f'  {vname:<20} {st["cagr"]:>6.1%} {st["sharpe"]:>5.3f} {st["sortino"]:>5.2f} {st["calmar"]:>5.2f} {st["maxdd"]:>6.1%} {bs:>+6.3f}  {st["terminal"]:>7.1f}x')

# Delta analysis
print(f'\n  Factorial decomposition (vs H0_O0):')
base_st = calc_stats(np.array(results['H0_O0']))
base_bear_ret = np.array(results['H0_O0'])[bear_mask[:len(results['H0_O0'])]]
base_bs = calc_stats(base_bear_ret).get('sharpe',0) if calc_stats(base_bear_ret) else 0
for vname in ['H0_O1','H1_O0','H1_O1']:
    dr=np.array(results[vname]);st=calc_stats(dr)
    bear_ret=dr[bear_mask[:len(dr)]] if len(bear_mask)>=len(dr) else dr[dr<0]
    bs=calc_stats(bear_ret).get('sharpe',0) if calc_stats(bear_ret) else 0
    print(f'    {vname:<20} ΔCAGR={st["cagr"]-base_st["cagr"]:>+6.1%} ΔShrp={st["sharpe"]-base_st["sharpe"]:>+6.3f} ΔMaxDD={st["maxdd"]-base_st["maxdd"]:>+6.1%} ΔBear={bs-base_bs:>+6.3f}')

# Interaction effect
st_00=calc_stats(np.array(results['H0_O0']))
st_01=calc_stats(np.array(results['H0_O1']))
st_10=calc_stats(np.array(results['H1_O0']))
st_11=calc_stats(np.array(results['H1_O1']))
b00=base_bs
dr01=np.array(results['H0_O1']);b01=calc_stats(dr01[bear_mask[:len(dr01)]]).get('sharpe',0) if calc_stats(dr01[bear_mask[:len(dr01)]]) else 0
dr10=np.array(results['H1_O0']);b10=calc_stats(dr10[bear_mask[:len(dr10)]]).get('sharpe',0) if calc_stats(dr10[bear_mask[:len(dr10)]]) else 0
dr11=np.array(results['H1_O1']);b11=calc_stats(dr11[bear_mask[:len(dr11)]]).get('sharpe',0) if calc_stats(dr11[bear_mask[:len(dr11)]]) else 0
int_sharpe=(st_11['sharpe']-st_00['sharpe'])-(st_01['sharpe']-st_00['sharpe'])-(st_10['sharpe']-st_00['sharpe'])
int_bear=(b11-b00)-(b01-b00)-(b10-b00)
print(f'\n  Interaction effects:')
print(f'    Sharpe interaction: {int_sharpe:>+.3f} (positive=synergistic)')
print(f'    Bear   interaction: {int_bear:>+.3f} (positive=synergistic)')

# Annual returns
print(f'\n  Annual returns:')
eq_series = {v: pd.Series(np.cumprod(1+np.array(results[v])),index=eq_dates_pd) for v in VARIANTS}
for v in VARIANTS:
    ann = {str(d.year):round(float(r),4) for d,r in eq_series[v].resample('YE').last().pct_change().dropna().items()}
    y22=ann.get('2022',0);y24=ann.get('2024',0);y25=ann.get('2025',0)
    print(f'    {v:<20} 2022={y22:>+6.1%} 2024={y24:>+6.1%} 2025={y25:>+6.1%}')

print(f'\n  Total elapsed: {time.time()-t0:.1f}s')
print('  === SPLIT-WINDOW 2×2 FACTORIAL COMPLETE ===')
