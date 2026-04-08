#!/usr/bin/env python3
"""GitHub Actions用: yfinance → public/api/ へJSON出力"""
import json, time, os
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
from scipy import stats

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "scripts" / "theme_ranking_raw.json"
API = ROOT / "public" / "api"
DET = API / "theme-details"
PM = {"1日":1,"5日":5,"10日":10,"1ヶ月":21,"2ヶ月":42,"3ヶ月":63,"半年":126,"1年":252}

def load_master():
    with open(RAW, encoding="utf-8") as f: raw = json.load(f)
    all_items = raw["all_themes"]
    themes = [t for t in all_items if t.get("related")]
    etfs = [t for t in all_items if t.get("isETF")]
    stocks = [t for t in all_items if t.get("isIndividualTicker")]
    tickers = set()
    for t in themes:
        for tk in t["related"].split(","): tk=tk.strip(); tickers.add(tk) if tk else None
    for e in etfs: tickers.add(e["name"])
    print(f"Master: {len(themes)} themes, {len(etfs)} ETFs, {len(tickers)} tickers")
    return themes, etfs, stocks, sorted(tickers), raw

def fetch_prices(tickers):
    print(f"Fetching {len(tickers)} tickers...")
    all_prices = {}; failed = []
    for i in range(0, len(tickers), 50):
        batch = tickers[i:i+50]
        print(f"  Batch {i//50+1}/{(len(tickers)-1)//50+1}")
        try:
            data = yf.download(" ".join(batch), period="2y", auto_adjust=True, progress=False, threads=True)
            closes = data["Close"] if isinstance(data.columns, pd.MultiIndex) else data[["Close"]].rename(columns={"Close":batch[0]})
            for col in closes.columns:
                s = closes[col].dropna()
                if len(s)>10: all_prices[col]=s
                else: failed.append(col)
        except Exception as e: print(f"    ERROR: {e}"); failed.extend(batch)
        time.sleep(0.3)
    if failed: print(f"  Failed: {len(failed)}")
    df = pd.DataFrame(all_prices).sort_index(); df.index = pd.to_datetime(df.index)
    print(f"Prices: {df.shape[1]} tickers, {df.shape[0]} days")
    return df

def calc_returns(themes, etfs, stocks, prices):
    print("Computing returns...")
    theme_results = []
    for t in themes:
        tks = [x.strip() for x in t["related"].split(",") if x.strip()]
        avail = [x for x in tks if x in prices.columns]
        if not avail: continue
        entry = {"name":t["name"],"slug":t["slug"],"industry":t["industry"],"theme1":t["theme1"],"theme2":t["theme2"],"related":t["related"],"theme_no":t.get("theme_no"),"text":t.get("text",""),"is_special":t.get("is_special",False),"color_num":t.get("color_num"),"tickerPerformances":{},"日中":None}
        for pname, ndays in PM.items():
            if len(prices)<=ndays: entry[pname]=None; continue
            tk_rets = {}
            for tk in avail:
                p = prices[tk].dropna()
                if len(p)>ndays: tk_rets[tk]=round(float(p.iloc[-1]/p.iloc[-1-ndays]-1),4)
            if tk_rets:
                entry[pname]=round(np.mean(list(tk_rets.values())),4)
                for tk,ret in tk_rets.items():
                    entry["tickerPerformances"].setdefault(tk,{"日中":None})[pname]=ret
            else: entry[pname]=None
        theme_results.append(entry)
    theme_results.sort(key=lambda x:x.get("1日") or -999, reverse=True)
    for i,r in enumerate(theme_results): r["rank"]=i+1
    etf_results = []
    for e in etfs:
        tk=e["name"]
        if tk not in prices.columns: continue
        entry={"name":tk,"isETF":True,"日中":None}; p=prices[tk].dropna()
        for pn,nd in PM.items(): entry[pn]=round(float(p.iloc[-1]/p.iloc[-1-nd]-1),4) if len(p)>nd else None
        etf_results.append(entry)
    stock_results = []
    for s in stocks:
        tk=s["name"]
        if tk not in prices.columns: continue
        entry={"name":tk,"isIndividualTicker":True,"tickerPerformances":{},"日中":None}; p=prices[tk].dropna(); tp={"日中":None}
        for pn,nd in PM.items(): ret=round(float(p.iloc[-1]/p.iloc[-1-nd]-1),4) if len(p)>nd else None; entry[pn]=ret; tp[pn]=ret
        entry["tickerPerformances"][tk]=tp; stock_results.append(entry)
    return theme_results, etf_results, stock_results

def calc_sparklines(themes, prices):
    print("Computing sparklines...")
    weekly=prices.resample("W-FRI").last(); result={}
    for t in themes:
        tks=[x.strip() for x in t["related"].split(",") if x.strip()]
        avail=[x for x in tks if x in weekly.columns]
        if not avail: continue
        avg=weekly[avail].mean(axis=1).dropna()
        if len(avg)<2: continue
        n=min(52,len(avg)-1); base=avg.iloc[-n-1]; series=((avg.iloc[-n:]/base)-1).round(4)
        result[t["slug"]]={"dates":[d.strftime("%Y-%m-%d") for d in series.index],"values":series.tolist()}
    print(f"  Sparklines: {len(result)}"); return result

def calc_alpha_beta(themes, prices):
    print("Computing α/β/R²...")
    ab_periods={"1ヶ月":21,"3ヶ月":63,"半年":126,"1年":252}; all_ab={}
    for pname,pdays in ab_periods.items():
        if len(prices)<=pdays: continue
        period_ab={}
        for t in themes:
            tks=[x.strip() for x in t["related"].split(",") if x.strip()]
            avail=[x for x in tks if x in prices.columns]
            if len(avail)<2: continue
            sub=prices[avail].iloc[-pdays:].pct_change().dropna()
            if len(sub)<10: continue
            theme_ret=sub.mean(axis=1); tk_stats={}
            for tk in avail:
                if tk not in sub.columns: continue
                y,x=sub[tk].values,theme_ret.values; mask=np.isfinite(x)&np.isfinite(y)
                if mask.sum()<10: continue
                sl,ic,rv,_,_=stats.linregress(x[mask],y[mask])
                tk_stats[tk]={"alpha":round(float(ic*252),4),"beta":round(float(sl),4),"r2":round(float(rv**2),4)}
            if tk_stats: period_ab[t["slug"]]=tk_stats
        all_ab[pname]=period_ab
    return all_ab

def main():
    API.mkdir(parents=True, exist_ok=True); DET.mkdir(parents=True, exist_ok=True)
    themes, etfs, stocks, tickers, raw = load_master()
    prices = fetch_prices(tickers)
    theme_res, etf_res, stock_res = calc_returns(themes, etfs, stocks, prices)
    ranking = {"all_themes":theme_res+etf_res+stock_res,"themes":theme_res[:50],
        "all_periods":list(PM.keys()),"periods":list(PM.keys()),
        "ranking_limited":False,"restrictions_enabled":False,"user_tier":"self-hosted",
        "data_source":"yfinance","last_update":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latest_stock_date":prices.index[-1].strftime("%Y-%m-%d"),"is_market_open":False}
    with open(API/"theme_ranking.json","w",encoding="utf-8") as f: json.dump(ranking,f,ensure_ascii=False)
    print(f"✓ theme_ranking.json ({len(theme_res)} themes)")
    sp = calc_sparklines(themes, prices)
    with open(API/"sparklines.json","w",encoding="utf-8") as f: json.dump(sp,f,ensure_ascii=False)
    ab = calc_alpha_beta(themes, prices)
    with open(API/"alpha_beta.json","w",encoding="utf-8") as f: json.dump(ab,f,ensure_ascii=False)
    for t in themes:
        tks=[x.strip() for x in t["related"].split(",") if x.strip()]
        avail=[x for x in tks if x in prices.columns]
        detail={"slug":t["slug"],"name":t["name"],"tickers":avail,"prices":[]}
        if avail:
            sub=prices[avail].iloc[-252:]
            for date,row in sub.iterrows():
                rec={"date":date.strftime("%Y-%m-%d")}
                for tk in avail:
                    if pd.notna(row.get(tk)): rec[tk]=round(float(row[tk]),2)
                detail["prices"].append(rec)
        with open(DET/f"{t['slug']}.json","w",encoding="utf-8") as f: json.dump(detail,f,ensure_ascii=False)
    print("DONE")

if __name__ == "__main__": main()
