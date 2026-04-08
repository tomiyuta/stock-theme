#!/usr/bin/env python3
"""全銘柄のメタデータ(企業名/時価総額/セクター/株価)をyfinanceから取得"""
import json, time, os
from pathlib import Path
import yfinance as yf

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "scripts" / "theme_ranking_raw.json"
OUT = ROOT / "public" / "api" / "stock_meta.json"

def get_all_tickers():
    with open(RAW, encoding="utf-8") as f:
        raw = json.load(f)
    tickers = set()
    for t in raw["all_themes"]:
        if t.get("related"):
            for tk in t["related"].split(","):
                tk = tk.strip()
                if tk: tickers.add(tk)
        if t.get("isETF"): tickers.add(t["name"])
        if t.get("isIndividualTicker"): tickers.add(t["name"])
    return sorted(tickers)

def mc_label(mc):
    if mc is None: return ""
    if mc >= 200e9: return "超大型"
    if mc >= 10e9: return "大型"
    if mc >= 2e9: return "中型"
    return "小型"

def fetch_meta(tickers):
    meta = {}
    total = len(tickers)
    for i, tk in enumerate(tickers):
        if i % 50 == 0:
            print(f"  [{i}/{total}] ...")
        try:
            info = yf.Ticker(tk).info
            meta[tk] = {
                "name": info.get("longName") or info.get("shortName") or tk,
                "sector": info.get("sector") or "",
                "industry": info.get("industry") or "",
                "mc": mc_label(info.get("marketCap")),
                "price": round(info.get("previousClose") or 0, 2),
            }
        except Exception as e:
            print(f"  WARN: {tk} -> {e}")
            meta[tk] = {"name": tk, "sector": "", "industry": "", "mc": "", "price": 0}
        time.sleep(0.05)
    return meta

def main():
    print("Loading tickers...")
    tickers = get_all_tickers()
    print(f"Found {len(tickers)} tickers")
    print("Fetching metadata from yfinance...")
    meta = fetch_meta(tickers)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1)
    print(f"✓ Saved {len(meta)} entries to {OUT}")

if __name__ == "__main__":
    main()
