#!/usr/bin/env python3
"""Scrape ALL 913 individual stock details from stock-themes.com."""
import json, os, time
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError

OUT = Path("/Users/yutatomi/Downloads/stock-theme/research/stock_themes_data")
META = Path("/Users/yutatomi/Downloads/stock-theme/public/api/stock_meta.json")
BASE = "https://stock-themes.com"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

meta = json.load(open(META))
all_tickers = sorted(meta.keys())
print(f"Total tickers in stock_meta: {len(all_tickers)}")

# Check which we already have
existing = set()
for f in OUT.glob("ticker_*.json"):
    stem = f.stem
    if not any(x in stem for x in ["themes_","segments_","prices_"]):
        existing.add(stem.replace("ticker_",""))
print(f"Already scraped: {len(existing)}")
remaining = [tk for tk in all_tickers if tk not in existing]
print(f"Remaining: {len(remaining)}")

def fetch(path, filename):
    fp = OUT / filename
    if fp.exists() and fp.stat().st_size > 10: return True
    try:
        req = Request(BASE + path, headers=HEADERS)
        with urlopen(req, timeout=10) as r:
            data = r.read()
        with open(fp, "wb") as f: f.write(data)
        return True
    except: return False

success = 0; fail = 0
for i, tk in enumerate(remaining):
    r1 = fetch(f"/api/market-movers/ticker/{tk}", f"ticker_{tk}.json")
    r2 = fetch(f"/api/ticker-themes/{tk}", f"ticker_themes_{tk}.json")
    r3 = fetch(f"/api/ticker-segments/{tk}", f"ticker_segments_{tk}.json")
    r4 = fetch(f"/api/ticker-monthly-prices/{tk}", f"ticker_prices_{tk}.json")
    if r1: success += 1
    else: fail += 1
    if i % 100 == 0 and i > 0:
        print(f"  [{i}/{len(remaining)}] success={success} fail={fail}")
    time.sleep(0.15)

print(f"\nDone: {success} success, {fail} fail out of {len(remaining)}")
files = list(OUT.glob("*.json"))
print(f"Total files: {len(files)}, {sum(f.stat().st_size for f in files)/1024/1024:.1f}MB")
