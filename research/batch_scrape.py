#!/usr/bin/env python3
"""Batch scrape remaining tickers from stock-themes.com. Run repeatedly until done."""
import json, time, sys
from pathlib import Path
from urllib.request import urlopen, Request

D = Path('/Users/yutatomi/Downloads/stock-theme/research/stock_themes_data')
M = Path('/Users/yutatomi/Downloads/stock-theme/public/api/stock_meta.json')
B = 'https://stock-themes.com'
H = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
BATCH = int(sys.argv[1]) if len(sys.argv) > 1 else 30

meta = json.load(open(M))
have = set()
for f in D.glob('ticker_*.json'):
    s = f.stem
    if 'themes_' not in s and 'segments_' not in s and 'prices_' not in s:
        have.add(s.replace('ticker_', ''))

remaining = [tk for tk in sorted(meta.keys()) if tk not in have]
print(f"Total={len(meta)} Have={len(have)} Remaining={len(remaining)}", flush=True)
if not remaining:
    print("ALL DONE", flush=True)
    sys.exit(0)

batch = remaining[:BATCH]
print(f"Batch: {len(batch)} tickers ({batch[0]}..{batch[-1]})", flush=True)

def fetch(p, fn):
    fp = D / fn
    if fp.exists() and fp.stat().st_size > 5:
        return True
    try:
        req = Request(B + p, headers=H)
        with urlopen(req, timeout=8) as r:
            with open(fp, 'wb') as f:
                f.write(r.read())
        return True
    except:
        return False

ok = 0; fail = 0
for i, tk in enumerate(batch):
    r1 = fetch(f'/api/market-movers/ticker/{tk}', f'ticker_{tk}.json')
    fetch(f'/api/ticker-themes/{tk}', f'ticker_themes_{tk}.json')
    fetch(f'/api/ticker-segments/{tk}', f'ticker_segments_{tk}.json')
    fetch(f'/api/ticker-monthly-prices/{tk}', f'ticker_prices_{tk}.json')
    if r1:
        ok += 1
    else:
        fail += 1
    if (i + 1) % 10 == 0:
        print(f"  [{i+1}/{len(batch)}] ok={ok} fail={fail}", flush=True)
    time.sleep(0.12)

new_remaining = len(remaining) - len(batch)
print(f"BATCH COMPLETE: ok={ok} fail={fail} remaining={new_remaining}", flush=True)
