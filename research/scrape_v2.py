#!/usr/bin/env python3
"""Comprehensive stock-themes.com scraper v2 - all endpoints, all tickers."""
import json, os, time, sys
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError

OUT = Path("/Users/yutatomi/Downloads/stock-theme/research/stock_themes_data")
OUT.mkdir(parents=True, exist_ok=True)
BASE = "https://stock-themes.com"
H = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
stats = {"ok": 0, "skip": 0, "fail": 0}

def fetch(path, fn=None, force=False):
    fn = fn or path.replace("/api/","").replace("/","_").replace("?","_").strip("_") + ".json"
    fp = OUT / fn
    if fp.exists() and not force and fp.stat().st_size > 100:
        stats["skip"] += 1; return json.load(open(fp))
    try:
        req = Request(BASE + path, headers=H)
        with urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        with open(fp, "w") as f: json.dump(data, f, ensure_ascii=False)
        stats["ok"] += 1
        return data
    except Exception as e:
        stats["fail"] += 1
        return None


# === 1. Core (previously failed) ===
print("=== 1. Core ===")
tr = fetch("/api/theme-ranking", "theme_ranking_full.json", force=True)
fetch("/api/sparklines-all", "sparklines_all.json", force=True)
fetch("/api/market-movers/", "market_movers_full.json", force=True)
fetch("/api/macro-themes", "macro_themes.json", force=True)
fetch("/api/weekday-returns", "weekday_returns.json")
fetch("/api/last-updated", "last_updated.json", force=True)
fetch("/api/zukai-pages", "zukai_pages.json")
fetch("/api/zukai-slug-map", "zukai_slug_map.json")
print(f"  Core: ok={stats['ok']} skip={stats['skip']}")

# === 2. Dip Alerts ===
print("=== 2. Dip Alerts ===")
for p in ["1D","5D","10D","1M","2M","3M","6M","1Y"]:
    fetch(f"/api/dip-alerts?period={p}", f"dip_alerts_{p}.json", force=True)

# === 3. Extract all tickers from theme-ranking ===
all_themes = tr.get("all_themes", []) if tr else []
all_tickers = []
for t in all_themes:
    name = t.get("name", "")
    related = t.get("related", "")
    if not related or len(related.split(",")) <= 1:
        if name and name.isalpha() and name == name.upper() and len(name) <= 5:
            all_tickers.append(name)
    if related:
        for tk in related.split(","):
            tk = tk.strip()
            if tk and tk.isalpha() and tk == tk.upper() and len(tk) <= 5:
                all_tickers.append(tk)
all_tickers = sorted(set(all_tickers))
print(f"=== Total unique tickers: {len(all_tickers)} ===")


# === 4. Ticker Details (all tickers) ===
print(f"=== 4. Ticker Details ({len(all_tickers)} tickers) ===")
for i, tk in enumerate(all_tickers):
    fetch(f"/api/market-movers/ticker/{tk}", f"ticker_{tk}.json")
    fetch(f"/api/ticker-themes/{tk}", f"ticker_themes_{tk}.json")
    fetch(f"/api/ticker-segments/{tk}", f"ticker_segments_{tk}.json")
    fetch(f"/api/ticker-monthly-prices/{tk}", f"ticker_prices_{tk}.json")
    if i % 50 == 0 and i > 0:
        print(f"  [{i}/{len(all_tickers)}] ok={stats['ok']} skip={stats['skip']} fail={stats['fail']}")
    time.sleep(0.15)
print(f"  Tickers done: ok={stats['ok']} skip={stats['skip']} fail={stats['fail']}")

# === 5. Zukai Reports ===
print("=== 5. Zukai Reports ===")
zp = fetch("/api/zukai-pages", "zukai_pages.json")
if zp:
    for page in zp.get("items", []):
        rid = page.get("report_id", "")
        if rid:
            fetch(f"/api/report-overlays/{rid}", f"zukai_overlays_{rid}.json")
            fetch(f"/api/report-ticker-details/{rid}", f"zukai_tickers_{rid}.json")
            fetch(f"/api/report-catalysts/{rid}", f"zukai_catalysts_{rid}.json")
            fetch(f"/api/report-theme-links/{rid}", f"zukai_themelinks_{rid}.json")
            time.sleep(0.2)

# === 6. Macro Theme Details ===
print("=== 6. Macro Details ===")
mt = fetch("/api/macro-themes", "macro_themes.json")
if mt:
    for topic in mt.get("topics", []):
        tid = topic.get("id", "")
        if tid:
            fetch(f"/api/macro-topics/{tid}", f"macro_detail_{tid}.json")
            time.sleep(0.3)

# === Summary ===
files = list(OUT.glob("*.json"))
total = sum(f.stat().st_size for f in files)
print(f"\n{'='*60}")
print(f"Total: {len(files)} files, {total/1024/1024:.1f} MB")
print(f"ok={stats['ok']} skip={stats['skip']} fail={stats['fail']}")

