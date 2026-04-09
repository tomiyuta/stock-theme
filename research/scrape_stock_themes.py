#!/usr/bin/env python3
"""Comprehensive stock-themes.com data extraction."""
import json, os, time
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError

OUT = Path("/Users/yutatomi/Downloads/stock-theme/research/stock_themes_data")
OUT.mkdir(parents=True, exist_ok=True)

BASE = "https://stock-themes.com"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

def fetch_json(path, filename=None):
    url = BASE + path
    fn = filename or path.replace("/api/", "").replace("/", "_").replace("?", "_") + ".json"
    fp = OUT / fn
    try:
        req = Request(url, headers=HEADERS)
        with urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        with open(fp, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        sz = os.path.getsize(fp)
        print(f"  ✓ {fn} ({sz/1024:.1f}KB)")
        return data
    except Exception as e:
        print(f"  ✗ {path}: {e}")
        return None

# === 1. Core endpoints ===
print("=== 1. Core Data ===")
theme_ranking = fetch_json("/api/theme_ranking", "theme_ranking.json")
fetch_json("/api/last-updated", "last_updated.json")
fetch_json("/api/weekday-returns", "weekday_returns.json")
fetch_json("/api/mc-labels", "mc_labels.json")

# === 2. Market Movers ===
print("\n=== 2. Market Movers ===")
for period in ["1D", "5D", "1M", "3M"]:
    fetch_json(f"/api/market-movers/{period}", f"movers_{period}.json")

# === 3. Macro Themes ===
print("\n=== 3. Macro Themes ===")
macro = fetch_json("/api/macro-themes", "macro_themes.json")

# === 4. Zukai Pages ===
print("\n=== 4. Zukai (Visual Reports) ===")
zukai_pages = fetch_json("/api/zukai-pages", "zukai_pages.json")
zukai_slugs = fetch_json("/api/zukai-slug-map", "zukai_slug_map.json")

# === 5. Dip Alerts ===
print("\n=== 5. Dip Alerts ===")
for period in ["1D", "5D", "10D", "1M", "3M"]:
    fetch_json(f"/api/dip-alerts?period={period}", f"dip_alerts_{period}.json")

# === 6. Zukai Report Details (per report) ===
print("\n=== 6. Zukai Report Details ===")
if zukai_pages:
    items = zukai_pages.get("items", [])
    for page in items:
        rid = page.get("report_id", "")
        if rid:
            fetch_json(f"/api/report-overlays/{rid}", f"zukai_overlays_{rid}.json")
            fetch_json(f"/api/report-ticker-details/{rid}", f"zukai_tickers_{rid}.json")
            fetch_json(f"/api/report-catalysts/{rid}", f"zukai_catalysts_{rid}.json")
            fetch_json(f"/api/report-theme-links/{rid}", f"zukai_themelinks_{rid}.json")
            time.sleep(0.3)

# === 7. Individual Theme Details (all 207) ===
print("\n=== 7. Theme Details ===")
if theme_ranking:
    all_themes = theme_ranking.get("all_themes", [])
    themes_only = [t for t in all_themes if t.get("related") and len(t.get("related","").split(",")) > 1]
    print(f"  Found {len(themes_only)} themes to fetch")
    for i, t in enumerate(themes_only):
        slug = t.get("slug", "")
        if slug:
            fetch_json(f"/api/infographic/{slug}", f"theme_{slug}.json")
            if i % 20 == 0: print(f"  [{i}/{len(themes_only)}]")
            time.sleep(0.2)

# === 8. Sample Ticker Details (top tickers) ===
print("\n=== 8. Ticker Details (sample) ===")
sample_tickers = ["NVDA", "AAPL", "COHR", "LITE", "AVGO", "MRVL", "FN", "AAOI",
                   "ANET", "CSCO", "GLW", "CIEN", "SITM", "KEYS", "MTSI", "VIAV",
                   "CLS", "DELL", "HPE", "AMD", "MU", "SMCI", "TSM", "ASML",
                   "SPY", "QQQ", "XLE", "XLK", "XLB", "SHY"]
for tk in sample_tickers:
    fetch_json(f"/api/market-movers/ticker/{tk}", f"ticker_{tk}.json")
    fetch_json(f"/api/ticker-themes/{tk}", f"ticker_themes_{tk}.json")
    fetch_json(f"/api/ticker-segments/{tk}", f"ticker_segments_{tk}.json")
    fetch_json(f"/api/ticker-monthly-prices/{tk}", f"ticker_prices_{tk}.json")
    time.sleep(0.2)

# === 9. Macro Theme Details ===
print("\n=== 9. Macro Theme Details ===")
if macro:
    topics = macro.get("topics", [])
    for topic in topics:
        tid = topic.get("id", "")
        if tid:
            fetch_json(f"/api/infographic/macro/{tid}", f"macro_{tid}.json")
            time.sleep(0.3)

# === Summary ===
files = list(OUT.glob("*.json"))
total_size = sum(f.stat().st_size for f in files)
print(f"\n{'='*60}")
print(f"Total: {len(files)} files, {total_size/1024/1024:.1f} MB")
print(f"Saved to: {OUT}")
