#!/usr/bin/env python3
import json, os
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "scripts" / "theme_ranking_raw.json"
API = ROOT / "public" / "api"
DET = API / "theme-details"

def main():
    API.mkdir(parents=True, exist_ok=True)
    DET.mkdir(parents=True, exist_ok=True)
    with open(RAW, encoding="utf-8") as f:
        raw = json.load(f)
    all_items = raw["all_themes"]
    themes = [t for t in all_items if t.get("related")]
    etfs = [t for t in all_items if t.get("isETF")]
    stocks = [t for t in all_items if t.get("isIndividualTicker")]

    ranking = {
        "all_themes": all_items,
        "themes": sorted(themes, key=lambda t: t.get("rank") or 999)[:50],
        "all_periods": raw.get("all_periods", ["1日","5日","10日","1ヶ月","2ヶ月","3ヶ月","半年","1年"]),
        "periods": raw.get("all_periods", ["1日","5日","10日","1ヶ月","2ヶ月","3ヶ月","半年","1年"]),
        "ranking_limited": False,
        "restrictions_enabled": False,
        "user_tier": "self-hosted",
        "data_source": raw.get("data_source", "daily"),
        "last_update": raw.get("last_update", datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M:%S")),
        "latest_stock_date": raw.get("latest_stock_date", ""),
        "is_market_open": False,
    }
    with open(API / "theme_ranking.json", "w", encoding="utf-8") as f:
        json.dump(ranking, f, ensure_ascii=False)
    print(f"✓ theme_ranking.json ({len(themes)} themes, {len(etfs)} ETFs, {len(stocks)} stocks)")

    periods_ordered = ["1日","5日","10日","1ヶ月","2ヶ月","3ヶ月","半年","1年"]
    sparklines = {}
    for t in themes:
        slug = t.get("slug")
        if not slug: continue
        vals = [round(t[p], 4) for p in periods_ordered if t.get(p) is not None]
        if len(vals) >= 3:
            sparklines[slug] = {"dates": periods_ordered[:len(vals)], "values": vals}
    with open(API / "sparklines.json", "w", encoding="utf-8") as f:
        json.dump(sparklines, f, ensure_ascii=False)
    print(f"✓ sparklines.json ({len(sparklines)})")

    with open(API / "alpha_beta.json", "w", encoding="utf-8") as f:
        json.dump({}, f)
    print("✓ alpha_beta.json (empty)")

    count = 0
    for t in themes:
        slug = t.get("slug")
        if not slug: continue
        tickers = [tk.strip() for tk in t.get("related", "").split(",") if tk.strip()]
        detail = {"slug": slug, "name": t["name"], "tickers": tickers, "prices": []}
        with open(DET / f"{slug}.json", "w", encoding="utf-8") as f:
            json.dump(detail, f, ensure_ascii=False)
        count += 1
    print(f"✓ theme_details/ ({count} files)")
    print(f"\nDONE → {API}")

if __name__ == "__main__":
    main()
