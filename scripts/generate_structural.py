#!/usr/bin/env python3
"""Generate structural analysis JSON from scraped zukai data for infographic page."""
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "research" / "stock_themes_data"
OUT = ROOT / "public" / "api" / "structural.json"

REPORT_NAMES = {
    "optical": "光接続", "server": "AIサーバー・ラック内",
    "semi_manufacturing": "半導体製造", "semi_equip": "半導体製造装置",
    "agentic_ai": "エージェントAI", "compute_providors": "AIコンピュート提供者",
    "dc_infra": "DC電源・熱管理", "dc_power": "DC電力供給",
    "dc_server": "サーバー", "natgas": "天然ガス",
    "oil_us": "石油：国内", "oil_global": "石油：グローバル",
    "nuclear": "原子力発電", "pe_credit": "プライベートクレジット",
    "software_enterprise": "エンタプライズSW", "software_specialized": "業種特化SW",
    "software_cyber": "サイバーセキュリティ", "space": "宇宙",
    "humanoids": "ヒューマノイド", "agg": "農業",
    "coal": "石炭", "rare_earth": "レアアース・リチウム",
    "construction": "インフラ建設", "finance_consumer": "消費者金融テック",
    "travel": "旅行",
}

def build():
    if not DATA.exists():
        print("No stock_themes_data, skipping"); return
    reports = []
    for rid, rname in REPORT_NAMES.items():
        tf = DATA / f"zukai_tickers_{rid}.json"
        cf = DATA / f"zukai_catalysts_{rid}.json"
        if not tf.exists(): continue
        td = json.load(open(tf))
        catalysts = []
        if cf.exists():
            for c in json.load(open(cf)).get("items", []):
                title = c.get("title", "")
                # Generate short label: extract key phrase
                short = title[:80]
                # Remove ticker references like *AVGO*
                import re
                short = re.sub(r'\*[A-Z]+\*', '', short)
                # Take first clause (before first particle/break)
                for sep in ["が", "で", "の", "を", "に", "は", "と", "――"]:
                    idx = short.find(sep)
                    if 3 < idx < 20:
                        short = short[:idx]
                        break
                if len(short) > 15:
                    short = short[:13] + "…"
                catalysts.append({"num": c["num"], "title": title[:80], "short": short.strip()})
        tickers = []
        for item in td.get("items", []):
            tk = item.get("ticker", "")
            if not tk: continue
            drivers = []
            for i in range(1, 11):
                cv = item.get(f"c{i}", "")
                if cv and cv.strip() != "-":
                    ch = cv.strip()[0]
                    sym = ch if ch in "◎○△" else "—"
                    score = {"◎": 1.0, "○": 0.5, "△": 0.25}.get(ch, 0)
                else:
                    sym = "—"; score = 0
                drivers.append({"sym": sym, "s": score})
            desc = (item.get("description") or "")[:120]
            tickers.append({"tk": tk, "drivers": drivers, "desc": desc})
        reports.append({"id": rid, "name": rname, "catalysts": catalysts,
                        "tickers": tickers, "n_tickers": len(tickers),
                        "n_catalysts": len(catalysts)})

    # Cross-ticker: which tickers appear in most reports
    tk_reports = defaultdict(list)
    for r in reports:
        for t in r["tickers"]:
            tk_reports[t["tk"]].append(r["name"])
    cross_tickers = [{"tk": tk, "count": len(reps), "reports": reps}
                     for tk, reps in sorted(tk_reports.items(), key=lambda x: -len(x[1]))
                     if len(reps) >= 2]

    output = {"reports": reports, "cross_tickers": cross_tickers[:30],
              "total_reports": len(reports), "total_tickers": len(tk_reports)}
    with open(OUT, "w") as f:
        json.dump(output, f, ensure_ascii=False)
    print(f"✓ structural.json: {len(reports)} reports, {len(tk_reports)} tickers, {OUT.stat().st_size/1024:.0f}KB")

if __name__ == "__main__":
    build()
