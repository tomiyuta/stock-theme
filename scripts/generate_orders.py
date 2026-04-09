#!/usr/bin/env python3
"""A1+A2: Position Ledger + Orders generation for PRISM_MH20_CAP35."""
import json, csv, os
from pathlib import Path
from datetime import datetime, date

ROOT = Path(__file__).resolve().parent.parent
LEDGER_PATH = ROOT / "data" / "ledger" / "positions.json"
ORDERS_DIR = ROOT / "data" / "orders"
SIGNALS_PATH = ROOT / "public" / "api" / "prism" / "signals.json"
MIN_HOLD_DAYS = 20

def load_ledger():
    if LEDGER_PATH.exists():
        with open(LEDGER_PATH) as f:
            return json.load(f)
    return {"as_of_date": None, "positions": [],
            "metadata": {"strategy": "PRISM_MH20_CAP35", "schema_version": "v1"}}

def save_ledger(ledger):
    LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LEDGER_PATH, "w") as f:
        json.dump(ledger, f, indent=2, ensure_ascii=False)

def load_signals():
    with open(SIGNALS_PATH) as f:
        return json.load(f)

def generate_orders(today_str=None):
    if today_str is None:
        today_str = date.today().strftime("%Y-%m-%d")

    signals = load_signals()
    pp = signals.get("production_portfolio", {})
    target_weights = pp.get("weights", {})
    prices = pp.get("prices", {})
    sec_map = pp.get("sector_map", {})
    ledger = load_ledger()
    prev_date = ledger.get("as_of_date")

    # Build current holdings map
    held = {p["ticker"]: p for p in ledger["positions"] if p["status"] == "active"}

    # Advance holding_days for existing positions
    if prev_date and prev_date != today_str:
        for p in ledger["positions"]:
            if p["status"] == "active":
                p["holding_days"] = p.get("holding_days", 0) + 1
                p["eligible_to_exit"] = p["holding_days"] >= MIN_HOLD_DAYS

    orders = []
    new_positions = []

    # Process each target ticker
    target_equities = {tk: w for tk, w in target_weights.items() if tk != "SHV"}
    for tk, tw in target_equities.items():
        if tk in held:
            # Existing position
            orders.append({"date": today_str, "action": "HOLD", "ticker": tk,
                          "current_weight": held[tk].get("target_weight", 0),
                          "target_weight": tw, "delta_weight": 0,
                          "reason": "continuing", "min_hold_blocked": False,
                          "sector": sec_map.get(tk, ""), "theme": held[tk].get("theme_at_entry", "")})
        else:
            # New entry
            orders.append({"date": today_str, "action": "BUY", "ticker": tk,
                          "current_weight": 0, "target_weight": tw, "delta_weight": tw,
                          "reason": "new_entry", "min_hold_blocked": False,
                          "sector": sec_map.get(tk, ""), "theme": ""})
            new_positions.append({
                "ticker": tk, "status": "active", "entry_date": today_str,
                "entry_price": prices.get(tk, 0), "holding_days": 0,
                "target_weight": tw, "sector": sec_map.get(tk, ""),
                "theme_at_entry": "", "min_hold_days": MIN_HOLD_DAYS,
                "eligible_to_exit": False, "peak_price_since_entry": prices.get(tk, 0),
            })

    # Process held positions NOT in target
    for tk, pos in held.items():
        if tk not in target_equities and tk != "SHV":
            if pos.get("eligible_to_exit", False):
                orders.append({"date": today_str, "action": "SELL", "ticker": tk,
                              "current_weight": pos.get("target_weight", 0),
                              "target_weight": 0, "delta_weight": -pos.get("target_weight", 0),
                              "reason": "expired_and_removed", "min_hold_blocked": False,
                              "sector": pos.get("sector", ""), "theme": pos.get("theme_at_entry", "")})
                pos["status"] = "closed"; pos["exit_date"] = today_str; pos["exit_reason"] = "removed"
            else:
                orders.append({"date": today_str, "action": "HOLD", "ticker": tk,
                              "current_weight": pos.get("target_weight", 0),
                              "target_weight": 0, "delta_weight": 0,
                              "reason": f"min_hold_active({pos.get('holding_days',0)}d/{MIN_HOLD_DAYS}d)",
                              "min_hold_blocked": True,
                              "sector": pos.get("sector", ""), "theme": pos.get("theme_at_entry", "")})

    # Update ledger
    for p in new_positions:
        ledger["positions"].append(p)
    # Update weights for active positions
    for p in ledger["positions"]:
        if p["status"] == "active" and p["ticker"] in target_equities:
            p["target_weight"] = target_equities[p["ticker"]]
            px = prices.get(p["ticker"], 0)
            if px > p.get("peak_price_since_entry", 0):
                p["peak_price_since_entry"] = px
    ledger["as_of_date"] = today_str
    ledger["cash_proxy"] = {"ticker": "SHV", "weight": target_weights.get("SHV", 0)}

    # Save orders CSV
    ORDERS_DIR.mkdir(parents=True, exist_ok=True)
    orders_path = ORDERS_DIR / f"orders_{today_str}.csv"
    if orders:
        keys = ["date","action","ticker","current_weight","target_weight","delta_weight",
                "reason","min_hold_blocked","sector","theme"]
        with open(orders_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader(); w.writerows(orders)
    # Save ledger
    save_ledger(ledger)
    return orders, ledger

if __name__ == "__main__":
    import sys
    today = sys.argv[1] if len(sys.argv) > 1 else None
    orders, ledger = generate_orders(today)
    print(f"✓ Orders: {len(orders)} entries")
    for o in orders:
        blocked = " 🔒" if o.get("min_hold_blocked") else ""
        print(f"  {o['action']:5s} {o['ticker']:6s} {o['target_weight']:.1%} {o['reason']}{blocked}")
    active = [p for p in ledger["positions"] if p["status"]=="active"]
    print(f"✓ Ledger: {len(active)} active positions, as_of={ledger['as_of_date']}")
    print(f"  Cash proxy: SHV {ledger['cash_proxy']['weight']:.1%}")
