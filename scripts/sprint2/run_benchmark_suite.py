#!/usr/bin/env python3
"""Sprint 2: Run BM2/BM3/BM5 vs PRISM v1 benchmark suite."""
import sys, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import pandas as pd
from replay_engine import SnapshotStore, PriceProvider, ReplayEngine, summarize_performance
from strategies import BM2SpyShvSwitch, BM3SectorRotation, BM5DirectStockMomentum, PrismV1Replay

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--snapshots", required=True)
    parser.add_argument("--outdir", default="output/benchmarks")
    parser.add_argument("--initial-capital", type=float, default=1_000_000.0)
    parser.add_argument("--bm3-top-n", type=int, default=3)
    parser.add_argument("--bm5-top-n", type=int, default=10)
    args = parser.parse_args()
    print("Loading prices..."); prices = PriceProvider(args.project_root)
    store = SnapshotStore(args.snapshots)
    engine = ReplayEngine(store, prices, initial_capital=args.initial_capital)
    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    strategies = [BM2SpyShvSwitch(), BM3SectorRotation(top_n=args.bm3_top_n),
                  BM5DirectStockMomentum(top_n=args.bm5_top_n), PrismV1Replay()]
    rows = []
    for strat in strategies:
        print(f"Running {strat.name}...")
        result = engine.run(strat)
        summary = summarize_performance(result["daily"], result["trades"])
        summary["Strategy"] = strat.name
        outdir.mkdir(parents=True, exist_ok=True)
        result["daily"].to_csv(outdir/f"{strat.name}_daily.csv", index=False)
        result["trades"].to_csv(outdir/f"{strat.name}_trades.csv", index=False)
        rows.append(summary)
        eq = result["daily"]
        if not eq.empty: print(f"  → ${eq['equity'].iloc[0]:,.0f} → ${eq['equity'].iloc[-1]:,.0f}")
    df = pd.DataFrame(rows)[["Strategy","CAGR","MaxDD","Sharpe_daily","Turnover_trades","AvgHoldingDays_proxy","WorstDay"]]
    df.to_csv(outdir/"benchmark_summary.csv", index=False)
    print("\n"+"="*90); print(df.to_string(index=False)); print("="*90)

if __name__ == "__main__":
    main()
