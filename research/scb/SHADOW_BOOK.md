# PRISM-Rα Shadow Book — Phase 2

## Status
- **Created:** 2026-04-10
- **Version:** A5-lite_shadow_v1
- **PRISM Live:** A4 (raw 1M) — DO NOT MODIFY
- **Shadow:** A5-lite (α63 × shrink(r²)) — OBSERVE ONLY
- **Freeze:** All parameters frozen until 6+ forward rebalances

## Frozen Parameters
```
alpha_window:     63 trading days
shrinkage:        r2<0.10 → r2×2 | 0.10≤r2≤0.50 → 0.20+(r2-0.10)×2 | r2>0.50 → 1.0
theme_score:      0.70×rank(mom63) + 0.30×rank(decel)
stock_score:      rank(α63) × shrink(r²_63)
top_themes:       10
picks_per_theme:  1
min_members:      4
sector_cap:       3 themes/sector
rebalance:        20 trading days
```

## Backtest Summary (124 days, 7 rebalances)
```
                        A4 (raw 1M)    A5-lite (α63×shrink)
CAGR                    78.8%          190.2%
Sharpe                  1.49           2.25
MaxDD                   -26.6%         -21.9%
Monthly diff:           +3.54%/mo (6/7 wins)
Overlap:                36% avg
```

## Additional Diagnostics

### A. Cost Sensitivity
```
        10 bps     25 bps     50 bps
A4      89.2%      85.4%      79.2%     (CAGR)
A5     214.9%     210.0%     202.0%     (CAGR)
Edge    +126%      +125%      +123%     (robust across all cost levels)
```

### B. Best-Month Dependence
```
All months:           +3.67%/mo  (6/7)
Excl best 1 month:    +2.68%/mo  (5/6)
Excl best 2 months:   +1.34%/mo  (4/5)
→ Edge survives removal of best months. Not single-outlier driven.
```

### C. Diff Attribution
```
By Sector:
  Technology:          +14.38%  ← dominant source
  Consumer Cyclical:    +6.66%
  Industrials:          +4.79%
  Comms:                +2.12%
  Healthcare:           -0.10%
  Basic Materials:      -0.63%
  Energy:               -1.56%

By Theme (top contributors to A5-A4 diff):
  semi-consumables:     +6.87%
  dc-optical-device:    +5.03%
  satellite-comms:      +4.51%
  dc-optical-semi:      +3.27%
  nextgen-batteries:    +3.24%
  (worst: gold-mining -2.56%, silver-mining -2.51%)

Top-3 Concentration:
  A4: 47%   A5: 35%  ← A5 is LESS concentrated (better diversified)
```

### D. Caveats
- n=7 rebalances: p≈0.063 (sign test), not statistically significant
- PIT problem: current theme membership used for historical backtest
- Technology sector dominance: if tech underperforms, A5 edge may narrow
- 252 days total history: insufficient for walk-forward / DSR

---

## Rebalance Log Template

Each monthly rebalance records:

```csv
rebalance_date,signal_cutoff,version,theme,ticker,strategy,alpha63,r2_63,shrink_factor,raw_1m_rank,sector,mc_bucket,weight,entry_px
```

Diff summary per period:
```csv
rebalance_date,a4_gross,a5_gross,a4_net,a5_net,diff_gross,diff_net,overlap_pct,new_names_pct,a4_names,a5_names
```
