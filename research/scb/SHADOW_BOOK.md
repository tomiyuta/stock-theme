# PRISM-Rα Shadow Book — Phase 2

## Status
- **Created:** 2026-04-10
- **Last updated:** 2026-04-10 (post-5yr extended backtest + ChatGPT review integration)
- **Version:** A5-lite_shadow_v1
- **PRISM Live:** A4 (raw 1M) — DO NOT MODIFY
- **Shadow:** A5-lite (α63 × shrink(r²)) — OBSERVE ONLY
- **Freeze:** All parameters frozen until production promotion gate
- **Classification:** ~~A4の上位互換~~ → **Tech優位局面で効きやすい、正の歪みを持つ、やや集中型のresidual-alpha overlay**

## Frozen Parameters
```
alpha_window:     63 trading days
shrinkage:        r2<0.10 → r2×2 | 0.10≤r2≤0.50 → 0.20+(r2-0.10)×2 | r2>0.50 → 1.0
theme_score:      0.70×rank(mom63) + 0.30×rank(decel)
stock_score:      rank(α63) × shrink(r²_63)
top_themes:       10
picks_per_theme:  1
min_members:      4
sector_cap:       3 themes/sector (FROZEN)
rebalance:        20 trading days
```

---

## Extended Backtest (5 years, 73 rebalances, research-only)

### Data
```
Source:     Norgate Data Platinum (Total Return Adjusted Close)
Period:    2020-01-02 ~ 2026-04-09 (1,575 trading days)
Tickers:   846/847 (99.9% coverage, TMRC only failure)
Panel:     1,444,411 rows
PIT:       FROZEN membership (2026-04-08) projected backward — contaminated
```

### Performance
```
                        A4 (raw 1M)    A5-lite (α63×shrink)
CAGR                    50.4%          67.3%
Sharpe                  1.29           1.41           (+0.12)
MaxDD                   -39.7%         -42.0%         (-2.3pt WORSE)
Monthly diff:           +1.13%/mo (37/70=53%, p=0.360 NOT SIGNIFICANT)
Year wins:              6/7 (2020-2024 + 2026 A5勝, 2025 A4勝)
Half-split:             H1=+1.50% H2=+0.77% (BOTH+, but decaying)
```

### Turnover Audit (corrected per Doc 11 review)
```
CRITICAL FIX: Previous cost calculation used cross-sectional overlap (42%).
              Correct measure is time-series turnover per strategy.

                    A4          A5-lite
Time-series TO:     93%/rebal   83%/rebal   ← A5 is LESS churny
Cross-sect overlap: 43% (misleading — not the right metric for costs)

Corrected total costs (72 rebalances):
  @10bps:           13.4%       11.9%
  @25bps:           33.4%       29.8%
  @50bps:           66.8%       59.5%

Note: A5 has lower turnover than A4 — residual α picks are more persistent.
      But both strategies have very high turnover (~85-93% per 20-day rebalance).
```

### Sector Attribution (net active return)
```
Tech share of net active: ~76% (NOT 43% as previously reported)
  正の寄与合計: +132.10%
  負の寄与合計:  -57.46%
  純差分:        +74.64%
  うちTech:      +56.81% → 76% of net

Non-Tech only diff: +0.24%/period (56% positive) → weak but not zero
```

### Concentration
```
A4 top5: 26% (VKTX, APP, AREC, HUT, MARA)
A5 top5: 38% (APLD, APP, ONDS, PLTR, VKTX)
→ A5 is MORE concentrated (reversed from 1-year test)
```

### 1-Year vs 5-Year Comparison (expectation calibration)
```
                    1yr (7 rebals)    5yr (73 rebals)    Direction
Sharpe improvement:  +0.76            +0.12              ↓ shrank 6x
Monthly diff:        +3.54%           +1.13%             ↓ shrank 3x
Monthly win rate:    86%              53%                ↓ shrank to near-random
MaxDD:               A5 better        A5 worse           ↓ reversed
Concentration:       A5 better        A5 worse           ↓ reversed

→ 1-year test was significantly over-optimistic.
  Realistic expectation: +1%/mo, ~55% win rate, with higher vol and DD.
```

---

## Revised Assessment (post-5yr + ChatGPT review)

### What survived
- ✅ A5-lite selects genuinely different stocks (58% different from A4)
- ✅ Residual α concept is not dead — direction positive across 6/7 years
- ✅ Effect survives best-month removal
- ✅ Feasibility is perfect (0% fallback, 0 zero-pick)
- ✅ A5 has LOWER turnover than A4 (more persistent picks)

### What collapsed
- ❌ "Clean upgrade over A4" — no, it's a higher-risk variant
- ❌ "DD improvement" — reversed over 5 years
- ❌ "Better diversification" — reversed, A5 is more concentrated
- ❌ "Statistically significant" — p=0.360, not even close

### Correct characterization
```
A5-lite = "低〜中勝率だが勝つときに大きい、Tech偏重の残差α overlay"
         NOT "毎月コツコツ勝つ改善策"
```

### PIT non-symmetry warning (Doc 11)
```
A5 uses theme-ex-self return as OLS regressor.
→ membership errors affect α and R² estimation directly.
→ PIT contamination is LARGER for A5 than for A4.
→ A5's advantage may be partially inflated by PIT.
This cannot be resolved retroactively — only forward PIT-safe data can settle it.
```

---

## Q1: PIT Governance (unchanged + reinforced)

### Gold Layer: Forward PIT-safe archiving (MANDATORY)
- Script: `capture_snapshot.py`
- First snapshot: 2026-04-10
- Rule: `retrieved_at <= trade_date - 1営業日` only

### Silver Layer: Reading rules
- ✅ Read: A5-A4 relative diff only
- ❌ Do NOT use absolute CAGR/Sharpe as evidence
- ⚠ NEW: PIT asymmetry means even relative diff is partially contaminated for A5

### Evidence rules
- 2026-04-08以前: research-only (both 1yr and 5yr tests)
- Forward PIT-safe only for production decisions

---

## Q2: Tech Concentration Governance (updated)

### sector_cap = 3 (FROZEN, do not change)
### NEW monitoring requirement from 5yr results:

```
Required each rebalance:
  tech_weight
  tech_contribution
  tech_share_of_net_active (target: < 60%)
  non_tech_only_diff (target: ≥ 0)
  top5_contribution_share (target: < 50%)
  single_name_max_contribution (target: < 25% of cumulative diff)
```

### Alert thresholds (flag, do not act)
- Rolling 6-rebal avg Tech share of net active > 60% → FLAG
- non_tech_only_diff negative for 2 consecutive → FLAG
- top5 share > 50% → FLAG

### v2 candidate (research backlog, NOT now)
- Priority 1: `score = (α63 / residual_vol63) × shrink` or `t_stat(α63) × shrink`
  → Directly addresses "raw α favors high-vol sectors" problem
- Priority 2: A4/A5 blend (50/50 or A4 core + A5 overlay)
  → More natural production form than full replacement
- Priority 3: Bronze membership stress tests

---

## Q3: Forward Promotion Gates (updated with 5yr lessons)

### Sign test framework (unchanged)
```
12 rebals: pilot candidate (10+ wins needed)
18 rebals: production review (13+ wins needed)
24 rebals: strong confirmation (17+ wins needed)
```

### Economic gates (ALL must pass — 3 NEW from 5yr results)
1. median monthly diff > +0.5%
2. cumulative active return > +8% (at 18 rebalances)
3. A5 MaxDD not worse than A4 by >5pt
4. No single month contributes >35% of cumulative edge
5. First-half and second-half both positive
6. Forward PIT-safe snapshot ONLY
7. 25bps cost-adjusted (using TIME-SERIES turnover) edge still positive
8. **NEW: Tech share of net active return < 60%**
9. **NEW: Non-Tech active diff ≥ 0**
10. **NEW: Top-5 active contributor share < 50%**

---

## Q4: Dispersion Re-introduction (unchanged, reinforced)

Status: EXCLUDED. 5yr results make early re-introduction even less justified.
Minimum wait: 12 forward rebalances + all pre-registered gates.
Sequence: diagnostic → tie-breaker → small weight → full component.

---

## File Inventory
```
SHADOW_BOOK.md                    This governance document
capture_snapshot.py               PIT-safe snapshot archiver
snapshots/                        Daily membership+price+ranking archives
feasibility_test.py               P1+P2 original (1yr) backtest
verify_parquet.py                 Independent re-verification (1yr)
backtest_extended.py              Extended (5yr) backtest with Norgate
norgate_fetch.py                  Windows data extraction script
norgate_theme_panel.parquet       5yr panel (1.4M rows, Norgate)
norgate_coverage_report.txt       846/847 coverage report
theme_daily_panel.parquet         1yr panel (244K rows, stock-themes.com)
ticker_meta.parquet               915 ticker metadata
ticker_list_847.txt               Target ticker list
theme_membership_frozen.json      Frozen membership definition
feasibility_report.csv            1yr feasibility log
shadow_book_records.csv           Shadow book initial record (2026-03-31)
NORGATE_INSTRUCTIONS.md           Windows extraction instructions
```
