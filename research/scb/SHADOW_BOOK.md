# PRISM-Rα Shadow Book — Phase 2

## Status
- **Created:** 2026-04-10
- **Version:** A5-lite_shadow_v1
- **PRISM Live:** A4 (raw 1M) — DO NOT MODIFY
- **Shadow:** A5-lite (α63 × shrink(r²)) — OBSERVE ONLY
- **Freeze:** All parameters frozen until production promotion gate

## Frozen Parameters
```
alpha_window:     63 trading days
shrinkage:        r2<0.10 → r2×2 | 0.10≤r2≤0.50 → 0.20+(r2-0.10)×2 | r2>0.50 → 1.0
theme_score:      0.70×rank(mom63) + 0.30×rank(decel)
stock_score:      rank(α63) × shrink(r²_63)
top_themes:       10
picks_per_theme:  1
min_members:      4
sector_cap:       3 themes/sector (FROZEN — do not change to 2)
rebalance:        20 trading days
```

---

## Backtest Summary (124 days, 7 rebalances, research-only)
```
                        A4 (raw 1M)    A5-lite (α63×shrink)
CAGR                    78.8%          190.2%
Sharpe                  1.49           2.25
MaxDD                   -26.6%         -21.9%
Monthly diff:           +3.54%/mo (6/7 wins)
Overlap A4∩A5:          36% avg
Top-3 concentration:    A4=47%  A5=35%
```
**⚠ These numbers are research-only. Absolute CAGR must NOT be used as adoption evidence due to PIT contamination.**

---

## Q1: PIT (Point-in-Time) Governance

### Gold Layer: Forward PIT-safe archiving (MANDATORY)
- Script: `capture_snapshot.py`
- Run: every rebalance day (minimum), ideally daily
- Saves: `snapshots/membership_YYYYMMDD.csv`, `prices_YYYYMMDD.csv`, `ranking_YYYYMMDD.csv`
- Rule: `retrieved_at <= trade_date - 1営業日` のスナップショットのみ使用
- First snapshot: 2026-04-10

### Silver Layer: How to read existing backtest
- ✅ Read: A5-A4 stock selection effect (relative diff)
- ❌ Do NOT read: A5 CAGR 190% as absolute adoption evidence
- Rationale: A4 and A5 share the same frozen membership → diff is less contaminated than absolutes

### Bronze Layer: Membership stress tests (deferred to v2)
- Leave-one-member-out
- Leave-hub-out (3+ theme membership)
- Random deletion stress (500-1000 runs)
- Trigger: only if forward results are ambiguous

### Production evidence rules
- 2026-04-08以前: research-only
- 2026-04-09以降 (self-archived): forward admissible
- Production promotion: forward PIT-safe sample ONLY

---

## Q2: Tech Concentration Governance

### Current rule: sector_cap = 3 (FROZEN)
- Do NOT change to 2. Rationale:
  1. A4/A5 comparison is pure Layer 2 diff. Changing cap contaminates Layer 1.
  2. Long-only sector neutralization loses information (literature consensus).
  3. Tech dominance is likely α scale issue, not theme count issue.

### Required monitoring (each rebalance)
```
- tech_weight:           % of portfolio in Technology sector
- tech_contribution:     cumulative return contribution from Tech names
- tech_names_ratio:      Tech names / total names
- tech_share_of_diff:    (A5_tech - A4_tech) / (A5_total - A4_total)
- non_tech_only_diff:    A5_non_tech - A4_non_tech
```

### Alert thresholds (do NOT act, only flag)
- Rolling 6-rebalance avg active Tech weight > +10pt → FLAG
- Cumulative active return Tech share > 60% → FLAG
- non_tech_only_diff turns negative for 2 consecutive periods → FLAG

### v2 candidate (NOT now)
- α standardization: `score = (α63 / residual_vol63) × shrink` or `t_stat(α63) × shrink`
- Only after shadow freeze period ends

---

## Q3: Forward Observation & Promotion Gates

### Sign test framework (one-sided, H0: A5 ≤ A4)
```
Stage              Rebalances   Required wins   p (one-sided)   Decision
───────────────────────────────────────────────────────────────────────
Continue shadow    12           8-9             0.194/0.073     Observe only
Pilot candidate    12           10+             0.019           Small pilot OK
Production review  18           13+             0.048           Review starts
Strong production  24           17+             0.032           High confidence
```

### Economic gates (ALL must pass for production)
1. median monthly diff > +0.5%
2. cumulative active return > +8% (at 18 rebalances)
3. A5 MaxDD not worse than A4 by >5pt
4. No single month contributes >35% of cumulative edge
5. First-half and second-half both positive
6. Forward PIT-safe snapshot ONLY (no backtest evidence)
7. 25bps cost-adjusted edge still positive

### Timeline
- n=12 (~1 year): pilot eligibility
- n=18 (~1.5 years): production review
- n=24 (~2 years): strong confirmation
- Current: n=0 forward (shadow started 2026-04-10)

---

## Q4: Dispersion Re-introduction Protocol

### Current status: EXCLUDED from v1 score
### Re-introduction: NOT before 12 forward rebalances

### Pre-registered conditions (ALL required)
1. **Direction gate**: mean Spearman IC(disp21, next20d return) ≥ +0.03, positive in 8/12+ rebalances
2. **Economic gate**: Q5-Q1 next20d spread ≥ +1.0% avg, positive in 8/12+ rebalances
3. **Robustness gate**: effect survives ex-Tech and ex-Energy/Materials subsets
4. **Incremental gate**: A5+disp tie-breaker beats A5-alone on net return without DD worsening

### Re-introduction sequence (sequential, not parallel)
1. Phase D0: diagnostic logging only (current)
2. Phase D1: tie-breaker only (after 12 rebalances + all gates pass)
3. Phase D2: small weight ≤ 0.10 (after 6 more rebalances in D1)
4. Phase D3: full score component (after 6 more in D2)

### Required diagnostic logging (start now, every rebalance)
```csv
rebalance_date, theme, disp21, mom63, decel, next_20d_return
```

---

## Rebalance Log Schema

### Per-rebalance record (append to shadow_book_records.csv)
```csv
rebalance_date, signal_cutoff, version, theme, theme_rank,
ticker, A4_selected, A5_selected,
alpha63, r2_63, shrink_factor, raw_1m, score_a5,
sector, mc_bucket, weight, entry_px
```

### Per-rebalance summary (append to shadow_book_summary.csv)
```csv
rebalance_date, a4_names, a5_names, overlap_count, overlap_pct,
a4_gross, a5_gross, a4_net, a5_net, diff_gross, diff_net,
tech_weight_a4, tech_weight_a5, tech_contribution_diff,
non_tech_diff, fallback_count,
snapshot_id, pit_safe
```

---

## File Inventory
```
capture_snapshot.py          PIT-safe snapshot capture script
snapshots/                   Daily/rebalance membership+price+ranking archives
feasibility_test.py          P1+P2 backtest script
verify_parquet.py            Independent re-verification script
shadow_book_records.csv      Per-ticker selection log
shadow_book_summary.csv      Per-rebalance summary (TODO: generate)
theme_daily_panel.parquet    Long panel (244K rows, research-only)
ticker_meta.parquet          915 ticker metadata
feasibility_report.csv       Feasibility log
SHADOW_BOOK.md               This file
```
