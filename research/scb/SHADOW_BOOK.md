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
stock_score:      α63 × shrink(r²_63)
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

### Gate 11 (candidate, not mandatory)
```
Cost-stressed active diff remains ≥ 0 at 100bps
Rationale: theme/small-cap stocks have higher real-world slippage than 50bps.
Status: supplementary gate, not blocking.
```

---

## Turnover & Cost Definitions (fixed per Doc 11/13 audit)

```
one_way_turnover_t = 0.5 × Σ_i |w_target,i,t − w_pretrade,i,t|
  (fraction of portfolio traded, 0=no change, 1=full replacement)

avg_turnover = mean(one_way_turnover_t across all rebalances)
cum_turnover = Σ_t one_way_turnover_t

cost_drag_t = 2 × tc_one_side × one_way_turnover_t
  (round-trip cost per rebalance)

Measured values (5yr, 72 rebalances):
  A4 avg time-series turnover: 93%/rebal
  A5 avg time-series turnover: 83%/rebal
  Cross-sectional overlap A4∩A5: 43% (NOT used for cost — different metric)

Total cost drag (cumulative, 72 rebalances):
  @10bps round-trip: A4=13.4%  A5=11.9%
  @25bps round-trip: A4=33.4%  A5=29.8%
  @50bps round-trip: A4=66.8%  A5=59.5%
  These are cumulative cost deductions from gross return, NOT CAGR.
```

---

## Forward Shadow Monitoring Checklist (each rebalance)

```
1. realized_active_diff (A5_net - A4_net)
2. A4_turnover / A5_turnover (time-series, NOT cross-sectional)
3. 25bps and 50bps net active diff
4. cumulative Tech share of net active return
5. rolling 6-rebalance active Tech weight
6. non_tech_only_diff
7. top5_active_contributor_share
8. single_name_max_contribution (% of cumulative diff)
9. A5_MaxDD vs A4_MaxDD
10. snapshot_id + pit_safe flag
```

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

---

## ERRATA — Score Definition (2026-04-10)

```
Historical note:
The implementation has consistently used:
  stock_score = α63 × shrink(r²_63)

This is confirmed in:
  - backtest_extended.py line 103: s5[tk] = a63*shrk
  - generate_prism_r.py line 148: score5 = a63*shrk

The wording "rank(α63) × shrink(r²_63)" appearing in earlier versions of
meta.json, SHADOW_BOOK.md, and related text was a documentation error only.
No backtest or shadow-book result was generated from a rank(α)-based model.

Effective correction (2026-04-10):
All documentation and metadata now use: α63 × shrink(r²_63)
```

---

## Order Evaluation Cadence (added 2026-04-10)

```
Routine rebalance evaluation: every 20 business days
Off-cycle sell evaluation: none (current implementation)
Manual override: only for data integrity / delisting / execution impossibility

Interpretation:
MinHold 20 is binding only for exits evaluated before 20 business days have elapsed.
Under the current cadence, MinHold may be non-binding for routine rebalance exits
and binding mainly for exceptional same-cycle removals (e.g. VALE/RIO day-0 case).
```

---

## Tech Concentration Interpretation (corrected 2026-04-10)

```
Current figure: Tech share of net active return = 76%

This measures RETURN CONCENTRATION, not ALLOCATION CONCENTRATION.
76% of A5's net active return was attributable to Technology.
This must NOT be interpreted as a 76% portfolio weight allocation to Technology.

Sector tension must be evaluated separately using:
  - active_tech_weight (portfolio allocation)
  - tech_name_count (how many Tech names held)
  - blocked_tech_weight (blocked positions in Tech)
  - sector_cap_suppression_count (how often Tech new buys were blocked)
```

---

## Forward Monitoring Additions (2026-04-10)

Additional items for each rebalance (appended to existing checklist):
```
11. minhold_block_count
12. minhold_blocked_weight
13. fraction_of_rebalances_where_minhold_is_binding
14. active_tech_weight (allocation, not return)
15. tech_name_count
16. blocked_tech_weight
17. sector_cap_suppression_count
```


---

## Research Priority: A5-SNR and Beyond (confirmed 2026-04-10)

### Decision Context

stock-themes.com の α/β 逆推定により、以下が確定:
- stock-themes は自己除外なしの OLS（β/R² が 10-15% 過大推定）
- PRISM-R の自前 OLS は自己除外ありで推定汚染が少ない
- 現行 A5-lite の弱点は raw α の大きさに引っ張られること
- r² は α の信頼性ではなく回帰全体の説明力
- 次に磨くべきは「大きい α」ではなく「信頼できる α」

詳細: `research/stock_themes_alpha_beta_analysis.md`

### Confirmed Research Phases

```
Phase 1a: A5-SNRa
  alpha_cum63 = α̂_daily × 63
  resid_vol63 = std(ε_daily) × √63
  score = alpha_cum63 / resid_vol63
  （shrink(r²) なし — 二重ペナルティ回避）

Phase 1b: A5-SNRb
  score = (alpha_cum63 / resid_vol63) × shrink(r²)
  （shrink の追加効果を分離検証）

Phase 2: A5-Quality
  noise_path = √(Σ ε_t²)
  quality = |alpha_cum63| / (|alpha_cum63| + noise_path)
  score = alpha_cum63 × quality
  （signed residual sum ではなく path noise で測定）

Phase 3: A5-T（診断用のみ、独立候補ではない）
  score = alpha_cum63 × clip(|tα|/3, 0, 1)
  plain OLS SE → HAC/NW は diagnostic 比較に留める
  （63日窓では α/resid_vol と α/SE(α) はほぼ同順序）

v3+: HybridBench / Dynamic β / Kalman
```

### Branch Governance

```
A4:          live production
A5-lite:     shadow（凍結維持、independent forward clock）
A5-SNRa/b:  research-shadow（別 clock、spec freeze 後に n カウント開始）
A5-Quality:  research-shadow（別 clock）

原則:
  - A5-lite の証拠で A5-SNR を昇格させない
  - A5-SNR の証拠で A5-Quality を正当化しない
  - 各枝は spec freeze 後に別の n カウントを開始
  - production 判断は PIT-safe forward shadow のみ
```

### Historical BT Protocol (direction check only)

```
必須確認項目（方向性のみ）:
  - A5-SNRa vs A5-lite: median 月次 diff の符号
  - A5-SNRa vs A5-lite: Tech share of net active return の変化方向
  - A5-SNRa vs A5-lite: Top5 集中度の変化方向
  - A5-SNRa vs A5-lite: non-Tech active diff の変化方向
  - A5-SNRa vs A5-lite: MaxDD の悪化/改善

確認しないもの（PIT 汚染のため）:
  - 絶対 CAGR / Sharpe
  - 統計的有意性（p 値）
  - パラメータ最適化
```

### stock-themes.com Data Usage Rules

```
使ってよい（auditor / confirmer として）:
  ✅ sign agreement（自前 α と stock-themes α の符号一致）
  ✅ 3M/6M/12M の α 符号安定性
  ✅ alpha_tval による外部確認
  ✅ theme_factor / individual_factor の帰属分析

使ってはいけない（selector として）:
  ❌ stock-themes α をそのまま ranking に使用
  ❌ self-inclusive β/R² を confidence として主利用
  ❌ individual_factor を ex-ante alpha と解釈

stock-themes = selector ではなく auditor / confirmer
```


---

## Audit Addendum — Research Branch Selection (2026-04-10)

Status: governance-relevant addendum
Effect on frozen A5-lite spec: none

### Decision

```
A5-SNRb:    primary research-shadow（採用）
A5-Quality: secondary / parked（2021年赤旗未解消）
A5-SNRa:    rejected（過剰ペナルティ）
```

### Key Findings from Historical Diagnostics

#### 1. MaxDD窓は別エピソード

```
A5-lite:   -42.0%  Peak 2021-11-08 → Trough 2022-09-26（2022年ベアマーケット）
A5-SNRb:   -37.1%  Peak 2025-02-14 → Trough 2025-03-10（2025年2月急落）
A5-Quality:-37.1%  Peak 2025-02-14 → Trough 2025-04-08（同上、回復遅延）
```

解釈: SNRb/Qualityは2022年DDを軽減した結果、2025年の小DDが新MaxDDになった。
「同じクラッシュを浅く避けた」のではなく「別のクラッシュが新たなMaxDD」。

#### 2. Tech share未改善

```
A5-lite:  Tech share of return = 39.3%
A5-SNRb:  Tech share of return = 39.9%（ほぼ同等）
```

SNRbの改善はTech依存低下ではなく、volatility/concentration/DD profileの改善。

#### 3. Top5集中度はSNRbで改善

```
A5-lite:   Top5 = 38.3%
A5-SNRb:   Top5 = 34.1%（-4.2pt改善）
A5-Quality:Top5 = 41.3%（+3.0pt悪化）
```

#### 4. Quality 2021年はAPLD 1銘柄依存（赤旗）

```
Quality 2021 (+165%):
  Top-1: APLD = 58.7%
  Top-3: 70.3%
  Top-5: 79.2%
```

Qualityをprimaryに昇格させる根拠は現時点でない。

### Branch Labels（公式）

```
A5-lite:    return-seeking residual-alpha selector
A5-SNRb:    risk-normalized residual-alpha alternative（primary research-shadow）
A5-Quality: path-smoothness-biased, regime-sensitive candidate（parked secondary）
A5-SNRa:    rejected（over-penalization）
```

### Governance Implications

1. A5-SNRb を次の primary research-shadow として承認
2. A5-Quality は secondary のまま保留（forward clock 開始しない）
3. A5-SNRa は棄却
4. A5-lite の凍結仕様・昇格時計・ゲートは変更しない
5. Historical evidence は方向確認のみ（PIT汚染のため）

### 重要な位置づけ

全候補がA5-liteに対してmedian月次diffで非優位。
したがってこれらはA5-liteの上位互換ではなく、
payoff distributionを変える枝（リスク調整版）として扱う。

```
A5-lite  = typical month champion
A5-SNRb  = risk-shaped sibling
A5-Quality = regime-sensitive convex branch（parked）
```

Any research branch introduced after A5-lite must run on an independent
forward clock and must not reset, contaminate, or reinterpret the frozen
A5-lite shadow record.


---

## Appendix R3 — BFM-v1 Research Branch Governance (2026-04-10)

Status: spec-frozen research branch
Effect on A5-lite / A5-SNRb: none

### Purpose

Layer 1（テーマ選定）の quality 改善。
「強いテーマ」ではなく「良い強さのテーマ」を選ぶ。
Layer 2 は A5-SNRb を固定し、Layer 1 単独差分を検証する。

### Score Definition (frozen)

```
入力特徴量（theme-details/日次価格から計算）:
  R63:               63営業日テーマ累積リターン
  R126:              126営業日テーマ累積リターン
  decel:             直近過熱剥落（現行PRISM互換）
  breadth63:         63日リターン正の構成銘柄比率
  breadth_persist63: 63日間の日次平均参加率
  concentration63:   テーマ内上位寄与のHerfindahl指数
  theme_vol63:       テーマ日次ボラ × √252

スコア:
  TrendBlock    = mean(rank(R63), rank(R126), rank(decel))
  BreadthBlock  = mean(rank(breadth63), rank(breadth_persist63))
  FragilityBlock = mean(rank(concentration63), rank(theme_vol63))

  BFM_score = TrendBlock + BreadthBlock - FragilityBlock

選定: BFM_score上位10テーマ / sector_cap=3 / greedy
```

### Backtest Design

```
比較対象（Layer 1差分のみ）:
  Base: current Layer 1 + A5-SNRb
  BFM:  BFM-v1 Layer 1  + A5-SNRb

Layer 2, Layer 3, EXIT CONSTITUTION は完全固定。
```

### Historical で見るもの / 見ないもの

```
見る（方向確認のみ）:
  - median月次active diffの符号
  - Top5 active contributor shareの変化方向
  - Tech share of active returnの変化方向
  - Non-Tech active diff
  - MaxDD episodeの変化方向
  - selected theme breadth/concentration統計

見ない:
  - 絶対CAGR / 絶対Sharpe
  - p値による採否
  - パラメータ最適化
```

### Branch Management

```
A4:          live
A5-lite:     frozen shadow（independent clock）
A5-SNRb:     primary research-shadow（independent clock）
BFM-v1:      Layer 1 research branch（independent clock、spec frozen）
```


### BFM-v1 Failure Memo (2026-04-10)

```
BFM-v1 は Layer 1 full replacement として棄却。
CAGR -32pt / theme overlap 28% → 「別戦略化」。
breadth/fragility を主役にすると高成長テーマを体系的に排除。
重み調整では救済しない。設計思想を変更し BFM-v2 へ。
```

### BFM-v2: Quality Filter 型（spec frozen）

```
役割: Layer 1 の品質フィルタ（主スコア置換ではない）
設計: 強いテーマを残したまま「悪い強さ」だけ除外

Stage 1: 現行テーマスコアで上位25テーマを仮採用
Stage 2: 以下を除外（veto型）
  - breadth63 が候補群内 下位30%
  - concentration63 が候補群内 上位20%
  - theme_vol63 が候補群内 上位20%
Stage 3: 残った中からscore_base上位10テーマを採用

Layer 2: A5-SNRb固定
```


### BFM-v2 Decision Memo (2026-04-10)

```
Status: risk-adjusted pass（Layer 1 replacement としては未確定）
Role:   risk-managed Layer 1 alternative

Historical結果 vs Base(Current-L1 + A5-SNRb):
  CAGR:    -14.0pt ❌  (57.8% → 43.8%)
  Vol:     -12.0pt ✅  (39.3% → 27.4%)
  Sharpe:  +0.130  ✅✅ (1.471 → 1.601) ← 初のSharpe改善候補
  Sortino: +0.007  ≒
  Calmar:  +0.269  ✅✅ (1.561 → 1.829)
  MaxDD:   +13.1pt ✅✅ (-37.1% → -23.9%)
  Theme overlap: 47%（BFM-v1の28%から回復）
  Avg vetoed: 11.9/25テーマ
  Median月次diff: -0.0049 ↓（典型月ではBaseが優位）

解釈:
  - Sharpe/Calmar/MaxDD改善を達成した最初の候補
  - ただし典型月では負け、upside captureも削っている
  - Base+SNRbの上位互換ではなく、低リスク分岐
  - BFM-v1→v2で設計変更（主スコア→品質フィルタ）が正しかった証拠

ラベル:
  BFM-v2 = risk-managed Layer 1 alternative（research-shadow）
  BFMv2_research_clock = independent（A5-SNRb/A5-liteを汚染しない）
```


---

## Session Close — Final Status (2026-04-10)

### Official Labels (confirmed by 12 ChatGPT reviews)

```
A4:          live core
A5-lite:     return-seeking residual-alpha selector（frozen shadow）
A5-SNRb:     risk-normalized residual-alpha alternative（primary research-shadow, forward clock active）
BFM-v2:      risk-managed Layer 1 alternative（research-shadow候補, Sharpe+0.13/Calmar+0.27/MaxDD+13pt）
A5-Quality:  path-smoothness-biased（secondary, parked）
A5-SNRa:     rejected（over-penalization）
BFM-v1:      rejected（over-defensive）
```

### Next Actions (priority order, confirmed)

```
1. BFM-v2 forward clock開始（別clock）
2. Theme Correlation Budget 診断実装（制約ではなくログ）
3. CRA-v1（stock-themes confirmation overlay）
4. Dip Sleeve
5. Continuity Filter
6. Vol overlay
```

### Not To Do

```
- A5-lite の再定義
- BFM-v2 の production 昇格
- A5-SNRb の追加tuning
- stock-themes α/β の ranking利用
- 3本以上の新枝を同時に走らせること
- HybridBench / Kalman
```


### Theme Correlation Budget — Alert Thresholds (provisional, 2026-04-10)

```
暫定アラート閾値（実務用、文献閾値ではない）:
  effective_n_themes < 4.0     → concentration alert
  max_pairwise_corr > 0.85     → severe overlap alert
  high_corr_pairs / all_pairs > 10%  → cluster review
  cluster_weight_top1 > 40%    → diversification warning

初回診断値（2026-04-09スナップショット）:
  effective_n_themes = 2.9     ⚠ ALERT（<4.0）
  max_pairwise_corr = 0.928   ⚠ SEVERE（>0.85）
  n_high_corr_pairs = 7/45    ⚠ 15.6%（>10%）
  avg_pairwise_corr = 0.277

解釈:
  10テーマ保有でも実質2.9独立ベット。
  max corr 0.928は同一潜在因子の重複保有を示唆。
  診断継続し、3-6リバランス蓄積後にsoft cluster budgetを検討。
```


### CRA-v1 Decision Memo (2026-04-10)

```
Status: marginal / parked（SNRbとの差異が微小）

Historical結果 vs SNRb:
  全指標で差 < 0.02。Stock overlap 92%。
  confirmationの影響が微小な理由:
    - stock-themes α/tvalは静的スナップショット（rolling更新なし）
    - 大半の銘柄で既にsign agreementが成立
    - 0.70 + 0.30×audit の重みではランキング逆転が稀

判定:
  CRA-v1は理論的には妥当だが、現行データでは実質的な改善がない。
  premium APIの定期再取得が可能になった場合に再評価。
  現時点ではparked。forward clockは開始しない。
```


### Dip Sleeve Status (2026-04-10)

```
Status: diagnostic only（forward観測中、取引なし）
実装: generate_prism_r.pyに組込み済み、CI/CD自動更新

フィルタ条件:
  - テーマがENTRY or WATCH（trend_rank ≤ 35）
  - win_rate ≥ 60%
  - sample_n ≥ 20
  - window ∈ {1M, 1-2M, 2-3M}
  - days_since ≤ 60

初回結果（2026-04-09）:
  Total: 18 alerts / Qualified: 4
  銀鉱山 (rank=24, WATCH): 3件
  金鉱山 (rank=26, WATCH): 1件
  → 貴金属テーマに集中

制約: dip_alertsは静的スナップショット。
       historical BTは不可。forward診断のみ。
       premium APIの定期再取得が必要。
```


### Continuity Filter Status (2026-04-10)

```
Status: diagnostic only（tie-breaker候補、単独戦略化しない）
実装: generate_prism_r.pyに組込み済み、CI/CD自動更新

指標:
  sign_consistency: スパークライン方向転換の少なさ（1.0=完全単調）
  monotonic_ratio:  上昇週の比率
  jumpiness:        週次変化の標準偏差/平均（低い方が滑らか）
  alpha_sign_positive: stock-themes 7期間α中の正符号数

初回結果（2026-04-10）:
  MTZ:  sign=0.58 mono=0.68 jump=2.3 α+=7/7 ← 最高品質
  PARR: sign=0.60 mono=0.60 jump=3.8 α+=7/7
  GEV:  sign=0.54 mono=0.68 jump=2.5 α+=6/7
  PWR:  sign=0.46 mono=0.70 jump=2.3 α+=2/7 ← 要注意
  VZ:   sign=0.44 mono=0.60 jump=3.2 α+=4/7 ← 要注意

用途: selector ではなく veto/tie-breaker に留める
```

### Vol Overlay Status (2026-04-10)

```
Status: diagnostic only（shadow overlay、gross調整は未適用）
実装: generate_prism_r.pyに組込み済み、CI/CD自動更新

初回結果（2026-04-10）:
  realized_vol_20d = 0.378（37.8%）
  realized_vol_63d = 0.369（36.9%）
  target_vol = 0.25（25%）
  gross_scale_20d = 0.662 ← 33.8%縮小が示唆
  gross_scale_63d = 0.678 ← 32.2%縮小が示唆
  would_reduce = true

解釈:
  現在のポートフォリオは target_vol 25% を大幅に上回っている。
  Vol overlay を適用した場合、gross を ~66% に縮小する必要がある。
  ただし現時点では診断のみ。実gross調整はforwardデータ蓄積後に検討。
```


### Appendix R5 — Continuity Filter Governance (2026-04-11)

```
Role:   quality filter / tie-breaker（独立alpha branchではない）
Status: diagnostic only, ENABLE_CONTINUITY_ACTIVE = False
Clock:  独立。選定に影響する場合は新clock開始。
理論根拠: Frog-in-the-Pan — gradual informationはdiscrete informationより
          強く持続的なcontinuationを生みやすい。

指標:
  sign_consistency:     スパークライン方向転換の少なさ
  monotonic_ratio:      上昇週の比率
  jumpiness:            週次変化のCV（低い方が滑らか）
  alpha_sign_positive:  stock-themes 7期間α中の正符号数
```

### Appendix R6 — Vol Overlay Governance (2026-04-11)

```
Role:   risk-management overlay（独立alpha branchではない）
Status: diagnostic only, ENABLE_VOL_ACTIVE = False
Clock:  独立。grossを変更する場合は新clock開始。
理論根拠: Moreira-Muir — realized volatilityが高い時に
          エクスポージャーを落とすとSharpeが改善しうる。

数式:
  gross_t = min(gross_base, target_vol / realized_vol_t)
  target_vol = 25%
  realized_vol = 20日 or 63日
```


---

## PRISM-RQ Naming (2026-04-11)

```
PRISM → PRISM-R → PRISM-RQ

  PRISM:    Layer 1 (theme momentum) + Layer 2 (raw 1M return)
  PRISM-R:  Layer 1 (同上) + Layer 2 (α63 × shrink(r²))
  PRISM-RQ: Layer 1 (BFM-v2品質フィルタ) + Layer 2 (A5-SNRb residual-SNR)
            R = Residual（SNRb継承）
            Q = Quality（BFM-v2品質フィルタ）

構造:
  Layer 1: score = 0.70×rank(mom63) + 0.30×rank(decel) → top25 → BFM-v2 veto → top10
  Layer 2: score = (alpha_cum63 / resid_vol63) × shrink(r²)
  Layer 3: 10銘柄 × 等ウェイト × 20営業日リバランス

統治レイヤー（診断中）:
  - Theme Correlation Budget
  - Continuity Filter (ENABLE=False)
  - Vol Overlay (ENABLE=False)
  - CRA-v1 (parked)
  - Dip Sleeve (diagnostic)
```


---

## GMAX-K3 Ablation BT Results (2026-04-11)

```
===============================================================================================
Strategy             CAGR      Vol   Sharpe    MaxDD    LogGr    Term$   WorstM
===============================================================================================
  G0_A5lite        67.2%   42.9%   1.565  -42.0%   0.514    19.1x  -16.9%
  G1_conc5        145.3%   66.7%   2.179  -48.7%   0.897   172.8x  -18.5%
  G2_rawA         179.1%   69.7%   2.568  -54.7%   1.026   362.5x  -27.7%
  G3a_kelly        64.6%   41.0%   1.576  -39.3%   0.498    17.5x  -17.3%
  G3b_3theme       66.0%   52.9%   1.248  -49.7%   0.507    18.4x  -24.8%
  G4_klsize        36.6%   43.7%   0.838  -51.9%   0.312     6.0x  -22.3%
  G5_panic         36.7%   43.7%   0.840  -51.9%   0.313     6.0x  -22.3%
===============================================================================================

Ablation効果:
  G0→G1: 10→5テーマ+corr budget  = +78.1pt CAGR ← 最大改善源
  G1→G2: shrink除去→raw α        = +33.8pt CAGR ← 二番目
  G2→G3a: raw α→α/σ²             = -114.5pt     ← 壊滅
  G3a→G3b: 5→3テーマ              = +1.4pt       ← 無効
  G3b→G4: Kelly-lite sizing       = -29.4pt      ← 大幅悪化
  G4→G5: panic de-gearing         = +0.1pt       ← 無効

結論:
  CAGR最大化の本体 = 「集中」+「raw α右裾の保存」
  Kelly的正規化(α/σ², variance sizing) = このuniverseでは全て逆効果
  理由: noisy短期α推定をσ²で割ると高conviction winnerを二重に罰する

公式ラベル:
  G2_CAGR_MAX = concentrated cluster-aware raw residual momentum
  位置づけ: pure growth branch（A5-SNRbのrisk-adjusted branchとは別管理）
```

### 次の検証候補（未実施）

```
Z2: Layer 1をR252/R126/R63 trend stackに置換
Z3: テーマ数 4/5/6 sweep
Z4: 銘柄数/テーマ 1/2/3 sweep
Z5: holding period 10/20/40日
```


---

## W5 Consistency Weighting Decision Memo (2026-04-12)

### 判定: B — shadow並走（即採用しない）

```
ChatGPT #1: "economically plausible, statistically unproven" → B (shadow)
ChatGPT #2: "本物の可能性が高いが production に上げるには早い" → B (shadow)
```

### BT結果

```
W0 等ウェイト:   CAGR=179.1%  Sharpe=2.847  MaxDD=-47.7%  ← 現行
W5 一貫性加重:   CAGR=234.0%  Sharpe=3.058  MaxDD=-48.5%  ← 最有望
W8 幾何平均:     CAGR=228.7%  Sharpe=3.034  MaxDD=-49.1%  ← W5と類似
```

### W5の式（固定、改変禁止）

```
pos_count = Σ 1(R21>0, R63>0, R126>0)    # 0-3
avg_ret = mean(max(R21,0), max(R63,0), max(R126,0))
raw_weight = pos_count × (1 + avg_ret)
weight = normalize(raw_weight), single theme cap = 30%
```

### 理論的位置づけ

- Kelly **ではない**
- momentum quality / trend consistency weighting と解釈すべき
- Frog-in-the-Pan: 連続的・滑らかな情報流入がモメンタムを強くする
- W5/W7/W8の3つが同方向 → 共通因子（multi-horizon consistency）

### 即採用しない理由

1. CAGR +55ptは改善幅が大きすぎる（過学習リスク）
2. 5年・73リバランス・PIT汚染・複数ルール比較
3. 2021/2024の強気相場依存の可能性

### 昇格基準（6-12リバランス後）

```
必須:
  ① net diff > 0 が過半
  ② median diff >= 0
  ③ single-theme contribution share が W0 より悪化しない
  ④ worst month が W0 より著しく悪化しない
  ⑤ weight turnover が許容範囲
  ⑥ cost-adjusted diff > 0

強い昇格条件:
  ⑦ 前半・後半とも正
  ⑧ 複数レジームで効いている
  ⑨ theme_weight_top1 が cap に張り付き続けない
```

### 実装方針

```
今やること:
  - W5を独立shadowとしてgenerate_g2max.pyに追加（W0と同一selector）
  - R126欠損時: valid horizon のみで計算（bottom送りしない）
  - 30% cap維持
  - turnover・concentration指標を記録

今やらないこと:
  - W5-v2/v3/v4の派生
  - capの最適化
  - R252の追加
  - Kelly系との再統合
```


---

## BEAST Mode Decision Memo (2026-04-12)

### 定義

```
BEAST mode = W5b一貫性加重 WITHOUT 30% cap
  = G2-MAX の全パラメータを維持しつつ、単一テーマウェイト上限を撤廃
```

### BT結果

```
                     CAGR      Vol   Sharpe   Calmar    MaxDD     Terminal  平均最大W
G2-MAX (W5b cap30)  235.1%   78.7%   2.986   4.906   -47.9%    1,037x     24.4%
BEAST (W5b nocap)   283.1%  102.9%   2.750   5.690   -49.8%    2,236x     29.1%

差分:
  ΔCAGR=+48pt  ΔSharpe=-0.24  ΔMaxDD=-1.8pt  ΔCalmar=+0.78  ΔTerminal=+1,199x

単一テーマ最大ウェイト:
  cap30: max=30.0%, >50%発生=0%
  BEAST: max=61.9%, >50%発生=11%（73リバランス中8回）
```

### 判定: 非採用（記録のみ）

```
理由:
  ① Sharpe -0.24 = リスク効率の悪化
  ② Vol 103% = 年間±100%の振幅
  ③ 最大61.9%を1テーマに配分 = 事実上の単銘柄賭け
  ④ G2-MAXは既に攻撃型。BEASTは投機に近い

用途:
  「G2-MAXでもまだ物足りない」場合の参考値。
  実装はしない。
```


---

## BEAST ChatGPT Review Memo (2026-04-12)

### 判定: Research Alpha Candidate（Production Approved ではない）

```
ChatGPT #1: 性能A+ / 実運用信頼度B- / 監査前提の採用候補
ChatGPT #2: 性能A+ / 監査状態:未承認 / 運用判定:Paper trade/小口限定まで
```

### 評価サマリー

```
強み:
  ① 収益力が異常に高い（CAGR 278%）
  ② リスク調整後も極めて良い（Sharpe 2.70, Sortino 6.34）
  ③ 下方偏差ベースでも優秀
  ④ DDを踏んでもCAGRが圧倒

弱み:
  ① MaxDD -46.7% は Red（-40%超はヘッジ/regime filterの説明責任が必要）
  ② 高集中 × 高ベータ × トレンド追随 = モメンタムクラッシュ脆弱性
  ③ 高成績すぎて過学習・データ漏洩・相場適合の疑い
  ④ 実運用ではturnover/slippage/税コストで大幅劣化の可能性

性格: 「戦車ではなくレーシングカー。速いが路面が変わると死ぬ」
```

### 必須監査項目（7本、未実施）

```
1. 仕様凍結: ルールを1ファイルに固定
2. PIT + delisting込み再計算: ここで崩れるなら即終了
3. gross / net / stress-cost 3本並列: grossのみは無価値
4. CSCV / PBO: family全体で実施、PBO<10%=Green
5. DSR: 試行回数Mを正直に入れる、DSR≥0.95=Green
6. rolling / expanding Walk-Forward: OOS retention確認
7. panic/high-vol/rebound slice: MaxDDの正体を分解
```

### 現時点のラベル

```
BEAST = Research Alpha Candidate
  → Paper trade / 小口限定まで
  → 最大の論点は「過学習」ではなく「尾部リスクと実装現実性」
  → MaxDD -46.7%は「不運な月があった」ではなく
    「戦略が何で壊れるかをまだ理解していないサイン」
```


---

## BEAST Audit Results (2026-04-12, beast_audit.py 598d682)

### Rubric Summary

```
TEST                              RESULT        THRESHOLD       VERDICT
─────────────────────────────────────────────────────────────────────
Walk-Forward OOS retention        70.2%         ≥50%            ✅ PASS
Cost 2x Calmar                    2.23          >1.5            ✅ PASS
Param perturbation sign           5/6           all positive    ❌ FAIL
DSR confidence                    58.0%         ≥95%=Green      🔴 RED
MaxDD                             -43.2%        >-40%           🔴 RED
Annual turnover                   1046%         documented      ⚠ HIGH
PIT/delisting                     NOT TESTED    pass            ⚠ PENDING
CSCV/PBO                          NOT TESTED    <10%            ⚠ PENDING
```

### Critical Findings

```
1. DSR RED (58%): Sharpe 1.68は108試行後の期待最大値(1.09)を上回るが、
   日次リターンの歪度(5.1)と尖度(78.9)が極端に高く、統計的信頼度が低い。
   → Sharpeが見た目ほど信頼できない

2. Bear Market Collapse: SPY 63d<0局面でSharpe=-0.40, CAGR=-19%
   → モメンタムクラッシュの構造的脆弱性がChatGPT指摘通り確認された

3. MaxDD #1: -43.2% (2021-11→2022-07, 163日下落→239日回復 = 計402日)
   MaxDD #2: -43.0% (2025-02→2025-04, 42日下落→124日回復)
   → 2回とも-40%超え。偶発ではなく構造的

4. 集中度: max single weight=51.5%, Effective N min=3.4
   → 実質3銘柄に過半集中する月がある

5. Turnover 1046%/年: 極端に高い。実務的にはスプレッド30bpでも
   Sharpe retention=88%を維持するが、税コストは未考慮

6. 前半/後半: Sharpe 2.24→2.03 (安定だが後半低下)
   Rolling Sharpe: 0.26〜2.31 (大幅変動)
```

### 結論更新

```
BEAST = Research Alpha Candidate のまま変更なし

追加で確定した事項:
  - 構造的にベア相場で壊れる（Sharpe -0.40）
  - DSRがRED（統計的信頼度不十分）
  - 高集中 + 高turnover + 高尾度 = レーシングカー特性の数値的裏付け
  - ただしcost耐性は意外に高い（2x costでもCalmar>1.5）
  - OOS retention 70%は合格水準

W5b(cap30)が運用戦略として正しい選択であることが監査で再確認された。
BEAST nocapは参考値としてダッシュボード表示を継続。
```


---

## BEAST Audit — ChatGPT Final Analysis (2026-04-12)

### 最終判定

```
BEAST:      CONDITIONAL / ATTACK-SLEEVE ONLY
W5b(cap30): CURRENT WINNER / DEFAULT CANDIDATE
```

### ChatGPT分析の要点

```
1. 主犯特定: 「過学習」ではなく「bear regime脆弱性」が主犯
   → 問題は「原因不明の不安定性」ではなく、regime dependency

2. DSR RED解釈: 「Sharpe無価値」ではなく「額面通り信じるな」
   → 尖度79 = "普段は普通、たまに爆発"の世界
   → 少数日の大勝で全体が持ち上がり、見た目のSharpeが美化

3. W5b優位の根拠: 偶然ではなく設計差で説明可能
   → BEASTの弱点（集中/tail/bear crash）をcapが設計的に潰した結果
   → 「監査結果で採用理由を説明できるようになった」

4. 予想より良かった点:
   → OOS retention 70.2%（本物の信号成分あり）
   → net Sharpe retention 88%（コスト耐性が意外に高い）
   → Cost 2x Calmar 2.23（弱点は「コスト」ではなく「regime/集中/tail」）
```

### ChatGPT推奨 次ステップ（優先順）

```
A. Bear kill switch設計（最優先・ROI最大）
   SPY 63d<0で負けると判明 → 打ち手の検証:
   ① cash化 ② hold数縮小 ③ cap追加引下げ ④ defensive ETF混合 ⑤ re-entry遅延

B. 集中度制御の系統比較（次点）
   cap 20/25/30/35/equal/vol-adjusted
   最適化軸 = CAGR最大化ではなく Sharpe/Calmar/MaxDD/DSR総合最適

C. Tail dependence分解（3番手）
   top 1%/top 5日/top 10日を除いたCAGR/Sharpe/Calmar
   → 崩れればDSR REDの意味がさらに重くなる
```


---

## ChatGPT Cap Grid + Tail Decomp Specs (2026-04-12)

### B_CAP_GRID_SPEC v1.0 — 要約

```
目的: capが concentration をどこまで削り、どこで alpha dilution に転じるかを探索
方法: cap以外を完全凍結し、capだけを変える one-factor experiment

Phase B1: 静的 hard-cap ラダー
  uncapped / 50% / 40% / 35% / 30% / 25% / 20% / 15%
  優先順: 30→25→20→35→40→15→uncapped→50

Phase B2: redistribution比較（B1上位2仕様のみ）
  pro-rata vs rank-score

Phase B3: adaptive cap（B1/B2勝者1本のみ）
  regime-adaptive / breadth-adaptive / vol-adaptive

判定: Composite score
  30% OOS/Net/Calmar + 30% MaxDD/Recovery/Bear + 20% DSR/skew/kurt + 20% concentration

事前仮説: cap25とcap30が本命
```

### C_TAIL_DECOMP_SPEC v1.0 — 要約

```
目的: Sharpeの見かけの良さの源泉を分解 + DSRを押し下げている要因を特定

4層分解:
  C1: Distribution tail — best/worst day removal sensitivity
  C2: Higher-moment panel — skew/kurtosis/DSR by subsample
  C3: Top-5 DD archaeology — 各DDの構造分解
  C4: ES/CED block — serial-loss fragility vs jump-loss
  C5: ES contribution — 銘柄/テーマ別tail寄与
  C6: Incremental ratio contribution — Sharpe/Calmar寄与分解
  C7: Regime-conditioned tail map — state別tail分布
  C8: Tail concentration scoreboard — 最終1枚サマリー

実行順: B1→C1→C3→C4→C5→C7→B2→B3→bear kill switch統合
```

### ChatGPT最終判定

```
BEAST: 「偽アルファ」ではない。しかし単独で本番採用できる完成戦略でもない。
  → アルファの実在性: 一定程度あり
  → 汎化性: 想定より良い
  → 実装耐性: 想定より良い
  → 尾部リスク: 依然として重い
  → bear regime脆弱性: 構造的
  → 統計的信頼度: まだ弱い

W5b(cap30): BEASTより運用体として完成度が高い
  → Sharpe上, MaxDD浅, cap集中抑制, 弱点を設計的に潰した結果

最重要発見:
  ① 主犯 = bear regimeでの構造的脆弱性（過学習ではない）
  ② DSR RED = Sharpe無価値ではなく「額面通り信じるな」
  ③ W5b優位 = 偶然ではなく設計差で説明可能
```


---

## B+C Audit Results (2026-04-12, cap_tail_audit.py)

### B1: Static Cap Ladder — 完全結果

```
    Cap    CAGR   Sharpe  MaxDD   DSR    BearS   top1   HHI   effN
  nocap  108.1%  1.683  -43.2%  58.0%  -0.404  20.5%  0.129  7.7
  cap50  108.2%  1.685  -43.2%  58.1%  -0.404  20.4%  0.129  7.8
  cap40  108.2%  1.696  -43.2%  58.0%  -0.401  20.1%  0.127  7.9
  cap35  108.0%  1.705  -43.0%  58.0%  -0.406  19.6%  0.124  8.1
  cap30  105.3%  1.761  -42.4%  59.9%  -0.405  18.8%  0.119  8.4  ← 現行採用
  cap25  101.8%  1.819  -41.9%  62.8%  -0.401  17.7%  0.116  8.6
  cap20   95.3%  1.835  -41.4%  66.5%  -0.401  16.5%  0.112  8.9  ← Composite winner
  cap15   84.4%  1.770  -41.5%  70.6%  -0.422  14.2%  0.107  9.3
```

### B1 分析

```
1. Sharpe: nocap(1.68)→cap20(1.84)まで単調増加、cap15で反転(1.77)
   → alpha dilution開始点 = cap15付近

2. DSR: nocap(58%)→cap15(71%)まで単調改善
   → capを締めるほどDSRは改善（集中依存が減る）

3. MaxDD: -43.2%→-41.4% 全体で1.8pt改善のみ
   → capではMaxDDを本質的に改善できない。regime問題。

4. Bear Sharpe: 全cap水準で -0.40前後、ほぼ不変
   → capはbear脆弱性に無効。bear kill switchが別途必要。

5. cap30 vs cap25:
   cap25が Sharpe(+0.06), DSR(+3pt), MaxDD(+0.5pt), Bear(+0.004) で全勝
   cap30が CAGR(+3pt), Calmar(+0.05) で勝つが差は小さい
```

### C1: Best/Worst Day Removal (cap30)

```
  top5d  CAGR retention=59% Sharpe retention=71%  ← YELLOW（70%基準を下回る）
  top10d CAGR retention=39% Sharpe retention=50%  ← RED
  bot5d  CAGR=130%(+24%) Sharpe=2.22(+26%)       ← 下位5日除去で大幅改善
  bot3m  CAGR=137%(+30%) MaxDD=-37.5%(+5pt改善)

  判定: right-tail dependence = YELLOW
  → 上位5日で41%のCAGRが消える。「少数の大勝に依存」
  → ただし50%は超えているのでREDまでは行かない
```

### C4: ES/CED Block

```
         ES95    CED95   CED/ES
  nocap  109.6%  39.4%   0.36
  cap30  104.7%  38.7%   0.37
  cap25  101.2%  38.1%   0.38
  cap20   97.0%  37.7%   0.39

  CED/ES比 ≒ 0.37 → serial-loss fragility は moderate（extreme ではない）
  capでES95が改善: 110%→97%（cap20で-12pt）
```

### C5: ES Contribution

```
  Top3 ES contribution: -0.5%
  Top5 ES contribution: -0.7%

  判定: GREEN
  → ES tailが特定銘柄に集中していない。よく分散されている。
```

### C7: Regime-Conditioned Tail Map (cap30)

```
  Bull+HighVol:  CAGR=366% Sharpe=5.61  ← 最高性能局面
  Bull+LowVol:   CAGR=102% Sharpe=1.59  ← 安定した収益
  Bear+HighVol:  CAGR=  2% Sharpe=0.05  ← ほぼ死に体
  Bear+LowVol:   CAGR=-81% Sharpe=-1.95 ← 構造的崩壊

  Worst 10%月: 57%がbear regime → bear kill switchで半分以上のtailを削れる可能性
```

### 統合判定

```
Composite Winner: cap20（Sharpe最高+DSR最高）
ChatGPT事前予想: cap25-30が本命 → cap20-25が実測の最適レンジ

cap30（現行）は conservative choice としてなお有効だが、
cap25への移行は Sharpe/DSR/MaxDD全てで改善する。

全cap水準でBear Sharpe≒-0.40のため、
capだけではbear問題を解決できない → bear kill switchが次の最優先。
```


---

## BEAST Kill Switch Design (ChatGPT, 2026-04-12)

### 核心

```
Kill switchの本体は「bearを避ける装置」ではなく、
「panic stateでのhigh-vol reboundに巻き込まれない装置」

4層構造:
  Outer shell:  長期トレンド（200DMA）で大きな熊相場を回避
  Core shell:   panic/high-vol/rebound を直接検知 ← 本丸
  Inner shell:  戦略自身のvol/DDで自己防衛
  Re-entry:     戻るときだけ慎重に（intermediate horizon + earnings-aware）
```

### 状態遷移: NORMAL → CAUTION → KILL

```
NORMAL:
  条件: Trend200=1 AND M63≥0 AND RV21_pct<75 AND Breadth63≥40%
  action: cap=25%, gross=通常, 通常ranking

CAUTION（2/4条件で発火）:
  条件: Trend200=0 / M63<0 / RV21_pct≥75 / Breadth63<35%
  action: gross=50%, cap=20%, 直近反発銘柄除外, 12-7horizon寄り

KILL:
  主条件: M63<0 AND RV21_pct≥75 AND Rebound20≥8%（同時）
  副条件: StratDD20≤-10% / Breadth63<25% / Trend200=0 のうち2/3
  action: gross=0-25%, 新規買い停止, ポジション解消, SHV/BIL退避

Re-entry: KILL→CAUTION→NORMAL（各2回連続リバランスで確認、hysteresis）
```

### 実装Phase順

```
Phase 1: cap25をベースラインに（B1監査で確認済み）
Phase 2: always-on vol targeting（平時から効かせる）
Phase 3: panic-rebound KILL（本丸）
Phase 4: 2段階再エントリー + hysteresis
Phase 5: re-entry rankingをintermediate horizon + earnings-awareに寄せる
```

### 成否判定基準

```
Bear Sharpe: -0.40から明確改善するか
MaxDD: -41%台から-35%未満へ近づくか
CAGR retention: static cap25比で80%以上残るか
whipsaw率: KILL→復帰→再KILLが過剰でないか
off率: 退避時間が長すぎてalphaを殺していないか
```

### 採るべきでない案

```
❌ cap微調整を主戦場にする（Bear Sharpeが治らないことが判明済み）
❌ 個別stop-lossを主役にする（ES contributionが分散＝犯人は個別銘柄ではなく状態）
❌ crowding指標を一次トリガーにする（監視のみ）
❌ beta hedgeを主役にする（vol scaling + cash retreatが筋）
```


---

## Kill Switch BT Results (2026-04-12, d371aad)

### 結論: ChatGPT設計のkill switchは逆効果。Vol scalingが唯一有効。

```
K4_fullKS: Bear Sharpe -0.94（ベースライン-0.40より大幅悪化）
  原因: 2022年に6ヶ月で5回の状態遷移（whipsaw）
  → DDは食らうが回復は逃す

K2_volscale: Bear Sharpe -0.10（ベースラインから+0.30改善）★唯一の勝者
  CAGR retention: 93.6%
  連続的調整 > 離散的switch

ChatGPTの処方箋の何が正しく何が間違っていたか:
  ✅ 「panic stateでのhigh-vol reboundが主犯」→ 正しい
  ✅ 「vol scalingはcash retreatより効く」→ BT確認
  ❌ 「4層state machineが有効」→ whipsawで逆効果
  ❌ 「NORMAL/CAUTION/KILL離散遷移」→ 連続調整に劣る
  △ 「SMA200フィルタは外殻の安全装置」→ 回復を逃して逆効果

次のアクション:
  vol scaling単独の最適化（target vol 20-60%のグリッド）を行い、
  cap25 + vol scalingの組み合わせが最終候補となるか検証。
```


---

## Kill Switch Research — Final Conclusion (2026-04-12)

### 確定した判断

```
棄却:
  ❌ Binary kill switch (SMA200, state machine) — whipsawで逆効果
  ❌ cap30維持 — cap25が全指標で支配
  ❌ vol scalingをBear問題の解決策とする — Bear Sharpe不変

採用:
  ✅ cap25を新ベースライン
  ✅ downside_vol_30をリスク整形装置として候補（alpha基準とは別管理）
  ✅ Bear問題の主戦場を「selection logic」に移行

2本のベースライン:
  Alpha baseline:        cap25 raw (CAGR=101% Sharpe=1.81 MaxDD=-42%)
  Risk-managed baseline: cap25 + downside_vol_30 (CAGR=81% Sharpe=2.00 MaxDD=-36%)
```

### Kill switchの正しい理解

```
できること（risk budget control）:
  ✅ 総エクスポージャー調整
  ✅ realized riskの平滑化
  ✅ MaxDDと回復特性のトレードオフ調整

できないこと（signal repair）:
  ❌ bearで負けない銘柄を作る
  ❌ 同時下落するロングブックをプラス期待値に変える
  ❌ panic/reboundで崩れる選定ロジックの修正

位置づけ: 主役→降格。
  → 常時ONのリスク整形器（downside vol scaling）
  → 研究用の診断変数（regime flag）
```

### 次の研究テーマ（銘柄選定ロジック再設計）

```
Phase 1: ランキング窓の再設計
  → recent momentum vs 12-2 vs 12-7 vs blended horizon
  → raw vs residual momentum (Blitz-Huij-Martens)
  根拠: Novy-Marx「直近<12-7ヶ月の中間ホライズン」

Phase 2: 選定オーバーレイ
  → earnings surprise / revision / quality filter
  → formation-period volatility penalty
  根拠: Fan et al「高vol銘柄はmomentum効果を失う」
         Novy-Marx「earnings momentumでcrashが減る」

Phase 3: downside_vol_30オーバーレイ（最後）
  → 何を買うかが決まってから、どれだけ持つかを調整

評価基準（優先順）:
  1. Bear Sharpe  2. MaxDD/Recovery  3. Sharpe/Calmar
  4. Crash month hit rate  5. Down-market capture
```

### ChatGPT文献リファレンス

```
Barroso-Santa-Clara (2015): vol scalingでmomentum crash耐性改善
Moreira-Muir (2017): 高vol時のリスク縮小が広い因子群で有効
Daniel-Moskowitz (2016): panic state損失はvolだけでは説明不能
Cederburg et al (2020): vol-managed portfolioのOOS実装価値は過大評価されやすい
Wang-Yan (2021): downside vol > total vol、fixed-weight blendが有効
Novy-Marx (2012): 12-7ヶ月horizonがmomentumの予測力最高
Blitz-Huij-Martens (2011): residual momentumがtotal momentumより良い
Fan et al (2022): formation-period高vol銘柄はmomentum効果を失う
Faber (2007): トレンドフィルタはDD改善に有効だがwhipsaw注意
```


---

## 全検証の最終記録 (2026-04-12)

### セッション全体の研究フロー

```
W5b研究 → 全戦略比較 → 正しいスコアリング修正 → PRISM-R W5b採用
→ BEAST ChatGPT評価 → BEAST institutional audit (10テスト)
→ Cap grid (8水準) → Kill switch BT (7バリアント)
→ Vol scaling optimization (24バリアント) → Cap30 vs Cap25精査
```

### 1. W5b一貫性加重の効果（正しいスコアリング使用）

```
戦略         W0 Sharpe   W5b Sharpe   ΔSharpe   採否
PRISM          1.356       1.447       +0.091    ⏳ shadow並走（実運用戦略）
PRISM-R        1.559       1.753       +0.194    ✅ 採用（cap30）
PRISM-RQ       1.030       0.973       -0.057    ❌ 有害（等ウェイト維持）
G2-MAX         2.847       2.986       +0.139    ✅ 採用済み（cap30）

結論: raw α系（PRISM/PRISM-R/G2-MAX）にW5bが有効、SNRb系に無効
```

### 2. 誤ったBTの修正記録

```
誤り: backtest_w5b_all.py がPRISM/PRISM-RにG2-MAXのスコアリングを適用
  → テーマ選定式、銘柄選定方式、相関フィルタの3点が全て異なっていた
  → 結果が10〜50倍過大に表示されていた

修正: backtest_w5b_correct.py で generate_bt_returns.py と同一ロジックに修正
  → W0がダッシュボードBTとほぼ一致することを確認
  PRISM: 50.2%≈49.2% ✅  PRISM-R: 66.9%≈65.6% ✅
```

### 3. BEAST Institutional Audit (10テスト)

```
✅ PASS:
  Walk-Forward OOS retention: 70.2% (≥50%)
  Cost 2x Calmar: 2.23 (>1.5)

❌ FAIL:
  Parameter perturbation: 5/6 positive
  DSR confidence: 58.0% (RED, ≥95%必要)
  MaxDD: -43.2% (RED, >-40%必要)

⚠ 注意:
  Annual turnover: 1046% (極端)
  PIT/delisting: 未テスト
  CSCV/PBO: 未テスト

ChatGPT判定: Research Alpha Candidate / Production不可
```

### 4. Cap Grid Comparison (B1: 8水準)

```
    Cap    CAGR   Sharpe  MaxDD   DSR    BearS
  nocap  108.1%  1.683  -43.2%  58.0%  -0.404
  cap50  108.2%  1.685  -43.2%  58.1%  -0.404
  cap40  108.2%  1.696  -43.2%  58.0%  -0.401
  cap35  108.0%  1.705  -43.0%  58.0%  -0.406
  cap30  105.3%  1.761  -42.4%  59.9%  -0.405  ← 現行
  cap25  101.8%  1.819  -41.9%  62.8%  -0.401
  cap20   95.3%  1.835  -41.4%  66.5%  -0.401
  cap15   84.4%  1.770  -41.5%  70.6%  -0.422

発見:
  ① Sharpe: nocap→cap20まで単調増加、cap15で反転（alpha dilution開始）
  ② DSR: capを締めるほど単調改善
  ③ MaxDD: 全水準で-41〜-43% → capではMaxDD解決不能
  ④ Bear Sharpe: 全水準で≒-0.40 → capではbear解決不能
```

### 5. Tail Decomposition (C1-C7)

```
C1 (best/worst removal, cap30):
  top5d除去: CAGR retention=59% (YELLOW)
  bot5d除去: CAGR=130%(+24%), Sharpe=2.22(+26%)
  → right-tail dependence = YELLOW（少数日依存は中程度）

C4 (ES/CED):
  CED/ES比 ≒ 0.37 → serial-loss fragility moderate

C5 (ES contribution):
  Top3 share = -0.5% → GREEN（特定銘柄に集中していない）

C7 (regime map, cap30):
  Bull+HighVol: CAGR=366% Sharpe=5.61
  Bear+LowVol:  CAGR=-81% Sharpe=-1.95 ← 構造的崩壊
  Worst 10%月の57%がbear regime
```

### 6. Kill Switch BT (7バリアント)

```
B0_cap30:         CAGR=105% Sharpe=1.75 MaxDD=-42% Bear=-0.41
B1_cap25:         CAGR=101% Sharpe=1.81 MaxDD=-42% Bear=-0.40
K1_sma200:        CAGR= 89% Sharpe=1.69 MaxDD=-42% Bear=-0.90 ❌ 回復を逃す
K2_volscale:      CAGR= 95% Sharpe=1.72 MaxDD=-45% Bear=-0.10 ★（アーティファクト疑い）
K3_sma+vol:       CAGR= 82% Sharpe=1.58 MaxDD=-44% Bear=-0.83 ❌
K4_fullKS:        CAGR= 89% Sharpe=1.68 MaxDD=-46% Bear=-0.94 ❌ whipsaw
K5_fullKS+vol:    CAGR= 82% Sharpe=1.58 MaxDD=-53% Bear=-1.05 ❌ 最悪

結論: Binary kill switch は全て逆効果。
  2022年に6ヶ月で5回の状態遷移 = whipsawの嵐。
  K2_volscaleのBear Sharpe -0.10はrebalance-level実装によるアーティファクト。
```

### 7. Vol Scaling Optimization (24バリアント)

```
Phase 1 (Total Vol, cap25ベース):
  vol20: CAGR=43% Sharpe=1.80 MaxDD=-24% Bear=-0.34
  vol30: CAGR=65% Sharpe=1.89 MaxDD=-35% Bear=-0.39
  vol40: CAGR=85% Sharpe=1.92 MaxDD=-44% Bear=-0.42

Phase 2 (Downside Vol, 全水準でtotal volに勝る):
  dvol25: CAGR=67% Sharpe=1.95 MaxDD=-30% Bear=-0.40
  dvol30: CAGR=81% Sharpe=2.00 MaxDD=-36% Bear=-0.42  ← 最良バランス
  dvol40: CAGR=104% Sharpe=2.05 MaxDD=-46% Bear=-0.50

Phase 3 (Blend):
  50raw+50vs: CAGR=94% Sharpe=1.93 MaxDD=-43% Bear=-0.41
  → blendではBear改善なし

結論: vol scalingはSharpe/MaxDD改善に有効だがBear Sharpe不変。
  日次実装ではBear Sharpe≒-0.40が全バリアントで不変。
  vol scalingは「リスク整形器」であり「Bear修正器」ではない。
```

### 8. Cap30 vs Cap25の最終判定

```
                cap30        cap25        差分
CAGR           105.3%       101.8%      -3.5pt
Sharpe          1.761        1.819      +0.058
MaxDD          -42.4%       -41.9%      +0.5pt
DSR             59.9%        62.8%      +2.9pt
Bear Sharpe    -0.405       -0.401      +0.004
Calmar          2.48         2.43       -0.05

cap binding差: 73回中2回のみ差異

判定: 統計的に区別不能なノイズ範囲内。
  cap30を維持。移行コストに見合わない。
  BT上の数百分の1の違いで実装変更するのは過学習と同構造。
```

### 最終的な現行最適解

```
╔══════════════════════════════════════════════════════════════╗
║  現行構成（変更なし）:                                         ║
║    PRISM:     等ウェイト（実運用）                              ║
║    PRISM-R:   W5b cap30 ← 維持                              ║
║    PRISM-RQ:  等ウェイト ← 維持                               ║
║    G2-MAX:    W5b cap30 ← 維持                              ║
║                                                              ║
║  棄却済み:                                                    ║
║    ❌ BEAST (nocap): DSR RED, MaxDD RED                      ║
║    ❌ Binary kill switch: 全バリアント逆効果                    ║
║    ❌ Cap25移行: cap30と統計的に区別不能                        ║
║    ❌ Vol scaling単独: Bear Sharpe不変                        ║
║                                                              ║
║  未解決:                                                      ║
║    ⏳ Bear Sharpe ≒ -0.40（構造的、銘柄選定ロジックの問題）     ║
║    ⏳ downside_vol_30の採否（リスク整形装置として研究継続）      ║
║    ⏳ 銘柄選定再設計（residual/12-7/earnings/formation-vol）   ║
╚══════════════════════════════════════════════════════════════╝
```


---

## Next Research: Ranking/Selection Redesign (ChatGPT, 2026-04-12)

### 核心問題の再定義

```
Bear問題の真因（4層分解）:

1. 「強い銘柄」の定義がprice-only total return → recent-heavy
   → Novy-Marx: 12-7ヶ月の中間ホライズンの方が予測力が高い

2. 共通因子の追い風を銘柄固有のαと区別していない
   → Blitz-Huij-Martens: residual momentumはconventional比で2倍のrisk-adjusted利益
   → Ehsani-Linnainmaa: stock momentumはfactor momentumを間接タイミング

3. price continuation背後のfundamental underreactionを使っていない
   → Novy-Marx: earnings/fundamental momentumがprice momentumをかなり説明

4. junk/high-beta/high-vol winnerを選んでいる
   → Asness-Frazzini-Pedersen QMJ: quality銘柄が有意なrisk-adjusted return
   → Fan et al: 高vol銘柄はmomentum効果を失う
```

### 最有力候補: RIQM（Residual Intermediate Quality Momentum）

```
Score_price = z(resid_ret_12_7_vs_MKT+Industry)
Score_fund  = z(gross_profitability or ROE)
Penalty     = z(formation_semivol_63)
Final       = 0.55 × Score_price + 0.25 × Score_fund - 0.20 × Penalty

狙い:
  ① 共通因子を抜く（residual化）
  ② recent-heavyをやめる（12-7 intermediate horizon）
  ③ junk/high-vol winnerを落とす（quality + vol penalty）
```

### 実装Phase順

```
Phase 1: Raw → Intermediate-horizon
  12-7, 12-2, 6-2 の比較

Phase 2: Intermediate → Residualized
  MKT+Industry と MKT+SMB+HML+Industry を比較

Phase 3: Residualized → Quality/Fundamental Overlay
  profitability / quality + earnings/fundamental

Phase 4: Soft regime reweighting
  binary switchではなく、weightsを少しずらす
```

### 捨てるべきもの

```
❌ binary kill switchの再設計
❌ pure recent winnersの深掘り
❌ signal 10本以上の巨大composite
❌ total-vol overlayにBear改善を期待すること
```


---

## Bear Resolution Phase 1-2 Analysis (ChatGPT, 2026-04-12)

### 確定した事後分布の更新

```
1. 主レバー = intermediate horizon（residualではない）
   → 12-7ヶ月が文献通り最有望
   → recent-heavyが Bear問題の主因の一つ

2. Theme層には情報がある（殺してはいけない）
   → residual momentumの失敗 = theme/industry/factorがalpha carrier
   → Moskowitz-Grinblatt: industry momentum = stock momentumの大部分を説明

3. 正しい方向 = Theme momentum残す → Stock側をredesign
   → full residualization → 棄却
   → partial de-factoring（theme-relative stock strength）→ 検討候補
```

### Phase 3: 3ブランチ × cap grid

```
Branch 1: Bear specialist = G_both_12_7 × cap{20,25,30}
Branch 2: DD specialist = E_stock_12_7 + current_theme × cap{20,25,30}
Branch 3: Balanced = F_stock_6_2 + current_theme × cap{20,25,30}
```

### Phase 4-5（後工程）

```
Phase 4: partial residual
  E_stock_12_7 + theme-relative(λ=0.2,0.4,0.6)
  F_stock_6_2 + theme-relative(λ=0.2,0.4,0.6)

Phase 5: quality overlay
  Final = 0.7 × Score_price + 0.3 × Score_quality
  quality = gross profitability / ROE / leverage penalty
```


---

## Bear Resolution Phase 4 Results + ChatGPT Analysis (2026-04-12)

### Phase 4 結果

```
                CAGR    Sharpe  MaxDD    Bear     2022
A_current      111.7%   1.911  -40.9%  +0.156    +4.8%  ← baseline
F_stk6_2       105.3%   1.820  -44.6%  +0.438    +0.6%  ← 実務本命
Fmix_25         77.9%   1.649  -40.7%  +0.587    +1.7%  ← Bear+DD最良
G_both12_7      97.8%   1.786  -46.7%  +0.538   +31.0%  ← Bear専門

失敗:
  Partial de-theming (lambda=0.2/0.4): 効果ゼロ（Fmix_50と同一結果）
  Quality overlay (Fmix50_Q): CAGR=74% Sharpe=1.33（quality proxyが粗すぎ）
  Fmix blends: 6-2と12-7のシグナルが部分的に相殺
```

### ChatGPT確定事項

```
1. 主レバーはresidualではなくintermediate horizon（確定）
2. Theme層は情報を持つ — 消してはいけない（確定）
3. Full residualizationはこの宇宙では不適合（確定）
4. Cap は二次レバー（scoring method > cap）（確定）
5. Vol penaltyは主役ではない → veto/tie-breakerに降格（確定）

ChatGPT推奨の設計方針:
  「Theme layerは残す。Stock layerを6-2を基準に12-7を混ぜる。
   Residualはfullではなくlight de-themingに留め、
   最後にqualityでwinnerを確認する。」
```

### Bear研究の全体マップ（Phase 1-4完了）

```
Phase 1-2: 12バリアント → intermediate horizon が主レバーと判明
Phase 3:   12バリアント → F_stk6_2_c30が実務本命と確定
Phase 4:    9バリアント → Fmix/de-theming/qualityは追加効果なし

合計33バリアント tested。
最終結論: F_stk6_2（現行theme + 銘柄選定6-2ヶ月horizon）が最適解。
  CAGR -6pt（許容範囲）でBear Sharpe 3倍改善。
```


---

## ChatGPT Final Judgment on Bear Research (2026-04-12)

### 確定した3つの中核知見

```
1. 主レバーはranking horizon（capでもoverlayでもない）
   → Novy-Marx: 12-7ヶ月のintermediate horizonが予測力最高
   → BT確認: F_stk6_2がBear Sharpe 3倍改善

2. Theme層には情報がある — residual化は不適合
   → Moskowitz-Grinblatt: industry momentumがstock momentumの多くを説明
   → Ehsani-Linnainmaa: stock momentumはfactor momentumの間接タイミング
   → BT確認: residual系はCAGR 20-28%に崩壊

3. Overlay（vol/kill switch）は「リスク整形」であって「シグナル修理」ではない
   → Wang-Yan: downside vol > total vol だが Bear問題は解決しない
   → BT確認: vol scalingでBear Sharpe不変
```

### 戦略ラベル

```
F_stk6_2:      本番候補（CAGR 105% Bear +0.44 実務最適）
A_current:     現行基準（比較用ベンチマーク）
G_both12_7:    Bear専門枝（研究用ベンチマーク）
E_stk12_7:     DD専門枝（MaxDD -34.5%最良）
```

### ChatGPT推奨の次セッション優先順位

```
1位: F_stk6_2をPRISM-Rに実装
  → 33バリアント研究で「効くもの」が分離済み
  → deployment-level validationが必要

2位: Quality dataソース調査
  → SEC companyfacts API（無料、XBRLベース）
  → Sharadar（25年fundamentals、active/delisted coverage）
  → FMP（analyst estimates API含む）

3位: downside_vol_30 overlayの最終採否（本体ではなく補助）
```

### Bear研究の全体統計

```
総バリアント数: 33 + 24(vol) + 7(kill switch) + 8(cap) + 10(audit) = 82バリアント
総実行時間: ~30分
主要発見:
  ✅ intermediate horizon → Bear Sharpe 3倍改善（Phase 1-2で発見）
  ❌ residual momentum → 信号破壊
  ❌ binary kill switch → whipsawで逆効果
  ❌ vol scaling → Sharpe改善だがBear不変
  ❌ quality proxy → データが粗すぎ（Phase 4で失敗）
  △ Fmix blend → 追加効果なし（Phase 4）
  △ partial de-theming → 効果ゼロ（Phase 4）
```


---

## stock-themes.com Data Audit for Quality/Earnings Research (2026-04-12)

### 結論: 財務データは存在しない。ただし統計的quality proxyは構築可能。

### stock-themes.com が持つデータ

```
1. beta_alpha_all.json（208テーマ × 全銘柄 × 7期間）
   期間: 5D, 10D, 1M, 2M, 3M, 6M, 12M
   各期間のフィールド:
     alpha, alpha_ann, alpha_pval, alpha_tval,
     beta, individual_factor, r2,
     theme_factor, theme_return
   → 7期間 × 9フィールド = 63変数/銘柄/テーマ

2. theme_ranking.json（全テーマ × 銘柄パフォーマンス）
   期間: 日中, 1日, 5日, 10日, 1ヶ月, 2ヶ月, 3ヶ月, 半年, 1年
   → 8期間のリターン/銘柄

3. stock_meta.json（915銘柄）
   sector, industry, mc(大型/中型/小型), price, exchange

4. dip_alerts.json（テーマ単位のdip signal）
   29パターン × win_rate × med_excess
```

### stock-themes.com が持たないデータ

```
❌ EPS / earnings surprise / SUE
❌ revenue / revenue growth
❌ ROE / ROA / gross profitability
❌ margins (gross/operating/net)
❌ leverage / debt-to-equity
❌ cash flow / FCF
❌ book value
❌ analyst estimates / revisions
❌ PE ratio / PB ratio
```

### beta_alpha_all.jsonから構築可能なquality proxy

```
① Alpha Stability Score
   = 7期間のうちalpha_ann > 0の期間数 / 7
   意味: 多くの期間で正のαを持つ銘柄 = 安定したα生成器

② Alpha T-value Quality
   = mean(abs(alpha_tval)) across periods
   意味: t値が高い = αが統計的に有意 = ノイズではない

③ R² Consistency
   = mean(r2) across periods
   意味: テーマとの連動が安定 = systematic behavior

④ Multi-Period Performance Consistency（theme_rankingから）
   = 8期間のうちリターン > 0の期間数 / 8
   意味: W5bと同じ発想を銘柄レベルに適用

⑤ Market Cap Filter
   = stock_metaのmc ≠ 'micro' AND mc ≠ 'small'
   意味: junk/low-liquidity銘柄の除外
```

### 外部データなしで可能な改善

```
A. beta_alpha quality proxy → Phase 4で失敗したquality overlayを
   stock-themes固有のデータで再構築可能
   → BT検証で効果測定が必要

B. market cap filter → junk winner排除
   → 即実装可能、データは既にstock_metaに存在

C. multi-period consistency → stock-level W5b
   → theme_ranking APIから取得可能
```

### 外部データが必要な改善（stock-themes不可）

```
→ SEC companyfacts API（無料）: EPS/revenue/profitability
→ Sharadar（有料）: 25年のfundamentals + delisting
→ FMP API（freemium）: analyst estimates + earnings surprise
→ yfinance info（無料/制限あり）: trailingPE, returnOnEquity等
```


---

## stock-themes.com Data Investigation for Quality Proxy (2026-04-12)

### 調査結果: beta_alpha_all.json が最有力データソース

```
データ構造:
  849銘柄 × 208テーマ × 7期間（5D/10D/1M/2M/3M/6M/12M）
  各セル:
    alpha        — OLS残差α
    alpha_ann    — 年率化α
    alpha_pval   — p値
    alpha_tval   — t値
    beta         — テーマβ
    r2           — 決定係数
    individual_factor — テーマ外の固有リターン
    theme_factor — テーマ寄与リターン
    theme_return — テーマ全体リターン
```

### SEC/Sharadar不要で構築可能な代理指標（4つ）

```
1. Alpha Stability Score（QMJ quality代理）
   = Σ𝟙(alpha_tval > 1 across 3M, 6M, 12M) / 3
   意味: 複数期間で統計的に有意なα = 「質の高いwinner」
   文献: QMJ(Asness)のprofitability/safetyの代理
   データ: beta_alpha_all.json → 即利用可能

2. Individual Factor（Residual Momentum軽量代理）
   = individual_factor (6M or 12M)
   意味: テーマ要因を除いた固有リターン
   文献: Blitz-Huij-Martensのresidual momentum
   注意: Phase1-2でfull residualは失敗したが、
         stock-themes APIが事前計算済みの値なので軽量に試せる

3. Beta Penalty（Bear脆弱性フィルタ）
   = beta (6M or 12M)
   意味: テーマに対する感応度。高beta = bear時に深く掘る
   文献: Daniel-Moskowitzのmomentum crash
   用途: high-beta winnerを罰する → bear耐性改善

4. R2 Filter（テーマ乗り銘柄除外）
   = r2 (3M)
   意味: テーマとの連動度。r2≒1 = テーマに乗っただけ（固有αなし）
   用途: r2が極端に高い銘柄を除外 → stock-specific continuationを抽出
```

### stock-themes.com API全エンドポイント

```
/api/theme-ranking        — 1126テーマ × 8期間パフォーマンス + tickerPerformances
/api/theme-movers-batch   — テーマ別ムーバー
/api/dip-alerts           — 急落アラート
/api/last-updated         — 最終更新日時
/api/report-catalysts/{id}  — zukai: カタリスト分析（テキスト）
/api/report-ticker-details/{id} — zukai: 銘柄詳細（c1-c10 ◎/△/- 評価）
/api/report-overlays/{id} — zukai: バリューチェーン図
/api/themes/custom/*      — カスタムテーマ
/api/watchlist/themes     — ウォッチリスト
```

### zukai report-ticker-details（限定的）

```
一部テーマのみ（光接続, サーバー, 半導体製造等）
各銘柄: c1-c10のカタリスト評価（◎=強い, △=普通, -=該当なし）
+ description（財務情報含むテキスト）

Quality proxy構築:
  △ ◎の数でスコア化は可能だが全テーマには存在しない
  △ descriptionからのNLP抽出は複雑すぎ → 見送り
```

### 次セッションへの推奨

```
F_stk6_2 + beta_alpha_all.jsonの4指標を組み合わせたBT:
  1. F_stk6_2をベースに
  2. alpha_stability_score でwinnerの品質を確認
  3. beta で high-beta winnerを罰する
  4. individual_factor でテーマ外の固有モメンタムを加算

期待:
  alpha_stability + beta_penalty が Bear Sharpe をさらに改善する可能性
  individual_factorがresidual momentumの「テーマ宇宙に適合した」版として機能する可能性
```


---

## E_stk12_7 + downside_vol_30 利用方法（ChatGPT, 2026-04-12）

### 核心: 2つは直交する役割

```
E_stk12_7      = 「何を持つか」を変える守備エンジン（selection改変）
downside_vol_30 = 「どれだけ持つか」を変えるリスク整形装置（overlay）

→ 代替案ではなく、別レイヤーで組み合わせるもの
```

### ChatGPT推奨の最終構造

```
案1（実務標準）: F_stk6_2 75% + E_stk12_7 25%（overlay なし）
案2（バランス本命）: F_stk6_2 70%(dvol30付き) + E_stk12_7 30%
案3（守備重視）: F_stk6_2 50% + E_stk12_7 50%

三役分担:
  selection alpha    = F_stk6_2
  defensive selection = E_stk12_7
  risk shaping       = downside_vol_30（F側のみ）
```

### 避けるべきこと

```
❌ E_stk12_7を「相場が悪い時だけ入れる」→ binary switchの再発
❌ downside_vol_30をBear解決策として売る → BTとズレる
❌ E + downside overlayを両方フルにかける → 期待リターンを二重に削る
```

### 将来拡張

```
E_stk12_7 + quality overlay が最も筋の良い拡張
  → 12-7 intermediate horizonのunderreaction
  → + quality/profitabilityで「良いwinner」確認
  → beta_alpha_all.jsonのAlpha Stability Scoreで代理可能
```


---

## Split-Window 2×2 Factorial BT Spec (2026-04-12)

### ChatGPT合意点（3名）

```
✅ 63日は下限として妥当だが、主窓の本命ではない（126日がより強い）
✅ 21日は文献的に最弱（短期反転汚染）
✅ OLS窓とα形成期間は分離すべき
✅ shrink(R²)は主補正として弱い（t値ベースの方が筋が良い）
✅ theme層は残す（factor/industry carrier）
```

### ChatGPT分岐点と統合判定

```
#1: stock rankingを6-2mo horizonに変更（効果量既知）
#2: split-window（β=126d, α=63d）を先に測る（効果量未知）
#3: 2×2 factorial で因果分解する（#1と#2の交互作用を同時測定）

採用: #3（2×2 factorial）
理由: formation horizonとOLS推定窓の効果を因果分解できる

252/63はfirst-passではなくsecond-pass challengerに回す
shrink(R²)は今は変えない（因果帰属を壊さないため）
```

### 2×2 Factorial Design

```
        OLS=63d (O0)     OLS=126d (O1)
      ┌──────────────┬──────────────┐
H0    │ A: H0/O0     │ B: H0/O1    │  ← current formation
(63d) │ = A_current   │ = split126  │
      ├──────────────┼──────────────┤
H1    │ C: H1/O0     │ D: H1/O1    │  ← 6-2mo formation
(6-2) │ = F_stk6_2   │ = combo     │
      └──────────────┴──────────────┘

B-A = split-windowの純粋効果
C-A = formation horizonの純粋効果（既知: Bear +0.28）
D-C = formation変更後のsplit-window追加効果
D-B = split-window後のformation追加効果
D-A = 両方変更の合計効果
(D-A) - (B-A) - (C-A) = 交互作用
```

### 判定ロジック

```
優先順:
  1. Bear Sharpe
  2. CAGR retention vs A_current
  3. MaxDD
  4. Sharpe
  5. 2022年挙動
```

### split-window実装仕様

```
Step 1: β/R²推定（OLS=126d or 252d）
  r_i(s) = a_i + b_i * r_theme_ex_self(s) + u_i(s)
  over s = t-125...t (126d) or t-251...t (252d)

Step 2: α形成（直近63d）
  alpha_daily = mean(r_i(s) - b_i * r_theme_ex_self(s))
  over s = t-62...t
  alpha_cum = 63 * alpha_daily

Step 3: スコア
  score = alpha_cum × shrink(R²_from_step1)
```


### Split-Window 2×2 Factorial BT結果 (2026-04-12)

```
                   CAGR    Sharpe  MaxDD    Bear
H0_O0 (baseline)  82.4%   1.898  -43.1%  -2.826
H0_O1 (split126)  87.0%   2.013  -43.4%  -2.767
H1_O0 (6-2mo)     75.1%   1.803  -40.4%  -2.728
H1_O1 (combo)     59.7%   1.433  -39.3%  -2.775

Δ vs baseline:
  split-window:  ΔCAGR=+4.6%  ΔSharpe=+0.114  ΔMaxDD=-0.3%  ΔBear=+0.060
  6-2mo形成:     ΔCAGR=-7.3%  ΔSharpe=-0.095  ΔMaxDD=+2.7%  ΔBear=+0.099
  combo:         ΔCAGR=-22.7% ΔSharpe=-0.465  ΔMaxDD=+3.7%  ΔBear=+0.051

Interaction effects:
  Sharpe: -0.484 (STRONGLY anti-synergistic)
  Bear:   -0.107 (anti-synergistic)
```

### 因果分解の結論

```
★ 最重要発見: split-windowと6-2mo formationは反相乗的（交互作用=-0.484）
  → 両方同時に適用すると互いの効果を相殺する
  → どちらか一方を選ぶべき

split-window単独 (H0_O1):
  ✅ CAGR +4.6pt（改善）
  ✅ Sharpe +0.11（最良）
  ✅ Bear +0.06（微改善）
  ❌ MaxDD -0.3pt（ほぼ同等）
  → 「推定品質の改善」だけでCAGR/Sharpeが上がる

6-2mo formation単独 (H1_O0):
  ✅ MaxDD +2.7pt（改善）
  ✅ Bear +0.10（改善）
  ❌ CAGR -7.3pt（悪化）
  ❌ Sharpe -0.095（悪化）
  → Bear/MaxDDは改善するが、alpha効率は下がる

判定:
  split-window (β=126d, α=63d) が圧倒的に効率が良い
  = CostゼロでSharpe/CAGRが改善する「free lunch」
  6-2mo formationはBear/MaxDD改善のために対価を払う設計
```

### 注意事項

```
  ① このBTは等ウェイト（W5bなし）で実行
  ② Bear Sharpe計算方法がPhase 1-4 BTと異なる（絶対値は非比較可能、Δのみ有効）
  ③ 次ステップ: split-window (H0_O1) をPRISM-Rの本番コードに実装
```


---

## ChatGPT 3名全会一致: split-window実装判定 (2026-04-12)

### 判定

```
採用:  H0_O1 = split-window (β=126d, α形成=63d) → 本番実装
維持:  H1_O0 = 6-2mo formation → DEF参考枝として別建て維持
棄却:  H1_O1 = split-window + 6-2mo → 反相乗的、棄却

適用対象:
  ✅ PRISM-R:  OLS α63×shrink(R²) → split-window直接適用
  ✅ PRISM-RQ: SNRb×shrink(R²) → 同じスクリプトで同時改善
  △ G2-MAX:   raw α63 (shrinkなし) → 部分適用可能だが2変更になる
  ❌ PRISM:    OLS不使用 → 適用不可
```

### ChatGPT全員の合意理由

```
1. split-windowはFREE LUNCH (CAGR+4.6pt, Sharpe+0.11, MaxDD不変)
2. β推定を長くするのはrolling OLS文献で標準的
3. 形成窓と推定窓を分離する方が統計的に自然
4. 6-2moとの組み合わせは反相乗的 = 同じ信号源を二重に削る
5. shrink(R²)は今は変えない（因果帰属を壊さないため）
```

### 実装仕様

```
変更前: ols_ab(y_63d, x_63d) → α_cum63, β_63, R²_63
変更後: split_alpha(y_126d, x_126d, y_63d, x_63d) → α_cum_63|126, β_126, R²_126

Step 1: β/R²推定 (126d)
  r_i(s) = a + b*r_theme_ex_self(s) + u, s ∈ [t-125...t]
Step 2: α形成 (63d)
  alpha_daily = mean(r_i(s) - β_126*r_theme_ex_self(s)), s ∈ [t-62...t]
  alpha_cum = alpha_daily × 63
Step 3: スコア
  score = alpha_cum × shrink(R²_126)
```


---

## 本セッション戦略変更 最終記録 (2026-04-12)

### 変更マトリクス

```
戦略       スコアリング変更         表示/UI変更              データ再生成
─────────────────────────────────────────────────────────────────────
PRISM-R    split-window導入         W5b実装/DEF線/UI改修     BT+shadow ✅
           β=126d, α=63d           split-window記述         cumulative ✅
           BT: CAGR +6.2pt         リバランスUI(案2+5)
           採用 ✅

PRISM-RQ   split-window導入         split-window記述         (PRISM-R共有)
           (PRISM-R共通backend)
           採用 ✅

G2-MAX     split-window 棄却        リバランスUI(案2+5)      shadow ✅
           OLS=63d維持(REVERT)      旧OLS記述維持
           BT: CAGR -11.3pt
           棄却→REVERT ❌

PRISM      変更なし                 リバランスUI(案2+5)      なし
           (OLS不使用)
```

### split-window BT結果（全バリアント）

```
戦略×ウェイト          ΔCAGR     ΔSharpe   ΔMaxDD    判定
────────────────────────────────────────────────────────
PRISM-R W5b           +6.2pt    +0.001    -1.3pt    採用 ✅
PRISM-R BEAST         +3.4pt    -0.024    -0.5pt    採用 ✅
PRISM-R 等W           +4.8pt    +0.075    -1.7pt    採用 ✅
PRISM-R DEF           ±0.0      ±0.0      ±0.0      対象外
G2-MAX W5b (WARMUP252)+3.3pt    +0.034    -1.6pt    条件依存
G2-MAX W5b (dashboard)-11.3pt   -0.135    -0.9pt    棄却 ❌
G2-MAX BEAST(dashboard)-18.7pt  -0.167    -8.9pt    棄却 ❌
```

### G2-MAX乖離問題の記録

```
g2max_split_bt.py (WARMUP=252, W5b=銘柄レベル): CAGR +3.3pt → 改善に見えた
ダッシュボード条件 (WARMUP=126, W5b=テーマレベル): CAGR -11.3pt → 実際は有害

乖離原因:
  ① WARMUP差 (126 vs 252) → 2020年の爆発期を含むかで20倍の累積差
  ② W5b加重基準 (テーマ vs 銘柄) → 完全に異なるウェイト配分
  ③ OLS minimum (20 vs 10) → 選定候補の違い

教訓: BTの条件がダッシュボードと一致していなければ結論は信頼できない
```

### 確定した法則（研究82+バリアント）

```
✅ ranking horizon >> cap >> overlay >> residual
✅ theme層はfactor/industry carrier → 消してはいけない
✅ split-windowはshrink(R²)ありの戦略で有効、raw αの戦略で有害
✅ 6-2moとsplit-windowは反相乗的（交互作用=-0.484）→ 併用不可
✅ BT条件（WARMUP, W5b基準）が結論を逆転させうる → 条件一致が必須
```

### ファイル変更一覧

```
スコアリング:
  scripts/generate_prism_r.py → split_alpha()導入 (f89c914)
  scripts/generate_g2max.py → split_alpha()導入→REVERT (0354908→68b2d57)

BT再生成:
  research/scb/generate_bt_returns.py → split_alpha() + DEF系列 (f89c914)
  public/api/prism-r/cumulative_returns.json → 再生成済み
  public/api/prism-r/shadow_comparison.json → 再生成済み
  public/api/prism-g2/shadow_comparison.json → 旧版で再生成済み

フロントエンド:
  public/prism-r.html → W5b/DEF/split-window記述/UI改修
  public/prism-rq.html → split-window記述/キャッシュ修正
  public/prism-g2.html → UI改修のみ（split-window記述なし）
  public/prism.html → UI改修のみ

インフラ:
  vercel.json → HTMLページ no-cache ヘッダー
  public/sw.js → st-v7→st-v8
  全8ページ → theme_ranking.json cache bust

研究BT:
  research/scb/split_window_bt.py → 2×2 factorial (b817011)
  research/scb/g2max_split_bt.py → G2-MAX A/B (af1483c)
  research/scb/bear_resolution_bt.py → Phase 1-2 (e7dbd65)
  research/scb/bear_phase3.py → Phase 3 (e3d89fe)
  research/scb/bear_phase4.py → Phase 4 (c3794b6)
```


---

## PIT Review Checkpoint — 2026-04-13

**Status: CLOSED_SILVER (provisional, not Gold/final)**
**Effect on live strategy: none**
**Effect on interpretation of historical backtests: material**

### Summary

A comprehensive PIT review was conducted on historical theme-membership contamination.
B1 (IPO filter), D1 (diff measurement), E4-lite (3-tier bounds), and winner-alpha
stress-lite were executed in sequence. ChatGPT dual-review was conducted at each stage.

### Key Findings

#### 1. B1 (IPO filter) is a no-op

The Norgate price join already excludes pre-IPO members implicitly.
Panel construction naturally limits each date to tickers with existing price data.
Therefore, "未存在銘柄の先読み" was never the binding contamination channel.

Evidence: B1 filter removed 0 rows from 4,609,548 (0.0%).
Temporal coverage confirms natural filtering:
  2000: 426/846 tickers (50%) — missing tickers simply have no price rows
  2010: 545/846 (64%)
  2020: 751/846 (89%)
  2026: 846/846 (100%)

#### 2. Dominant remaining contamination channels

- **Winner selection bias**: current frozen members are retrospective survivors
- **Loser omission**: 0 delisted tickers in frozen membership (complete loser erasure)
- **Ex post theme definition**: themes like "AI半導体コア" may not have existed in 2000

#### 3. Over 25 years, A5 does NOT outperform A4

```
Production BT (25yr, 310 months):
              CAGR    Sharpe  MaxDD
  A4 等W     +24.4%   0.91   -51.4%   ← A4 wins on Sharpe
  A5 α63    +25.2%   0.88   -57.8%
  W5b        +27.2%   0.87   -54.6%
  BEAST      +26.1%   0.84   -61.5%
  DEF        +26.8%   1.03   -52.5%   ← best Sharpe
  SPY         +8.1%   0.59   -50.8%
```

This reversal (vs 5yr BT where A5 > A4) is NOT explained by PIT contamination.
Both Upper and Middle tiers show A4 > A5. It is period dependence.

#### 4. Theme-layer excess over SPY is robust

All tested strategies exceed SPY in Upper, Middle, and Stress views.
This structural value of the theme layer is the most reliable historical finding.

#### 5. E4-lite bounds (Upper / Middle / Stress)

```
                    UPPER (207テーマ)    MIDDLE (45テーマ,IPO<2005)   STRESS (30%α haircut)
Strategy          CAGR   Sharpe MaxDD   CAGR   Sharpe MaxDD         CAGR   Sharpe MaxDD
A4 等W           +24.4%  0.91  -51.4%  +17.6%  0.85  -43.0%       +24.3%  0.90  -51.4%
A5 α63          +25.2%  0.88  -57.8%  +16.3%  0.80  -54.3%       +25.6%  0.89  -56.5%
W5b              +27.2%  0.87  -54.6%  +17.3%  0.81  -52.8%       +27.5%  0.89  -52.9%
DEF              +26.8%  1.03  -52.5%  +16.8%  0.85  -54.2%       +26.7%  1.02  -52.5%
SPY               +8.1%  0.59  -50.8%   +8.1%  0.59  -50.8%        +8.1%  0.59  -50.8%
```

Middle is a low-contamination / low-novelty subset (NOT a true lower bound).
CAGR ~35% lower in Middle mixes contamination removal with opportunity loss.

#### 6. Winner-alpha haircut stress (30%) is immaterial

Stress-lite specification: at each rebalance, top 2 α scores per theme
are reduced by 30%. This targets winner selection bias in α estimation.

Result: ΔSharpe ≈ 0 for all strategies (max -0.01).
This does NOT prove theme structure is the sole edge source.
It indicates that winner-alpha overweighting within surviving frozen
membership is not a major driver under the tested specification.

### Interpretation

This review does NOT eliminate PIT contamination.
It does NOT prove that theme structure is the sole source of edge.

It does establish:
- Pre-IPO contamination channel is already neutralized (B1 = no-op)
- Winner-alpha bias within survivors does not materially change conclusions
- The most reliable claim is "theme layer > SPY"
- The least reliable claim is "A5 > A4" and "absolute CAGR values"

### Reliability Classification (post-PIT review)

```
MORE RELIABLE:
  ✅ Theme layer > SPY (directional, all 3 views confirm)
  ✅ A4 ≥ A5 over 25yr sample (directional, all 3 views confirm)
  ✅ DEF has best risk-adjusted profile (Sharpe leader in Upper)

LESS RELIABLE:
  ⚠ Absolute CAGR / Sharpe magnitudes (Middle shows ~35% CAGR haircut)
  ⚠ A5 superiority over A4 (5yr finding, not supported in 25yr)
  ⚠ Any claim depending on exact historical membership fidelity

NOT TESTED:
  ❌ Loser omission impact (no delisted injection performed)
  ❌ Ex post theme definition impact (theme existence not verified)
  ❌ Full lower-bound reconstruction
```

### Remaining Open Items

```
Still unresolved (deferred to future phases):
  - Loser omission: full adversarial injection with GICS donor pool
  - Ex post theme definition: theme existence verification pre-2010
  - C1 (GICS proxy): high-purity themes only, current GICS ≠ fully PIT
  - B3 full bootstrap: 4 scenarios × 200-500 passes
  - Historical GICS vendor (S&P Global / Compustat): cost/benefit TBD
  - TNIC text-based classification: research-grade, not immediate
```

### Operational Implication

No live strategy change triggered.
Main governance effect:
- Historical absolute values remain non-canonical
- Directional claims usable with caution
- Forward PIT-safe evidence (capture_snapshot.py) remains primary promotion standard
- Silver Layer rules unchanged: relative comparisons only, no absolute CAGR for decisions
