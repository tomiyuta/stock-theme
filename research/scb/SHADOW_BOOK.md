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
