# PRISMATIC Session Handoff — 2026-04-13 FINAL

## 一文要約

PRISMATICの主力ロジックは、A5の複雑なα推定ではなく、A4テーマ選択とDEF銘柄選択の組み合わせである。A5は研究枝へ降格し、DEFテーマ選択は棄却する。

## Git状態
```
HEAD: 1ef3f13 (clean, pushed)
Session commits: 12 (770f0a2 → 1ef3f13)
```

## Hard Findings

```
PIT_CHAPTER         = CLOSED_SILVER
A5_GT_A4_25Y        = FALSE (全3ビュー + 2×2 tournament確認済み)
THEME_LAYER_GT_SPY  = ROBUST (全ビューで維持)
PRODUCTION_DEF      = L1:A4-theme × L2:DEF-stock (Sharpe 1.02)
A5_SCORING          = DEMOTED to research-only (両L1でマイナス)
DEF_THEME_SELECT    = REJECTED (A4テーマより劣後)
ABSOLUTE_CAGR       = NOT_TRUSTWORTHY (Middle tierで~35%低下)
WINNER_ALPHA_HAIRCUT = IMMATERIAL (ΔSharpe ≈ 0)
```

## Strategy Hierarchy (FROZEN)

```
Tier 1 (Production):
  Flagship: DEF = A4-theme × DEF-stock (Sharpe 1.02, best risk-adjusted)
  Baseline: A4  = A4-theme × A4-stock  (Sharpe 0.91, simplest robust)

Tier 2 (Research-only):
  A5 = alpha scoring系 (付加価値なし、温存のみ)

Tier 3 (Lab):
  G2-MAX / BEAST / W5b = 実験室 (絶対値禁止)
```

## Stop-Doing List

```
× A5の追加改良（CRA、continuity、split-window微調整）
× DEFテーマ選択の深掘り
× A5優位前提のUI/説明
× G2-MAXの絶対CAGRで語る
× Historical GICS購入（cost/benefit不良）
× B4単一係数補正
```

## Next-Phase Priority

```
1. DEFをデフォルト表示に昇格するかの最終判断
2. A5コードの研究枝分離（本番依存を切る）
3. Forward PIT蓄積進捗確認（capture_snapshot.py）
4. BAM May observation protocol (5/1 14:00 JST)
```

## 2×2 Tournament Results (確定)

```
                         L2=A4stock    L2=A5alpha
L1=A4-theme (3M mom)     Sh=0.84       Sh=0.82     ← A5はマイナス
L1=DEF-theme (7-12mo)    Sh=0.71       Sh=0.65     ← DEFテーマは劣後

Production DEF (L1=A4 × L2=DEF-stock): Sh=1.02     ← 最良
→ DEFの価値はテーマ選択ではなく銘柄選択ロジック（12-7mo intermediate α）
```

## Key File Paths

```
SHADOW_BOOK:  research/scb/SHADOW_BOOK.md (PIT Review + Tournament記録済み)
Production BT: research/scb/generate_bt_returns.py
PIT Brief:    research/scb/PIT_DECONTAMINATION_BRIEF.md
Dashboard:    public/prism.html (ロジックカード更新済み)
```

## 次チャット冒頭指示

```
stock-theme プロジェクトの続き。
research/scb/HANDOFF_20260413_PM.md を読め。
research/scb/SHADOW_BOOK.md の末尾200行も読め。
戦略階層は凍結済み。DEF=flagship, A4=baseline, A5=research-only。
```
