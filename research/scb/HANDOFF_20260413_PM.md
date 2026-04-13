# PRISMATIC Session Handoff Summary (2026-04-13 PM)

## Git状態
```
HEAD: 5eef3f8 (clean, pushed)
Repo: /Users/yutatomi/Downloads/stock-theme
Commits this session: 8 (770f0a2 → 5eef3f8)
```

## 本セッションで完了したこと（MECE）

### A. Norgate PITデータセット構築（完了 ✅）
- 全米株式34,823銘柄（Active 13,937 + Delisted 20,886）の日次価格取得
- PIT指数構成（SP500/400/600, Russell 1000/2000/3000）取得
- ETF 47銘柄、メタデータ（GICS 4階層）取得
- トリム: 2,230MB → 338MB（R3000+テーマ×close only）
- theme_panel_v2: 207テーマ×846銘柄、2000〜2026（旧5yr→25yr拡大）

### B. 全4戦略の25yr BT実行・ダッシュボード反映（完了 ✅）
- generate_bt_returns.py: v2パネル+新metadata接続
- gen_cum_rq.py / gen_cum_g2.py: SPY yfinance→Norgate ETF置換
- longterm_bm5_v2.py / longterm_backtest_v2.py: 新self-containedスクリプト
- 全ダッシュボード: sortino/calmar追加、25年表記、G2対数スケール

### C. PIT Review Chapter（CLOSED_SILVER ✅）
- B1(IPOフィルタ) = no-op（Norgate joinで暗黙適用済み）
- E4-lite 3ティア（Upper/Middle/Stress）実行
- Winner α haircut 30% stress-lite 実行 → immaterial（ΔSharpe≈0）
- 確定: theme>SPY robust, A5>A4 not supported over 25yr
- ChatGPT dual-review 3往復で設計検証済み
- SHADOW_BOOK記録済み

## PIT Review 確定結論（戦略判断に直結）

```
HARD FINDINGS:
  ✅ テーマ層 > SPY は頑健（全3ビューで維持）
  ✅ A5 > A4 は25年では不成立（期間依存、PIT汚染とは独立）
  ✅ DEF が最良Sharpe（1.03、Upper）
  ✅ Winner α bias は結論に影響しない（stress-lite確認済み）
  ⚠ 絶対CAGR値は信頼不能（Middle tierで~35%低下）

RELIABILITY CLASSIFICATION:
  MORE RELIABLE: theme>SPY, A4≥A5, DEF best risk-adjusted
  LESS RELIABLE: absolute CAGR/Sharpe, A5 superiority
  NOT TESTED: loser omission, ex post theme definition
```

## 現在のダッシュボード最終値（25yr PIT BT）

```
PRISM / PRISM-R (310 months, 2000-07 ~ 2026-04):
  A4 等W     CAGR=+24.4%  Sharpe=0.91  Sortino=1.83  Calmar=0.47  MaxDD=-51.4%
  A5 α63    CAGR=+25.2%  Sharpe=0.88  Sortino=1.68  Calmar=0.44  MaxDD=-57.8%
  W5b        CAGR=+27.2%  Sharpe=0.87  Sortino=1.84  Calmar=0.50  MaxDD=-54.6%
  BEAST      CAGR=+26.1%  Sharpe=0.84  Sortino=1.74  Calmar=0.43  MaxDD=-61.5%
  DEF        CAGR=+26.8%  Sharpe=1.03  Sortino=2.03  Calmar=0.51  MaxDD=-52.5%  ★
  SPY        CAGR=+8.1%   Sharpe=0.59  Sortino=0.82  Calmar=0.16  MaxDD=-50.8%

PRISM-RQ (310 months):
  A5-SNRb    CAGR=+26.3%  Sharpe=0.92  Sortino=1.36  Calmar=0.49  MaxDD=-54.1%

G2-MAX (309 months):
  W5b        CAGR=+149.5% Sharpe=1.63  Sortino=2.93  Calmar=2.54  MaxDD=-58.9%
  BEAST      CAGR=+132.6% Sharpe=0.87  Sortino=1.46  Calmar=1.85  MaxDD=-71.8%

BM3+制約 (24yr ETF): Sharpe=0.57  MaxDD=-33.1%  ← 完全PIT
BM5+制約 (24yr PIT個別株): Sharpe=0.77  MaxDD=-21.2%  ← 完全PIT
```

## PIT Review が戦略方針に与える影響

```
変更前（5yr BTベース）:
  ・A5のα補正がA4より優位 → α scoring が戦略の核
  ・W5b/BEASTが最高CAGR → 集中投資が最適
  ・DEFは補助的

変更後（25yr PIT + PIT Review）:
  ・A4 ≧ A5 → α scoring の付加価値は限定的
  ・テーマ層自体のモメンタムが主要エッジ
  ・DEFが最良Sharpe → リスク調整では防御型が最適
  ・W5b/BEASTのCAGR優位は絶対値信頼不能
  ・G2-MAXの127億倍は完全にartifact
```

## 次フェーズ候補（優先度順）

### 優先度1: 戦略アーキテクチャの再評価
- [ ] A5のα scoringを外してA4純モメンタムにした場合のforward影響を検討
- [ ] DEFの運用昇格判断（Sharpe最良→メイン戦略候補か？）
- [ ] PRISM-R vs PRISM（A5 vs A4）のダッシュボード表現見直し
- [ ] 「A5優位」前提で書かれたUI文言・ロジックカードの更新

### 優先度2: Forward PIT蓄積の加速
- [ ] capture_snapshot.pyのGitHub Actions組み込み確認
- [ ] 蓄積データの自動バリデーション追加
- [ ] 6ヶ月後（2026-10）の初回PIT-free BT設計

### 優先度3: ダッシュボード品質
- [ ] iPhone全ページレイアウト検証（リバランスUI改修後）
- [ ] prism.htmlのロジックカード「長期検証」セクション更新（25yr結果反映）
- [ ] 戦略系譜セクションの旧5yr数値をPIT Review結果で更新

### 優先度4: 研究バックログ
- [ ] beta_alpha_all.jsonからquality proxy構築
- [ ] PIT Review open items: loser omission本格評価、C1 GICS proxy
- [ ] B3 full bootstrap（200-500パス）
- [ ] BAM May observation protocol (5/1 14:00 JST)

## キーファイルパス
```
新規データ:
  research/scb/norgate_us_prices.parquet        224 MB  (trimmed, R3000+theme)
  research/scb/norgate_index_membership.parquet   76 MB
  research/scb/norgate_us_metadata.parquet        0.5 MB
  research/scb/norgate_etf_prices.parquet         10 MB
  research/scb/norgate_theme_panel_v2.parquet     28 MB

E4-lite中間ファイル:
  research/scb/norgate_theme_panel_e4_middle.parquet  (45テーマ, IPO<2005)
  research/scb/norgate_theme_panel_e4_lower.parquet   (28テーマ, IPO<2000)

PIT Review:
  research/scb/PIT_DECONTAMINATION_BRIEF.md     (ChatGPT用ブリーフ)
  research/scb/SHADOW_BOOK.md                   (PIT Review Checkpoint追記済み)

新BT v2スクリプト:
  scripts/longterm_bm5_v2.py     (BM5, 外部依存排除)
  scripts/longterm_backtest_v2.py (BM3, 外部依存排除)
```

## 次チャット冒頭指示

```
stock-theme プロジェクトの続き。
research/scb/HANDOFF_20260413_PM.md を読め。
research/scb/SHADOW_BOOK.md の末尾140行も読め。
PIT章はCLOSED_SILVER。次フェーズに進む。
```
