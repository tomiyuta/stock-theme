# PRISM v2 — 判定基準（Sprint 2開始前に固定）

## 採用ベースライン（Sprint 2結果に基づき更新）

```
Current adopted baseline: PRISM_MH20

Reason:
Among tested PRISM variants, MH20 delivered the best overall balance of
CAGR (+31.1%), MaxDD (-6.0%), and Sharpe (1.46), outperforming BM3 on
CAGR, MaxDD, and Sharpe over the 126-day reconstructed historical period.

PRISM v1 (no min hold) is retired as the default — it was over-trading.
```

## Sprint 2 ベンチマーク結果

| Strategy | CAGR | MaxDD | Sharpe | Trades | AvgHold | WorstDay |
|---|---|---|---|---|---|---|
| BM2 (SPY/SHV) | -2.2% | -5.1% | -0.15 | 249 | 46.8 | -2.7% |
| BM3 (Sector) | +19.4% | -7.0% | 1.41 | 747 | 39.3 | -2.1% |
| BM5 (Direct Stock) | +41.5% | -44.4% | 0.83 | 2276 | 31.2 | -15.3% |
| PRISM v1 | +28.9% | -9.1% | 1.16 | 4476 | 25.0 | -7.2% |
| **PRISM MH20** | **+31.1%** | **-6.0%** | **1.46** | 13404* | 30.7 | -4.8% |

*Trades count is a replay artifact (daily flatten/rebuild). Actual position changes are far fewer.

## 比較対象

| ID | ベンチマーク | 内容 |
|---|---|---|
| BM2 | SPY/SHV regime switching | K-gate OPEN→SPY, CLOSED→SHV |
| BM3 | Sector ETF rotation | Layer 1通過セクターETFを等加重 |
| BM5 | Direct stock momentum | テーマ層なし、915銘柄から直接3M上位選定 |
| PRISM v1 | Full stack | 現行4層フィルタ |

## 評価指標

- CAGR
- MaxDD
- Sharpe
- Turnover（年間回転率）
- Avg holding period
- Worst month
- Crash window performance（MaxDD期間中のリターン）

## テーマ層の価値判定基準

```
テーマ層に価値ありとみなす条件:
- BM3/BM5 に対し CAGR で明確劣後しない
- かつ MaxDD または Turnover または Worst month のいずれかで有意な改善がある
- 単なるリターン上振れだけでは採用しない
- PRISMがBM3と同等以下なら、テーマ層は不要な複雑さと判断する
```

## データセット区分

| Dataset | 期間 | 性質 | 用途 |
|---|---|---|---|
| A: Reconstructed | 2025-07-09〜2026-04-08 (189日) | テーマ構成=現在定義固定 | 相対比較・予備検証 |
| B: True forward | 2026-04-09〜 | 当時の状態そのもの | 本番監査・最終採用判定 |

## バイアス注記

- BM2: バイアスなし（ETFのみ）
- BM3: バイアスなし（セクターETFのみ）
- BM5: survivorship bias低（現在の915銘柄固定）
- PRISM v1: テーマ構成固定バイアス中（現在定義を過去へ投影）
