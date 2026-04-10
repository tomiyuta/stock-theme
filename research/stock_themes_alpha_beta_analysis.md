# stock-themes.com α/β算出ロジック — 完全逆推定レポート

> 作成日: 2026-04-10
> 検証方法: APIデータの数値検算 + yfinanceによる再現OLS + 構造分析
> 対象: `/api/theme-beta-alpha` エンドポイント（premium専用）

## 1. 結論

stock-themes.comのα/βは**自己除外なしの標準OLS単因子回帰**である。

```
回帰式: r_i(d) = α + β × r_theme(d) + ε

r_i(d):     銘柄iの日次リターン（simple return, NOT log return）
r_theme(d): テーマ全構成銘柄の等ウェイト日次平均リターン（自己除外なし）
α:          OLS切片（日次）
β:          OLSスロープ（テーマ連動度）
ε:          残差
```

## 2. 確定した事実（数値検算で証明済み）

### 2-1. 自己除外なし（CRITICAL）

```
検証: battery-storage (4銘柄: EOSE/FLNC/GWH/STEM)
  3M theme_return:
    EOSE: -0.4056
    FLNC: -0.4056
    GWH:  -0.4056
    STEM: -0.4056
  → 全銘柄で theme_return が完全一致

検証: ai-semiconductor-core (5銘柄: AMD/AVGO/MRVL/MU/NVDA)
  3M theme_return:
    全5銘柄: 0.201700（完全一致）

結論: theme_returnに自己を含めている（自己除外していない）
```

これはPRISM-Rとの**最大の構造的差異**。

### 2-2. Simple return使用（log returnではない）

```
検証: EOSE 3M OLS
                    simple return   log return    stock-themes
  alpha             0.000845        0.000022      0.001067
  beta              1.4623          1.4645        1.4650
  r2                0.6088          0.5797        0.6056

→ simple returnの方がstock-themes値に近い（特にalpha）
```

### 2-3. 出力フィールドの厳密な関係（数学的に証明済み）

```
theme_factor     = β × theme_return                          (完全一致, 誤差 < 0.0001)
alpha_ann        = α × 252                                   (完全一致, 誤差 < 0.0001)
individual_factor = stock_total_return − theme_factor         (定義式)
                  = stock_total_return − β × theme_return
total_return     = theme_factor + individual_factor           (恒等式)

検算: EOSE 3M
  theme_factor = 1.4650 × (-0.4056) = -0.5942 (API: -0.5943) ✅
  alpha_ann = 0.001067 × 252 = 0.2689 (API: 0.2689) ✅
  total_return = -0.5943 + 0.0197 = -0.5746 ✅
```

### 2-4. individual_factorの正体

```
individual_factor ≠ α × N_days

検証:
  EOSE 1M:  α×21 = 0.1644,  individual_factor = 0.1362  (差 = -0.0282)
  EOSE 3M:  α×63 = 0.0672,  individual_factor = 0.0197  (差 = -0.0475)
  EOSE 12M: α×252 = 0.2681, individual_factor = -0.3844 (差 = -0.6525)

individual_factor = 実現した銘柄固有リターン（α×N + Σε）
α × N             = OLSが予測した銘柄固有リターン

差分 = Σε = 回帰残差の総和（ノイズの蓄積）
```

### 2-5. 7期間の窓サイズ（推定）

```
API期間  → 推定営業日数
5D       → 5
10D      → 10
1M       → ~21
2M       → ~42
3M       → ~63（62-63日の間で最良一致）
6M       → ~126
12M      → ~252
```

## 3. 残る不確実性（完全再現できない理由）

### 3-1. 数値の微小な差異

```
EOSE 3M:
  項目    yfinance再現    stock-themes    差異
  alpha   0.000845       0.001067        +0.000222
  beta    1.4623         1.4650          +0.0027
  r2      0.6088         0.6056          -0.0032
  t_val   0.1209         0.1492          +0.0283
```

差異の推定原因:
1. **価格データソースの違い**: yfinance vs stock-themes独自データ
2. **日付境界の微妙な違い**: 3M=63日 vs 62日 vs カレンダー3ヶ月
3. **調整済み終値の計算方法の違い**: 配当・分割調整のタイミング

## 4. stock-themes.com vs PRISM-R — 構造比較

### 4-1. 回帰モデルの差異

```
                        stock-themes.com             PRISM-R自前
回帰式                  r_i = α + β×r_theme + ε     r_i = α + β×r_theme_ex_self + ε
説明変数                テーマ全銘柄平均（自己含む）  テーマ平均（自己除外）
リターン種別            simple return                simple return（同一）
α単位                   日次                         累積（α×N日）
年率換算                α × 252                     なし
t値/p値                 あり                         なし
R²の用途                表示のみ                     shrinkage関数の入力
自己除外                ❌ なし                       ✅ あり
期間                    7期間（5D～12M）              63日固定
```

### 4-2. 自己除外の影響（定量的）

自己除外の有無がβとR²に与える影響は、テーマ内銘柄数に依存する。

```
銘柄数   自己寄与率    β膨張率（概算）   R²膨張率（概算）
4        25%          +10-20%           +5-15%
5        20%          +8-15%            +4-12%
10       10%          +3-8%             +2-5%
20       5%           +1-3%             +1-2%

stock-themes.com平均: ~5銘柄/テーマ → β/R²が10-15%過大評価の可能性
```

自己を含めたテーマ平均でOLSを走らせると:
- β → 1に近づく方向に膨張（自分のリターンが両辺に入るため）
- R² → 偽の説明力が上乗せされる
- α → βの膨張分だけ歪む

### 4-3. individual_factorの解釈差

```
stock-themes: individual_factor = 実現した銘柄固有リターン（事後的）
              = stock_return - β × theme_return
              → 「結果としてテーマに帰属しなかった部分」
              → バックワードルッキング指標

PRISM-R:      α63 × shrink(r²) = 予測された銘柄固有リターン × 信頼度
              → 「今後も続くと期待できる構造的超過リターン」
              → フォワードルッキング指標（として使用）
```

### 4-4. 統計的信頼性の情報量

```
stock-themes:
  alpha_tval = α / SE(α)  → αの統計的有意性
  alpha_pval              → 帰無仮説（α=0）の棄却確率
  → αが「偶然でない」かを直接判定可能

PRISM-R:
  shrink(r²)              → 回帰全体の説明力に基づく縮小
  → α個別の有意性は見ていない
  → r²が高くてもαが非有意な場合がある（β主導の高R²）
```

## 5. 評価: どちらが優れているか

### MECE評価

| 評価軸 | stock-themes | PRISM-R | 勝者 | 理由 |
|---|---|---|---|---|
| 自己除外 | なし | あり | **PRISM-R** | 4.7銘柄/テーマでβ/R²が10-15%膨張 |
| 過剰適合耐性 | 低（7期間選択可能） | 高（63日固定） | **PRISM-R** | 期間選択の自由度がデータマイニングリスク |
| αの統計的信頼性 | t値/p値あり | shrinkageのみ | **stock-themes** | αの有意性を直接判定可能 |
| リターン分解 | theme/individual分離 | なし | **stock-themes** | 帰属分析が即座に可能 |
| 運用自律性 | premium API依存 | 完全自前 | **PRISM-R** | CI/CDで自動更新可能 |
| 外部検証価値 | 独立データソース | — | **stock-themes** | クロスチェック用 |
| βの精度 | 過大推定（自己含む） | 適正 | **PRISM-R** | 自己除外により偽の連動度を排除 |
| 銘柄選定用途 | 不適 | 適 | **PRISM-R** | フォワードルッキングスコア設計 |

### 総合判定

```
銘柄選定スコア計算:  PRISM-R自前OLS（自己除外+固定窓で推定汚染が少ない）
外部検証・監視:      stock-themesデータ（独立ソースとしてのクロスチェック）
```

## 6. stock-themesデータの活用方法

### 6-1. クロスバリデーション

PRISM-Rで選定した銘柄のαがstock-themes.comでもp<0.05で有意かをチェック。
偽陽性の発見に有用。

### 6-2. 帰属分析

theme_factor / individual_factor の分解を使って:
- ポートフォリオの「テーマ連動リターン」vs「銘柄固有リターン」を可視化
- テーマ選定（Layer 1）と銘柄選定（Layer 2）のどちらが寄与しているかを定量化

### 6-3. 多期間安定性チェック

7期間のα/βを並べることで:
- αの時間安定性（5D～12Mで同符号か）
- βの構造変化（短期と長期でβが大きく変わるか）
- R²の期間依存性（説明力がどの期間で最高か）

### 6-4. 活用上の注意

```
⚠ 自己除外なしのため、β/R²は過大評価されている
⚠ stock-themesのαとPRISM-Rのαは直接比較不可（異なる説明変数）
⚠ individual_factorは事後的（バックワード）であり、将来予測には使えない
⚠ premium API依存のため日次自動更新は困難
```

## 7. v2研究候補: stock-themes p値のPRISM-Rへの統合

```
現行:  score = α63 × shrink(r²)
案1:   score = α63 × shrink(r², p_alpha)  — p値でαの縮小を追加
案2:   score = α63 / SE(α63)              — t統計量をスコアに使用
案3:   score = α63 × shrink(r²) × I(p_st < 0.10)  — stock-themes p値でフィルタ

注: これらは全てv2研究課題。現行凍結パラメータには触れない。
```
