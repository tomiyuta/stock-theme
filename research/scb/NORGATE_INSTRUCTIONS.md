# Norgate Data 取得手順書（Windows クラウド端末用）

## 目的
847テーマ構成銘柄の日次価格を5年分取得し、A5-lite戦略のバックテスト期間を
252日(7リバランス) → 約1,260日(55リバランス) に拡張する。

## 前提条件
- [x] Norgate Data Platinum契約
- [ ] NDU (Norgate Data Updater) インストール済み
- [ ] Python 3.8+ インストール済み
- [ ] norgatedata, pandas, pyarrow インストール済み

---

## 手順

### Step 1: NDUの起動とデータ更新

1. Windowsデスクトップから **Norgate Data Updater (NDU)** を起動
2. 左メニューの **"Update"** をクリック
3. **全データベースの更新が完了するまで待つ**（初回は30分程度）
4. ステータスバーに "Update complete" と表示されることを確認
5. **NDUは閉じずに起動したまま**にしておく（Python APIがNDU経由でアクセスする）

### Step 2: Pythonパッケージ確認

コマンドプロンプト（cmd）またはPowerShellで:

```cmd
pip install norgatedata pandas pyarrow numpy
```

確認:
```cmd
python -c "import norgatedata; print(norgatedata.version())"
python -c "import pandas; print(pandas.__version__)"
```

### Step 3: ファイル配置

以下の3ファイルを**同じフォルダ**に配置する（例: `C:\norgate_fetch\`）:

```
C:\norgate_fetch\
  ├── norgate_fetch.py              ← メインスクリプト
  ├── ticker_list_847.txt           ← 847銘柄リスト
  └── theme_membership_frozen.json  ← テーマ構成定義
```

**これら3ファイルはMac側の以下にある:**
```
~/Downloads/stock-theme/research/scb/norgate_fetch.py
~/Downloads/stock-theme/research/scb/ticker_list_847.txt
~/Downloads/stock-theme/research/scb/theme_membership_frozen.json
```

転送方法（いずれか）:
- Google Drive / OneDrive / Dropbox 経由
- USB / SCP / RDP経由のコピー&ペースト
- GitHubリポジトリから `git pull`

### Step 4: スクリプト実行

```cmd
cd C:\norgate_fetch
python norgate_fetch.py
```

**実行時間の目安: 5〜15分**（847銘柄×5年分のAPI呼び出し）

### 期待される出力

```
======================================================================
Norgate Data Fetcher for A5-lite Backtest
======================================================================
  pandas 2.x.x, numpy 1.x.x
  norgatedata 1.0.xx
  NDU status: OK

Target tickers: 847
Themes: 207

Fetching daily prices from 2020-01-01...
  [  1/847] A         (0s elapsed, 0 ok, 0 failed)
  [ 50/847] ADSK      (12s elapsed, 49 ok, 1 failed)
  ...
  [847/847] ZBRA      (180s elapsed, 780 ok, 67 failed)

  Done in 180s
  OK: 780, Failed: 67, Coverage: 92%

  Raw prices: 1,200,000 rows → output/norgate_raw_prices.parquet
  Panel: 5,600,000 rows → output/norgate_theme_panel.parquet

COMPLETE
```

### Step 5: 出力ファイルの確認

`output\` フォルダに3ファイルが生成される:

| ファイル | サイズ目安 | 用途 |
|---|---|---|
| `norgate_theme_panel.parquet` | 20-50MB | **Mac側に転送する本体** |
| `norgate_raw_prices.parquet` | 10-30MB | バックアップ（転送不要） |
| `norgate_coverage_report.txt` | 数KB | **カバレッジ確認用（転送する）** |

### Step 6: Mac側への転送

1. `norgate_theme_panel.parquet` と `norgate_coverage_report.txt` をMacに転送
2. Mac側で以下に配置:
```
~/Downloads/stock-theme/research/scb/norgate_theme_panel.parquet
~/Downloads/stock-theme/research/scb/norgate_coverage_report.txt
```

### Step 7: Mac側でバックテスト実行

Claudeに以下を指示:
```
norgate_theme_panel.parquetを使ってA4 vs A5-liteの拡張バックテストを実行せよ
```

---

## トラブルシューティング

### NDU接続エラー
```
WARNING: Unable to obtain valid status from Norgate Data
```
→ NDUアプリが起動していない。タスクバーにNDUアイコンがあるか確認。

### "Symbol not found" エラー
→ 正常。テーマ銘柄847のうち一部はNorgate未収録（新興小型株など）。
   カバレッジ80%以上あれば検証可能。

### "No data returned" エラー
→ そのティッカーの上場日がSTART_DATEより後の可能性。正常。

### pyarrow関連エラー
```
pip install pyarrow
```

### メモリ不足
→ START_DATEを "2022-01-01" に変更して3年分に縮小。
   `norgate_fetch.py`の21行目付近を編集:
```python
START_DATE = "2022-01-01"  # 3年分に短縮
```

---

## 重要な留保

- テーマ構成は**2026年4月時点で凍結**したもの。過去に投影するためPIT問題あり
- **絶対CAGR**を採用根拠にしてはならない（Silver層ルール）
- **A4 vs A5の相対差分**のみ有効な読み方
- **production昇格はforward PIT-safeデータのみ**で判断（ガバナンス変更なし）
- この拡張バックテストは「方向性の確認」と「レジーム耐性の検証」が目的
