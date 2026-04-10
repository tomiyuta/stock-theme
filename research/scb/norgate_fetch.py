"""
Norgate Data 取得スクリプト（Windows用）
========================================
目的: 847テーマ構成銘柄の日次Adjusted Close価格を5年分取得し、
      A5-lite戦略のバックテスト用long panelを生成する。

前提:
  - Norgate Data Platinum契約済み
  - NDU (Norgate Data Updater) がインストール・起動済み
  - Python 3.8+ インストール済み
  - norgatedata パッケージインストール済み (pip install norgatedata)

実行方法:
  1. NDUを起動し、データ更新完了を待つ
  2. このスクリプトと同じフォルダに以下2ファイルを配置:
     - ticker_list_847.txt
     - theme_membership_frozen.json
  3. コマンドプロンプトで: python norgate_fetch.py
  4. 完了後、outputフォルダに以下が生成される:
     - norgate_theme_panel.parquet  ← メインの出力（Mac側に転送）
     - norgate_coverage_report.txt  ← カバレッジレポート
     - norgate_raw_prices.parquet   ← 生の価格データ（バックアップ）
"""

import sys
import os
import json
import time
from datetime import datetime, date
from pathlib import Path

print("=" * 70)
print("Norgate Data Fetcher for A5-lite Backtest")
print("=" * 70)

# === Step 0: 依存チェック ===
try:
    import numpy as np
    import pandas as pd
    print(f"  pandas {pd.__version__}, numpy {np.__version__}")
except ImportError as e:
    print(f"ERROR: {e}")
    print("  pip install pandas numpy pyarrow")
    sys.exit(1)

try:
    import norgatedata as nd
    print(f"  norgatedata {nd.version()}")
except ImportError:
    print("ERROR: norgatedata not installed")
    print("  pip install norgatedata")
    sys.exit(1)

# NDU接続チェック
try:
    status = nd.status()
    print(f"  NDU status: {status}")
except Exception as e:
    print(f"WARNING: NDU status check failed: {e}")
    print("  NDUが起動しているか確認してください")

# === Step 1: 入力ファイル読み込み ===
SCRIPT_DIR = Path(__file__).resolve().parent
TICKER_FILE = SCRIPT_DIR / "ticker_list_847.txt"
MEMBERSHIP_FILE = SCRIPT_DIR / "theme_membership_frozen.json"
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

START_DATE = "2020-01-01"   # 5年+α（余裕を持たせる）
END_DATE = None              # 最新まで

if not TICKER_FILE.exists():
    print(f"ERROR: {TICKER_FILE} not found")
    sys.exit(1)
if not MEMBERSHIP_FILE.exists():
    print(f"ERROR: {MEMBERSHIP_FILE} not found")
    sys.exit(1)

with open(TICKER_FILE) as f:
    tickers = [line.strip() for line in f if line.strip()]
print(f"\nTarget tickers: {len(tickers)}")

with open(MEMBERSHIP_FILE) as f:
    theme_members = json.load(f)
print(f"Themes: {len(theme_members)}")

# === Step 2: Norgate全銘柄リストを取得 ===
print("\nFetching Norgate database symbols...")
try:
    # US Equitiesデータベースの全シンボル
    norgate_symbols = set()
    for db_name in nd.databases():
        try:
            syms = nd.database_symbols(db_name)
            if syms:
                norgate_symbols.update(syms)
        except:
            pass
    print(f"  Norgate total symbols: {len(norgate_symbols)}")
except Exception as e:
    print(f"  WARNING: Could not list all databases: {e}")
    print("  Will try fetching each ticker individually")
    norgate_symbols = None

# === Step 3: 価格データ取得 ===
print(f"\nFetching daily prices from {START_DATE}...")
print("  This may take 5-15 minutes for 847 tickers.\n")

results = {}   # ticker -> DataFrame
failed = []
skipped = []
t0 = time.time()

for i, tk in enumerate(tickers):
    if (i + 1) % 50 == 0 or i == 0:
        elapsed = time.time() - t0
        print(f"  [{i+1:3d}/{len(tickers)}] {tk:8s}  "
              f"({elapsed:.0f}s elapsed, "
              f"{len(results)} ok, {len(failed)} failed)")

    try:
        # norgatedata.price_timeseries returns a numpy recarray
        pricedata = nd.price_timeseries(
            tk,
            stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
            start_date=START_DATE,
            # end_date は指定しない → 最新まで
        )

        if pricedata is None or len(pricedata) == 0:
            failed.append((tk, "no data returned"))
            continue

        # recarrayをDataFrameに変換
        df = pd.DataFrame(pricedata)

        # カラム名の正規化（Norgateのバージョンにより異なる場合がある）
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if cl == 'date': col_map[c] = 'date'
            elif cl in ('close', 'adjusted close', 'adj close', 'adjclose'):
                col_map[c] = 'close'

        df = df.rename(columns=col_map)

        if 'date' not in df.columns or 'close' not in df.columns:
            # フォールバック: 最初のカラムをdate、最後をcloseとして扱う
            if len(df.columns) >= 2:
                df.columns = ['date'] + list(df.columns[1:-1]) + ['close']
            else:
                failed.append((tk, f"unexpected columns: {list(df.columns)}"))
                continue

        df['date'] = pd.to_datetime(df['date'])
        df['ticker'] = tk
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df[['date', 'ticker', 'close']].dropna()

        if len(df) < 10:
            failed.append((tk, f"only {len(df)} rows"))
            continue

        results[tk] = df

    except Exception as e:
        failed.append((tk, str(e)[:80]))

elapsed = time.time() - t0
print(f"\n  Done in {elapsed:.0f}s")
print(f"  OK: {len(results)}, Failed: {len(failed)}, "
      f"Coverage: {len(results)/len(tickers):.0%}")

# === Step 4: 生価格データ保存 ===
if results:
    raw_df = pd.concat(results.values(), ignore_index=True)
    raw_df = raw_df.sort_values(['ticker', 'date']).reset_index(drop=True)
    raw_path = OUTPUT_DIR / "norgate_raw_prices.parquet"
    raw_df.to_parquet(raw_path, index=False)
    print(f"\n  Raw prices: {len(raw_df):,} rows → {raw_path}")
    print(f"  Date range: {raw_df.date.min().date()} ~ {raw_df.date.max().date()}")
    print(f"  Tickers: {raw_df.ticker.nunique()}")
else:
    print("\nERROR: No data retrieved. Check NDU status.")
    sys.exit(1)

# === Step 5: テーマ×銘柄のlong panel生成 ===
print("\nBuilding theme panel (frozen membership)...")
panel_rows = []
theme_coverage = {}

for theme, members in theme_members.items():
    available = [tk for tk in members if tk in results]
    theme_coverage[theme] = {
        'total': len(members),
        'available': len(available),
        'missing': [tk for tk in members if tk not in results]
    }
    for tk in available:
        df = results[tk].copy()
        df['theme'] = theme
        panel_rows.append(df[['date', 'theme', 'ticker', 'close']])

panel_df = pd.concat(panel_rows, ignore_index=True)
panel_df = panel_df.sort_values(['theme', 'ticker', 'date']).reset_index(drop=True)
panel_path = OUTPUT_DIR / "norgate_theme_panel.parquet"
panel_df.to_parquet(panel_path, index=False)

print(f"  Panel: {len(panel_df):,} rows → {panel_path}")
print(f"  Themes: {panel_df.theme.nunique()}")
print(f"  Tickers: {panel_df.ticker.nunique()}")
print(f"  Date range: {panel_df.date.min().date()} ~ {panel_df.date.max().date()}")

# === Step 6: カバレッジレポート ===
report_path = OUTPUT_DIR / "norgate_coverage_report.txt"
with open(report_path, 'w') as f:
    f.write(f"Norgate Coverage Report\n")
    f.write(f"Generated: {datetime.now().isoformat()}\n")
    f.write(f"Start date: {START_DATE}\n")
    f.write(f"{'='*70}\n\n")

    f.write(f"SUMMARY\n")
    f.write(f"  Target tickers:    {len(tickers)}\n")
    f.write(f"  Retrieved:         {len(results)} ({len(results)/len(tickers):.0%})\n")
    f.write(f"  Failed:            {len(failed)}\n")
    f.write(f"  Panel rows:        {len(panel_df):,}\n")
    f.write(f"  Date range:        {panel_df.date.min().date()} ~ "
            f"{panel_df.date.max().date()}\n")
    f.write(f"  Trading days:      {panel_df.date.nunique()}\n\n")

    # テーマ別カバレッジ
    f.write(f"THEME COVERAGE\n")
    f.write(f"{'Theme':<40s} {'Total':>5s} {'Avail':>5s} {'%':>5s} {'Missing':>30s}\n")
    f.write(f"{'-'*90}\n")
    low_coverage = []
    for theme in sorted(theme_coverage.keys()):
        tc = theme_coverage[theme]
        pct = tc['available'] / tc['total'] if tc['total'] > 0 else 0
        missing_str = ','.join(tc['missing'][:5])
        if len(tc['missing']) > 5:
            missing_str += f"...+{len(tc['missing'])-5}"
        f.write(f"{theme:<40s} {tc['total']:>5d} {tc['available']:>5d} "
                f"{pct:>5.0%} {missing_str:>30s}\n")
        if pct < 0.75:
            low_coverage.append((theme, pct, tc['missing']))

    f.write(f"\n\nLOW COVERAGE THEMES (<75%): {len(low_coverage)}\n")
    for theme, pct, missing in low_coverage:
        f.write(f"  {theme}: {pct:.0%} missing={missing}\n")

    # 取得失敗銘柄
    f.write(f"\n\nFAILED TICKERS: {len(failed)}\n")
    for tk, reason in sorted(failed):
        f.write(f"  {tk}: {reason}\n")

print(f"  Report: {report_path}")

# === 完了 ===
print(f"\n{'='*70}")
print(f"COMPLETE")
print(f"{'='*70}")
print(f"\n次のステップ:")
print(f"  1. {OUTPUT_DIR / 'norgate_theme_panel.parquet'} をMacに転送")
print(f"  2. Mac側で ~/Downloads/stock-theme/research/scb/ に配置")
print(f"  3. カバレッジレポートを確認: {report_path}")
print(f"  4. Macで verify_parquet.py を拡張パネルで再実行")
