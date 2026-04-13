"""PIT-safe snapshot capture — daily theme membership + prices archive.
Integrated into daily_update.yml GitHub Actions pipeline.
Saves to data/pit-snapshots/ for forward PIT accumulation.
"""
import json, pandas as pd, numpy as np
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
API = ROOT / 'public' / 'api'
SNAP_DIR = ROOT / 'data' / 'pit-snapshots'
SNAP_DIR.mkdir(parents=True, exist_ok=True)

today = datetime.now().strftime('%Y%m%d')

# === 1. Theme membership snapshot ===
membership = []
for f in sorted((API / 'theme-details').glob('*.json')):
    d = json.load(open(f))
    slug = d.get('slug', f.stem)
    tickers = d.get('tickers', [])
    for tk in tickers:
        membership.append({'theme': slug, 'ticker': tk})

mem_df = pd.DataFrame(membership)
mem_df['snapshot_date'] = today
mem_df['retrieved_at'] = datetime.now().isoformat()

# === 2. Theme price snapshot ===
prices = []
for f in sorted((API / 'theme-details').glob('*.json')):
    d = json.load(open(f))
    slug = d.get('slug', f.stem)
    tickers = d.get('tickers', [])
    if not d.get('prices'): continue
    last_price = d['prices'][-1]
    date = last_price.get('date', '')
    for tk in tickers:
        close = last_price.get(tk)
        if close is not None:
            prices.append({'theme': slug, 'ticker': tk,
                           'date': date, 'close': float(close)})

price_df = pd.DataFrame(prices)
price_df['snapshot_date'] = today
price_df['retrieved_at'] = datetime.now().isoformat()

# === 3. Theme ranking snapshot ===
ranking_file = API / 'theme_ranking.json'
rank_df = pd.DataFrame()
if ranking_file.exists():
    tr = json.load(open(ranking_file))
    themes = [t for t in tr.get('all_themes', [])
              if t.get('related') and len(t.get('related', '').split(',')) > 1]
    rank_rows = []
    for t in themes:
        rank_rows.append({
            'theme': t.get('slug', t.get('name', '')),
            'name': t.get('name', ''),
            'rank': t.get('rank', ''),
            'industry': t.get('industry', ''),
            'members_hash': hash(t.get('related', '')),
            'n_members': len(t.get('related', '').split(',')),
        })
    rank_df = pd.DataFrame(rank_rows)
    rank_df['snapshot_date'] = today
    rank_df['retrieved_at'] = datetime.now().isoformat()

# === Save ===
mem_path = SNAP_DIR / f'membership_{today}.csv'
price_path = SNAP_DIR / f'prices_{today}.csv'
rank_path = SNAP_DIR / f'ranking_{today}.csv'

mem_df.to_csv(mem_path, index=False)
price_df.to_csv(price_path, index=False)
if not rank_df.empty:
    rank_df.to_csv(rank_path, index=False)

print(f'PIT snapshot {today}: {mem_df.theme.nunique()} themes, '
      f'{mem_df.ticker.nunique()} tickers, {len(price_df)} prices → {SNAP_DIR}/')
