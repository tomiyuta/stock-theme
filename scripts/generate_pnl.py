#!/usr/bin/env python3
"""
generate_pnl.py — P&L tracking for PRISM and PRISM-R
Outputs:
  public/api/prism/pnl.json      (PRISM real P&L from ledger)
  public/api/prism-r/pnl.json    (PRISM-R virtual P&L from virtual_ledger)
"""
import json, os
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
API_PRISM = ROOT / 'public' / 'api' / 'prism'
API_PRISM_R = ROOT / 'public' / 'api' / 'prism-r'
API_PRISM_RQ = ROOT / 'public' / 'api' / 'prism-rq'
API_PRISM_G2 = ROOT / 'public' / 'api' / 'prism-g2'
DATA_R = ROOT / 'data' / 'prism-r'
DATA_RQ = ROOT / 'data' / 'prism-rq'
DATA_G2 = ROOT / 'data' / 'prism-g2'

def load_json(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# =============================================================
# PRISM P&L
# =============================================================
def compute_prism_pnl():
    ledger = load_json(API_PRISM / 'ledger.json')
    constituents = load_json(API_PRISM / 'constituents.json')
    price_map = {c['ticker']: c.get('price', 0) for c in constituents if c.get('price')}
    as_of = ledger.get('as_of_date', '')
    
    positions = []
    total_invested = 0.0
    total_current = 0.0
    
    for p in ledger.get('positions', []):
        if p['status'] != 'active':
            continue
        tk = p['ticker']
        entry_px = p.get('entry_price', 0)
        current_px = price_map.get(tk, entry_px)
        weight = p.get('target_weight', 0)
        
        pnl_pct = (current_px - entry_px) / entry_px if entry_px > 0 else 0
        pnl_weighted = pnl_pct * weight
        peak_px = p.get('peak_price_since_entry', entry_px)
        dd_from_peak = (current_px - peak_px) / peak_px if peak_px > 0 else 0
        
        positions.append({
            'ticker': tk,
            'sector': p.get('sector', ''),
            'theme': p.get('theme_at_entry', ''),
            'entry_date': p.get('entry_date', ''),
            'entry_price': round(entry_px, 2),
            'current_price': round(current_px, 2),
            'peak_price': round(peak_px, 2),
            'weight': round(weight, 4),
            'holding_days': p.get('holding_days', 0),
            'pnl_pct': round(pnl_pct, 4),
            'pnl_weighted': round(pnl_weighted, 6),
            'dd_from_peak': round(dd_from_peak, 4),
            'trail8_alert': dd_from_peak <= -0.08,
            'minhold_blocked': not p.get('eligible_to_exit', True),
        })
        total_invested += entry_px * weight
        total_current += current_px * weight
    
    total_pnl = (total_current - total_invested) / total_invested if total_invested > 0 else 0
    
    # Sort by P&L descending
    positions.sort(key=lambda x: x['pnl_pct'], reverse=True)
    
    winners = [p for p in positions if p['pnl_pct'] > 0]
    losers = [p for p in positions if p['pnl_pct'] < 0]
    
    result = {
        'as_of_date': as_of,
        'generated_at': datetime.now().isoformat(),
        'strategy': 'PRISM',
        'status': 'LIVE_PAPER',
        'summary': {
            'total_positions': len(positions),
            'total_pnl_pct': round(total_pnl, 4),
            'winners': len(winners),
            'losers': len(losers),
            'best': positions[0]['ticker'] if positions else None,
            'best_pnl': positions[0]['pnl_pct'] if positions else 0,
            'worst': positions[-1]['ticker'] if positions else None,
            'worst_pnl': positions[-1]['pnl_pct'] if positions else 0,
            'trail8_alerts': sum(1 for p in positions if p['trail8_alert']),
            'blocked_count': sum(1 for p in positions if p['minhold_blocked']),
        },
        'positions': positions,
    }
    save_json(API_PRISM / 'pnl.json', result)
    print(f'PRISM P&L: {len(positions)} positions, total={total_pnl:+.2%}')
    return result

# =============================================================
# PRISM-R Virtual P&L
# =============================================================
def compute_prism_r_pnl():
    comp = load_json(API_PRISM_R / 'shadow_comparison.json')
    snapshot_date = comp.get('snapshot_date', '')
    
    # Load or create virtual ledger
    vledger_path = DATA_R / 'virtual_ledger.json'
    if vledger_path.exists():
        vledger = load_json(vledger_path)
    else:
        vledger = {'positions': {}, 'history': [], 'created_at': datetime.now().isoformat()}
    
    # Current A5-R picks with prices
    current_picks = {}
    for c in comp.get('comparisons', []):
        s = next((x for x in c['stocks'] if x['ticker'] == c['a5_pick']), {})
        current_picks[c['a5_pick']] = {
            'theme': c.get('theme_name', ''),
            'price': s.get('price', 0),
            'alpha63': s.get('alpha63', 0),
            'score': s.get('score_a5', 0),
            'theme_state': c.get('theme_state', ''),
            'full_rank': c.get('full_rank', 0),
        }
    
    # Update virtual ledger: add new picks, keep existing entry prices
    for tk, info in current_picks.items():
        if tk not in vledger['positions']:
            vledger['positions'][tk] = {
                'entry_date': snapshot_date,
                'entry_price': info['price'],
                'theme': info['theme'],
                'status': 'active',
            }
    
    # Mark exited positions
    for tk in list(vledger['positions'].keys()):
        pos = vledger['positions'][tk]
        if tk not in current_picks and pos['status'] == 'active':
            pos['status'] = 'closed'
            pos['exit_date'] = snapshot_date
            pos['exit_price'] = 0  # will be updated if price available
    
    # Save updated virtual ledger
    save_json(vledger_path, vledger)
    
    # Compute P&L for active positions
    positions = []
    total_invested = 0.0
    total_current = 0.0
    weight = 1.0 / max(len(current_picks), 1)  # equal weight
    
    for tk, info in current_picks.items():
        vpos = vledger['positions'].get(tk, {})
        entry_px = vpos.get('entry_price', info['price'])
        current_px = info['price']
        entry_date = vpos.get('entry_date', snapshot_date)
        
        pnl_pct = (current_px - entry_px) / entry_px if entry_px > 0 else 0
        pnl_weighted = pnl_pct * weight
        
        positions.append({
            'ticker': tk,
            'theme': info['theme'],
            'theme_state': info['theme_state'],
            'entry_date': entry_date,
            'entry_price': round(entry_px, 2),
            'current_price': round(current_px, 2),
            'weight': round(weight, 4),
            'alpha63': round(info.get('alpha63', 0), 3),
            'pnl_pct': round(pnl_pct, 4),
            'pnl_weighted': round(pnl_weighted, 6),
        })
        total_invested += entry_px * weight
        total_current += current_px * weight
    
    total_pnl = (total_current - total_invested) / total_invested if total_invested > 0 else 0
    positions.sort(key=lambda x: x['pnl_pct'], reverse=True)
    
    winners = [p for p in positions if p['pnl_pct'] > 0]
    losers = [p for p in positions if p['pnl_pct'] < 0]
    
    # Closed positions from ledger
    closed = []
    for tk, pos in vledger['positions'].items():
        if pos['status'] == 'closed':
            closed.append({
                'ticker': tk,
                'theme': pos.get('theme', ''),
                'entry_date': pos.get('entry_date', ''),
                'exit_date': pos.get('exit_date', ''),
                'entry_price': pos.get('entry_price', 0),
            })
    
    result = {
        'as_of_date': snapshot_date,
        'generated_at': datetime.now().isoformat(),
        'strategy': 'PRISM-R',
        'status': 'SHADOW_VIRTUAL',
        'summary': {
            'total_positions': len(positions),
            'total_pnl_pct': round(total_pnl, 4),
            'winners': len(winners),
            'losers': len(losers),
            'best': positions[0]['ticker'] if positions else None,
            'best_pnl': positions[0]['pnl_pct'] if positions else 0,
            'worst': positions[-1]['ticker'] if positions else None,
            'worst_pnl': positions[-1]['pnl_pct'] if positions else 0,
            'closed_count': len(closed),
        },
        'positions': positions,
        'closed_positions': closed,
    }
    save_json(API_PRISM_R / 'pnl.json', result)
    print(f'PRISM-R P&L: {len(positions)} positions, total={total_pnl:+.2%}, closed={len(closed)}')
    return result

# =============================================================
# PRISM-RQ Virtual P&L (SNRb picks from BFM-v2 filtered themes)
# =============================================================
def compute_prism_rq_pnl():
    comp_path = API_PRISM_R / 'shadow_comparison.json'  # RQ data is in prism-r shadow
    if not comp_path.exists():
        print('PRISM-RQ P&L: no shadow_comparison.json'); return None
    comp = load_json(comp_path)
    snapshot_date = comp.get('snapshot_date', '')
    DATA_RQ.mkdir(parents=True, exist_ok=True)
    vledger_path = DATA_RQ / 'virtual_ledger.json'
    vledger = load_json(vledger_path) if vledger_path.exists() else {'positions': {}, 'history': [], 'created_at': datetime.now().isoformat()}
    # Use snrb_pick (not a5_pick) for PRISM-RQ
    current_picks = {}
    for c in comp.get('comparisons', []):
        tk = c.get('snrb_pick')
        if not tk: continue
        s = next((x for x in c['stocks'] if x['ticker'] == tk), {})
        current_picks[tk] = {
            'theme': c.get('theme_name', ''), 'price': s.get('price', 0),
            'score': s.get('score_snrb', 0), 'theme_state': c.get('theme_state', ''),
        }
    weight = 1.0 / max(len(current_picks), 1)
    for tk, info in current_picks.items():
        if tk not in vledger['positions']:
            vledger['positions'][tk] = {'entry_date': snapshot_date, 'entry_price': info['price'], 'theme': info['theme'], 'status': 'active'}
    for tk in list(vledger['positions'].keys()):
        if tk not in current_picks and vledger['positions'][tk]['status'] == 'active':
            vledger['positions'][tk]['status'] = 'closed'
            vledger['positions'][tk]['exit_date'] = snapshot_date
    save_json(vledger_path, vledger)
    positions = []; total_inv = 0.0; total_cur = 0.0
    for tk, info in current_picks.items():
        vp = vledger['positions'].get(tk, {})
        ep = vp.get('entry_price', info['price']); cp = info['price']
        pnl = (cp - ep) / ep if ep > 0 else 0
        positions.append({'ticker': tk, 'theme': info['theme'], 'entry_price': round(ep,2), 'current_price': round(cp,2), 'weight': round(weight,4), 'pnl_pct': round(pnl,4), 'pnl_weighted': round(pnl*weight,6)})
        total_inv += ep * weight; total_cur += cp * weight
    total_pnl = (total_cur - total_inv) / total_inv if total_inv > 0 else 0
    closed = [{'ticker':tk,'theme':v.get('theme',''),'entry_date':v.get('entry_date',''),'exit_date':v.get('exit_date',''),'entry_price':v.get('entry_price',0)} for tk,v in vledger['positions'].items() if v.get('status')=='closed']
    result = {'as_of_date': snapshot_date, 'strategy': 'PRISM-RQ', 'status': 'SHADOW_VIRTUAL',
              'summary': {'total_positions': len(positions), 'total_pnl_pct': round(total_pnl, 4), 'closed_count': len(closed)}, 'positions': positions, 'closed_positions': closed}
    API_PRISM_RQ.mkdir(parents=True, exist_ok=True)
    save_json(API_PRISM_RQ / 'pnl.json', result)
    print(f'PRISM-RQ P&L: {len(positions)} positions, total={total_pnl:+.2%}')
    return result

# =============================================================
# G2-MAX Virtual P&L (concentrated raw α, 5 themes)
# =============================================================
def compute_prism_g2_pnl():
    comp_path = API_PRISM_G2 / 'shadow_comparison.json'
    if not comp_path.exists():
        print('G2-MAX P&L: no shadow_comparison.json'); return None
    comp = load_json(comp_path)
    snapshot_date = comp.get('snapshot_date', '')
    DATA_G2.mkdir(parents=True, exist_ok=True)
    vledger_path = DATA_G2 / 'virtual_ledger.json'
    vledger = load_json(vledger_path) if vledger_path.exists() else {'positions': {}, 'history': [], 'created_at': datetime.now().isoformat()}
    current_picks = {}
    for c in comp.get('comparisons', []):
        if c.get('stocks'):
            s = c['stocks'][0]
            tk = s['ticker']
            w5b_w = c.get('w5b_weight', 1.0/max(len(comp.get('comparisons',[])),1))
            current_picks[tk] = {'theme': c.get('theme_name', ''), 'price': s.get('price', 0), 'weight': w5b_w}
    for tk, info in current_picks.items():
        if tk not in vledger['positions']:
            vledger['positions'][tk] = {'entry_date': snapshot_date, 'entry_price': info['price'], 'theme': info['theme'], 'status': 'active'}
    for tk in list(vledger['positions'].keys()):
        if tk not in current_picks and vledger['positions'][tk]['status'] == 'active':
            vledger['positions'][tk]['status'] = 'closed'
            vledger['positions'][tk]['exit_date'] = snapshot_date
    save_json(vledger_path, vledger)
    positions = []; total_inv = 0.0; total_cur = 0.0
    for tk, info in current_picks.items():
        vp = vledger['positions'].get(tk, {})
        ep = vp.get('entry_price', info['price']); cp = info['price']
        w = info.get('weight', 1.0/max(len(current_picks),1))
        pnl = (cp - ep) / ep if ep > 0 else 0
        positions.append({'ticker': tk, 'theme': info['theme'], 'entry_price': round(ep,2), 'current_price': round(cp,2), 'weight': round(w,4), 'pnl_pct': round(pnl,4)})
        total_inv += ep * w; total_cur += cp * w
    total_pnl = (total_cur - total_inv) / total_inv if total_inv > 0 else 0
    closed = [{'ticker':tk,'theme':v.get('theme',''),'entry_date':v.get('entry_date',''),'exit_date':v.get('exit_date',''),'entry_price':v.get('entry_price',0)} for tk,v in vledger['positions'].items() if v.get('status')=='closed']
    result = {'as_of_date': snapshot_date, 'strategy': 'G2-MAX', 'status': 'SHADOW_VIRTUAL',
              'summary': {'total_positions': len(positions), 'total_pnl_pct': round(total_pnl, 4), 'closed_count': len(closed)}, 'positions': positions, 'closed_positions': closed}
    save_json(API_PRISM_G2 / 'pnl.json', result)
    print(f'G2-MAX P&L: {len(positions)} positions, total={total_pnl:+.2%}')
    return result

# =============================================================
# Forward Overlay — append monthly returns to cumulative_returns.json
# =============================================================
def update_forward_overlay():
    from datetime import date
    today = date.today()
    
    for api_dir, pnl_key, label in [
        (API_PRISM, 'a4', 'PRISM'),
        (API_PRISM_R, 'a5', 'PRISM-R'),
        (API_PRISM_RQ, 'a5', 'PRISM-RQ'),
        (API_PRISM_G2, 'a5', 'G2-MAX'),
    ]:
        cum_path = api_dir / 'cumulative_returns.json'
        pnl_path = api_dir / 'pnl.json'
        if not cum_path.exists() or not pnl_path.exists():
            continue
        
        cum = load_json(cum_path)
        pnl = load_json(pnl_path)
        fwd = cum.get('forward_overlay', {'dates': [], 'a4': [], 'a5': [], 'SPY': []})
        
        # Get portfolio total P&L as the current snapshot return
        total_pnl = pnl.get('summary', {}).get('total_pnl_pct', 0)
        as_of = pnl.get('as_of_date', str(today))
        
        # Only update if as_of date is newer than last forward entry
        if fwd['dates'] and as_of <= fwd['dates'][-1]:
            print(f'{label} forward: already up to date ({as_of})')
            continue
        
        # Append daily snapshot (will be aggregated to monthly on chart side)
        fwd['dates'].append(as_of)
        
        # For the strategy return, use total_pnl from pnl.json
        # This is cumulative since entry, so we store as growth factor
        if pnl_key == 'a4':
            fwd['a4'].append(round(1 + total_pnl, 6))
        else:
            fwd['a5'].append(round(1 + total_pnl, 6))
        
        # SPY: fetch from last known price in constituents or use 0
        # We'll compute SPY return from the BT boundary
        spy_val = fwd.get('SPY', [])
        if len(spy_val) < len(fwd['dates']):
            # Pad SPY with 1.0 placeholder (will be updated with real data)
            while len(spy_val) < len(fwd['dates']):
                spy_val.append(1.0)
            fwd['SPY'] = spy_val
        
        cum['forward_overlay'] = fwd
        save_json(cum_path, cum)
        print(f'{label} forward: appended {as_of} (growth={1+total_pnl:.4f})')

# =============================================================
# Rebalance Diff — track monthly portfolio changes
# =============================================================
def update_rebalance_diffs():
    """Compare current picks with previous snapshot, save diff for each strategy."""
    strategies = [
        {'api': API_PRISM, 'data': ROOT / 'data', 'label': 'PRISM',
         'picks_fn': lambda: _get_prism_picks()},
        {'api': API_PRISM_R, 'data': DATA_R, 'label': 'PRISM-R',
         'picks_fn': lambda: _get_shadow_picks(API_PRISM_R, 'a5_pick')},
        {'api': API_PRISM_RQ, 'data': DATA_RQ, 'label': 'PRISM-RQ',
         'picks_fn': lambda: _get_shadow_picks(API_PRISM_R, 'snrb_pick')},
        {'api': API_PRISM_G2, 'data': DATA_G2, 'label': 'G2-MAX',
         'picks_fn': lambda: _get_g2_picks()},
    ]
    for s in strategies:
        s['data'].mkdir(parents=True, exist_ok=True)
        prev_path = s['data'] / 'prev_picks.json'
        curr = s['picks_fn']()
        if not curr:
            print(f'{s["label"]} diff: no current picks')
            continue
        curr_tickers = {p['ticker'] for p in curr}
        curr_map = {p['ticker']: p for p in curr}
        # Load previous
        if prev_path.exists():
            prev = load_json(prev_path)
        else:
            prev = {'tickers': [], 'picks': [], 'date': ''}
        prev_tickers = set(prev.get('tickers', []))
        prev_map = {p['ticker']: p for p in prev.get('picks', [])}
        # Compute diff
        added = sorted(curr_tickers - prev_tickers)
        removed = sorted(prev_tickers - curr_tickers)
        unchanged = sorted(curr_tickers & prev_tickers)
        diff = {
            'as_of': curr[0].get('date', '') if curr else '',
            'prev_date': prev.get('date', ''),
            'added': [curr_map[tk] for tk in added],
            'removed': [prev_map.get(tk, {'ticker': tk}) for tk in removed],
            'unchanged': [curr_map[tk] for tk in unchanged],
            'summary': {
                'total_now': len(curr_tickers),
                'total_prev': len(prev_tickers),
                'added': len(added), 'removed': len(removed), 'unchanged': len(unchanged),
            }
        }
        save_json(s['api'] / 'rebalance_diff.json', diff)
        # Save current as prev for next run
        save_json(prev_path, {'tickers': sorted(curr_tickers), 'picks': curr,
                               'date': curr[0].get('date', '')})
        chg = f'+{len(added)}/-{len(removed)}/={len(unchanged)}'
        print(f'{s["label"]} diff: {chg}')

def _get_prism_picks():
    sig_path = ROOT / 'data' / 'snapshots' / 'latest' / 'signals.json'
    if not sig_path.exists(): return []
    sig = load_json(sig_path)
    date = sig.get('snapshot_date', '')
    picks = []
    for tk, w in sig.get('production_portfolio', {}).get('weights', {}).items():
        if tk == 'SHV' or w <= 0: continue
        price = sig.get('production_portfolio', {}).get('prices', {}).get(tk, 0)
        picks.append({'ticker': tk, 'theme': '', 'price': price, 'date': date})
    return picks

def _get_shadow_picks(api_dir, pick_key):
    comp_path = api_dir / 'shadow_comparison.json'
    if not comp_path.exists(): return []
    comp = load_json(comp_path)
    date = comp.get('snapshot_date', '')
    picks = []
    for c in comp.get('comparisons', []):
        tk = c.get(pick_key)
        if not tk: continue
        s = next((x for x in c.get('stocks', []) if x['ticker'] == tk), {})
        picks.append({'ticker': tk, 'theme': c.get('theme_name', ''), 'price': s.get('price', 0), 'date': date})
    return picks

def _get_g2_picks():
    comp_path = API_PRISM_G2 / 'shadow_comparison.json'
    if not comp_path.exists(): return []
    comp = load_json(comp_path)
    date = comp.get('snapshot_date', '')
    picks = []
    for c in comp.get('comparisons', []):
        if c.get('stocks'):
            s = c['stocks'][0]
            picks.append({'ticker': s['ticker'], 'theme': c.get('theme_name', ''), 'price': s.get('price', 0), 'date': date})
    return picks

if __name__ == '__main__':
    compute_prism_pnl()
    compute_prism_r_pnl()
    compute_prism_rq_pnl()
    compute_prism_g2_pnl()
    update_forward_overlay()
    update_rebalance_diffs()
    print('Done.')
