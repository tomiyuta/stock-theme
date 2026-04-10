# PRISM_EXIT_IMPLEMENTATION_NOTES_v2

> Status: Reference appendix to PRISM_EXIT_CONSTITUTION_v2.md
> Purpose: Implementation-level detail for future code changes (Phase 2+)
> DO NOT implement immediately — wait for event-driven trigger

## 1. 状態変数（Phase 2で導入）

SecurityState per ticker:
- desired_weight_raw / desired_weight_post_dedupe / desired_weight_final
- actual_weight_pre_repair / actual_weight_final
- theme_rank / theme_state (ENTRY/WATCH/EXIT)
- trigger_set (T1/T2/T3/T5)
- exit_signal_reason
- blocked_flag / blocked_reason
- cap_scaled_flag / cap_scale_reason
- position_state (TARGET_CONTINUE/BLOCKED_MINHOLD/SELL_ELIGIBLE/NOT_HELD)
- decision_code
- trail8_alert / trail8_drawdown (monitoring only)

PortfolioState:
- atk_cap / gross_exposure_pre/final / cash_pre/final
- sector_weights_pre/final
- blocked_count / blocked_weight_total
- new_entry_suppressed_count
- cap_violation_before/after_repair

## 2. Decision Codes

### 実売買系
- BUY_NEW: 新規採用
- SELL_EXPIRED_REMOVED: MinHold満了後target外
- SELL_INVALID_DATA: データ異常
- SELL_DELISTED: 上場廃止

### 保持系
- HOLD_TARGET: target継続保有
- HOLD_BLOCKED_MINHOLD: MinHoldで保持
- NO_ACTION: 非保有/変化なし

### 修復系（signal exitではなくcap repair由来）
- SCALE_NEW_FOR_CAP: new entryを縮小
- SCALE_BLOCKED_FOR_CAP: blockedを比例縮小
- SCALE_TARGET_FOR_CAP: target継続も比例縮小

## 3. Cap Repair順序
1. NEW銘柄を縮小
2. BLOCKED銘柄を比例縮小
3. TARGET銘柄を比例縮小

## 4. ログテーブル（Phase 3）
- orders_decisions.csv: 全銘柄のdecision code + weight変遷
- monitor_alerts.csv: TRAIL8/T3/DD（注文系から分離）
- portfolio_repair_log.csv: cap修復の独立監査

## 5. 最低限のUnit Tests（Phase 3）
- Test A: blocked存在時もgross <= atk_cap
- Test B: blocked存在時もnew sector <= sector_cap
- Test C: blocked銘柄のfull liquidationは起きない（MinHold中）
- Test D: invalid_dataはMinHold無視
- Test E: T3単独では退出しない
