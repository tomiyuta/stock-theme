# PRISM_EXIT_CONSTITUTION_v2

> Effective from: 2026-04-10
> Code status: Phase 0 (document only, minimal code change)
> Order evaluation cadence: rebalance day only (every 20 trading days)

## 0. 目的

PRISMの売却ロジックを以下の3層に分離して定義する。
- Layer A: Signal Exit
- Layer B: Execution Permission
- Layer C: Portfolio Reconciliation

目的は、売却シグナル・執行制約・実ポートフォリオ整合を混同しないこと、
およびMinHoldブロック時のcap violationに対する運用事故を防止すること。

## 1. 基本認識

### 1.1 実証済みのコア
現時点で実証済みの売却制御は **MinHold 20営業日** のみ。
24年BTにおいて回転抑制とDD改善の効果が確認済み。

### 1.2 未実証の例外
`Gap Stop -8%`は未検証。現状の実装は「gap stop」ではなく
`peak_since_entry`に対する`trailing drawdown stop`。
`generate_orders.py`がリバランス日にのみ動作するため、
emergency stopではなく「20営業日に1回判定するtrailing DD alert」にすぎない。

### 1.3 T3の位置づけ
T3（acceleration True→False）はtheme score内のacceleration情報と重複する可能性が高い。
T3はexecution triggerではなくdiagnostic/annotationとする。

### 1.4 MinHold 20のbinding性
order evaluationが20営業日ごとにしか行われないため、
MinHold 20はroutine exitsに対して非bindingである可能性がある。
MinHoldが実際にbindingなのは:
- リバランス間隔内にテーマが脱落した場合（off-cycle event）
- K-gate反転で枠が急縮小した場合
- 初回リバランスで前回の銘柄がまだ若い場合
のみ。

---

## 2. Layer A: Signal Exit（ターゲット除外判定）

Layer Aは「次回target_weightsから除外するか」を判定する層。ここでは売却は確定しない。

### A-1. Exit drivers
- T1: K-gate状態変化によるatk_cap縮小
- T2: Theme rankがexit thresholdを超過（正式退出理由）
- T3: acceleration stop（diagnostic only）
- T5: 新テーマ参入による押し出し

### A-2. ヒステリシス
- Entry threshold: `rank <= 20`
- Watch zone: `21 <= rank <= 35`
- Exit threshold: `rank > 35`

現行20/30は過敏と判断。persistence（2回連続）はPhase 2で検証。

### A-3. T3の扱い
- T3はログ出力のみ
- T3単独ではtargetから除外しない
- 実際のexit driverはT2のみ

### A-4. 正式exit_signal_reason
- THEME_EXIT_RANK
- REMOVED_BY_CAPACITY
- DUPLICATE_REPLACED
- INVALID_DATA
- DELISTED

T3単独は含めない。

---

## 3. Layer B: Execution Permission（売却許可判定）

Layer Bは、Layer Aで除外された銘柄を「今回実際に売ってよいか」を判定する層。

### B-1. MinHold
- holding_days < 20 AND ticker ∉ target_weights → HOLD_BLOCKED_MINHOLD
- holding_days >= 20 AND ticker ∉ target_weights → SELL_ELIGIBLE

### B-2. TRAIL8の扱い
TRAIL8（旧Gap Stop -8%）は**注文系から完全除外**する。
- auto-sellしない
- MinHold overrideを許可しない
- monitoring pipelineへ移管
- ログ名は`TRAIL8_ALERT`
- `Gap Stop`という名称は使用しない

### B-3. TRAIL8の再検討条件
executionに戻すには以下3点を先に固定すること:
1. 比較対象（前日比 / entry比 / peak_since_entry比）
2. 評価頻度（毎営業日 / リバランス日のみ）
3. 保護目的（catastrophic-loss exit / trailing profit protection）

---

## 4. Layer C: Portfolio Reconciliation（実ポートフォリオ整合）

最優先のgovernance論点。

### C-1. 状態変数（desired_weight=0だがactual>0の曖昧状態を排除）
- desired_weight
- actual_weight_pre / actual_weight_post
- blocked_weight / blocked_reason
- days_held / sell_eligible
- target_member_flag / trigger_set

### C-2. 制約の優先順位
1. Data integrity / delisting / execution impossibility
2. atk_cap（hard）
3. MinHold
4. sector_cap（legacy blocked=soft / new buys=hard）
5. signal preference / ranking

### C-3. atk_cap
hard constraint。blocked holdingsを含むactual gross exposureがatk_capを超えてはならない。
超過時は新規買付を比例縮小またはスキップ。

### C-4. sector_cap（非対称運用）
- legacy blocked holdings: soft（即時強制売却しない）
- new buys: hard（追加購入は禁止）
- 実装: legacy overageを許容しつつ、同セクターの新規買付を禁止

### C-5. blocked holdingsによる予算圧迫時
1. blocked holdingsのactual weightを確定
2. 残余余力 = atk_cap - blocked_gross
3. new buysは残余余力の範囲でのみ実行
4. 不足時はnew buysを比例縮小
5. 必要ならrank下位のnew buysをスキップ

### C-6. forced liquidationの例外
MinHoldより上位に来るfull liquidationは以下に限定:
- 上場廃止
- 明白なデータ異常
- 執行不能状態
- hard capを新規買付縮小だけでは解消できない場合

---

## 5. パイプライン分離

### Order pipeline（実売買に影響するものだけ）
- theme selection → target generation → MinHold判定 → cap repair → order generation

### Monitoring pipeline（観測・警告のみ、注文権限なし）
- TRAIL8_ALERT / T3単独 / peak-to-current DD / anomalous volume

**monitoring出力をorder pipelineの入力にしてはならない。**
ファイル/関数単位で分離する。

---

## 6. 採用 / 保留 / 禁止

### 即時採用
- 3層分離 / MinHold 20維持 / T2=exit driver / T3=diagnostic
- Exit hysteresis: entry<=20 / exit>35
- atk_cap hard / sector_cap非対称
- TRAIL8のmonitoring分離（auto-sell hard-disable）

### 保留（Phase 2以降）
- exit persistence（2回連続）
- TRAIL8のexecution化
- ATR-based stop / gradual phase-out / dynamic MinHold
- PRISM-R専用exitロジック

### 禁止
- 未検証TRAIL8によるauto-sell
- T3単独での売却
- blocked holdingsを無視したcap判定
- desired_weight=0とactual_weight>0を混同する実装
- legacy blockedに対するsector_capの即時hard enforcement
- rank>30即脱落に戻す

---

## 7. PRISM-Rへの適用

PRISM-R（A5系）はshadow段階のため、exit governanceはPRISM本体と同一。
- PRISM-R専用exitは作らない
- A5の低turnover観測だけでは専用exitを正当化しない
- live化時もまず本Constitutionを継承

---

## 8. 実装ロードマップ

Phase 0A（今すぐ）: 本文書確定保存
Phase 0B（今すぐ）: TRAIL8 auto-sell hard-disable
Phase 1（次回リバランス前）: rank>30→35 / T3 diagnostic / read-only監査追加
Phase 2（Layer C tension実観測後）: 状態変数分離 / cap repair / decision code
Phase 3（6回目以降）: ログ完備 / unit tests / replay audit

## 9. 一文要約

PRISMの売却ロジックはMinHold 20が唯一の実証済みコアであり、
その他のexit関連ルールはsignal/permission/reconciliationの3層に分けて再定義する。
最優先は閾値最適化ではなく、MinHoldブロック下でのactual portfolioとcap制約の整合ルール明文化。
