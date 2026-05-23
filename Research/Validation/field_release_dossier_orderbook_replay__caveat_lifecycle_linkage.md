# Field Release Dossier: orderbook_replay__caveat_lifecycle_linkage

## 放开的对象

- object_name: `orderbook_replay__caveat_lifecycle_linkage`
- object_type: `caveat_namespace`
- release_bucket: `admit_with_explicit_caveat_only`
- caveat_level: `caveat_namespace`
- manual_review_required: `True`
- contains_caveat_fields: `True`

## 来源层

- source_layer: `candidate_cleaned.orders + candidate_cleaned.trades`
- raw_fields: `OrderType`, `Ext[0]`, `BidOrderID`, `AskOrderID`, `VolumePre`
- builder: `python -m Scripts.build_orderbook_replay_caveat`

## 派生逻辑

Materializes lifecycle_events and trade_linkage evidence tables with explicit caveat metadata; does not output reconstructed depth.

## 证据材料

- `Research/Validation/field_release_mechanism_2026-05.md`
- `Research/Validation/orderbook_replay_semantic_release_2026-05-23.md`
- `Research/Audits/hshare_orderbook_reconstruction_probe_20260522.md`
- `Research/Reports/orderbook_replay_caveat_20260522.md`

## 允许用途 (allowed uses)

- orderbook replay lifecycle/linkage DQA
- active-order presence and side-match evidence
- input evidence for crossed-book residue investigation

## 禁止宣称 (forbidden claims)

- reconstructed depth is admitted
- top-of-book or full book replay is strategy-ready
- Level, full Ext, BrokerNo, BidVolume, or AskVolume semantics are released

## 当前 blocker

Reconstructed depth remains blocked until crossed-book residue is explained or bounded by contract-level thresholds.

## 下游使用边界

- Allowed as caveat-only DQA materialization.
- Not a verified default table and not a reconstructed-depth feed.

## 下游 namespace

- `orderbook_replay__caveat_lifecycle_linkage`

## verified default 边界

- verified_default_admission: `False`
- 除非 registry 后续显式升级为 `admit_now`，否则不得静默并入 `verified_orders` / `verified_trades` 默认表。
