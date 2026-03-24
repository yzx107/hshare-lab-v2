# Verified Layer v1 Design

## Scope

本设计说明只定义 verified v1 的保守实现骨架，不要求立刻跑全量 materialization。

## Purpose

verified v1 的目标是：

- 把当前已可接受的 conservative structural fields 从 `candidate_cleaned` 提升到更高置信度入口
- 保持与 field policy、verified admission policy、reference policy 一致
- 不提前放行高风险 vendor-defined semantics

## Inputs

- `candidate_cleaned` stage partitions
- [verified_field_policy_2026-03-15.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_field_policy_2026-03-15.json)
- [field_policy_2026-03-15.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/field_policy_2026-03-15.json)
- [reference_policy_2026-03-15.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/reference_policy_2026-03-15.json)
- [verified_admission_matrix_2026-03-18.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_matrix_2026-03-18.md)
- [semantic_lifecycle_2025.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/semantic_lifecycle_2025.md)
- [semantic_lifecycle_2026.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/semantic_lifecycle_2026.md)
- relevant semantic / DQA outcomes when available

## Current Annual Basis

- `2025`
  - `linkage = pass`
  - `research_time_grade = coarse_only`
  - `full-year lifecycle = 246/246, 0 failed`
- `2026`
  - `linkage = pass`
  - `time_anchor = pass`
  - `research_time_grade = fine_ok`
  - `full-year lifecycle = 48/48, 0 failed`

这些结论足够支持 conservative verified v1 的 structural build，
但还不足以自动放行 caveat fields 或 linkage-native semantics。

## Current Repo Status

- `verified_orders` / `verified_trades` v1 builder has now been implemented in the repo.
- `2025` full-year verified v1 materialization is now complete with a checked-in full-year report.
- `2026` full-year verified v1 materialization has corresponding checked-in acceptance/report artifacts.
- The existence of verified outputs should be interpreted as: conservative structural tables are materialized successfully.
- It should not be interpreted as: higher-risk semantic fields have been promoted, or semantic verification for `TradeDir`, `BrokerNo`, queue semantics, or event-type semantics is finished.

## v1 Output Tables

- `verified_trades`
- `verified_orders`

Not in v1 default scope:

- `verified_trade_order_linkage`
- `broker_reference`

These need more semantic / reference handling and should not be forced into the first conservative release.

## Immediate v1 Build Decision

当前默认实现只应：

- materialize `verified_orders`
- materialize `verified_trades`

当前默认不应：

- materialize `verified_trade_order_linkage`
- 把 `BidOrderID / AskOrderID` 直接提升到 verified 默认表
- 把 `OrderType / Dir / BrokerNo / Level / VolumePre / Type / Ext` 混入 verified v1

## Field Selection Rule

verified v1 should include only:

- mechanically safe tech fields
- `admit_now` fields from verified admission policy

verified v1 should exclude by default:

- `admit_with_explicit_caveat_only`
- `keep_out_for_now`

## Table Contract Shape

### verified_orders

Expected core columns:

- `date`
- `table_name`
- `source_file`
- `ingest_ts`
- `row_num_in_file`
- `SeqNum`
- `OrderId`
- `Time`
- `Price`
- `Volume`

### verified_trades

Expected core columns:

- `date`
- `table_name`
- `source_file`
- `ingest_ts`
- `row_num_in_file`
- `TickID`
- `Time`
- `Price`
- `Volume`

## Required Metadata

verified v1 rows or table-level manifests should carry:

- `verified_policy_version`
- `field_policy_version`
- `reference_policy_version`
- `source_layer = candidate_cleaned`
- `admission_rule = admit_now_only`
- `contains_caveat_fields = false`
- `reference_join_applied = false`

## What v1 Explicitly Does Not Mean

verified v1 does not mean:

- official HKEX native field mapping is confirmed
- all included fields are semantically final
- downstream modules may ignore provenance and field caveats

In particular:

- `SeqNum` and `Time` may appear in verified v1 as project-level structural fields
- they still must not be described as confirmed official native fields
- verified v1 should not silently embed broker / participant lookup labels as fact columns

## Recommended Build Sequence

1. Read verified admission policy.
2. Build allowed column list per table.
3. Materialize `verified_orders` and `verified_trades`.
4. Write summary manifest with policy version and excluded-field list.
5. Defer linkage and broker enrichment tables to later phases.

## Build Boundary

verified v1 is intentionally conservative.

If a future change needs:

- `BrokerNo`
- `Dir`
- `Level`
- `BidOrderID / AskOrderID`
- `OrderType`
- `Type`

then that change should first update semantic status and admission policy, rather than silently expanding verified scope.

This remains true after the current `2025` and `2026` full-year verified builds: implementation progress does not override semantic or admission policy gates.
