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
- relevant semantic / DQA outcomes when available

## v1 Output Tables

- `verified_trades`
- `verified_orders`

Not in v1 default scope:

- `verified_trade_order_linkage`
- `broker_reference`

These need more semantic / reference handling and should not be forced into the first conservative release.

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
- `source_layer = candidate_cleaned`
- `admission_rule = admit_now_only`
- `contains_caveat_fields = false`

## What v1 Explicitly Does Not Mean

verified v1 does not mean:

- official HKEX native field mapping is confirmed
- all included fields are semantically final
- downstream modules may ignore provenance and field caveats

In particular:

- `SeqNum` and `Time` may appear in verified v1 as project-level structural fields
- they still must not be described as confirmed official native fields

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
