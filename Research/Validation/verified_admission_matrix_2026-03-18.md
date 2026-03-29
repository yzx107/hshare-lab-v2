# Verified Admission Matrix 2026-03-18

## Scope

本页把当前已经完成的 `2025/2026` full-year `schema / linkage / lifecycle`
结论，压成 verified v1 的实际准入矩阵。

它不新增重任务，不触发新的 full-year materialization；
只回答：

- 哪些字段/对象现在可以进入 verified v1
- 哪些虽然已有 semantic 进展，但仍不进入 verified v1 默认范围
- `2025` 与 `2026` 的年度差异如何影响 verified 的使用边界

## Inputs

- [verified_admission_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_boundary_2026-03-15.md)
- [verified_layer_v1_design.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_layer_v1_design.md)
- [semantic_lifecycle_2025.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/semantic_lifecycle_2025.md)
- [semantic_lifecycle_2026.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/semantic_lifecycle_2026.md)
- [dqa_linkage_2025.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/dqa_linkage_2025.md)
- [dqa_linkage_2026.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/dqa_linkage_2026.md)
- [research_admissibility_matrix.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Audits/research_admissibility_matrix.md)
- [SEMANTIC_MATRIX.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/SEMANTIC_MATRIX.md)

## Annual Basis

- `2025`
  - `ID-linkage = pass`
  - `time_anchor = unavailable`
  - `research_time_grade = coarse_only`
  - `full-year lifecycle = 246/246, 0 failed`
  - lifecycle 年度结果以 `pass / weak_pass` 为主，适合作为结构骨架，不适合作为 fine-grained event semantics 证据

- `2026`
  - `ID-linkage = pass`
  - `time_anchor = pass`
  - `lag_linkage = pass`
  - `research_time_grade = fine_ok`
  - `full-year lifecycle = 48/48, 0 failed`
  - lifecycle 年度结果为 `pass`，可作为 verified admission 的强结构基线

## Matrix

| object_or_field_group | 2025 basis | 2026 basis | verified_v1_decision | notes |
| --- | --- | --- | --- | --- |
| `verified_orders` core structural columns | sufficient | sufficient | `admit_now` | `date, table_name, source_file, ingest_ts, row_num_in_file, SeqNum, OrderId, Time, Price, Volume` |
| `verified_trades` core structural columns | sufficient | sufficient | `admit_now` | `date, table_name, source_file, ingest_ts, row_num_in_file, TickID, Time, Price, Volume` |
| `OrderId` as project structural order key | lifecycle/linkage backbone established | lifecycle/linkage backbone established | `admit_now` | 可作为 verified structural identifier；不宣称官方 native field identity |
| `TickID` as project structural trade key | mechanically safe | mechanically safe | `admit_now` | 保守进入 verified，不宣称官方 `TradeID` mapping 已确认 |
| `SeqNum` / `Time` as project structural sequencing/time columns | `coarse_only` | `fine_ok` | `admit_now_with_year_caveat` | 两年都可进入 verified v1，但 `2025` 不得被当作 fine-grained timing anchor |
| `BidOrderID / AskOrderID` linkage columns | direct equality works, native meaning still unverified | direct equality works, native meaning still unverified | `keep_out_for_now` | 可继续服务 DQA / semantic / research admissibility；不进入 verified v1 默认表 |
| `verified_trade_order_linkage` table | too strong for v1 | too strong for v1 | `defer` | 等下一阶段 verified 扩容，而不是现在默认 materialize |
| `OrderType` | no full-year pass-level semantic release | `weak_pass` | `admit_with_explicit_caveat_only` | 不进 verified v1 默认表；若后续暴露，需显式 caveat |
| `TradeDir / Dir` | vendor-derived aggressor proxy with `coarse_only` caveat | vendor-derived aggressor proxy with manual-review caveat | `admit_with_explicit_caveat_only` | 不进 verified v1 默认表；若后续暴露，必须写明 `Dir=1=sell`, `Dir=2=buy`, `Dir=0=other/special bucket`，且仍不得当 confirmed signed side 使用 |
| `BrokerNo` | blocked | blocked | `keep_out_for_now` | 只允许 reference lookup 语境，不进 verified |
| `Level / VolumePre` | blocked | blocked | `keep_out_for_now` | queue/depth 语义未验证 |
| `Type / Ext` | vendor-defined only | vendor-defined only | `admit_with_explicit_caveat_only` | 不进 verified v1 默认表 |

## Immediate Verified v1 Decision

现在可以直接推进的只有：

- `verified_orders`
- `verified_trades`

并且默认只收纳：

- mechanically safe technical columns
- conservative structural columns
- current `admit_now` fields

现在仍然默认不做：

- `verified_trade_order_linkage`
- broker enrichment / participant enrichment
- `OrderType / Type / Ext / Dir` caveat namespace
- `BrokerNo / Level / VolumePre / BidOrderID / AskOrderID` 进 verified 默认表

## Build Rule

verified v1 的实现应满足：

- 不改变字段定义来迁就实现成本
- 不把 lifecycle / linkage 结论误写成“native semantics 已完全确认”
- `2025` 与 `2026` 可共享 conservative schema，但必须保留 year-level usage caveat
- verified manifest 必须明确：
  - `source_layer = candidate_cleaned`
  - `admission_rule = admit_now_only`
  - `contains_caveat_fields = false`
  - `reference_join_applied = false`

## What Changes Later

如果以后要把这些对象纳入 verified：

- `BidOrderID / AskOrderID`
- `verified_trade_order_linkage`
- `OrderType`
- `BrokerNo`

那应该先更新：

- semantic status
- verified admission boundary
- verified field policy

而不是直接扩表。

如果以后要把 `Dir` 放进 verified 默认表，或去掉 caveat-only 约束，也应先更新这三层 policy，而不是直接扩表。
