# Broker Reference Read-Only Boundary 2026-03-17

## Scope

本页只处理 broker / participant reference 的只读使用边界：

- `normalized/brokerno.utf8.csv`
- `normalized/List_of_Current_SEHK_EP.utf8.tsv`

它不负责确认 `BrokerNo` 的最终业务语义。

## Allowed Use

允许的用途：

- `BrokerNo -> broker name` lookup enrichment
- participant metadata descriptive join
- broker coverage / ambiguity analysis
- seat-summary style query enrichment
- 对照 `brokerno` 与 `SEHK_EP` 的名称、代码覆盖情况

## Default Output Labels

这类 join 结果默认应打成：

- `reference_lookup`
- `lookup_enriched`
- `reference_derived`

不应直接打成：

- `verified`
- `official_broker_identity`
- `semantic_truth`

## What The Join Does Not Prove

reference join 本身不能证明：

- `BrokerNo` 已正式等于 HKEX native `BrokerID`
- `BrokerNo=0` 有统一官方语义
- trades / orders 中的 broker-like 列已经通过 semantic verification

## Query-Safe Use

对于 Query 层，broker reference 的安全用法是：

- 做 seat summary enrichment
- 做 human-readable label enrichment
- 做 unmatched / ambiguous bucket 统计

即使 Query 使用了这些 join，也应继续保留：

- 原始 `BrokerNo`
- `reference_source`
- `reference_join_applied = true`
- `not_semantic_proof = true`

## Verified Boundary

verified v1 默认不应：

- 把 broker/participant 名称并入 core verified fact columns
- 把 reference join 结果当成 verified truth

若未来需要 sidecar enrichment，也应显式标成：

- `reference_derived`
- `lookup_only`
- `not_semantic_proof`

## Research Writing Rule

写研究结论时，最安全的表达是：

> Broker-related labels in this output are lookup enrichments derived from broker / participant reference tables. They improve readability and seat-summary usability, but they do not by themselves prove the official native meaning of `BrokerNo`.

## References

- [reference_usage_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/reference_usage_boundary_2026-03-15.md)
- [QUERY_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/QUERY_CONTRACT.md)
- [brokerno_zero_external_hypotheses.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/brokerno_zero_external_hypotheses.md)
