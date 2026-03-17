# Query Minimal Policy Example 2026-03-17

## Goal

演示一个最小、repo-safe 的 Query 结果该怎么写：

- 明确 `source_layer`
- 明确 reference join 只是 `reference_lookup`
- 附 provenance note
- 不把 `BrokerNo` 升级成 verified truth

## Minimal Output Skeleton

```md
# Example Query Note

- source_layer: candidate_cleaned
- year: 2026
- reference_join_applied: true
- reference_join_type: broker_name_lookup
- output_label: reference_lookup

Provenance note:
The upstream source is HKEX OMD-C family data. The observed order/trade content is compatible with Securities FullTick-style order-level coverage. However, the current vendor CSV export is not yet treated as a 1:1 official HKEX message dump, and vendor-defined fields are not automatically promoted to research-verified semantics.

Field caveat:
`BrokerNo` is used here only as a vendor-defined seat-like code for lookup enrichment. Joined broker labels improve readability but do not by themselves prove official native broker identity semantics.
```

## Minimal Safe Pattern

- 允许：`BrokerNo -> broker name` lookup enrichment
- 允许：席位汇总时单独保留 `unattributed / no-seat-record`
- 不允许：写成 `BrokerNo already equals official BrokerID`
- 不允许：把 joined broker name 列写成 `verified` 事实列

## References

- [QUERY_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/QUERY_CONTRACT.md)
- [query_report_policy_bridge_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/query_report_policy_bridge_2026-03-17.md)
- [broker_reference_readonly_boundary_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/broker_reference_readonly_boundary_2026-03-17.md)
