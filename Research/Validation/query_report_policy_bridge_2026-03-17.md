# Query / Report Policy Bridge 2026-03-17

## Purpose

本页把已经形成的 `field policy`、`reference policy`、`verified admission policy` 压成 Query / report 直接可执行的一页口径。

它不新增语义结论，只规定下游展示和写法。

## Query Output Rules

Query 结果默认应先区分三层：

- `candidate_cleaned`
- `verified`
- `reference_lookup`

不允许把三层混成一个“已证实 truth layer”。

## Report Rules

凡是 markdown 报告、query note、图表附注里出现以下任一情况：

- 使用了 `vendor-defined` 字段
- 使用了 `unverified-semantic` 字段
- 使用了 reference join
- 同时引用 `candidate_cleaned` 与 `verified`

都应补齐至少这四项：

- provenance note
- `source_layer`
- field policy check
- reference join caveat

## Safe Labels

优先使用：

- `reference_lookup`
- `lookup_enriched`
- `vendor-defined`
- `candidate interpretation`
- `project-level verified structural field`

避免使用：

- `officially confirmed by reference`
- `verified by lookup table`
- `semantics proven by vendor readme`
- `confirmed official mapping`
- `verified business meaning`

## Keep-Out Handling

如果 Query / report 提到下列字段：

- `BrokerNo`
- `Level`
- `BidOrderID`
- `AskOrderID`
- `BidVolume`
- `AskVolume`
- `VolumePre`

则默认动作应是：

- 允许描述其存在、覆盖、分布、稳定性、join feasibility
- 不允许把它们写成已验证业务语义
- 不允许因为 reference join 或常识性猜测而升级成 truth claim

## Caveat-Only Handling

如果 Query / report 提到下列字段：

- `Dir`
- `OrderType`
- `Ext`
- `Type`

则默认动作应是：

- 允许做枚举、漂移、共现、生命周期形状描述
- `Dir` 若被解释，必须明确写成 vendor-derived aggressor proxy：`1=sell`, `2=buy`, `0=other`
- `Dir=0` 与 `Type in {U,X,P,D,M}` 的特殊桶应单独处理，不应混入 normal signed-flow
- 若进入结果展示，应显式写明 `vendor-defined` / `not research-verified`
- 不应默认并入 `verified` 口径

## Minimal Checklist

写 Query 结果或研究短报时，至少问这 5 个问题：

1. 结果读的是 `candidate_cleaned` 还是 `verified`？
2. 有没有 reference join？
3. 有没有提到 `vendor-defined` / `unverified-semantic` 字段？
4. 有无 provenance note？
5. 有没有把 lookup enrichment 偷换成 semantic proof？

## References

- [QUERY_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/QUERY_CONTRACT.md)
- [research_report_template.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Reports/research_report_template.md)
- [provenance_note_template.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/provenance_note_template.md)
- [reference_usage_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/reference_usage_boundary_2026-03-15.md)
- [verified_admission_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_boundary_2026-03-15.md)
