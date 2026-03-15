# Golden Sample Policy 2026-03-15

## Scope

本说明定义 Hshare Lab v2 的 golden sample 在当前阶段应如何选、如何用、以及不能被误用成什么。

它不触发新的大规模样本扫描，也不冻结具体日期清单本身；
它先冻结 selection rule、usage rule 和 promotion rule。

## Purpose

golden sample 的作用不是替代 full-year evidence，而是：

- 给 contract / DQA / semantic verification 提供稳定复查入口
- 给 smoke test 和 regression test 提供真实数据锚点
- 减少后续每次讨论样本日时的随意性

## Golden Sample Buckets

### A. Smoke Sample

用途：

- 新脚本首次跑通
- schema / row-count / basic validity sanity check
- visible + resumable 行为确认

要求：

- 单日真实数据
- 单日能在可接受时间内完成
- 能覆盖 orders / trades 主表

结论边界：

- 只能证明 pipeline 能跑通
- 不能替代 representative sample 或 full-year admissibility

### B. Representative Sample

用途：

- DQA 边界观察
- semantic verification 初步结论
- 跨日期稳定性检查

要求：

- 至少 3 个交易日
- 不同月份分布
- 尽量覆盖常态交易日，而非只选异常日
- 若分年份语义明显不同，应按年份各自维护

结论边界：

- 可以支持 `candidate`、`weak_pass`、`allow_with_caveat` 级别结论
- 不能直接替代 full-year coverage 结论

### C. Golden Sample

用途：

- 作为 repo 内统一复查入口
- 用于 contract / DQA / semantic note 的固定引用
- 用于脚本回归测试与研究入口样本说明

要求：

- 从 representative sample 中挑选
- 日期和选入理由必须显式记录
- 每个年份应有最少一组 orders / trades 主样本
- 如存在独立 source group 风险，需明确是否排除

结论边界：

- 支持“固定对照样本”角色
- 不等于“最有代表性的全年事实”
- 不自动升级为 verified evidence

## Selection Rule

golden sample 选取时至少记录：

- `year`
- `trade_date`
- `why_selected`
- `contains_orders`
- `contains_trades`
- `known_caveats`
- `excluded_source_groups`
- `intended_uses`

推荐补充：

- `time_grade`
- `linkage_grade`
- `semantic_focus`

## Current Project Guidance

在当前项目阶段，golden sample 应优先服务：

- stage contract regression
- DQA smoke / representative rerun
- semantic boundary reproduction
- report examples and query examples

不应优先服务：

- 直接做 alpha 样本
- 直接做生产化统计 benchmark
- 替代 raw inventory / full-year coverage

## Raw Inventory Relationship

raw inventory 和 golden sample 是互补而不是替代关系：

- raw inventory 回答“原始世界全貌是什么”
- golden sample 回答“后续验证固定从哪些真实样本入口复查”

在 raw inventory 未完全落盘前，golden sample 可以先冻结 policy；
但最终冻结日期清单时，应尽量参考 raw inventory 输出与 source-group inventory 结果。

## Source Group Guardrail

如果某个 source group 已知需要隔离，例如 `HKDarkPool`：

- 默认不进入主 golden sample
- 除非该 golden sample 的目标本来就是该 source group 专项验证
- 如纳入，必须显式写明 `source_group_specific = true`

## Minimal Manifest Shape

建议未来的 golden sample manifest 至少包含：

- `sample_tier`
- `year`
- `trade_date`
- `table_scope`
- `source_group_scope`
- `selection_reason`
- `known_caveats`
- `allowed_uses`

## Promotion Rule

一个日期从普通样本升级为 golden sample，至少需要：

- 已用于一次真实 smoke test
- 已用于一次 representative / semantic / DQA 复查
- 没有已知 source-group 混淆未说明
- 在文档中能清楚说明为什么保留它

## Recommended Rule

> Golden sample is a reproducibility anchor, not a shortcut around inventory or semantic verification.
