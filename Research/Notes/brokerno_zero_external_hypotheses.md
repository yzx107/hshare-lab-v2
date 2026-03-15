# BrokerNo=0 External Hypothesis Triage

## Purpose

本笔记将一份外部 Gemini 研究摘要重新整理为三个层级：

- repo 已证实事实
- 外部候选假设
- 待验证问题

它不是 semantic verification 结论，不覆盖 contract、audit 或 admissibility matrix。

## Three-Column View

| Topic | Repo-Confirmed | External Hypotheses | Pending Validation |
|---|---|---|---|
| 数据品类 | 项目当前 raw 数据的上游采购来源已确认属于 HKEX `OMD-C` family；`candidate_cleaned` stage 层仍只做 typed projection，不提前注入业务语义 | 当前 CSV / stage 列形态可能是官方 feed 之上的 vendor/export 视图，部分字段可能与 Broker Queue 或其他消息组合有关 | 需要按 `year + source_group + raw member` 继续对齐具体导出路径，不能把当前列名直接等同于官方 binary message |
| `BrokerNo` 字段本身 | `BrokerNo` 在 trades / orders stage 中都只被保留为字符串字段，要求保留前导零，不做角色解释 | 在某些 feed 或 vendor 视图中，`BrokerNo` 可能承载 participant / queue member / disclosed broker 标识 | 需要先做 source-group inventory，再判断不同年份和来源是否共用同一含义 |
| `BrokerNo=0` 总体含义 | repo 当前没有任何已验证的固定解释；不能当成项目事实使用 | 可能是 placeholder、anonymous、system-generated、queue-empty default、vendor "others/unified" 等 | 需要先做真实数据分布统计：按 `table/date/source_group` 统计占比，再和 `Dir`、`Type`、`BidOrderID`、`AskOrderID`、`OrderType` 等上下文字段交叉 |
| `0 = market order` | repo 内没有证据支持；当前 contract 也禁止把邻近字段先解释成语义字段 | 某些供应商或盘口重建实现里，会把 market order 或最高优先级映射到 `0` 或“Level 0” | 需要验证 `BrokerNo=0` 是否与可疑的即时成交轨迹、特定 `OrderType`、特定 `Level` 模式稳定共现；在验证前不得默认采用 |
| `0 = odd lot` | repo 内没有证据支持；当前也没有 odd-lot 已验证标签 | 一些外部资料会把碎股市场、特殊撮合或聚合视图映射到 `0` | 需要先拿到可验证的 odd-lot 判定路径，再看 `BrokerNo=0` 是否稳定集中在该子集 |
| `0 = anonymous / unavailable / system` | repo 内没有证据支持单一解释 | 部分原始 feed 或 vendor 导出可能把未披露、不可用或系统生成值写成 `0` | 需要检查原始文档、空队列情形、字段缺失模式，以及 `BrokerNo=0` 是否集中出现在特定消息类型或 linkage 结构 |
| `0 = vendor convention` | repo 内没有证据支持任何 vendor-specific mapping | 不同 vendor 可能把 `0` 标成 `Others`、`Unified` 或类似聚合标签 | 需要把 raw/source provenance 和下游导出规则对应起来，不能跨 vendor 复用解释 |

## Current Project-Safe Conclusion

- 当前最安全的项目口径是：`BrokerNo` 仍然只是 mechanical stage field。
- `BrokerNo=0` 目前不能被默认解释成 market order、odd lot、anonymous bucket 或任何单一 broker role。
- 在 semantic verification 完成前，所有 `BrokerNo=0` 相关研究都必须写明“基于 candidate_cleaned，语义未确认”。

## Repo Grounding

- `AGENTS.md` 明确规定：`BrokerNo` 不能当成已知语义。
- `DATA_CONTRACT.md` 明确规定：`candidate_cleaned` 只做 typed projection，不提前注入业务语义。
- `STAGE_SCHEMA.md` 明确规定：`BrokerNo` 只保留前导零，不做角色解释。

## Candidate Verification Path

1. 先做 `BrokerNo` real-data smoke，确认 `0` 的占比、值域和按表分布。
2. 再做 representative sample，按 `table/date/source_group` 看 `BrokerNo=0` 是否稳定。
3. 对 `BrokerNo=0` 做上下文对照：
   - `Trades`: `Dir`、`Type`、`BidOrderID`、`AskOrderID`
   - `Orders`: `OrderType`、`Level`、`VolumePre`
4. 如果要引用外部 HKEX / vendor 文档，必须把“文档原意”和“本项目字段落地方式”分开写，不能直接映射。

## Usage Rule

若后续再遇到 “`BrokerNo=0` 是什么” 这类 query，默认回答应先落在下面这句上：

> In Hshare Lab v2, `BrokerNo=0` is currently an unverified stage value, not a confirmed semantic category.
