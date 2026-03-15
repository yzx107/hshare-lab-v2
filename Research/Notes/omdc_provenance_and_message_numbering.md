# OMD-C Provenance And Message Numbering

## Purpose

本笔记用于正式记录两件事：

- 当前 `2025/2026` 港股数据的上游采购来源已确认属于 HKEX `OMD-C` family
- 官方 `OMD-C` binary interface 与当前 CSV / stage 列形态之间，仍存在需要逐项核对的 message 编号与字段落地差异

它是 provenance 与边界说明，不是字段语义已验证结论。

## Repo-Safe Conclusion

- 可以确认：当前数据上游来自 HKEX `OMD-C` 体系。
- 可以保守推断：包含 `OrderId` 的逐笔新增 / 修改 / 删除内容，与 `Securities FullTick (SF)` 能力相符。
- 不能直接断言：当前 CSV / `candidate_cleaned` 文件就是官方 binary message 的 `1:1` 原样展开。
- 不能直接断言：当前字段名已经等同于官方 message field 语义。

## Official Grounding From The Local PDF

来源文件：

- [HKEX_OMD-C_Binary_Interface_Specifications_v1.23.pdf](/Users/yxin/Downloads/HKEX_OMD-C_Binary_Interface_Specifications_v1.23.pdf)

从该 PDF 可直接确认：

- `Add Order (30)` 明确描述为逐笔订单插入消息，且写明 market order 时 `Price = 0`
- `Modify Order (31)` 与 `Delete Order (32)` 是逐笔订单更新 / 删除消息
- `Trade (50)` 是逐笔成交消息
- `Broker Queue (54)` 是 broker queue 消息，包含某一侧最多前 `40` 个 broker IDs
- `Broker Queue` 为空时，官方表达是 `ItemCount = 0`
- odd lot 相关消息是 `Add Odd Lot Order (33)` / `Delete Odd Lot Order (34)`；trade 侧 odd lot 则出现在 `TrdType = 102`

## Why FullTick Is The Best Current Fit

- 当前数据包含 `OrderId`
- 当前数据包含订单级增量轨迹，而非只有 aggregate depth
- 这与 `Securities FullTick (SF)` 的订单级内容能力一致

因此，当前项目可以把 `OMD-C family + FullTick-compatible order-level content` 作为 provenance 描述。

## Numbering Mismatch To Keep Explicit

当前本地官方 PDF 使用的是 binary message 编号：

- `Add Order (30)`
- `Modify Order (31)`
- `Delete Order (32)`
- `Trade (50)`
- `Broker Queue (54)`

如果外部材料、供应商说明或后续接口文档出现：

- `302 / 303 / 304 / 311 / 315`

则当前应将其视为：

- 另一套编号体系
- 另一版本文档
- 或另一层封装 / 导出接口

在未完成文档对齐前，不应把 `302 == 30`、`315 == 54` 当成仓库事实写死。

## Field-Level Guardrail

即使 provenance 已确认属于 `OMD-C`，以下结论仍然不自动成立：

- `BrokerNo` 已确认等同官方 `BrokerID`
- `BrokerNo=0` 已有固定官方语义
- `TradeDir`、`Level`、`VolumePre` 已可按字段名直接解释
- 当前 `BidOrderID` / `AskOrderID` 一定是官方 trade message 原生字段

这些仍需通过 semantic verification 单独确认。

## Practical Project Wording

后续在 repo 中推荐统一表述为：

> The upstream source is HKEX OMD-C family data. The current CSV and stage representations contain FullTick-compatible order-level content, but they are not yet treated as a 1:1 official binary message dump. Field semantics still require explicit verification.
