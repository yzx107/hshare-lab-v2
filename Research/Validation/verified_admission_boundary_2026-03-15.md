# Verified Admission Boundary 2026-03-15

## Scope

本清单定义 verified layer 的第一版字段准入边界。

它只基于当前 contract、vendor / HKEX 文档分析、field policy 与已形成的 admissibility 边界；
不依赖新的 full-year 扫描，不触发 staging / DQA / linkage 重任务。

## Purpose

verified layer 的目标不是“把常用字段都搬进去”，而是：

- 只收纳 mechanically safe 或语义已验证字段
- 给后续 Query / research 一个更高置信度入口
- 明确哪些字段仍必须停留在 `candidate_cleaned` 或 `vendor-defined` 语境中使用

## Admission Buckets

### A. Admit Now

这些字段可以进入 verified 第一版，前提是仍保留 provenance、source_layer、year 等技术追溯列。

#### Orders

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

#### Trades

- `date`
- `table_name`
- `source_file`
- `ingest_ts`
- `row_num_in_file`
- `TickID`
- `Time`
- `Price`
- `Volume`

准入理由：

- 技术追溯列属于 mechanically safe
- `OrderId`、`TickID`、`Price`、`Volume` 可作为高价值结构列
- `SeqNum`、`Time` 目前虽未完成官方 native mapping，但可作为 verified layer 中的 conservative structural fields 使用，前提是不宣称官方字段原义

使用边界：

- `SeqNum`、`Time` 的 verified 含义应限定为 project-level verified structural field
- 不把它们写成“已确认官方 native sequence / native time field”

### B. Admit With Explicit Caveat Only

这些字段不建议默认进入 verified 第一版；若后续确实进入，必须带明确 caveat 元数据或单独命名空间。

#### Orders

- `OrderType`
- `Ext`

#### Trades

- `Dir`
- `Type`

原因：

- 它们已有稳定性或 vendor 定义支撑
- 但业务语义仍未完成 field-level verification
- 一旦直接放入 verified，很容易被下游误读成“已验证事件类型 / 业务标签”

推荐做法：

- 先不进入 verified 第一版
- 若后续有强需求，可进入 `verified_with_caveat` 或同等显式受限视图

### C. Keep Out Of Verified For Now

这些字段当前应继续停留在 `candidate_cleaned` / DQA / semantic verification 语境，不进入 verified 第一版。

#### Orders

- `Level`
- `BrokerNo`
- `VolumePre`

#### Trades

- `BrokerNo`
- `BidOrderID`
- `BidVolume`
- `AskOrderID`
- `AskVolume`

原因：

- 它们直接牵涉 queue、broker identity、trade-to-order linkage-native meaning 等高风险语义
- 当前项目已有明确 guardrail：这些语义尚未正式放行
- 若过早进入 verified，会污染 Query 和下游研究的默认口径

对于 `Dir`：

- 当前已足够进入 `caveat-only` 语境
- 但仍不足以进入 verified 默认表或被写成 confirmed signed-side truth

## Field-Specific Notes

### `OrderId`

- 可以作为 verified 的订单级结构主键
- 允许用于 lifecycle skeleton、linkage backbone、same-order consistency
- 不写成官方 binary field identity 已确认

### `TickID`

- 可以作为 verified 的成交级结构标识符
- 不写成已确认等于官方 `TradeID`

### `SeqNum`

- 可以作为 verified 的排序/序列检查辅助字段
- 其 verified 含义是“项目内已接受的结构序列列”，不是“官方 native sequence mapping”

### `Time`

- 可以作为 verified 的 coarse/project-level time field
- 适合 session bucket、coarse temporal consistency、project-level time slicing
- 不写成已完成官方 native time identity

### `Dir`

- 若后续进入 verified，只应进入 caveat-only namespace
- 当前最稳的写法是 vendor-derived aggressor proxy：
  - `Dir=1` = sell aggressor
  - `Dir=2` = buy aggressor
  - `Dir=0` = other / vendor-unclassified
- 它不应被写成 HKEX native aggressor-side field
- `Type in {U,X,P,D,M}` 应与 `Dir=0` 的特殊桶分开处理

## Query Impact

对于 Query layer，verified 第一版应支持：

- descriptive coverage
- structural linkage
- lifecycle-shape profiling
- coarse temporal slicing
- price / volume based descriptive studies

verified 第一版仍不应默认支持：

- queue position / depletion
- broker identity research
- confirmed aggressor-side flow
- official event-type inference
- native trade-to-order semantic reconstruction

## Recommended Rule

> Verified v1 should prefer conservative structural fields over semantically tempting vendor-defined fields. If a field is useful mainly because of an unverified business interpretation, it should stay out of verified until that interpretation is explicitly passed.
>
> A narrow exception is a vendor-derived proxy such as `Dir`: it may enter a caveat-only namespace once the proxy wording is explicit, but it still must stay out of the default verified surface.
