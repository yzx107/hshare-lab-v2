# Orderbook Top-of-Book Only Release 2026-05

## release 对象

本 release 只放开受限 namespace：`orderbook_replay__top_of_book_only`。

允许输出的对象仅限：

- `BestBidReplay`
- `BestAskReplay`
- `ReplaySpread`
- `ReplayMid`
- `TradeInsideBestBookFlag`
- `TopOfBookValidFlag`
- `ReplayQualityScore`
- `CrossedWindowFlag`
- `ReplayResidueFlag`
- `ReplayWindowExcludedFlag`
- `SameMillisecondBatchRiskFlag`

## 来源层

- source layer: `candidate_cleaned.orders` + `candidate_cleaned.trades`
- builder: `python -m Scripts.build_orderbook_top_of_book_only`
- registry: `manifests/field_release_registry.json`
- output namespace: `orderbook_replay__top_of_book_only`

`candidate_cleaned` 仍然只是 stage layer。builder 读取 stage 行后按 `OrderId` 维护 active order book，但 release 只允许把当前 best bid / best ask 及其质量 gate 物化出来。

## release bucket

- namespace bucket: `admit_top_of_book_only`
- quality flags bucket: `admit_with_explicit_caveat_only`
- `ReplayQualityScore`: `admit_with_explicit_caveat_only`
- verified default admission: `false`

这不是 full depth release，也不是 strategy-ready replay feed。

## allowed uses

- top-of-book-only DQA
- spread / mid 的 bounded descriptive checks
- trade price 是否位于 replay best bid/ask 内的报告
- 下游 backtest 的输入候选筛选，但必须先执行显式 quality filtering
- crossed / residue / same-millisecond 风险窗口的 manual review routing

## forbidden claims

- 不得宣称 `FullReconstructedDepth` 已放开
- 不得宣称 `Level` 是可靠十档深度
- 不得宣称 `BidVolume / AskVolume` 语义已验证
- 不得输出 queue position、queue semantics、fill priority 或 execution realism
- 不得宣称 full `Ext` 语义已验证
- 不得把 `ReplayQualityScore` 当成 alpha signal 或 strategy admission score
- 不得把 invalid / excluded windows 静默丢掉

## gating 条件

默认研究消费必须保留并过滤以下列：

- `TopOfBookValidFlag=true`
- `ReplayQualityScore=1.0`
- `CrossedWindowFlag=false`
- `ReplayResidueFlag=false`
- `ReplayWindowExcludedFlag=false`
- `SameMillisecondBatchRiskFlag=false`

`ReplayQualityScore` 当前只是一阶 bounded gate：`1.0` 等价于 `TopOfBookValidFlag=true`，`0.0` 表示该行不能进入默认研究样本。它不是连续质量评分，也不替代 residue 归因。

## 下游 namespace

只允许消费：

- `orderbook_replay__top_of_book_only.top_of_book_events`

禁止并入：

- `verified_orders`
- `verified_trades`
- 任何未显式声明 caveat 的 default research input

## 仍然 blocked

- `FullReconstructedDepth`
- `Level`
- `BidVolume`
- `AskVolume`
- full `Ext`
- queue semantics
- execution realism
- strict ordering-sensitive causality

这些对象只能作为 blocked registry entry 或 manual review 主题存在，不能由本 builder 物化为默认研究输入。
