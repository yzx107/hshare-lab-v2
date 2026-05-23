# Orderbook Research Handoff 2026-05

## handoff 结论

下游 `autoresearch / backtest` 现在可以消费的最小盘口研究输入是：

- namespace: `orderbook_replay__top_of_book_only`
- table: `top_of_book_events`
- builder: `python -m Scripts.build_orderbook_top_of_book_only`
- source layer: `candidate_cleaned.orders` + `candidate_cleaned.trades`

该输入只支持 top-of-book 研究，不支持 full reconstructed depth。

## 可以安全消费什么

默认研究 lane 可以在显式过滤后消费：

- `BestBidReplay`
- `BestAskReplay`
- `ReplaySpread`
- `ReplayMid`
- `TradeInsideBestBookFlag`
- `TopOfBookValidFlag`
- `ReplayQualityScore`

同时必须保留以下质量列，不能在输入层删除：

- `CrossedWindowFlag`
- `ReplayResidueFlag`
- `ReplayWindowExcludedFlag`
- `SameMillisecondBatchRiskFlag`

## 必须过滤什么

默认研究样本必须使用以下 filter：

```text
TopOfBookValidFlag = true
ReplayQualityScore = 1.0
CrossedWindowFlag = false
ReplayResidueFlag = false
ReplayWindowExcludedFlag = false
SameMillisecondBatchRiskFlag = false
```

不满足该 filter 的行只能进入 manual review lane 或 DQA 归因报告，不能伪装成正常 replay 样本。

## manual review lane

以下行允许进入 manual review lane：

- `CrossedWindowFlag=true`
- `ReplayResidueFlag=true`
- `ReplayWindowExcludedFlag=true`
- `SameMillisecondBatchRiskFlag=true`
- `TopOfBookValidFlag=false`
- `ReplayQualityScore=0.0`

manual review 的目的只能是解释 residue、排序风险和 exclusion policy，不得直接产出 alpha。

## 禁止进入 default verified input

以下对象不得进入 `verified_orders / verified_trades` 默认表：

- `orderbook_replay__top_of_book_only`
- `BestBidReplay`
- `BestAskReplay`
- `ReplaySpread`
- `ReplayMid`
- `TradeInsideBestBookFlag`
- `TopOfBookValidFlag`
- `ReplayQualityScore`
- 所有 replay quality flags

## 仍然绝对 blocked

- `FullReconstructedDepth`
- `Level`
- `BidVolume`
- `AskVolume`
- `BrokerNo`
- full `Ext`
- queue semantics
- execution realism
- strict ordering-sensitive causality

这些对象不能由下游通过 top-of-book surface 反向推断或包装成默认输入。

## 下游实现建议

下游 repo 应先读取 `manifests/orderbook_research_handoff.json`，再定位外置盘上的 `orderbook_replay__top_of_book_only/top_of_book_events` 分区。任何研究报告必须同时记录 filter 后样本数、filter 前样本数，以及被 quality flags 排除的窗口数量。
