# Field Release Dossier: orderbook_replay__top_of_book_only

## 放开的对象

- object_name: `orderbook_replay__top_of_book_only`
- object_type: `caveat_namespace`
- release_bucket: `admit_top_of_book_only`
- caveat_level: `top_of_book_only`
- manual_review_required: `true`
- contains_caveat_fields: `true`

## 来源层

- source_layer: `candidate_cleaned.orders + candidate_cleaned.trades`
- builder: `python -m Scripts.build_orderbook_top_of_book_only`
- raw_fields: `OrderType`, `Ext[0]`, `Price`, `Volume`

## 派生逻辑

该 namespace 按 `OrderId` 维护 active order book，但只输出 top-of-book 对象：

- `BestBidReplay`
- `BestAskReplay`
- `ReplaySpread`
- `ReplayMid`
- `TradeInsideBestBookFlag`
- `TopOfBookValidFlag`
- `ReplayQualityScore`

同时输出质量 gate：

- `CrossedWindowFlag`
- `ReplayResidueFlag`
- `ReplayWindowExcludedFlag`
- `SameMillisecondBatchRiskFlag`

其中 `ReplayQualityScore` 只是 bounded gate：`1.0` 表示 `TopOfBookValidFlag=true`，`0.0` 表示该行不能作为默认 top-of-book 样本消费。

## 证据材料

- `Research/Validation/field_release_mechanism_2026-05.md`
- `Research/Validation/orderbook_replay_semantic_release_2026-05-23.md`
- `Research/Audits/hshare_orderbook_reconstruction_probe_20260522.md`

## 允许用途

- top-of-book-only replay DQA
- spread / mid 的 bounded descriptive checks
- trade price 是否落在 replay best bid/ask 内的质量报告

## 禁止宣称

- 不得宣称 full reconstructed depth 已放开
- 不得宣称 `Level` 是可靠十档深度
- 不得推导 queue position、fill priority、execution realism
- 不得把 invalid / excluded windows 静默丢弃

## 当前 blocker

crossed-book residue 仍未被解释或被 contract-level 阈值约束；same-millisecond vendor batch ordering 也仍需更大样本验证。

## 下游使用边界

- 下游只能消费 `orderbook_replay__top_of_book_only`
- `TopOfBookValidFlag=false` 的行不能作为正常 top-of-book 样本使用
- 报告必须保留 `CrossedWindowFlag / ReplayResidueFlag / ReplayWindowExcludedFlag / SameMillisecondBatchRiskFlag`
- 默认研究过滤必须显式要求 `TopOfBookValidFlag=true` 且 `ReplayQualityScore=1.0`
- 该 namespace 不是 strategy-ready depth feed

## verified default 边界

- verified_default_admission: `false`
- 不得并入 `verified_orders` / `verified_trades` 默认表
