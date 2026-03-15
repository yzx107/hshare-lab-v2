# HKDarkPool External Hypothesis

## Scope

本笔记记录 `HKDarkPool` 当前在仓库中的事实层结论与外部假设边界。

## Repo-Safe Facts

- `HKDarkPool` 目前首先是一个在 raw 路径中观测到的 source group label。
- 针对该 label 的专项 inventory 已完成，显示其在 `2025` 中呈现稳定独立的 `7` 列 trade-like schema：
  - `time`
  - `price`
  - `share`
  - `turnover`
  - `side`
  - `type`
  - `brokerno`
- 它当前继续隔离处理，不并入主 `Orders / Trades` contract。

## Current Boundary

- 本地官方 / vendor technical reference 目前没有直接点名 `HKDarkPool`。
- 因此仓库当前不能把 `HKDarkPool` 当成官方已确认术语写死。
- 它当前只能被描述为：
  - observed raw source group label
  - candidate dark-pool-related source group

## External Hypothesis Provided In Discussion

本线程补充了一个外部假设：

- 在部分供应商口径里，`HkDarkPool` 可能用于标识港股暗池交易
- 暗池是 `Alternative Liquidity Pools`
- 这类成交在正式撮合系统外达成，成交后报备显示
- 它不同于 IPO `grey market / 暗盘`

## How To Use This Hypothesis

Safe:

- 作为外部假设池记录
- 作为后续语义验证候选方向
- 作为区分 `dark pool` 与 `grey market` 的分析提醒

Not safe:

- 直接升级为 repo 内 official/proven semantic truth
- 直接据此把 `HKDarkPool` 写成“官方暗池字段”
- 直接据此把相关成交并入主 verified contract

## Recommended Wording

> `HKDarkPool` is currently treated as an observed raw source group label. External discussion suggests a possible dark-pool-related interpretation, but the local official/vendor technical references do not directly confirm that term, so the repo does not yet promote it to verified semantics.
