# CLEANING_SPEC

## 当前定义

`cleaning` 在 v2 中只负责把 raw 转成结构化、类型稳定、可审计的中间层。

## 允许做的事

- CSV/ZIP 解包与读取
- 列名标准化
- 类型转换
- 时间戳统一格式化
- 空值标准化
- 明显坏行隔离
- 分区落盘
- manifest 与日志生成

## 不允许做的事

- 推断 `TradeDir` 的业务语义
- 推断 `BrokerNo` 的角色语义
- 把 `Level`、`VolumePre` 解释成已验证含义
- 在 cleaning 阶段补 side / queue / lifecycle

## 当前产物命名

- 当前只允许输出到 `candidate_cleaned`
- 未经 DQA 与 semantic verification，不得升级为 `verified`

## 当前待定

- 2025 candidate cleaned 的最终 schema
- partition 粒度
- candidate key 口径
- golden sample 清单
