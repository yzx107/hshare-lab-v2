# CLEANING_SPEC

## 当前定义

`cleaning` 在 v2 中只负责把 raw CSV 转成 `stage parquet`，也就是当前路径名上的 `candidate_cleaned`。

工作定义：

`Stage layer = loss-minimizing typed projection of raw CSV into partitioned parquet, without semantic enrichment.`

中文就是：

在不做语义增强的前提下，把 raw CSV 以尽量少信息损失的方式投影为分区 parquet。

## Stage Layer 原则

- 原始记录粒度不变：一条 raw 记录对应一条 stage 记录
- 原始字段尽量全保留，不因研究便利而提前删列
- 原始值尽量不改，只做最低限度工程标准化
- 工程表示要规范化，研究语义不能提前解释
- `candidate_cleaned` 是当前层的存储路径名，不表示字段语义已经被验证

## 允许新增的技术列

- `date`
- `source_file`
- `ingest_batch`
- `row_num_in_file`（可选）

这些列只服务于追溯、审计、分区和重跑，不承载研究语义。

## 允许做的事

- CSV/ZIP 解包与读取
- 列名标准化
- 类型转换
- 时间戳统一格式化
- 空值标准化
- 保留原始记录粒度下的逐行投影
- 明显坏行隔离
- 分区落盘
- 增加最少量技术追溯列
- manifest 与日志生成

## 不允许做的事

- 提前删掉仍可能有审计价值的原始字段
- 改变记录粒度，例如聚合、汇总、预先 linkage
- 把字段业务重命名成已解释语义
- 推断 `TradeDir` 的业务语义
- 把 `TradeDir` 改名成 `aggressor_side`
- 推断 `BrokerNo` 的角色语义
- 把 `Level` 改名成 `book_depth_level`
- 把 `Level`、`VolumePre` 解释成已验证含义
- 在 cleaning 阶段补 side / queue / lifecycle
- 提前做研究特征、alpha 因子、事件窗衍生列

## 当前产物命名

- 当前只允许输出到 `candidate_cleaned`
- `candidate_cleaned = stage parquet layer`
- 未经 DQA 与 semantic verification，不得升级为 `verified`

## Stage 最小标准化

对于 trades / orders 这类 raw 表，stage 层只做：

- 文本 CSV 到 typed parquet 的逐行投影
- schema 固定
- 类型稳定
- 空值规则统一
- 时间字段标准化
- 分区规则固定

不做：

- aggressor 推断
- book depth 推断
- queue 推断
- linkage 衍生
- research feature engineering

## 当前默认分区思路

- 先按 `table + date` 组织
- 不预设 `date + symbol` 细粒度切分
- 只有在单日文件体量和扫描模式明确证明必要时，才增加有限 shard

## 当前待定

- 2025 stage parquet / candidate_cleaned 的最终 schema
- partition 粒度
- candidate key 口径
- golden sample 清单
