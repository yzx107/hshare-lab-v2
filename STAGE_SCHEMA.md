# STAGE_SCHEMA

> Trades / Orders 的 stage parquet 合同草案

## 目标

本文件定义 v2 第一版 stage cleaning 合同：

- 只做工程标准化
- 保持原始记录粒度
- 只保留 `Trades` 与 `Orders` 两张逻辑表
- 不做任何语义增强

工作定义：

`Stage layer = loss-minimizing typed projection of raw CSV into partitioned parquet, without semantic enrichment.`

## Vendor References

The following upstream reference files support source-contract interpretation only:

- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/raw_vendor_notice_2026-01-01.txt`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/vendor/ReadMe.txt`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/normalized/ReadMe.utf8.txt`

These files justify the raw layout split between:

- `2025 -> OrderAdd / OrderModifyDelete / TradeResumes`
- `2026 -> order / trade`

They also document vendor-side field definitions, but they do **not** by themselves upgrade any field into
research-verified semantics.

## 原始表映射

### 2025

- `20250218/OrderAdd/*.csv` -> `Orders`
- `20250218/OrderModifyDelete/*.csv` -> `Orders`
- `20250218/TradeResumes/*.csv` -> `Trades`

### 2026

- `order/*.csv` -> `Orders`
- `trade/*.csv` -> `Trades`

## 分区与产物

- stage 根路径：`/Volumes/Data/港股Tick数据/candidate_cleaned`
- 分区策略：`table/date`
- 输出示例：
  - `candidate_cleaned/orders/date=2025-02-18/20250218_orders.parquet`
  - `candidate_cleaned/trades/date=2026-01-05/20260105_trades.parquet`

## Schema Spec v1

`stage parquet / candidate_cleaned_v1` 的 schema spec 现阶段固定为：

- 逻辑表仅保留 `orders` 与 `trades`
- 列集合以 [Scripts/stage_contract.py](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Scripts/stage_contract.py) 为唯一代码级 contract
- 同一年份内，同一逻辑表的 stage 列集合必须稳定
- 年份差异通过允许为空列处理，不通过拆出不同逻辑表处理
- stage 层允许保留 vendor-defined 但未验证语义的字段，只要不在清洗阶段改名或增强解释

因此，当前 repo 口径是：

- `schema spec = fixed at table-level across 2025/2026`
- `year-specific source differences = handled by nullable columns`

## Partition Spec v1

当前 partition spec 正式固定为：

- 分区键：`table_name + date`
- 路径形态：
  - `candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet`
  - `candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet`
- 当前不按 `symbol`、`source_group`、`session` 进一步下钻分区

原因：

- `date + table` 已足够支撑当前 DQA、semantic sample、Query 抽样和可恢复重跑
- 过早引入更细分区会把 source-group / semantic 不确定性提前固化进物理布局
- `symbol` 或其它更细分 shard 只有在明确证明扫描模式需要时才应追加

## Candidate Key Spec v1

当前只固定 conservative candidate key，不把它们升级成 verified business key。

### Orders

- project-level candidate key: `date + source_file + row_num_in_file`
- structural identity candidate: `date + SeqNum + OrderId`

说明：

- 前者用于追溯与工程级唯一性
- 后者仅用于结构分析与弱一致性检查
- 不能直接宣称 `date + SeqNum + OrderId` 是官方业务主键

### Trades

- project-level candidate key: `date + source_file + row_num_in_file`
- structural identity candidate: `date + TickID`

说明：

- `date + TickID` 可作为高价值结构标识候选
- 但当前仍不写成官方 `TradeID` 主键映射已确认

## Key Boundary

stage candidate key 的用途是：

- partition-level dedup / sanity check
- structural linkage / DQA 辅助
- project-level reproducibility

不允许把 stage candidate key 直接升级成：

- verified business identity
- official native primary key confirmation

## 执行规则

- 任务粒度固定为 `date + logical table`
- checkpoint 粒度固定为 `date + logical table`
- 允许多进程并行不同 task
- 每个 task 内按 source member 顺序增量写 parquet
- 写入前先落到 `.tmp`，完成后再原子替换正式文件
- manifest、heartbeat、checkpoint 必须同步更新

## 共享技术列

| Column | Type | Required | 说明 |
|---|---|---:|---|
| `date` | `date32` | yes | 分区日期 |
| `table_name` | `string` | yes | 逻辑表名，`orders` 或 `trades` |
| `source_file` | `string` | yes | zip 内标准化路径 |
| `ingest_ts` | `timestamp[us, UTC]` | yes | 本次 stage task 的写入时间 |
| `row_num_in_file` | `int64` | yes | 源 CSV member 内数据行号，从 1 开始；不含 header，坏行剔除前即固定 |

## Trades Stage Schema

| Column | Type | Required For Stage Admission | 说明 |
|---|---|---:|---|
| `SendTimeRaw` | `string` | no | 原始 `SendTime` 字面值保留列，便于回溯与 sanity check |
| `SendTime` | `timestamp[ns, UTC]` | no | 仅 2026 原始存在；2025 允许为空 |
| `SeqNum` | `int64` | no | 仅 2026 原始存在；2025 允许为空 |
| `TickID` | `int64` | yes | trade identity 原始字段，保留原名 |
| `Time` | `string` | yes | 标准化为零填充 `HHMMSS` 字符串 |
| `Price` | `float64` | yes | 只做类型标准化 |
| `Volume` | `int64` | yes | 只做类型标准化 |
| `Dir` | `int8` | no | 保留原始字段名，不重命名为解释性字段 |
| `Type` | `string` | no | 保留原始字面值 |
| `BrokerNo` | `string` | no | 保留前导零；可与 broker reference join，但不默认放行研究语义 |
| `BidOrderID` | `int64` | no | 不提前做 linkage |
| `BidVolume` | `int64` | no | 保留原始字段 |
| `AskOrderID` | `int64` | no | 不提前做 linkage |
| `AskVolume` | `int64` | no | 保留原始字段 |

## Orders Stage Schema

| Column | Type | Required For Stage Admission | 说明 |
|---|---|---:|---|
| `Channel` | `int32` | no | 仅 2026 原始存在；2025 允许为空 |
| `SendTimeRaw` | `string` | no | 原始 `SendTime` 字面值保留列，便于回溯与 sanity check |
| `SendTime` | `timestamp[ns, UTC]` | no | 仅 2026 原始存在；2025 允许为空 |
| `SeqNum` | `int64` | yes | 订单事件原始序号 |
| `OrderId` | `int64` | yes | 保留原始字段名 |
| `OrderType` | `int16` | yes | 保留原始枚举；vendor 定义已知，但未完成研究验证 |
| `Ext` | `string` | no | 保留原始字面值；vendor 定义已知，但未完成研究验证 |
| `Time` | `string` | yes | 标准化为零填充 `HHMMSS` 字符串 |
| `Price` | `float64` | yes | 只做类型标准化 |
| `Volume` | `int64` | yes | 只做类型标准化 |
| `Level` | `int32` | no | vendor 定义已知，但不解释为已验证盘口深度 |
| `BrokerNo` | `string` | no | 保留前导零；可与 broker reference join，但不默认放行研究语义 |
| `VolumePre` | `int64` | no | vendor 定义已知，但不解释为已验证 queue ahead |

## 显式允许的标准化

- 按字符串读入，再显式 cast，避免 schema 漂移和前导零丢失
- `Time` 标准化为零填充字符串，不强行合成研究语义时间戳
- 若原始存在 `SendTime`，则同时保留 `SendTimeRaw`
- `SendTime` 目前按 vendor 提供的 epoch nanoseconds 解析为 `timestamp[ns, UTC]`，sample run 必须做 sanity check
- 空值统一处理：`""`、`" "`、`"NULL"`、`"null"`、`"nan"`、`"NaN"` -> `null`
- `BrokerNo` 可在 query / DQA 层 join broker reference，但 stage 本身不内嵌名称映射
- 极明确坏行隔离：stage admission 必需列无法解析时，行不写入 stage parquet，并在 manifest 记录

## 明确禁止

- 把 `Dir` 改名成 `aggressor_side`
- 把 `OrderType` 当成已验证研究语义，即使 vendor 文档给了原始枚举说明
- 把 `Dir` 当成已验证 aggressor-side，即使 vendor 文档给了方向说明
- 把 `Level` 解释成已验证盘口深度
- 把 `VolumePre` 解释成已验证 queue ahead
- 回填 `BidOrderID/AskOrderID -> OrderId` linkage 结果
- 生成任何研究因子或 alpha 特征

## 每分区最少 manifest 字段

- `year`
- `date`
- `table_name`
- `status`
- `row_count`
- `source_member_count`
- `failed_member_count`
- `rejected_row_count`
- `raw_row_count`
- `rejection_reason_counts`
- `output_file`
- `output_bytes`
- `min_send_time`
- `max_send_time`
- `min_time`
- `max_time`
