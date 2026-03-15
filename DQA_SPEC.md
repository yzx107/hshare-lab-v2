# DQA_SPEC

## 目标

DQA 要回答两件事：

1. 数据结构是否可靠
2. 数据在研究上能否安全使用

## 模块

### A. Ingestion Completeness
- raw 文件数 vs cleaned 文件数
- 日期覆盖
- 空分区
- 异常小文件
- 坏文件与重试

### B. Schema Integrity
- schema drift
- 类型漂移
- 列缺失
- nullable 异常

### C. Row-Level Validity
- trades: `Price`、`Volume`、`Turnover`、`SeqNum`、`TickID`、`SendTime`
- orders: `Price`、`Volume`、`OrderId`、`OrderType`、`Level`、`VolumePre`

### D. Sequence and Time Integrity
- `SeqNum` 单调性
- `SendTime` 逆序率
- `SeqNum` vs `SendTime` 冲突
- 跨表时间范围一致性

### E. Session Quality
- session 值清单
- session by time histogram
- session 覆盖差异
- session 下字段分布差异

### F. Cross-Table Feasibility
- `BidOrderID / AskOrderID -> OrderId`
- 匹配率
- lag 分布
- unmatched rate
- broker consistency

### G. Broker Map Quality
- mapping coverage
- unique mapping
- matched pair consistency

## Reference Inputs

DQA may use the following read-only reference inputs:

- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/normalized/brokerno.utf8.csv`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/normalized/List_of_Current_SEHK_EP.utf8.tsv`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/normalized/ReadMe.utf8.txt`
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/raw_vendor_notice_2026-01-01.txt`

These references support:

- broker / participant lookup coverage
- source-contract drift interpretation
- vendor-definition awareness

They do **not** automatically upgrade a field into semantic truth.

## 执行要求

- 长任务必须 `visible + resumable`
- 至少输出：阶段日志、heartbeat、progress count、blockage hint、checkpoint
- 所有报告要可复现、可回溯
- 使用 broker / vendor reference 时，报告中应显式标注 reference source
- 对 vendor-documented fields，DQA 允许输出 `vendor-defined` 结论，但不得直接输出 `research-verified`
