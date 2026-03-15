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
- `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References/vendor/CFBC_File_Specification_wef_20250630.pdf`

These references support:

- broker / participant lookup coverage
- securities reference / file-layout interpretation
- source-contract drift interpretation
- vendor-definition awareness
- OMD-C family / FullTick-compatible provenance wording

They do **not** automatically upgrade a field into semantic truth.
In particular, they do **not** prove that the current vendor CSV is a `1:1` official HKEX message dump.

## 执行要求

- 长任务必须 `visible + resumable`
- 至少输出：阶段日志、heartbeat、progress count、blockage hint、checkpoint
- 所有报告要可复现、可回溯
- 使用 broker / vendor reference 时，报告中应显式标注 reference source
- 对 vendor-documented fields，DQA 允许输出 `vendor-defined` 结论，但不得直接输出 `research-verified`
- 对 `BidOrderID / AskOrderID -> OrderId`，DQA 允许输出 linkage feasibility / consistency，不得直接输出 official native field confirmation
- 对 `BrokerNo` 与 broker reference 的映射，DQA 允许输出 mapping coverage / ambiguity，不得直接输出 official identity proof
- 对 `Dir`、`Type`、`Level`、`VolumePre`、`Ext`、`OrderType`，DQA 允许输出枚举稳定性与 cross-field consistency，不得直接输出官方语义放行

## Field-Level DQA Boundary

### A. Official-Family-Compatible Fields

Current examples:

- orders: `OrderId`, `Price`, `Volume`
- trades: `Price`, `Volume`

DQA may report:

- non-null rate
- value range / invalid value rate
- distribution drift
- same-key consistency
- cross-table compatibility

DQA must not report:

- current vendor header has been confirmed as a `1:1` official HKEX native field mapping

### B. Vendor-Defined Fields

Current examples:

- orders: `SeqNum`, `OrderType`, `Ext`, `Time`, `Level`, `BrokerNo`, `VolumePre`
- trades: `Time`, `Dir`, `Type`, `BrokerNo`, `TickID`, `BidOrderID`, `BidVolume`, `AskOrderID`, `AskVolume`

DQA may report:

- coverage
- null pattern
- enum stability
- conditional presence
- reference join coverage
- linkage feasibility
- cross-field consistency

DQA must not report:

- official field identity confirmed
- business semantics confirmed
- verified research-safe truth

### C. Sensitive Unverified Semantics

The following fields require extra caution in DQA wording:

- `OrderType`
- `Ext`
- `Dir`
- `Type`
- `Level`
- `BrokerNo`
- `VolumePre`
- `BidOrderID`
- `BidVolume`
- `AskOrderID`
- `AskVolume`

For these fields, DQA wording should prefer:

- `stable`
- `observed`
- `vendor-defined`
- `consistent with`
- `compatible with`
- `candidate`

For these fields, DQA wording should avoid:

- `confirmed`
- `officially mapped`
- `proven semantic`
- `verified business meaning`
- `safe for direct alpha use`
