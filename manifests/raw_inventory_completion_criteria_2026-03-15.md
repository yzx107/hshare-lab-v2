# Raw Inventory Completion Criteria 2026-03-15

## Scope

本说明定义 raw inventory 在当前项目里什么叫“完成到可用”，以及它和 full-year DQA、golden sample 的关系。

它不要求现在立刻执行真实全年扫描，只冻结完成标准。

## Purpose

raw inventory 的目的不是做研究结论，而是给 raw layer 一个可追溯、可恢复、可观测的全貌清单。

## Minimum Completion Standard

对于某个年份，raw inventory 要达到“可用完成”，至少应具备：

- `files.jsonl`
- `files.parquet`
- `date_summary.json`
- `date_summary.parquet`
- `checkpoint.json`
- `heartbeat.json`
- `summary.json`
- 对应运行日志

## Minimum Summary Questions

`summary.json` 至少应回答：

- 扫了多少文件
- 扫了多少字节
- 覆盖起止日期
- 覆盖了多少个交易日
- 有多少零字节文件
- 有多少无法解析日期的文件
- 后缀类型分布是什么

## What Raw Inventory Can Support

- raw layer completeness baseline
- date coverage baseline
- file-shape anomaly spotting
- follow-up source-group inventory planning
- golden sample selection support

## What Raw Inventory Cannot Support

- field semantic verification
- linkage quality judgment
- verified layer promotion
- research admissibility on its own

## Completion Grades

### `cli_ready`

- 脚本存在
- 支持 checkpoint / heartbeat / resume
- 本地最小样例可运行

### `year_scanned`

- 某个年份真实 raw 目录已完成 inventory
- 产物齐全
- summary 可读

### `inventory_closed`

- `2025` 与 `2026` 均完成 `year_scanned`
- 异常项已至少形成初步说明
- golden sample 选择可参考 inventory 结果

## Recommended Rule

> Raw inventory is complete enough when it can reliably answer “what exists in raw” for a year without touching field semantics.
