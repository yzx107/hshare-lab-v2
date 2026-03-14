# Scripts

本目录是 Hshare Lab v2 的新脚本入口。

## 主栈分工

- `Parquet`：唯一工作格式；`CSV` 只留在 raw evidence
- `DuckDB`：主查询、主审计、主 linkage 检查
- `Polars`：主 mechanical ETL、主特征工程
- `PyArrow`：schema、dataset、metadata、manifest

## Stage Layer 工作定义

- `candidate_cleaned` 就是当前的 `stage parquet` 层
- 一条 raw CSV 记录对应一条 stage parquet 记录
- 目标不是“原样无脑复制”，也不是“提前研究加工”，而是最小保守标准化
- 允许类型、时间、空值、schema、分区、追溯列整理
- 不允许语义增强、业务重命名、linkage 衍生、研究特征

## 规则

- 新主线脚本只放在本目录，并通过 `python -m Scripts.<name>` 调用
- 所有长任务必须 `visible + resumable`
- 默认输出日志到外置盘 `logs/`
- 默认输出 manifest 到外置盘 `manifests/`
- notebook 只做探索，不作为正式 pipeline 事实源

## 当前入口

- `build_raw_inventory.py`：首个正式 CLI，负责 raw layer inventory、checkpoint、heartbeat、manifest
- `build_stage_parquet.py`：真实 stage cleaning 入口，对外保持 `date + table` task，可见/可恢复；实现上会把同日 `orders/trades` 合并为单次 zip 扫描并直读 member stream
- `freeze_candidate_cleaned.py`：冻结 `stage parquet / candidate_cleaned_2025_v1` contract 与落盘流程
- `run_dqa_coverage.py`：raw vs candidate_cleaned 覆盖校验
- `run_dqa_schema.py`：schema drift / type stability / nullability 校验
- `run_dqa_linkage.py`：`BidOrderID / AskOrderID -> OrderId` linkage feasibility
- `build_verified_layer.py`：verified tables materialization

## 推荐调用

- `python -m Scripts.build_raw_inventory --year 2025`
- `python -m Scripts.build_stage_parquet --year 2025 --max-days 3`
- `make raw-inventory-2025`
- `make stage-sample-2025`
- `python -m Scripts.run_dqa_coverage --print-plan`
- `python -m Scripts.run_dqa_schema --print-plan`
- `python -m Scripts.run_dqa_linkage --print-plan`
- `python -m Scripts.build_verified_layer --print-plan`

## build_raw_inventory 输出

- `files.jsonl`：文件级 manifest 流
- `checkpoint.json`：可恢复状态
- `heartbeat.json`：进度心跳
- `date_summary.json`：按日期聚合摘要
- `summary.json`：总览摘要
- `files.parquet` 与 `date_summary.parquet`：若环境已安装 `PyArrow`，则自动 materialize

## build_stage_parquet 输出

- `candidate_cleaned/{orders,trades}/date=YYYY-MM-DD/*.parquet`
- `manifests/stage_parquet_<year>/checkpoint.json`
- `manifests/stage_parquet_<year>/heartbeat.json`
- `manifests/stage_parquet_<year>/bundle_progress/*.json`
- `manifests/stage_parquet_<year>/partitions.jsonl`
- `manifests/stage_parquet_<year>/failures.jsonl`
- `manifests/stage_parquet_<year>/summary.json`

## run_dqa_coverage 输出

- `dqa/coverage/year=<year>/audit_stage_partitions.parquet`
- `dqa/coverage/year=<year>/audit_stage_row_reconciliation.parquet`
- `dqa/coverage/year=<year>/audit_stage_source_groups.parquet`
- `dqa/coverage/year=<year>/audit_stage_failures.parquet`
- `dqa/coverage/year=<year>/checkpoint.json`
- `dqa/coverage/year=<year>/heartbeat.json`
- `dqa/coverage/year=<year>/summary.json`
- `Research/Audits/dqa_coverage_<year>.md`

## run_dqa_schema 输出

- `dqa/schema/year=<year>/audit_schema_fingerprint.parquet`
- `dqa/schema/year=<year>/audit_field_nulls.parquet`
- `dqa/schema/year=<year>/audit_field_value_rules.parquet`
- `dqa/schema/year=<year>/audit_time_profile.parquet`
- `dqa/schema/year=<year>/checkpoint.json`
- `dqa/schema/year=<year>/heartbeat.json`
- `dqa/schema/year=<year>/summary.json`
- `Research/Audits/dqa_schema_<year>.md`

旧的 lowercase `scripts/` 目录视为 legacy，不再作为新主线入口。
