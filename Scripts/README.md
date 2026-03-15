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
- `run_dqa_linkage.py`：将 `BidOrderID / AskOrderID -> OrderId` 的 ID-level equality 与 time-usable linkage 分开审计
- `run_source_group_inventory.py`：指定 raw source group label 专项 inventory，例如仓库中观测到的 `2025 HKDarkPool`
- `run_semantic_idspace.py`：在 representative sample 上探测 old-format ID equality 与时间锚可用性
- `run_semantic_time_anchor.py`：在 `SendTime` 缺失时，评估 `Time` 是否仍能支撑 coarse temporal validation
- `semantic_contract.py`：semantic verification 的统一状态、输出 contract 与 admissibility 映射
- `run_semantic_lifecycle.py`：`OrderId lifecycle` probe
- `run_semantic_tradedir.py`：`TradeDir` probe
- `run_semantic_tradedir_contrast.py`：`Dir=1/2` contrast probe，检验 linkage / tick-rule / time-bucket 差异
- `run_semantic_ordertype.py`：`OrderType` probe
- `run_semantic_session.py`：`Session` probe
- `semantic_report.py`：聚合多个 semantic probe，并生成 admissibility bridge
- `run_semantic_framework.py`：搭建 `OrderId lifecycle`、`TradeDir / OrderType / Session` 骨架，并输出 semantic report / admissibility hooks
- `build_verified_layer.py`：按 verified admission policy materialize conservative research-ready tables；当前仍以 design / plan 为主，不默认放行高风险语义字段

## 当前研究边界

- `2026`：可进入 `linkage semantic verification`
- `2025`：针对 `HKDarkPool` raw source group label 的 inventory 已完成；当前只确认其为仓库观测到的独立 trade-like source group label，非官方术语已验证口径；ID-level linkage 已成立
- `2025`：`SendTime` 仍缺失，但 `Time` 在 `7` 个样本日、`50,521,238` 条 matched edges 上均呈现 `coarse_time_anchor_status = weak_pass`
- `2025`：`research_time_grade = coarse_only`
- `2026`：`research_time_grade = fine_ok`
- linkage 相关研究从现在开始拆年，不把 `2025/2026` 混成同一 linkage 范式
- `2026` 表内排序默认 `SeqNum` 优先，`SendTime` 用于时间窗与 lag 分析，不替代主排序锚

## 推荐调用

- `python -m Scripts.build_raw_inventory --year 2025`
- `python -m Scripts.build_stage_parquet --year 2025 --max-days 3`
- `make raw-inventory-2025`
- `make stage-sample-2025`
- `python -m Scripts.run_dqa_coverage --print-plan`
- `python -m Scripts.run_dqa_schema --print-plan`
- `python -m Scripts.run_dqa_linkage --print-plan`
- `python -m Scripts.run_source_group_inventory --print-plan`
- `python -m Scripts.run_semantic_idspace --print-plan`
- `python -m Scripts.run_semantic_time_anchor --print-plan`
- `python -m Scripts.run_semantic_lifecycle --print-plan`
- `python -m Scripts.run_semantic_tradedir --print-plan`
- `python -m Scripts.run_semantic_tradedir_contrast --print-plan`
- `python -m Scripts.run_semantic_ordertype --print-plan`
- `python -m Scripts.run_semantic_session --print-plan`
- `python -m Scripts.semantic_report --print-plan`
- `python -m Scripts.run_semantic_framework --print-plan`
- `python -m Scripts.run_source_group_inventory --year 2025 --group HKDarkPool`
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

## run_dqa_linkage 输出

- `dqa/linkage/year=<year>/audit_linkage_feasibility_daily.parquet`
- `dqa/linkage/year=<year>/checkpoint.json`
- `dqa/linkage/year=<year>/heartbeat.json`
- `dqa/linkage/year=<year>/summary.json`
- `Research/Audits/dqa_linkage_<year>.md`

## run_source_group_inventory 输出

- `dqa/source_inventory/year=<year>/group=<group>/audit_source_member_inventory.parquet`
- `dqa/source_inventory/year=<year>/group=<group>/audit_source_daily_summary.parquet`
- `dqa/source_inventory/year=<year>/group=<group>/audit_source_schema_fingerprints.parquet`
- `dqa/source_inventory/year=<year>/group=<group>/checkpoint.json`
- `dqa/source_inventory/year=<year>/group=<group>/heartbeat.json`
- `dqa/source_inventory/year=<year>/group=<group>/summary.json`
- `Research/Audits/source_inventory_<group>_<year>.md`

## run_semantic_framework 输出

- compatibility wrapper only
- delegates to `run_semantic_lifecycle.py`, `run_semantic_tradedir.py`, `run_semantic_ordertype.py`, `run_semantic_session.py`
- final outputs land in `dqa/semantic/year=<year>/...`
- final markdown lands in `Research/Audits/semantic_<year>_summary.md`

## semantic 2026 framework 输出

- `dqa/semantic/year=<year>/semantic_orderid_lifecycle_daily.parquet`
- `dqa/semantic/year=<year>/semantic_tradedir_daily.parquet`
- `dqa/semantic/year=<year>/semantic_ordertype_daily.parquet`
- `dqa/semantic/year=<year>/semantic_session_daily.parquet`
- `dqa/semantic/year=<year>/semantic_daily_summary.parquet`
- `dqa/semantic/year=<year>/semantic_yearly_summary.parquet`
- `dqa/semantic/year=<year>/semantic_admissibility_bridge.parquet`
- `Research/Audits/semantic_<year>_summary.md`

旧的 lowercase `scripts/` 目录视为 legacy，不再作为新主线入口。
