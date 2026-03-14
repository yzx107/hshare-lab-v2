# CHANGELOG

> Hshare Lab v2 reboot 留痕

---

## [Source-Inventory-v1] 2026-03-14 — 落地 HKDarkPool 专项 inventory CLI 并跑通全年 2025（Codex）

### 变更概述
- 新增 `run_source_group_inventory.py`，用于在扩主 contract 之前，对指定 raw source group 做可见、可恢复的专项 inventory
- 新增 `test_run_source_group_inventory.py`，覆盖合成 zip 上的 member inventory、daily summary、schema fingerprint 落盘
- 对真实 `2025` raw 全年执行 `HKDarkPool` inventory，输出至 `/tmp/hshare_hkdarkpool_inventory`
- 确认 `HKDarkPool` 并非 `2025-12-04` 单日异常，而是在 `246` 个交易日中命中 `44` 天、共 `142` 个 member、`935,527` 行
- 确认 `HKDarkPool` 当前呈现为稳定独立的 `7` 列 trade-like schema：`time, price, share, turnover, side, type, brokerno`

### 影响
- `HKDarkPool` 现在可按事实层 inventory 单独研究，不需要先并入主 `Orders / Trades` contract
- `2025` 的下一步从“先确认是否单日异常”推进为“old-format ID space investigation + 是否需要独立 verified 分支”

## [Stage-Sample-v1] 2026-03-14 — 完成真实 raw 单日 stage sample（Codex）

### 变更概述
- 对真实 raw `2025-02-18` 与 `2026-03-13` 跑通 `build_stage_parquet.py`
- 核对实际 parquet schema、`partitions.jsonl`、`source_groups.jsonl`、`failures/unmapped` 留痕
- 增加 `SendTimeRaw` 保留列，并验证 `SendTimeRaw -> SendTime` 与 `Time` 的样本对照
- 新增 `unmapped source members` 与 `rejection_reason_counts` 留痕能力

### 影响
- stage pipeline 已从“可执行”进入“经真实 raw 样本验证”
- 当前主要剩余风险从 schema/mapping 转向长任务 heartbeat 粒度与全量性能

## [Stage-Perf-v1] 2026-03-14 — 优化 zip 读取与同日双表扫描路径（Codex）

### 变更概述
- `build_stage_parquet.py` 改为通过 `ZipFile.open(...)` 直接读取 CSV member stream，不再先 `zf.read()` 整块复制到 Python bytes
- 保持外部 `date + table` task 语义不变，但内部将同一交易日的 `orders/trades` 合并为单次 zip 扫描、按 source group 分流写 parquet
- 新增 bundle 级测试，覆盖同日 `orders + trades` 一次处理的输出与兼容性

### 影响
- 降低单个 zip member 的额外内存拷贝开销
- 避免同日 `orders/trades` 各自重复扫描和重复解压同一个 raw zip

## [Stage-Observability-v1] 2026-03-14 — 补 bundle 级 progress/heartbeat（Codex）

### 变更概述
- `build_stage_parquet.py` 新增 `bundle_progress/*.json`，worker 会在 member 处理中持续刷新当前 source file、已处理 member 数以及两张表的中间行数
- 顶层 `heartbeat.json` 现在会聚合 `active_bundles`，不再只在整张表完成后跳一次
- `Scripts/runtime.write_json` 改为原子替换，避免主进程读取 progress 时撞上半截 JSON

### 影响
- 长任务现在具备 member-level 可见性，能直接看出当前卡在哪个 raw member
- progress 读写在并发场景下更稳，适合后续全量 stage 与 DQA 复用

## [DQA-Foundation-v1] 2026-03-14 — 落地 coverage/schema 两条可执行审计入口（Codex）

### 变更概述
- `run_dqa_coverage.py` 从 scaffold 变为正式 CLI，基于 stage manifests materialize `audit_stage_partitions`、`audit_stage_row_reconciliation`、`audit_stage_source_groups`、`audit_stage_failures`
- `run_dqa_schema.py` 从 scaffold 变为正式 CLI，基于 stage parquet materialize `audit_schema_fingerprint`、`audit_field_nulls`、`audit_field_value_rules`、`audit_time_profile`
- 两条 DQA 入口都补齐 `checkpoint`、`heartbeat`、`summary` 与 `Research/Audits/*.md` 报告输出
- 新增覆盖/模式审计测试，确保合成样本上可直接落盘并完成断言

### 影响
- v2 主线已从“只有 stage”推进到“stage + first-pass DQA foundation”
- 后续可以在不改 stage contract 的前提下，直接扩展 representative sample、全量跑数与更深的 research-oriented audit

## [DQA-Linkage-v1] 2026-03-14 — 落地轻量版 trade-order linkage feasibility CLI（Codex）

### 变更概述
- `run_dqa_linkage.py` 从 scaffold 变为正式 CLI，先回答 `BidOrderID / AskOrderID` 能否同日连到 `Orders.OrderId`
- 第一版只输出日级 `match rate / both-side match / negative lag rate`，不提前做 lifecycle、broker-side 或语义推断
- 新增合成样本测试，确保 linkage 结果能落盘为 `audit_linkage_feasibility_daily`

### 影响
- 高优先 audit 表中的第 5 张已经具备正式入口，可以直接接 representative sample 与真实 stage 产物
- 后续可以在不改 stage 层的前提下，逐步加深 linkage 诊断，而不是把 linkage 逻辑混回 cleaning

## [Representative-Sample-v1] 2026-03-14 — 跑通 2025/2026 representative sample 并明确拆年结论（Codex）

### 变更概述
- 对 `2025-01-02 / 2025-06-12 / 2025-12-04` 与 `2026-01-05 / 2026-02-24 / 2026-03-13` 跑通 representative sample stage
- 对上述 sample 同步跑完 `coverage / schema / linkage` 三条高优先 DQA
- 明确 `2025-12-04` 出现未映射 source group `HKDarkPool`
- 明确 `2026` direct linkage 三天全 `pass`，而 `2025` direct linkage 三天全 `fail`

### 影响
- `stage` 层已可放行到全量，不需要回头重写 stage 架构
- linkage 相关研究从现在开始按年份拆开推进
- `2026` 进入 linkage semantic verification；`2025` 先做 `HKDarkPool inventory + old-format ID space investigation`

## [Stage-Pipeline-v1] 2026-03-14 — 新增真实 stage cleaning 入口（Codex）

### 变更概述
- 新增 `STAGE_SCHEMA.md`，明确 2025/2026 raw source mapping 以及 Trades / Orders 的 stage schema
- 新增 `Scripts/build_stage_parquet.py`，支持 `date + table` task、checkpoint、heartbeat、分区 manifest、多进程并行
- 新增 `Scripts/stage_contract.py`，把 stage schema、技术列和 required-for-stage 规则集中定义
- 新增 stage 相关 `Makefile` 入口与端到端测试

### 影响
- v2 已经可以从 raw zip 真正进入 stage parquet，而不再只是停留在 contract 讨论
- stage cleaning 的边界、坏行处理、并行与断点续跑规则都有了代码级落点

## [Stage-Definition-v1] 2026-03-14 — 明确 stage parquet 的最小保守标准化口径（Codex）

### 变更概述
- 明确 `candidate_cleaned` 就是当前的 `stage parquet` 层
- 固定 stage 层定义为 “loss-minimizing typed projection of raw CSV into partitioned parquet, without semantic enrichment”
- 明确 stage 层保持原始记录粒度、尽量保留原始字段，只允许技术列与工程标准化
- 明确禁止字段语义重命名、aggressor 推断、book/queue 推断、linkage 衍生和研究特征

### 影响
- `candidate_cleaned` 不再容易被误解为研究层或语义已验证层
- 后续 Trades / Orders schema 规范可以直接围绕 stage contract 落地

## [Engineering-Scaffold-v1] 2026-03-14 — 固化 v2 主栈与脚本职责（Codex）

### 变更概述
- 新增 `pyproject.toml`，固定 `DuckDB + Polars + PyArrow` 主栈以及 `pytest + ruff` 轻量工程化约束
- 新增 `Makefile`，提供 raw inventory 与后续 DQA / verified 层的轻编排入口
- 将 `Scripts/` 从纯文档目录升级为 Python CLI 入口，并新增 `build_raw_inventory.py`
- 为 `candidate cleaned`、DQA、verified layer 建立占位脚本，明确职责边界与后续接线点

### 影响
- v2 不再只有“文档主线”，而是有了可以继续扩展的工程骨架
- raw inventory 成为第一个正式落地的可执行 pipeline 入口

## [Session-Handoff-v1] 2026-03-14 — 固化 v2 会话接续入口（Codex）

### 变更概述
- 明确 canonical repo 为 `/Users/yxin/AI_Workstation/Hshare_Lab_v2`
- 明确 `/Users/yxin/AI_Workstation/Hshare_Lab` 仅作为 `legacy evidence`
- 补充 GitHub 仓库地址与新会话入口文件
- 统一记录当前最关心的下一步为 `raw inventory`
- 修正 `README.md` 与 `DATA_CONTRACT.md` 中残留的旧仓库绝对路径

### 影响
- 后续新会话可以直接从 v2 仓库恢复，不会误入 legacy repo
- `raw inventory` 被固定为当前最优先的执行焦点

## [Reboot-v1] 2026-03-14 — 切换到 Hshare Lab v2 主线（Codex）

### 变更概述
- 顶层文档全部切换到 reboot 口径。
- 新建 `DATA_CONTRACT.md`、`CLEANING_SPEC.md`、`DQA_SPEC.md`、`SEMANTIC_MATRIX.md`、`LEGACY_STATUS.md`。
- 新主线明确为：`raw -> candidate cleaned -> DQA -> semantic verification -> verified layer`。

### 影响
- 旧版清洗、DQA、feature、查询逻辑不再视为事实源。
- 旧目录 `scripts/` 与旧 `src/features/` 保留为 `legacy evidence`，待后续按需清退或重写。

## [Data-Reset-v1] 2026-03-14 — 删除旧 cleaned/temp 数据层，仅保留 raw（Codex）

### 变更概述
- 已发起删除旧的 `/Volumes/Data/港股Tick数据/clean_parquet/`
- 已发起删除旧的 `/Volumes/Data/港股Tick数据/.tmp_parquet/`
- 新的 `/Volumes/Data/港股Tick数据/{candidate_cleaned,dqa,verified,manifests,logs}/` 将在旧目录清理完成后建立

### 影响
- 旧 cleaned 数据、旧 DQA temp、旧 DuckDB temp 正在退出新主线。
- 新主线从 raw inventory 和 contract definition 重新开始。
