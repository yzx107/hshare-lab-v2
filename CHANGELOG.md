# CHANGELOG

> Hshare Lab v2 reboot 留痕

---

## [Docs-Raw-Inventory-And-Policy-Closeout-v1] 2026-03-15 — 拉平 raw inventory、reference、golden sample 与 verified 准入状态（Codex）

### 变更概述
- 完成 `2025` 与 `2026` full-year raw inventory，并落盘到外置盘 manifest 目录
- 补充 `2025/2026` raw inventory notes 与总览页，明确 support files、跨年 notice 文件与 `inventory_closed` 项目口径
- 新增 verified admission boundary / policy、reference usage boundary / policy、golden sample policy、raw inventory completion criteria
- 收紧 `HKDarkPool` 口径：统一表述为仓库在 raw 中观测到的 source group label，而非当前官方/vendor reference 已直接确认的正式术语
- 同步更新 `README.md`、`PROGRESS.md`、`TASKS.md`、`QUERY_CONTRACT.md`、`DATA_CONTRACT.md`、`Research/References/README.md`

### 影响
- `raw inventory` 不再是未完成基础项，而是已完成并可引用的 baseline
- `verified`、`reference`、`golden sample` 三条规则线已从临时分析升级为 repo 内正式 policy
- 后续主线更明确收缩为：冻结 `golden sample` 具体日期清单、推进 full-year DQA、继续 semantic boundary 与 verified 实装

## [Semantic-Lifecycle-Hardening-v1] 2026-03-15 — 加固 lifecycle probe 的真实数据路径与可观测性（Codex）

### 变更概述
- 更新 `run_semantic_lifecycle.py`，在真实数据路径上补齐输入体量日志、字段日志与 compact scalar summary
- 将 lifecycle probe 的主统计从高物化中间结果收口为更轻的 scalar summary 聚合，降低 representative / real-data smoke 的运行压力
- 更新 `test_run_semantic_lifecycle.py`，把测试工作目录切到当前 workspace 根目录

### 影响
- lifecycle probe 在真实数据上更稳、更可看，也更适合后续 semantic deepening
- 当前提交不新增 lifecycle 的强语义结论，只做运行时稳定性与可观测性收口

## [Semantic-OrderType-v1] 2026-03-15 — 固化 OrderType representative evidence 与研究边界（Codex）

### 变更概述
- 更新 `run_semantic_ordertype.py`，补齐真实数据路径上的 compact summary、日志可见性与更稳妥的 `OrderId` 统计口径
- 更新 `test_run_semantic_ordertype.py`，并同步 `test_run_semantic_framework.py`、`test_semantic_report.py` 的 workspace-root 调用路径
- 对 `2026-01-05 / 2026-02-24 / 2026-03-13` representative sample 跑通 OrderType probe，并输出到 `/tmp/hshare_semantic_2026_ordertype_rep3_20260315_1633`
- 明确 `OrderType` 在 `2026` representative sample 上稳定为 `3` 个值，且同一 `OrderId` 多值轨迹占绝大多数
- 同步更新 `PROGRESS.md`、`TASKS.md`、`SEMANTIC_MATRIX.md`、`Research/Audits/research_admissibility_matrix.md` 与 repo 内审计结论

### 影响
- `OrderType` 不再只是“未知字段”，而是进入 `weak_pass + allow_with_caveat` 的正式项目口径
- 当前结论支持 `ordertype_weak_consistency_check / order_lifecycle_shape_by_event_count`
- 当前结论仍然保守：`event_semantics_inference` 继续阻塞，`execution_realism_or_fill_simulation` 仍需看 `Lifecycle`

## [Semantic-TradeDir-Contrast-v1] 2026-03-15 — 锁定 TradeDir 的 candidate directional signal 边界（Codex）

### 变更概述
- 新增 `run_semantic_tradedir_contrast.py`，对 `Dir=0/1/2` 的 linkage side 结构、`previous-trade price move`、`Time-derived bucket` mix 与 `Dir=0` 特殊性做轻量 contrast probe
- 新增 `test_run_semantic_tradedir_contrast.py`，覆盖合成三日样本上的候选方向信号判定
- 对 `2026-01-05 / 2026-02-24 / 2026-03-13` representative sample 跑通 contrast probe，并输出到 `/tmp/hshare_semantic_2026_tradedir_contrast_rep3_20260315_1700`
- 明确 `TradeDir` 在 `2026` representative sample 上稳定为 `{0,1,2}`，`Dir=1 / 2` 的 linkage gap 近乎为 `0`，但 `previous-trade price move` 上存在稳定且方向一致的差异
- 同步更新 `PROGRESS.md`、`TASKS.md`、`SEMANTIC_MATRIX.md`、`Research/Audits/research_admissibility_matrix.md` 与 repo 内审计结论

### 影响
- `TradeDir` 不再只是“值域稳定但未知”，而是进入 `candidate_directional_signal` 的正式项目口径
- 当前结论仍然保守：`admissibility_impact = requires_manual_review`
- `TradeDir` 仍不能直接映射为 confirmed aggressor side，也不能直接放行 signed-flow 研究模块
- 当前最自然的下一步从“继续啃 TradeDir”切到 `OrderId lifecycle / OrderType` 对 admissibility matrix 的影响项

## [Semantic-Framework-v1] 2026-03-15 — 搭建 2026 semantic verification framework 骨架（Codex）

### 变更概述
- 新增 `run_semantic_framework.py`，把 `OrderId lifecycle`、`TradeDir`、`OrderType`、`Session` 的 probe 骨架收口到统一 CLI
- 新增 `test_run_semantic_framework.py`，覆盖 lifecycle summary、field matrix、admissibility hooks 的最小样本输出
- 补充 `Scripts/README.md` 与 `Research/Audits/README.md`，把新的 semantic framework 入口和产物接到现有文档中

### 影响
- `2026` semantic verification 现在有了一个统一的轻量框架出口，不需要先改 stage contract 或跑重 I/O
- `OrderId lifecycle` 可先以事件数、`SeqNum`、`OrderType` 迁移候选做事实层审阅
- `TradeDir / OrderType / Session` 的语义验证可以在统一 report / admissibility 产物上继续加深，而不是散落在临时笔记里

## [Semantic-Framework-v2] 2026-03-15 — 拆分为 contract / probes / report 三层结构（Codex）

### 变更概述
- 新增 `semantic_contract.py`，统一 semantic area、status、confidence、blocking level、admissibility impact 与输出 schema
- 新增 `run_semantic_lifecycle.py`、`run_semantic_tradedir.py`、`run_semantic_ordertype.py`、`run_semantic_session.py`
- 新增 `semantic_report.py` 与 `SEMANTIC_2026_FRAMEWORK.md`
- 新增对应测试，覆盖 contract、4 个 runner 和 report 聚合

### 影响
- semantic verification 的骨架从单一 runner 细化为可扩展的 contract + probe + report 结构
- 后续每个 semantic area 都能独立迭代，同时保持输出 contract 和 admissibility 接口稳定

## [Docs-Full-Stage-Completion-v1] 2026-03-15 — 拉齐到 2025/2026 全量 staging 已完成（Codex）

### 变更概述
- 更新 `README.md`、`PROGRESS.md`、`TASKS.md`，将执行状态从“`2026` 全量 staging 进行中”改为“`2025/2026` 全量 staging 已完成”
- 明确 `2026` 全量 `staging` 于 `2026-03-15 02:03 CST` 完成，`2025` 全量 `staging` 于 `2026-03-15 06:31 CST` 完成
- 将当前最关心的下一步统一切到 `full-year DQA + semantic boundary`

### 影响
- 四个入口文件不再滞后于实际执行状态
- 当前主线从 `T-R03 staging` 正式切换到 `T-R04/T-R05`
- 后续如果开新 session，不会再误以为 `2026` 仍在后台长跑

## [Docs-Status-Alignment-v1] 2026-03-15 — 拉齐前置任务与当前执行主线的文档口径（Codex）

### 变更概述
- 更新 `README.md`、`PROGRESS.md`、`TASKS.md`，明确区分“架构前置是否完整”和“当前执行主线推进到哪里”
- 明确 `raw inventory`、旧外置盘清理、`golden sample` 冻结仍是待补基础项
- 明确当前执行主线已经后移到：`2026` 全量 staging、sample-year semantic boundary、research admissibility matrix

### 影响
- 后续阅读文档时，不会再把“前置未补齐”误读成“后续样本结论无效”
- 项目口径从“伪 waterfall”收口为“基础项待补、主线并行推进”的真实状态
- `raw inventory` 继续保留为待补基础项，但不再被写成当前唯一执行主线

## [Semantic-Time-Anchor-v1] 2026-03-14 — 新增 2025 粗粒度时间锚探针并确认 weak-pass（Codex）

### 变更概述
- 新增 `run_semantic_time_anchor.py`，用于在 `SendTime` 缺失时，评估 `Time` 是否还能支撑保守的 coarse temporal validation
- 新增 `test_run_semantic_time_anchor.py`，覆盖 `weak_pass` 与 `unavailable` 两类最小样本
- 对 `2025-01-02 / 2025-06-12 / 2025-12-04` representative sample 跑通 time-anchor probe，输出至 `/tmp/hshare_semantic_time_anchor_probe`
- 进一步补跑 `2025-02-28 / 2025-04-30 / 2025-07-04 / 2025-09-29` 四个扩展样本日，输出至 `/tmp/hshare_time_anchor_ext_2025` 与 `/tmp/hshare_semantic_time_anchor_ext`
- 确认三天的 `Orders.Time` 与 `Trades.Time` 非空率均为 `1.0`
- 确认 matched ID edges 上 `order_time <= trade_time` 比例约 `99.98%`，负秒级偏差仅约 `0.02%`
- 确认扩展到 `7` 个样本日、`50,521,238` 条 matched edges 后，全部样本日仍可标记为 `coarse_time_anchor_status = weak_pass`

### 影响
- `2025` 现在不只是 “ID-level linkage 成立但 SendTime 缺失”
- `2025` 进一步可表述为：`Time` 可支撑粗粒度时间一致性检查，但仍不能替代 `SendTime` 做精细 lag / queue / latency 研究
- `2025` 的下一步从“证明是否存在替代时间锚”推进为“界定 coarse temporal validation 的有效边界”
- `2025` 可派生为 `research_time_grade = coarse_only`
- `2026` 可派生为 `research_time_grade = fine_ok`

## [Source-Inventory-v1] 2026-03-14 — 落地 HKDarkPool 专项 inventory CLI 并跑通全年 2025（Codex）

### 变更概述
- 新增 `run_source_group_inventory.py`，用于在扩主 contract 之前，对指定 raw source group 做可见、可恢复的专项 inventory
- 新增 `test_run_source_group_inventory.py`，覆盖合成 zip 上的 member inventory、daily summary、schema fingerprint 落盘
- 对真实 `2025` raw 全年执行 `HKDarkPool` inventory，输出至 `/tmp/hshare_hkdarkpool_inventory`
- 确认 `HKDarkPool` 并非 `2025-12-04` 单日异常，而是在 `246` 个交易日中命中 `44` 天、共 `142` 个 member、`935,527` 行
- 确认 `HKDarkPool` 当前呈现为稳定独立的 `7` 列 trade-like schema：`time, price, share, turnover, side, type, brokerno`

### 影响
- `HKDarkPool` 现在可按事实层 inventory 单独研究，不需要先并入主 `Orders / Trades` contract
- `2025` 的下一步从“先确认是否单日异常”推进为“old-format temporal anchor / time-aware validation investigation + 是否需要独立 verified 分支”

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
- 明确 `2026` 的 ID-level + time-usable linkage 三天全 `pass`
- 通过 `run_semantic_idspace.py` 更正 `2025` 口径：ID-level direct equality 三天均为 `1.0`，但 orders 侧 `SendTime` 全空，lag-aware linkage DQA 应标记为 `time_anchor_unavailable`

### 影响
- `stage` 层已可放行到全量，不需要回头重写 stage 架构
- linkage 相关研究从现在开始按年份拆开推进
- `2026` 进入 linkage semantic verification；`2025` 先做 `HKDarkPool inventory + old-format temporal anchor investigation`

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
