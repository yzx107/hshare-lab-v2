# 任务分解

> Hshare Lab v2 reboot 任务 SSOT

---

## 会话接续

- canonical repo: `/Users/yxin/AI_Workstation/Hshare_Lab_v2`
- legacy evidence repo: `/Users/yxin/AI_Workstation/Hshare_Lab`
- GitHub: [yzx107/hshare-lab-v2](https://github.com/yzx107/hshare-lab-v2)
- 当前最关心的下一步：推进 `T-R02`，先把 raw inventory manifest 跑通
- 旧仓库不再修改，只保留为 `legacy evidence`

## T-R00: Reboot 主线切换
- **阶段**: Stage 0 需求/规格 + Stage 4 工程加固
- **状态**: ✅ 完成
- **目标**: 让旧世界失效，新主线具备清晰入口
- **验收门禁**: `README/PROGRESS/TASKS/CHANGELOG` 全部切到 reboot 口径

## T-R01: 删除旧 cleaned/temp 数据层
- **阶段**: Stage 1 数据清洗
- **状态**: 🔄 执行中
- **目标**: 外置盘仅保留 raw layer
- **验收门禁**: `clean_parquet/` 与 `.tmp_parquet/` 不再存在；新骨架目录已创建
- **可观测性**: 删除前记录边界，删除后复核目录树

## T-R02: 建立 Raw Inventory Manifest
- **阶段**: Stage 1 数据清洗
- **状态**: 🔄 执行中
- **目标**: 让 raw layer 具备可回溯性
- **验收门禁**:
  - `build_raw_inventory.py` CLI 已固化并可重复调用
  - `2025` 与 `2026` raw inventory 都落盘
  - 记录文件数、总字节数、日期覆盖、异常文件
  - manifest 可重复生成
- **可观测性**: 必须按日期或批次输出进度、心跳、错误计数
- **当前说明**: 这是 v2 主线当前最该先落地的执行任务

## T-R03: 定义 Stage Parquet / Candidate Cleaned Contract
- **阶段**: Stage 0 需求/规格 + Stage 1 数据清洗
- **状态**: 🔄 执行中
- **目标**: 定义 `stage parquet / candidate_cleaned_2025_v1`
- **验收门禁**:
  - `STAGE_SCHEMA.md` 固定 Trades / Orders 的 raw source mapping 与 stage schema
  - `build_stage_parquet.py` 可按 `date + table` task 执行并支持 resume
  - 同日 `orders/trades` 不重复扫 zip，默认走 direct zip streaming
  - `heartbeat.json` 需要包含 bundle 级 active progress，并可追踪当前 source member
  - 真实单日 sample run 已完成，且已核对 schema / mapping / time sanity / failures / unmapped
  - representative sample run 已完成：`2025 x 3`、`2026 x 3`
  - schema spec 固定
  - partition spec 固定
  - candidate key spec 固定
  - golden sample 列表固定

## T-R04: Mechanical DQA v1
- **阶段**: Stage 1 数据清洗
- **状态**: 🔄 进行中
- **目标**: 建立研究导向 DQA 基线
- **模块**:
  - Ingestion Completeness
  - Schema Integrity
  - Row-Level Validity
  - Sequence and Time Integrity
  - Session Quality
  - Cross-Table Feasibility
  - Broker Map Quality
- **当前进展**:
  - `run_dqa_coverage.py` 已 materialize `audit_stage_partitions / audit_stage_row_reconciliation / audit_stage_source_groups / audit_stage_failures`
  - `run_dqa_schema.py` 已 materialize `audit_schema_fingerprint / audit_field_nulls / audit_field_value_rules / audit_time_profile`
  - `run_dqa_linkage.py` 已 materialize `audit_linkage_feasibility_daily`
  - 三条 CLI 均具备 `checkpoint / heartbeat / summary / Research report`
  - representative sample 结论：`2026` direct linkage 三天全 `pass`；`2025` direct linkage 三天全 `fail`
  - representative sample 结论：`2025-12-04` 出现 `HKDarkPool`，当前作为 source inventory 问题隔离处理
- **验收门禁**:
  - 每个模块都有报告
  - 所有长任务都支持 checkpoint / resume
  - 所有长任务都有 heartbeat / progress / blockage hint

## T-R05: Semantic Verification Matrix
- **阶段**: Stage 3 研究验证
- **状态**: 🔄 规划中
- **目标**: 给关键字段打 `pass / fail / unknown`
- **首批字段**:
  - `TradeDir`
  - `BrokerNo`
  - `BidOrderID`
  - `AskOrderID`
  - `OrderId`
  - `OrderType`
- **当前策略**:
  - `2026`：进入 linkage semantic verification
  - `2025`：先做 `HKDarkPool inventory + old-format ID space investigation`
  - linkage 相关研究从现在开始拆年，不混年推进
  - `2026` 表内排序默认 `SeqNum` 优先

## T-R06: Verified Layer v1
- **阶段**: Stage 3 研究验证
- **状态**: ⏳ 待开始
- **目标**: 构建可直接研究使用的 verified tables
- **产物**:
  - `verified_trades`
  - `verified_orders`
  - `verified_trade_order_linkage`
  - `broker_reference`

## T-R07: Legacy 风险隔离
- **阶段**: Stage 4 工程加固
- **状态**: 🔄 持续中
- **目标**: 防止旧代码与旧报告误导新主线
- **验收门禁**:
  - 旧内容明确标记为 `legacy evidence`
  - 新脚本统一进入 `Scripts/`
  - 新研究输出统一进入 `Research/`
