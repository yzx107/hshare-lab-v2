# 任务分解

> Hshare Lab v2 reboot 任务 SSOT

---

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
- **状态**: ⏳ 待开始
- **目标**: 让 raw layer 具备可回溯性
- **验收门禁**:
  - `2025` 与 `2026` raw inventory 都落盘
  - 记录文件数、总字节数、日期覆盖、异常文件
  - manifest 可重复生成
- **可观测性**: 必须按日期或批次输出进度、心跳、错误计数

## T-R03: 定义 Candidate Cleaned Contract
- **阶段**: Stage 0 需求/规格 + Stage 1 数据清洗
- **状态**: ⏳ 待开始
- **目标**: 定义 `candidate_cleaned_2025_v1`
- **验收门禁**:
  - schema spec 固定
  - partition spec 固定
  - candidate key spec 固定
  - golden sample 列表固定

## T-R04: Mechanical DQA v1
- **阶段**: Stage 1 数据清洗
- **状态**: ⏳ 待开始
- **目标**: 建立研究导向 DQA 基线
- **模块**:
  - Ingestion Completeness
  - Schema Integrity
  - Row-Level Validity
  - Sequence and Time Integrity
  - Session Quality
  - Cross-Table Feasibility
  - Broker Map Quality
- **验收门禁**:
  - 每个模块都有报告
  - 所有长任务都支持 checkpoint / resume
  - 所有长任务都有 heartbeat / progress / blockage hint

## T-R05: Semantic Verification Matrix
- **阶段**: Stage 3 研究验证
- **状态**: ⏳ 待开始
- **目标**: 给关键字段打 `pass / fail / unknown`
- **首批字段**:
  - `TradeDir`
  - `BrokerNo`
  - `BidOrderID`
  - `AskOrderID`
  - `OrderId`
  - `OrderType`

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
