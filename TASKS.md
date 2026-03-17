# 任务分解

> Hshare Lab v2 reboot 任务 SSOT

---

## 会话接续

- canonical repo: `/Users/yxin/AI_Workstation/Hshare_Lab_v2`
- legacy evidence repo: `/Users/yxin/AI_Workstation/Hshare_Lab`
- GitHub: [yzx107/hshare-lab-v2](https://github.com/yzx107/hshare-lab-v2)
- 当前最关心的下一步：从 `2026 OrderId lifecycle` light smoke 开始，再逐步扩到 sample-level semantic verification 与 verified 实装
- 旧仓库不再修改，只保留为 `legacy evidence`

## 当前执行说明

- 当前任务不是严格按 `T-R00 -> T-R07` 串行完成
- `T-R01 / T-R02` 仍有未补基础项，但 `T-R03 / T-R04 / T-R05` 已经在 sample-year 层面并行推进
- 看状态时请区分：
  - `架构前置是否完整`
  - `当前执行主线是否已向后推进`

## T-R00: Reboot 主线切换
- **阶段**: Stage 0 需求/规格 + Stage 4 工程加固
- **状态**: ✅ 完成
- **目标**: 让旧世界失效，新主线具备清晰入口
- **验收门禁**: `README/PROGRESS/TASKS/CHANGELOG` 全部切到 reboot 口径

## T-R01: 删除旧 cleaned/temp 数据层
- **阶段**: Stage 1 数据清洗
- **状态**: 🔄 遗留收尾
- **目标**: 外置盘仅保留 raw layer
- **验收门禁**: `clean_parquet/` 与 `.tmp_parquet/` 不再存在；新骨架目录已创建
- **可观测性**: 删除前记录边界，删除后复核目录树

## T-R02: 建立 Raw Inventory Manifest
- **阶段**: Stage 1 数据清洗
- **状态**: ✅ 已完成（`inventory_closed`）
- **目标**: 让 raw layer 具备可回溯性
- **验收门禁**:
  - `build_raw_inventory.py` CLI 已固化并可重复调用
  - `2025` 与 `2026` raw inventory 都落盘
  - 记录文件数、总字节数、日期覆盖、异常文件
  - manifest 可重复生成
- **可观测性**: 必须按日期或批次输出进度、心跳、错误计数
- **当前说明**: 已补齐到真实全年落盘，并形成 notes / overview；后续只需做 reporting refinement，不再是 blocker

## T-R03: 定义 Stage Parquet / Candidate Cleaned Contract
- **阶段**: Stage 0 需求/规格 + Stage 1 数据清洗
- **状态**: 🔄 full-year staging 已完成，golden sample 已冻结，contract 收口待补
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
- **当前说明**: stage contract 所需的 schema / partition / candidate key / golden sample 文档级定义已完成；后续重点转向 DQA、semantic 与 verified 实装

## T-R04: Mechanical DQA v1
- **阶段**: Stage 1 数据清洗
- **状态**: 🔄 `coverage/schema/linkage` 已完成，转向 yearly summary 与 semantic / verified
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
  - `run_source_group_inventory.py` 已 materialize `audit_source_member_inventory / audit_source_daily_summary / audit_source_schema_fingerprints`
  - 四条 CLI 均具备 `checkpoint / heartbeat / summary / Research report`
  - `2025/2026` full-year `staging` 已完成，`2026` 完成 `96` task、`2025` 完成 `492` task，均为 `0 failed`
  - `2025/2026` full-year `schema DQA` 已完成，均为 `0 failed`
  - `2026` full-year `linkage DQA` 已完成：`48/48`、`0 failed`
  - `2025` full-year `linkage DQA` 已完成：`246/246`、`0 failed`
  - representative sample 结论：`2026` 的 ID-level + time-usable linkage 三天全 `pass`
  - representative sample 结论：`2025` 的 ID-level linkage 三天均成立，但因 orders 侧 `SendTime` 全空而处于 `time_anchor_unavailable`
  - `2025` 中观测到的 `HKDarkPool` raw source group label 的 inventory 结论：`246` 个交易日中 `44` 天命中、`142` 个 member、`935,527` 行、单一 `7` 列 trade-like schema
- **验收门禁**:
  - 每个模块都有报告
  - 所有长任务都支持 checkpoint / resume
  - 所有长任务都有 heartbeat / progress / blockage hint
  - 对新重任务遵守 `single-day smoke -> 1 sample -> 3 sample -> full-year` 阶梯加压

## T-R05: Semantic Verification Matrix
- **阶段**: Stage 3 研究验证
- **状态**: 🔄 sample-year 边界已建立
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
  - `2026 lifecycle`：下一步按 light 路线推进，先 `single-day smoke -> 1 sample -> 3 sample`
  - `2026 TradeDir`：representative sample 已收口为稳定三值编码 `{0,1,2}`，`Dir=1 / 2` 在 linkage 结构上近乎无差，但在 `previous-trade price move` 上存在稳定、方向一致的差异
  - `2026 TradeDir`：当前正式口径为 `status = candidate_directional_signal`，`admissibility_impact = requires_manual_review`
  - `2026 TradeDir`：当前不继续做 full-year 扩量，先把保守研究边界固化到矩阵和研究入口
  - `2026 OrderType`：representative sample 已收口为稳定三值编码，且同一 `OrderId` 多值轨迹占绝大多数
  - `2026 OrderType`：当前正式口径为 `status = weak_pass`，`admissibility_impact = allow_with_caveat`
  - `2026 OrderType`：当前支持 `ordertype_weak_consistency_check / order_lifecycle_shape_by_event_count`，但仍阻塞 `event_semantics_inference`
  - `2026 OrderType`：当前不继续做 full-year 扩量，先把保守研究边界固化到矩阵和研究入口
  - `2025`：针对 `HKDarkPool` raw source group label 的 inventory 已完成；ID-level linkage 已确认成立；`Time` 在 representative sample 上为 `coarse_time_anchor_status = weak_pass`
  - `2025`：扩展到 `7` 个样本日后，`Time` 仍全部为 `coarse_time_anchor_status = weak_pass`
  - `2025`：下一步转“界定 `Time` 可支撑的粗粒度验证边界”，而不是继续假设 `SendTime` 级 lag 校验可行
  - `2025`：`research_time_grade = coarse_only`
  - `2026`：`research_time_grade = fine_ok`
  - linkage 相关研究从现在开始拆年，不混年推进
  - `2026` 表内排序默认 `SeqNum` 优先

## T-R06: Verified Layer v1
- **阶段**: Stage 3 研究验证
- **状态**: 🔄 policy 已完成，实装待开始
- **目标**: 构建可直接研究使用的 verified tables
- **产物**:
  - `verified_trades`
  - `verified_orders`
  - `verified_trade_order_linkage`
- `broker_reference`

## T-R06A: Query / Report Policy Landing
- **阶段**: Stage 4 工程加固
- **状态**: ✅ 已完成
- **目标**: 把 field/reference/verified admission policy 接到 Query、报告模板与轻量检查器
- **验收门禁**:
  - Query / report bridge 文档已落地
  - broker reference read-only boundary 已落地
  - report checker 已覆盖 provenance、reference label、keep-out caveat 基础检查

## T-R07: Legacy 风险隔离
- **阶段**: Stage 4 工程加固
- **状态**: 🔄 持续中
- **目标**: 防止旧代码与旧报告误导新主线
- **验收门禁**:
  - 旧内容明确标记为 `legacy evidence`
  - 新脚本统一进入 `Scripts/`
  - 新研究输出统一进入 `Research/`
