# 项目进度

> Hshare Lab v2 reboot 进度 SSOT

---

## 会话接续

- canonical repo: `/Users/yxin/AI_Workstation/Hshare_Lab_v2`
- legacy evidence repo: `/Users/yxin/AI_Workstation/Hshare_Lab`
- GitHub: [yzx107/hshare-lab-v2](https://github.com/yzx107/hshare-lab-v2)
- 当前最关心的下一步：先做 raw inventory，把 raw 层沉淀成 manifest-first 的可回溯基线
- 旧仓库不再修改，只保留为 `legacy evidence`

## 当前结论

- 当前主线已从“继续清洗旧产物”切换为“重建研究基础设施 contract”
- 旧版逻辑整体失效，保留为 `legacy evidence`
- `raw layer` 保留
- `candidate_cleaned` 已明确为 `stage parquet` 层，而不是研究层
- `stage parquet` 真实单日 sample 已跑通：`2025-02-18` 与 `2026-03-13`
- 旧 `cleaned/temp` 数据层正在从新主线剥离

## Reboot Milestones

### R0: Reboot Freeze
- **目标**: 终止旧世界继续膨胀，建立新主线入口
- [x] 明确旧版逻辑失效，不再把旧 cleaned / DQA / feature 视为事实源
- [x] 重写顶层文档为 reboot 版
- [x] 建立新的 contract 文档骨架
- [x] 规划新的外置盘数据布局
- **状态**: ✅ 完成

### R1: Data Reset
- **目标**: 只保留 raw，清除旧 cleaned/temp 数据层
- [x] 确认 raw layer 位于 `/Volumes/Data/港股Tick数据/{2025,2026}`
- [x] 确认旧 cleaned layer 位于 `/Volumes/Data/港股Tick数据/clean_parquet`
- [x] 确认旧 temp layer 位于 `/Volumes/Data/港股Tick数据/.tmp_parquet`
- [/] 删除旧 `clean_parquet/`（删除命令已发起，外置盘仍在处理）
- [/] 删除旧 `.tmp_parquet/`（删除命令已发起，外置盘仍在处理）
- [ ] 建立新骨架目录：`candidate_cleaned/`、`dqa/`、`verified/`、`manifests/`、`logs/`
- **状态**: 🔄 执行中

### R2: Raw Inventory
- **目标**: 让 raw layer 可回溯、可核对、可重建
- [x] 建立 `build_raw_inventory.py` CLI 骨架
- [ ] 生成 `raw_inventory_2025`
- [ ] 生成 `raw_inventory_2026`
- [ ] 记录文件数、总字节数、日期覆盖、异常文件
- [ ] 生成 schema snapshot 与 source metadata
- **状态**: 🔄 已启动

### R3: Stage Parquet / Candidate Cleaned Contract
- **目标**: 定义 `stage parquet / candidate_cleaned_2025_v1`
- [x] 建立 `STAGE_SCHEMA.md`，固定 Trades / Orders 的首版 stage contract
- [x] 建立 `build_stage_parquet.py` CLI，支持 `date + table` task、checkpoint、heartbeat、多进程并行
- [x] 完成真实单日 sample run：`2025-02-18` 与 `2026-03-13`
- [x] 核对 raw source mapping、实际 parquet schema、`SendTimeRaw -> SendTime` 样本对照
- [ ] 固定 schema spec
- [ ] 固定 partition spec
- [ ] 固定 candidate key spec
- [ ] 选定 golden sample 日期与股票池
- **状态**: 🔄 已启动

### R4: DQA Framework
- **目标**: 建立研究导向 DQA，而不是传统 BI QA
- [ ] Ingestion Completeness
- [ ] Schema Integrity
- [ ] Row-Level Validity
- [ ] Sequence and Time Integrity
- [ ] Session Quality
- [ ] Cross-Table Feasibility
- [ ] Broker Map Quality
- **状态**: ⏳ 待开始

### R5: Semantic Verification
- **目标**: 给关键字段打 `pass / fail / unknown`
- [ ] `TradeDir`
- [ ] `BrokerNo`
- [ ] `BidOrderID / AskOrderID`
- [ ] `OrderId`
- [ ] `OrderType`
- [ ] `Level / VolumePre / Type`
- **状态**: ⏳ 待开始

### R6: Verified Layer
- **目标**: 输出 research-ready 表，而不是直接消费 cleaned 原表
- [ ] `verified_trades`
- [ ] `verified_orders`
- [ ] `verified_trade_order_linkage`
- [ ] `broker_reference`
- **状态**: ⏳ 待开始

## 当前阻塞

- raw inventory CLI 已建立，但尚未对真实 `2025/2026` raw layer 执行
- `stage parquet / candidate_cleaned_2025_v1` 已完成真实单日样本验收，但还没扩到多日样本
- 当前 heartbeat 仍以 `date + table` task 为粒度，长任务的 member-level 可见性还不够细
- `golden sample` 还没选定

## 当前状态

**状态**: reboot 已启动；旧世界冻结；新主线进入 `raw inventory + contract definition`，其中当前首要推进项是 raw inventory
