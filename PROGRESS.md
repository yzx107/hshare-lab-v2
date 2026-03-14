# 项目进度

> Hshare Lab v2 reboot 进度 SSOT

---

## 当前结论

- 当前主线已从“继续清洗旧产物”切换为“重建研究基础设施 contract”
- 旧版逻辑整体失效，保留为 `legacy evidence`
- `raw layer` 保留
- 旧 `cleaned/temp` 数据层已从新主线剥离

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
- [x] 删除旧 `clean_parquet/`
- [x] 删除旧 `.tmp_parquet/`
- [x] 建立新骨架目录：`candidate_cleaned/`、`dqa/`、`verified/`、`manifests/`、`logs/`
- **状态**: ✅ 完成

### R2: Raw Inventory
- **目标**: 让 raw layer 可回溯、可核对、可重建
- [ ] 生成 `raw_inventory_2025`
- [ ] 生成 `raw_inventory_2026`
- [ ] 记录文件数、总字节数、日期覆盖、异常文件
- [ ] 生成 schema snapshot 与 source metadata
- **状态**: ⏳ 待开始

### R3: Candidate Cleaned Contract
- **目标**: 定义 `candidate_cleaned_2025_v1`
- [ ] 固定 schema spec
- [ ] 固定 partition spec
- [ ] 固定 candidate key spec
- [ ] 选定 golden sample 日期与股票池
- **状态**: ⏳ 待开始

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

- 新版 raw inventory 脚本尚未建立
- `candidate_cleaned_2025_v1` 尚未重新定义
- `golden sample` 还没选定

## 当前状态

**状态**: reboot 已启动；旧世界冻结；新主线进入 `raw inventory + contract definition`
