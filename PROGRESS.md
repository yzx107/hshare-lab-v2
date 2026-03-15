# 项目进度

> Hshare Lab v2 reboot 进度 SSOT

---

## 会话接续

- canonical repo: `/Users/yxin/AI_Workstation/Hshare_Lab_v2`
- legacy evidence repo: `/Users/yxin/AI_Workstation/Hshare_Lab`
- GitHub: [yzx107/hshare-lab-v2](https://github.com/yzx107/hshare-lab-v2)
- 当前最关心的下一步：推进 `OrderId lifecycle` semantic verification，并继续收口 full-year DQA 与 verified 实装
- 旧仓库不再修改，只保留为 `legacy evidence`

## 当前结论

- 当前主线已从“继续清洗旧产物”切换为“重建研究基础设施 contract”
- 旧版逻辑整体失效，保留为 `legacy evidence`
- `raw layer` 保留
- `candidate_cleaned` 已明确为 `stage parquet` 层，而不是研究层
- `stage parquet` 真实单日 sample 已跑通：`2025-02-18` 与 `2026-03-13`
- representative sample 已跑通：`2025-01-02 / 2025-06-12 / 2025-12-04` 与 `2026-01-05 / 2026-02-24 / 2026-03-13`
- `2025/2026` sample 的 stage 均实现 `raw_row_count == row_count`，且 `rejected_row_count = 0`、`failed_member_count = 0`
- `2025` 与 `2026` 不能再按同一 linkage 语义版本处理
- `2026` representative sample 上，ID-level linkage 与 time-usable linkage 三天均 `pass`
- `2025` representative sample 上，`BidOrderID / AskOrderID -> OrderId` 的 ID-level direct equality 三天均为 `1.0`
- `2025` orders 侧 `SendTime` 在 representative sample 上为全空，因此 lag-aware linkage DQA 当前应解释为 `time_anchor_unavailable`，而不是 “direct linkage 全 fail”
- `run_semantic_time_anchor.py` 已确认 `2025` representative sample 的 `Orders.Time / Trades.Time` 非空率均为 `1.0`
- `2025` representative sample 上，matched ID edges 的 `Time` 一致性表现为 `coarse_time_anchor_status = weak_pass`
- `2025` representative sample 上，`order_time <= trade_time` 比例约 `99.98%`，负秒级偏差仅约 `0.02%`
- `2025` 扩展到 `7` 个样本日后，`coarse_time_anchor_status` 仍全部为 `weak_pass`
- `2025` 的 `Time`-anchor 扩展样本共覆盖 `50,521,238` 条 matched edges，平均 `order_time <= trade_time` 比例约 `99.9786%`
- `2025` 当前可记为 `research_time_grade = coarse_only`
- `2026` 当前可记为 `research_time_grade = fine_ok`
- `2025` 针对观测到的 raw source group label `HKDarkPool` 的专项 inventory 已完成：全年扫描 `246` 个交易日，其中 `44` 天命中、`142` 个 member、`935,527` 行
- 该 `HKDarkPool` 标签当前呈现为稳定独立的 `7` 列 trade-like schema：`time, price, share, turnover, side, type, brokerno`
- 该 `HKDarkPool` 标签首次出现于 `2025-07-04`，最后出现于 `2025-12-31`，当前继续隔离处理，不并入主 contract；仓库内官方/vendor reference 尚未直接确认它是正式官方术语
- linkage 相关研究从现在开始拆年推进；`2026` 表内排序默认 `SeqNum` 优先
- `2026` representative sample 上，`TradeDir` 已确认是稳定三值编码 `{0,1,2}`
- `2026` representative sample 上，`TradeDir` 的 `Dir=1 / 2` 并非靠 linkage 结构区分，但在 `previous-trade price move` 上存在稳定且方向一致的差异
- `2026 TradeDir` 当前可记为 `status = candidate_directional_signal`，但 `admissibility_impact` 仍应维持 `requires_manual_review`
- `2026` representative sample 上，`OrderType` 已确认是稳定三值编码，`distinct_ordertype_values_union = 3`
- `2026` representative sample 上，`OrderType` 的同一 `OrderId` 多值轨迹占绝大多数，`multi_ordertype_orderid_rate_avg = 0.9912119616165596`
- `2026 OrderType` 当前可记为 `status = weak_pass`，`admissibility_impact = allow_with_caveat`
- `2026 OrderType` 当前支持 `ordertype_weak_consistency_check / order_lifecycle_shape_by_event_count`，但仍不支持 `event_semantics_inference`
- `build_stage_parquet.py` 已优化为 direct zip streaming + same-day single-pass bundle，减少整块 member 读入和重复 zip 扫描
- `2026` 全量 `staging` 已完成：`96` 个 task、`0` failed，完成于 `2026-03-15 02:03 CST`
- `2025` 全量 `staging` 已完成：`492` 个 task、`0` failed，完成于 `2026-03-15 06:31 CST`
- `2025` full-year raw inventory 已完成：`250` files、`293,335,032,932` bytes、`247` distinct trade dates
- `2026` full-year raw inventory 已完成：`52` files、`89,509,988,059` bytes、`49` distinct trade dates
- `raw inventory` 当前可记为：`2025 = year_scanned`、`2026 = year_scanned`、combined status = `inventory_closed`
- `golden sample` policy、reference usage boundary、verified admission policy 已形成 repo 内正式文档
- `golden sample` 日期清单已正式冻结，并已落盘为 repo manifest
- 旧 `cleaned/temp` 数据层正在从新主线剥离

## 当前执行说明

- 当前不是严格 waterfall 执行；在 `stage / DQA / semantic sample` 并行推进的同时，`raw inventory` 已补齐到真实全年落盘
- 因此后续主线不再受 `raw inventory` 或 `golden sample` 阻塞，当前真正未收口的是 verified 实装、remaining semantic boundary 与 full-year DQA 总结
- 当前真正的执行主线是：
  - `2025/2026` semantic boundary 固化
  - `OrderId lifecycle` semantic verification
  - `research admissibility matrix` 收口

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
- **状态**: 🔄 遗留收尾

### R2: Raw Inventory
- **目标**: 让 raw layer 可回溯、可核对、可重建
- [x] 建立 `build_raw_inventory.py` CLI 骨架
- [x] 生成 `raw_inventory_2025`
- [x] 生成 `raw_inventory_2026`
- [x] 记录文件数、总字节数、日期覆盖、异常文件
- [x] 生成 summary / manifest / parquet artifacts
- [x] 补充 `2025/2026` inventory notes 与 overview
- **状态**: ✅ 已完成（`inventory_closed`）

### R3: Stage Parquet / Candidate Cleaned Contract
- **目标**: 定义 `stage parquet / candidate_cleaned_2025_v1`
- [x] 建立 `STAGE_SCHEMA.md`，固定 Trades / Orders 的首版 stage contract
- [x] 建立 `build_stage_parquet.py` CLI，支持 `date + table` task、checkpoint、heartbeat、多进程并行
- [x] 将同日 `orders/trades` 优化为单次 zip 扫描，并改为 `ZipFile.open(...)` 直读 CSV member stream
- [x] 完成真实单日 sample run：`2025-02-18` 与 `2026-03-13`
- [x] 核对 raw source mapping、实际 parquet schema、`SendTimeRaw -> SendTime` 样本对照
- [x] 完成 representative sample run：`2025 x 3`、`2026 x 3`
- [ ] 固定 schema spec
- [ ] 固定 partition spec
- [ ] 固定 candidate key spec
- [x] 选定 golden sample 日期与股票池
- **状态**: 🔄 full-year staging 已完成，contract 收口待补

### R4: DQA Framework
- **目标**: 建立研究导向 DQA，而不是传统 BI QA
- [x] Ingestion Completeness
- [x] Schema Integrity
- [x] Row-Level Validity
- [x] Sequence and Time Integrity
- [ ] Session Quality（sample 尚未单列固化）
- [x] Cross-Table Feasibility
- [ ] Broker Map Quality（尚未系统展开）
- **状态**: 🔄 sample-year 已完成，full-year 待跟进

### R5: Semantic Verification
- **目标**: 给关键字段打 `pass / fail / unknown`
- [/] `TradeDir`（`2026` representative sample 已收口为 `candidate_directional_signal`，仍需 manual review）
- [ ] `BrokerNo`
- [ ] `BidOrderID / AskOrderID`
- [ ] `OrderId`
- [/] `OrderType`（`2026` representative sample 已收口为 `weak_pass + allow_with_caveat`）
- [ ] `Level / VolumePre / Type`
- **状态**: 🔄 sample-year 边界已建立（按年份拆开）

### R6: Verified Layer
- **目标**: 输出 research-ready 表，而不是直接消费 cleaned 原表
- [ ] `verified_trades`
- [ ] `verified_orders`
- [ ] `verified_trade_order_linkage`
- [ ] `broker_reference`
- **状态**: ⏳ 待开始

## 当前阻塞 / 待补基础项

- `stage parquet / candidate_cleaned_2025_v1` 已完成 `2025/2026` full-year staging，但 schema / partition / candidate key 仍待正式收口
- `build_stage_parquet.py` 的 `heartbeat.json` 已聚合 `active_bundles`，可看到当前 member、已处理 member 数与两表中间行数
- `run_dqa_coverage.py`、`run_dqa_schema.py`、`run_dqa_linkage.py` 已从 scaffold 进入可执行 CLI，并有 `checkpoint / heartbeat / summary / report` 留痕
- `run_source_group_inventory.py` 已落地为正式 CLI，并已完成针对 `2025` 中 `HKDarkPool` raw source group label 的 inventory
- `HKDarkPool` 已确认不是 `2025-12-04` 单日偶发，而是 `2025-07-04` 到 `2025-12-31` 间多日反复出现的独立 source group label
- `run_semantic_idspace.py` 已落地为语义探针，确认 `2025` representative sample 的 ID-level linkage 成立，但 order-side 时间锚仍缺失
- `run_semantic_time_anchor.py` 已确认 `2025` 的 `Time` 可支持保守的 coarse temporal validation，但仍不能替代 `SendTime` 做精细 lag / queue / latency 研究
- `2025` 的下一步从“证明 ID 不直连”进一步收缩为：界定 `Time` 可支撑的粗粒度验证边界，以及哪些研究仍必须排除
- `TradeDir` 的下一步不再是扩 full-year，而是把当前 `candidate_directional_signal + requires_manual_review` 结论固化到研究入口
- `OrderType` 的下一步不再是扩 full-year，而是把当前 `weak_pass + allow_with_caveat` 结论固化到研究入口
- `2025/2026` 全量 staging 已完成，后续应优先回到 `OrderId lifecycle` 对 admissibility matrix 的直接影响项
## 当前状态

**状态**: reboot 已启动；旧世界冻结；`2025/2026` full-year staging 已完成；`2025/2026` raw inventory 已闭合；新主线切到 `full-year DQA + semantic boundary + verified admission implementation`
