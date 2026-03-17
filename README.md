# Hshare Lab v2

港股 tick / order / trade 研究基础设施重启版。

自 `2026-03-14` 起，本仓库进入 `reboot` 主线：旧版清洗、DQA、feature、查询逻辑统一降级为 `legacy evidence`，不再作为事实源或研究入口。

## 会话接续

- canonical repo: `/Users/yxin/AI_Workstation/Hshare_Lab_v2`
- legacy evidence repo: `/Users/yxin/AI_Workstation/Hshare_Lab`
- GitHub: [yzx107/hshare-lab-v2](https://github.com/yzx107/hshare-lab-v2)
- 切换 session 时优先从 `README.md`、`PROGRESS.md`、`TASKS.md`、`CHANGELOG.md` 进入
- 当前最关心的下一步：继续推进 `2025/2026` full-year DQA、`OrderId lifecycle` semantic verification 与 verified layer 实装

## 当前主线

当前主线按以下顺序推进：

1. `raw inventory`
2. `stage parquet / candidate_cleaned contract`
3. `DQA`
4. `semantic verification`
5. `verified layer`

说明：
- 这仍然是架构顺序，不等于当前执行严格串行
- 目前项目处于“前置项未全部收口，但 stage / DQA / semantic sample 已并行推进”的状态
- `raw inventory` 已对真实 `2025/2026` 全量落盘并形成 manifest / note / overview，当前不再是待补前置项

## 当前执行主线

- `2025/2026`：全量 `staging` 已完成，当前主线切到 `full-year DQA`
- `2025`：观测到的 raw source group label `HKDarkPool` 已隔离 inventory，`ID-linkage` 与 `Time` coarse anchor 边界已基本定住
- `research admissibility`：已形成 `2025 coarse_only / 2026 fine_ok` 的项目口径
- `raw inventory`：`2025/2026` 全年 manifest 已完成，当前进入已闭合 baseline 状态

## 当前原则

- `raw` 不可变，只补 metadata
- `cleaning` 只做最小保守标准化，不做主观语义解释
- `DQA` 必须 `visible + resumable`
- `semantic verification` 先于复杂 alpha
- 旧仓库 `/Users/yxin/AI_Workstation/Hshare_Lab` 只作为 `legacy evidence`，不再继续开发
- 旧目录 `scripts/`、旧 `src/features/`、旧 DQA 报告均视为 `legacy evidence`

## Upstream Provenance

- 当前 `2025/2026` 港股数据的上游采购来源已确认属于 HKEX `OMD-C` family
- 当前仓库中的 CSV / `candidate_cleaned` 列形态，不默认视为官方 binary message 的 `1:1` 原样展开
- 含 `OrderId` 的逐笔新增 / 修改 / 删除内容，与 `Securities FullTick (SF)` 能力相符
- 字段级语义仍必须逐项通过 semantic verification，不能仅凭 OMD-C provenance 直接放行 `BrokerNo`、`TradeDir`、`Level` 等解释

## Stage Layer 定义

- `candidate_cleaned` 就是当前的 `stage parquet` 层
- 一条 raw 记录对应一条 stage 记录，保持原始记录粒度
- 尽量保留原始字段，只补最少量技术列与工程标准化
- 允许类型、时间、空值、schema、分区标准化
- 不允许字段语义重命名、aggressor 推断、book/queue 推断、linkage 衍生、研究特征

## 主栈分工

- `Parquet`：唯一工作格式；`CSV` 只保留在 raw evidence 层
- `DuckDB`：主查询、主审计、主 linkage 检查、研究中间表 materialization
- `Polars`：主 ETL、主 mechanical transformation、主特征工程
- `PyArrow`：schema、dataset、metadata、manifest 底座
- `Python CLI + Jupyter`：正式 pipeline 与探索验证双轨并存
- `pytest + ruff + Make`：轻量工程化；暂缓 `Spark / Airflow / Dagster / Delta / Iceberg / Hudi`

## 新会话入口

- [README.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/README.md)
- [PROGRESS.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/PROGRESS.md)
- [TASKS.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/TASKS.md)
- [CHANGELOG.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/CHANGELOG.md)

## 核心规范

- [DATA_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DATA_CONTRACT.md)
- [CLEANING_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/CLEANING_SPEC.md)
- [STAGE_SCHEMA.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/STAGE_SCHEMA.md)
- [DQA_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/DQA_SPEC.md)
- [SEMANTIC_MATRIX.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/SEMANTIC_MATRIX.md)
- [LEGACY_STATUS.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/LEGACY_STATUS.md)

## 新目录约定

```text
Hshare_Lab_v2/
├── pyproject.toml           # 依赖与 lint/test 约束
├── Makefile                 # 轻量编排入口
├── Scripts/                 # 新主线脚本入口
├── Research/                # 审计、验证、研究产出
├── manifests/               # 清单、schema snapshot、fingerprint
├── logs/                    # 运行日志与心跳留痕
├── DATA_CONTRACT.md
├── CLEANING_SPEC.md
├── DQA_SPEC.md
├── SEMANTIC_MATRIX.md
└── LEGACY_STATUS.md
```

## 外置盘数据布局

```text
/Volumes/Data/港股Tick数据/
├── 2025/                    # raw layer
├── 2026/                    # raw layer
├── candidate_cleaned/       # stage parquet / 待验证 cleaned layer
├── dqa/                     # DQA 报告与中间产物
├── verified/                # verified research-ready layer
├── manifests/               # 数据清单
└── logs/                    # 长任务日志
```

旧版 `clean_parquet/` 和 `.tmp_parquet/` 已不再属于新主线。

## 工程入口

- `python -m Scripts.build_raw_inventory --year 2025`：首个正式 CLI，负责 raw inventory manifest
- `python -m Scripts.build_stage_parquet --year 2025 --max-days 3`：真实 stage cleaning 入口，按 `date + table` task 构建 parquet
- `make raw-inventory-2025` / `make raw-inventory-2026`：轻量编排入口
- `python -m pytest`：校验最小行为约束
- `python -m ruff check .`：保持脚本与规范一致

## 当前下一步

1. 对 `2025/2026` 的 full-year stage 产物跑 `coverage / schema / linkage` DQA
2. 继续推进 `2026` second-stage semantic verification 与 `2025` coarse-valid 研究边界
3. 按已定义准入 policy 实装 verified layer
4. 让 Query / report / verified 实现逐步接入新的 field/reference policy
5. 对 `coverage / schema / linkage` 的 full-year DQA 结果做正式收口

## Policy Navigation

- [policy_navigation_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/policy_navigation_2026-03-17.md)
- [query_report_policy_bridge_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/query_report_policy_bridge_2026-03-17.md)
- [broker_reference_readonly_boundary_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/broker_reference_readonly_boundary_2026-03-17.md)
