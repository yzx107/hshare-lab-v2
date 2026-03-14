# Hshare Lab v2

港股 tick / order / trade 研究基础设施重启版。

自 `2026-03-14` 起，本仓库进入 `reboot` 主线：旧版清洗、DQA、feature、查询逻辑统一降级为 `legacy evidence`，不再作为事实源或研究入口。

## 当前目标

当前主线不是继续全量重清洗，而是：

1. 保留 `raw layer`
2. 冻结 `candidate cleaned 2025`
3. 完成 `DQA + semantic verification`
4. 产出 `verified research-ready layer`
5. 再决定是否推广到更多年份

## 当前原则

- `raw` 不可变，只补 metadata
- `cleaning` 只做 mechanical transformation，不做主观语义解释
- `DQA` 必须 `visible + resumable`
- `semantic verification` 先于复杂 alpha
- 旧目录 `scripts/`、旧 `src/features/`、旧 DQA 报告均视为 `legacy evidence`

## 仓库入口

- [DATA_CONTRACT.md](/Users/yxin/AI_Workstation/Hshare_Lab/DATA_CONTRACT.md)
- [CLEANING_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab/CLEANING_SPEC.md)
- [DQA_SPEC.md](/Users/yxin/AI_Workstation/Hshare_Lab/DQA_SPEC.md)
- [SEMANTIC_MATRIX.md](/Users/yxin/AI_Workstation/Hshare_Lab/SEMANTIC_MATRIX.md)
- [LEGACY_STATUS.md](/Users/yxin/AI_Workstation/Hshare_Lab/LEGACY_STATUS.md)
- [PROGRESS.md](/Users/yxin/AI_Workstation/Hshare_Lab/PROGRESS.md)
- [TASKS.md](/Users/yxin/AI_Workstation/Hshare_Lab/TASKS.md)
- [CHANGELOG.md](/Users/yxin/AI_Workstation/Hshare_Lab/CHANGELOG.md)

## 新目录约定

```text
Hshare_Lab/
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
├── candidate_cleaned/       # 待验证 cleaned layer
├── dqa/                     # DQA 报告与中间产物
├── verified/                # verified research-ready layer
├── manifests/               # 数据清单
└── logs/                    # 长任务日志
```

旧版 `clean_parquet/` 和 `.tmp_parquet/` 已不再属于新主线。

## 当前下一步

1. 对 raw layer 建立 inventory 与 manifest
2. 定义 `candidate_cleaned_2025_v1` contract
3. 选取 `golden sample` 做语义验真
4. 按模块实现 DQA
5. 再决定 verified layer 的字段边界
