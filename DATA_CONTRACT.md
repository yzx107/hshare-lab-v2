# DATA_CONTRACT

## 目标

定义 Hshare Lab v2 的数据层职责、路径和不可变规则。

## Layers

### Layer 0: Raw
- **路径**:
  - `/Volumes/Data/港股Tick数据/2025`
  - `/Volumes/Data/港股Tick数据/2026`
- **规则**:
  - 不覆盖
  - 不改写业务内容
  - 只补 manifest / metadata
  - vendor 说明文件属于 supporting reference，不属于可改写业务数据

### Layer 1: Stage / Candidate Cleaned
- **路径**: `/Volumes/Data/港股Tick数据/candidate_cleaned`
- **规则**:
  - 这是 `stage parquet` 层，路径名仍保留为 `candidate_cleaned`
  - 只做 loss-minimizing typed projection
  - 保持原始记录粒度
  - 原始字段尽量全保留
  - 只允许技术列与工程标准化
  - 不提前注入业务语义
  - 不直接作为研究事实源
  - 可重建、可删除重跑、可版本化，但不得冒充 verified truth

### Layer 2: DQA
- **路径**: `/Volumes/Data/港股Tick数据/dqa`
- **规则**:
  - 输出 coverage、schema、validity、sequence、session、linkage、broker 报告
  - 长任务必须 `visible + resumable`

### Layer 3: Verified
- **路径**: `/Volumes/Data/港股Tick数据/verified`
- **规则**:
  - 只包含 mechanically safe 或语义已验证字段
  - 所有研究脚本优先从 verified layer 读取

### Supporting Layers
- **manifests**: `/Volumes/Data/港股Tick数据/manifests`
- **logs**: `/Volumes/Data/港股Tick数据/logs`
- **references**: `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/References`
  - 保存 vendor notice、vendor readme、broker / participant reference
  - 这些 reference 可支持 contract、query join 和 DQA 对照
  - 但它们本身不自动放行字段为 research-verified semantics

## Repo Conventions

- 新脚本统一放在 `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Scripts`
- 研究输出统一放在 `/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research`
- 旧 `scripts/` 目录视为 legacy

## Stage Layer 最小技术列

- `date`
- `source_file`
- `ingest_batch`
- `row_num_in_file`（可选）

这些列用于追溯、审计、分区和重跑，不代表字段语义已验证。

## Vendor Definition Boundary

If a field appears in vendor reference documents, its status becomes:

- `vendor-defined`

not:

- `research-verified`

Current examples include:

- `OrderType`
- `Ext`
- `Dir`
- `Level`
- `VolumePre`
- `BrokerNo`

These fields may be retained in stage, referenced in DQA, and joined to external reference tables,
but they still require semantic verification before being treated as research-safe truth.
