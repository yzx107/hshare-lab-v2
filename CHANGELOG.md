# CHANGELOG

> Hshare Lab v2 reboot 留痕

---

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
