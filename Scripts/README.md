# Scripts

本目录是 Hshare Lab v2 的新脚本入口。

## 规则

- 新主线脚本只放在本目录
- 所有长任务必须 `visible + resumable`
- 默认输出日志到外置盘 `logs/`
- 默认输出 manifest 到外置盘 `manifests/`

## 预期脚本

- `build_raw_inventory.py`
- `freeze_candidate_cleaned.py`
- `run_dqa_coverage.py`
- `run_dqa_schema.py`
- `run_dqa_linkage.py`
- `build_verified_layer.py`

旧的 lowercase `scripts/` 目录视为 legacy，不再作为新主线入口。
