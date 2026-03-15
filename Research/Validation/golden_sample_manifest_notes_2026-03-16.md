# Golden Sample Manifest Notes 2026-03-16

## Scope

本笔记解释正式 `golden sample` 清单为什么这样冻结。

对应 manifest:

- [golden_sample_manifest.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/manifests/golden_sample_manifest.json)

## Selection Summary

### Smoke samples

- `2025-02-18`
- `2026-03-13`

这些日期已经被真实 stage smoke 使用过，因此适合作为 pipeline / schema / regression 的轻量冒烟入口。

### Representative samples

- `2025`: `2025-01-02 / 2025-06-12 / 2025-12-04`
- `2026`: `2026-01-05 / 2026-02-24 / 2026-03-13`

这些日期已经被 representative stage、semantic 或 admissibility 边界工作复用，因此适合作为固定复查入口。

### Golden anchors

- `2025`: `2025-06-12`
- `2026`: `2026-02-24`

选择原则：

- 处于年份内部更居中的位置
- 已在 representative / semantic 复查中使用过
- 不依赖异常 source group 才能解释

## Source Group Guardrail

- 主 golden sample 一律使用 `source_group_scope = main_only`
- `HKDarkPool` 不进入主 golden sample
- `2025-12-04` 保留为 representative sample，但必须显式带上 `HKDarkPool` 隔离 caveat

## Repo-Safe Conclusion

> The golden sample set is now frozen at the policy-and-date level for mainline work. It is designed to support reproducibility and regression, not to replace full-year evidence or semantic verification.
