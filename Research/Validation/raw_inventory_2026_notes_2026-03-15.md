# Raw Inventory 2026 Notes 2026-03-15

## Scope

本笔记记录 `2026` full-year raw inventory 的首轮轻量解释结论。

来源：

- `/Volumes/Data/港股Tick数据/manifests/raw_inventory_2026/summary.json`
- `/Volumes/Data/港股Tick数据/manifests/raw_inventory_2026/files.jsonl`

## Summary

- `status = completed`
- `files_scanned = 52`
- `bytes_scanned = 89,509,988,059`
- `distinct_trade_dates = 49`
- `date_coverage_start = 2026-01-01`
- `date_coverage_end = 2026-03-13`
- `zero_byte_files = 0`
- `unknown_date_files = 3`

## Interpreted Explanation

### 1. `unknown_date_files = 3` is expected and non-blocking

The three unknown-date files are:

- `.DS_Store`
- `doc/ReadMe.txt`
- `doc/brokerno.csv`

These are support files rather than daily trading packages.

Therefore:

- they should remain visible in raw inventory
- they should not be interpreted as failed date extraction on core trade zips

### 2. `date_coverage_start = 2026-01-01` is driven by a notice file

The parsed `2026-01-01` file is:

- `doc/关于2026-01-01数据升级说明.txt`

This means the start date reflects:

- folder-level file coverage

not necessarily:

- first trading zip date

### 3. Core 2026 trading package set looks structurally clean

From current inventory:

- `.zip` remains the dominant suffix
- there are `48` zip files
- `zero_byte_files = 0`

This supports a clean first-pass raw completeness view for the current `2026` folder contents.

## Repo-Safe Conclusion

> The 2026 raw inventory completed successfully. The reported `2026-01-01` coverage start is driven by a vendor notice text file under `doc/`, while the three unknown-date files are support files rather than missing-date trading packages. The current raw inventory result is structurally healthy.
