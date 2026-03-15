# Raw Inventory 2025 Notes 2026-03-15

## Scope

本笔记记录 `2025` full-year raw inventory 的首轮轻量解释结论。

来源：

- `/Volumes/Data/港股Tick数据/manifests/raw_inventory_2025/summary.json`
- `/Volumes/Data/港股Tick数据/manifests/raw_inventory_2025/files.jsonl`
- `/Volumes/Data/港股Tick数据/manifests/raw_inventory_2025/date_summary.json`

## Summary

- `status = completed`
- `files_scanned = 250`
- `bytes_scanned = 293,335,032,932`
- `distinct_trade_dates = 247`
- `date_coverage_start = 2025-01-02`
- `date_coverage_end = 2026-01-01`
- `zero_byte_files = 0`
- `unknown_date_files = 3`

## Interpreted Explanation

### 1. `unknown_date_files = 3` is expected and non-blocking

The three unknown-date files are:

- `.DS_Store`
- `Doc/ReadMe.txt`
- `Doc/brokerno.csv`

These are non-trading support files under the raw year folder.

Therefore:

- they should remain visible in raw inventory
- they should not be treated as missing trading-day evidence
- they do not indicate a date-parsing failure on core daily zip files

### 2. `date_coverage_end = 2026-01-01` comes from a document file, not a 2026 trade zip

The parsed `2026-01-01` file is:

- `Doc/关于2026-01-01数据升级说明.txt`

This means:

- raw inventory is correctly scanning all files under the `2025` raw folder
- the max parsed date is influenced by a source-contract notice file
- the current date parser is file-path oriented, not trade-data-only

Therefore the `2026-01-01` end date should be interpreted as:

- folder-level file coverage end

not:

- last 2025 trading data date

### 3. Core 2025 trading files still appear structurally clean

From current inventory:

- the dominant suffix is `.zip`
- there are `246` zip files
- `zero_byte_files = 0`

This supports a clean first-pass conclusion that the core daily raw trading package set is present and non-empty.

## Practical Follow-Up

For later reporting, prefer distinguishing:

- `inventory_file_coverage_end`
- `inventory_trade_zip_coverage_end`

If needed, a future inventory refinement can exclude `Doc/` support files from the trade-date coverage calculation while still keeping them in the raw manifest.

## Repo-Safe Conclusion

> The 2025 raw inventory completed successfully. The reported `2026-01-01` coverage end is driven by a vendor notice text file under `Doc/`, not by a 2026 trading-data zip. The three unknown-date files are support files rather than missing-date trading packages, so the current inventory result is structurally healthy.
