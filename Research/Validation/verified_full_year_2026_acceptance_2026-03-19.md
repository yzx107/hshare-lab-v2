# Verified Full-Year 2026 Acceptance 2026-03-19

## Scope

本页记录 `verified layer v1` 在 `2026` full-year materialization 完成后的验收结论。

它只回答：

- full-year `verified_orders / verified_trades` 是否完整落盘
- manifest / checkpoint / 分区输出是否与当前 verified policy 一致
- 已完成的 smoke / representative sample 结果，是否与 full-year 对应日期完全一致

## Inputs

- [verified_layer_v1_design.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_layer_v1_design.md)
- [verified_admission_matrix_2026-03-18.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_matrix_2026-03-18.md)
- [verified_field_policy_2026-03-15.json](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_field_policy_2026-03-15.json)
- `/Volumes/Data/港股Tick数据/verified/manifests/year=2026/summary.json`
- `/Volumes/Data/港股Tick数据/verified/manifests/year=2026/checkpoint.json`
- `/Volumes/Data/港股Tick数据/verified/manifests/year=2026/verified_partitions.jsonl`
- `/Volumes/Data/港股Tick数据/logs/build_verified_layer_2026.log`

## Final Outcome

- output root: `/Volumes/Data/港股Tick数据/verified`
- run status: `completed`
- completed tasks: `96 / 96`
- failed tasks: `0`
- pending tasks: `0`
- tables materialized:
  - `verified_orders`
  - `verified_trades`

### Table Totals

| table | partitions | dates | rows | bytes |
| --- | ---: | ---: | ---: | ---: |
| `verified_orders` | 48 | 48 | 6,310,242,076 | 53,247,397,725 |
| `verified_trades` | 48 | 48 | 198,813,212 | 1,063,324,641 |

- total parquet bytes in summary: `54,310,722,366`
- disk usage on external SSD: about `51G`

## Policy Consistency Check

对 `96` 条 `verified_partitions.jsonl` 记录做全量一致性检查后：

- `admission_rule = admit_now_only`
- `contains_caveat_fields = false`
- `reference_join_applied = false`
- `source_layer = candidate_cleaned`
- `verified_policy_version = 2026-03-15`
- `research_time_grade = fine_ok`

并且：

- `verified_orders` 全年只有 `1` 套固定 `included_columns`
- `verified_trades` 全年只有 `1` 套固定 `included_columns`
- `verified_orders` 全年只有 `1` 套固定 `excluded_columns`
- `verified_trades` 全年只有 `1` 套固定 `excluded_columns`

这说明 full-year materialization 没有跨日期漂移，也没有把 caveat 字段默认混入 verified v1。

## Smoke / Sample Reconciliation

full-year 输出与此前真实数据验收结果做按日期回对：

- `2026-01-02` 与 3-day sample 完全一致
  - `verified_orders`: rows / bytes 均一致
  - `verified_trades`: rows / bytes 均一致
- `2026-02-09` 与 3-day sample 完全一致
  - `verified_orders`: rows / bytes 均一致
  - `verified_trades`: rows / bytes 均一致
- `2026-03-13` 与 3-day sample 完全一致
  - `verified_orders`: rows / bytes 均一致
  - `verified_trades`: rows / bytes 均一致
- `2026-03-13` 与 single-day smoke 完全一致
  - `verified_orders`: rows / bytes 均一致
  - `verified_trades`: rows / bytes 均一致

因此，已通过的 smoke / sample 结果没有在 full-year run 中发生漂移。

## Output Integrity Check

- `verified_orders/year=2026/date=*` 共 `48` 个日期目录
- `verified_trades/year=2026/date=*` 共 `48` 个日期目录
- 每个日期当前均落为单个 `part-00000.parquet`
- 未发现残留隐藏 parquet 临时文件、`.tmp`、`.partial` 或 `._*` 垃圾文件

这与本轮工程优化目标一致：允许更安全的 resume / 原子落盘，但不改变 verified 语义边界。

## Heavy Partitions

orders 仍然是全年主瓶颈。按 `output_row_count / output_bytes` 看，最重的几个 `orders`
分区是：

- `2026-03-09`: `211,170,311` rows, `1,760,772,412` bytes
- `2026-03-04`: `210,114,383` rows, `1,753,847,665` bytes
- `2026-02-03`: `192,807,940` rows, `1,622,875,145` bytes
- `2026-03-03`: `191,720,512` rows, `1,592,311,447` bytes
- `2026-02-05`: `183,314,734` rows, `1,539,354,183` bytes

trades 全年明显更轻，最大分区仍在数百万行量级。

## Boundary Reminder

本次 full-year 完成并不改变 verified v1 的边界：

- 仍只 materialize `verified_orders / verified_trades`
- 仍只放行 `admit_now` 字段
- 仍不默认放行 `BidOrderID / AskOrderID / BrokerNo / Dir / Level / VolumePre / Type / Ext / OrderType`
- `2026` 仍按 `research_time_grade = fine_ok` 使用
- 若后续扩表或放行 caveat 字段，必须先更新 policy / admission boundary，而不是直接改 build 结果

## Acceptance Conclusion

当前可以把 `2026 verified layer v1 full-year build` 记为：

- `build_status = accepted`
- `output_status = complete`
- `policy_status = conforming`
- `sample_reconciliation = exact_match`
- `retry_resume_safety = validated_in_real_run`

结论上，`2026` verified full-year 输出已达到可交付、可追溯、可恢复的 v1 验收状态。
