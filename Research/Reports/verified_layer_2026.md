# Verified Layer 2026（已验证层）

- 生成时间: 2026-04-06T11:32:31+00:00
- 状态: completed
- 完成任务数: 124
- 失败任务数: 0

## 选择范围

- label: full
- table: all
- include_caveat_columns: none
- selected_date_count: 62
- selected_task_count: 124
- first_selected_date: 2026-01-02
- last_selected_date: 2026-04-02

## 数据表

### verified_orders
- partitions: 62
- rows: 8275120571
- bytes: 102043005580
- first_date: 2026-01-02
- last_date: 2026-04-02

### verified_trades
- partitions: 62
- rows: 260755499
- bytes: 2361682002
- first_date: 2026-01-02
- last_date: 2026-04-02

## 示例分区

### verified_trades 2026-03-03
- output_row_count: 5620681
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, SendTime, Price, Volume
- included_caveat_columns: none
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: fine_ok

### verified_trades 2026-03-09
- output_row_count: 5621631
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, SendTime, Price, Volume
- included_caveat_columns: none
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: fine_ok

### verified_trades 2026-03-04
- output_row_count: 5414180
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, SendTime, Price, Volume
- included_caveat_columns: none
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: fine_ok

### verified_trades 2026-03-23
- output_row_count: 5443182
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, SendTime, Price, Volume
- included_caveat_columns: none
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: fine_ok

