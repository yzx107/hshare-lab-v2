# Verified Layer 2025（已验证层）

- 生成时间: 2026-04-06T12:06:21+00:00
- 状态: completed
- 完成任务数: 492
- 失败任务数: 0

## 选择范围

- label: full
- table: all
- include_caveat_columns: none
- selected_date_count: 246
- selected_task_count: 492
- first_selected_date: 2025-01-02
- last_selected_date: 2025-12-31

## 数据表

### verified_orders
- partitions: 246
- rows: 23834665960
- bytes: 199365517597
- first_date: 2025-01-02
- last_date: 2025-12-31

### verified_trades
- partitions: 246
- rows: 926544356
- bytes: 5031636014
- first_date: 2025-01-02
- last_date: 2025-12-31

## 示例分区

### verified_trades 2025-09-18
- output_row_count: 5984036
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- included_caveat_columns: none
- excluded_columns: SendTime, Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: coarse_only

### verified_trades 2025-10-13
- output_row_count: 6381794
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- included_caveat_columns: none
- excluded_columns: SendTime, Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: coarse_only

### verified_trades 2025-04-07
- output_row_count: 8130346
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- included_caveat_columns: none
- excluded_columns: SendTime, Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: coarse_only

### verified_trades 2025-10-09
- output_row_count: 5806220
- included_columns: date, table_name, source_file, instrument_key, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- included_caveat_columns: none
- excluded_columns: SendTime, Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- admission_rule: admit_now_only
- research_time_grade: coarse_only

