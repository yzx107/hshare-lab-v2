# Verified Layer 2025

- generated_at: 2026-03-24T16:22:21+00:00
- status: completed
- completed_count: 492
- failed_count: 0

## Selection

- label: full
- table: all
- selected_date_count: 246
- selected_task_count: 492
- first_selected_date: 2025-01-02
- last_selected_date: 2025-12-31

## Tables

### verified_orders
- partitions: 246
- rows: 23834665960
- bytes: 199275747160
- first_date: 2025-01-02
- last_date: 2025-12-31

### verified_trades
- partitions: 246
- rows: 926544356
- bytes: 5017887290
- first_date: 2025-01-02
- last_date: 2025-12-31

## Sample Partition Rows

### verified_orders 2025-04-07
- output_row_count: 210098849
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, SeqNum, OrderId, Time, Price, Volume
- excluded_columns: OrderType, Ext, Level, BrokerNo, VolumePre
- research_time_grade: coarse_only

### verified_trades 2025-04-07
- output_row_count: 8130346
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- research_time_grade: coarse_only

### verified_orders 2025-10-14
- output_row_count: 185755891
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, SeqNum, OrderId, Time, Price, Volume
- excluded_columns: OrderType, Ext, Level, BrokerNo, VolumePre
- research_time_grade: coarse_only

### verified_trades 2025-10-13
- output_row_count: 6381794
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- research_time_grade: coarse_only

