# Verified Layer 2026

- generated_at: 2026-03-18T17:26:45+00:00
- status: completed
- completed_count: 96
- failed_count: 0

## Tables

### verified_orders
- partitions: 48
- rows: 6310242076
- bytes: 53247397725
- first_date: 2026-01-02
- last_date: 2026-03-13

### verified_trades
- partitions: 48
- rows: 198813212
- bytes: 1063324641
- first_date: 2026-01-02
- last_date: 2026-03-13

## Sample Partition Rows

### verified_orders 2026-01-02
- output_row_count: 73819435
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, SeqNum, OrderId, Time, Price, Volume
- excluded_columns: OrderType, Ext, Level, BrokerNo, VolumePre
- research_time_grade: fine_ok

### verified_trades 2026-01-02
- output_row_count: 2104339
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- research_time_grade: fine_ok

### verified_orders 2026-01-05
- output_row_count: 116296039
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, SeqNum, OrderId, Time, Price, Volume
- excluded_columns: OrderType, Ext, Level, BrokerNo, VolumePre
- research_time_grade: fine_ok

### verified_trades 2026-01-05
- output_row_count: 4286164
- included_columns: date, table_name, source_file, ingest_ts, row_num_in_file, TickID, Time, Price, Volume
- excluded_columns: Type, Dir, BrokerNo, BidOrderID, BidVolume, AskOrderID, AskVolume
- research_time_grade: fine_ok

