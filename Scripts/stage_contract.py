from __future__ import annotations

from dataclasses import dataclass

import polars as pl
import pyarrow as pa

NULL_TOKENS = ["", " ", "NULL", "null", "nan", "NaN"]


@dataclass(frozen=True)
class StageColumn:
    name: str
    polars_dtype: pl.DataType
    arrow_dtype: pa.DataType
    required_for_stage: bool = False


@dataclass(frozen=True)
class StageTableContract:
    table_name: str
    source_groups_by_year: dict[str, tuple[str, ...]]
    business_columns: tuple[StageColumn, ...]
    tech_columns: tuple[StageColumn, ...]

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.business_columns if column.required_for_stage)

    @property
    def all_columns(self) -> tuple[StageColumn, ...]:
        return self.business_columns + self.tech_columns

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.all_columns)

    @property
    def arrow_schema(self) -> pa.Schema:
        return pa.schema([pa.field(column.name, column.arrow_dtype) for column in self.all_columns])


TECH_COLUMNS = (
    StageColumn("date", pl.Date, pa.date32()),
    StageColumn("table_name", pl.String, pa.string()),
    StageColumn("source_file", pl.String, pa.string()),
    StageColumn("ingest_ts", pl.Datetime(time_unit="us", time_zone="UTC"), pa.timestamp("us", tz="UTC")),
    StageColumn("row_num_in_file", pl.Int64, pa.int64()),
)


TRADES_COLUMNS = (
    StageColumn("SendTimeRaw", pl.String, pa.string()),
    StageColumn("SendTime", pl.Datetime(time_unit="ns", time_zone="UTC"), pa.timestamp("ns", tz="UTC")),
    StageColumn("SeqNum", pl.Int64, pa.int64()),
    StageColumn("TickID", pl.Int64, pa.int64(), required_for_stage=True),
    StageColumn("Time", pl.String, pa.string(), required_for_stage=True),
    StageColumn("Price", pl.Float64, pa.float64(), required_for_stage=True),
    StageColumn("Volume", pl.Int64, pa.int64(), required_for_stage=True),
    StageColumn("Dir", pl.Int8, pa.int8()),
    StageColumn("Type", pl.String, pa.string()),
    StageColumn("BrokerNo", pl.String, pa.string()),
    StageColumn("BidOrderID", pl.Int64, pa.int64()),
    StageColumn("BidVolume", pl.Int64, pa.int64()),
    StageColumn("AskOrderID", pl.Int64, pa.int64()),
    StageColumn("AskVolume", pl.Int64, pa.int64()),
)


ORDERS_COLUMNS = (
    StageColumn("Channel", pl.Int32, pa.int32()),
    StageColumn("SendTimeRaw", pl.String, pa.string()),
    StageColumn("SendTime", pl.Datetime(time_unit="ns", time_zone="UTC"), pa.timestamp("ns", tz="UTC")),
    StageColumn("SeqNum", pl.Int64, pa.int64(), required_for_stage=True),
    StageColumn("OrderId", pl.Int64, pa.int64(), required_for_stage=True),
    StageColumn("OrderType", pl.Int16, pa.int16(), required_for_stage=True),
    StageColumn("Ext", pl.String, pa.string()),
    StageColumn("Time", pl.String, pa.string(), required_for_stage=True),
    StageColumn("Price", pl.Float64, pa.float64(), required_for_stage=True),
    StageColumn("Volume", pl.Int64, pa.int64(), required_for_stage=True),
    StageColumn("Level", pl.Int32, pa.int32()),
    StageColumn("BrokerNo", pl.String, pa.string()),
    StageColumn("VolumePre", pl.Int64, pa.int64()),
)


CONTRACTS = {
    "trades": StageTableContract(
        table_name="trades",
        source_groups_by_year={
            "2025": ("TradeResumes",),
            "2026": ("trade",),
        },
        business_columns=TRADES_COLUMNS,
        tech_columns=TECH_COLUMNS,
    ),
    "orders": StageTableContract(
        table_name="orders",
        source_groups_by_year={
            "2025": ("OrderAdd", "OrderModifyDelete"),
            "2026": ("order",),
        },
        business_columns=ORDERS_COLUMNS,
        tech_columns=TECH_COLUMNS,
    ),
}
