from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.hshare_orderbook_replay import (
    SORT_MODES,
    HshareOrderBookReplay,
    as_decimal,
    as_int,
    default_table_path,
    event_sort_key,
    source_suffix,
    stringify_time,
    symbol_code,
)
from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    write_json,
)

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "caveat"
DEFAULT_RESEARCH_ROOT = Path(__file__).resolve().parents[1] / "Research"
NAMESPACE = "orderbook_replay__top_of_book_with_size_caveat"
SIZE_CAVEAT = "bounded_active_order_volume_replay_ext0_not_verified_executable_queue_size"

ORDER_COLUMNS = [
    "SendTime",
    "Time",
    "SeqNum",
    "OrderId",
    "OrderType",
    "Ext",
    "Price",
    "Volume",
    "Level",
    "VolumePre",
    "source_file",
]
TRADE_COLUMNS = [
    "SendTime",
    "Time",
    "SeqNum",
    "TickID",
    "Price",
    "Volume",
    "BidOrderID",
    "AskOrderID",
    "source_file",
]
OUTPUT_SCHEMA: dict[str, pl.DataType] = {
    "date": pl.Utf8,
    "symbol": pl.Utf8,
    "namespace": pl.Utf8,
    "source_layer": pl.Utf8,
    "SendTime": pl.Utf8,
    "TickID": pl.Int64,
    "SeqNum": pl.Int64,
    "TradePrice": pl.Float64,
    "TradeVolume": pl.Int64,
    "BestBidReplay": pl.Float64,
    "BestBidSizeReplay": pl.Int64,
    "BestAskReplay": pl.Float64,
    "BestAskSizeReplay": pl.Int64,
    "ReplaySpread": pl.Float64,
    "ReplayMid": pl.Float64,
    "TopOfBookValidFlag": pl.Boolean,
    "ReplayQualityScore": pl.Float64,
    "CrossedWindowFlag": pl.Boolean,
    "ReplayResidueFlag": pl.Boolean,
    "ReplayWindowExcludedFlag": pl.Boolean,
    "SameMillisecondBatchRiskFlag": pl.Boolean,
    "SizeSemanticsCaveat": pl.Utf8,
    "StrategyHandoffEligibleFlag": pl.Boolean,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build bounded top-of-book with size caveat handoff for OpenD agent replay."
    )
    parser.add_argument("--universe-path", type=Path, required=True)
    parser.add_argument("--dates", help="Comma-separated trading dates.")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--symbols", help="Optional comma-separated symbols.")
    parser.add_argument("--stage-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument("--sort-mode", choices=sorted(SORT_MODES), default="send_seq_order_first")
    parser.add_argument("--side-bit", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--heartbeat", action="store_true")
    parser.add_argument("--write-research-report", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.side_bit != 0:
        raise SystemExit("Only side-bit 0 is admitted for this caveated handoff.")
    dates = select_dates(args)
    if not dates:
        raise SystemExit("--dates or --date-from/--date-to must select at least one date")
    symbols = select_symbols(args.universe_path, args.symbols)
    if not symbols:
        raise SystemExit("No included universe symbols matched the requested filter.")

    logger = configure_logger(
        "build_opend_agent_strategy_handoff",
        args.log_root / NAMESPACE / "last_run.log",
    )
    manifest_dir = args.output_root / NAMESPACE / "manifests"
    ensure_dir(manifest_dir)
    partitions_path = manifest_dir / "partitions.jsonl"
    summary_path = manifest_dir / "summary.json"
    heartbeat_path = manifest_dir / "heartbeat.json"
    if args.overwrite_existing:
        reset_files([partitions_path, summary_path, heartbeat_path])

    existing = load_existing_partitions(partitions_path) if args.resume else {}
    total_tasks = len(dates) * len(symbols)
    completed = 0
    failures: list[dict[str, Any]] = []
    partition_records = dict(existing)
    for trade_date in dates:
        pending_symbols: list[str] = []
        for symbol in symbols:
            path = output_path(args.output_root, date=trade_date, symbol=symbol)
            key = partition_key(trade_date, symbol)
            if args.resume and not args.overwrite_existing and path.exists():
                partition_records[key] = partition_row(
                    trade_date,
                    symbol,
                    path,
                    resumed=True,
                    output_rows=None,
                )
                completed += 1
                maybe_write_heartbeat(
                    args.heartbeat, heartbeat_path, total_tasks, completed, failures
                )
                continue
            pending_symbols.append(symbol)
        if not pending_symbols:
            continue
        order_rows_by_symbol = read_date_symbol_rows(
            args.stage_root,
            trade_date,
            table="orders",
            columns=ORDER_COLUMNS,
            symbols=pending_symbols,
            limit_rows=args.limit_rows,
        )
        trade_rows_by_symbol = read_date_symbol_rows(
            args.stage_root,
            trade_date,
            table="trades",
            columns=TRADE_COLUMNS,
            symbols=pending_symbols,
            limit_rows=args.limit_rows,
        )
        for symbol in pending_symbols:
            path = output_path(args.output_root, date=trade_date, symbol=symbol)
            key = partition_key(trade_date, symbol)
            try:
                rows = materialize_rows(
                    date=trade_date,
                    symbol=symbol,
                    order_rows=order_rows_by_symbol.get(symbol, []),
                    trade_rows=trade_rows_by_symbol.get(symbol, []),
                    sort_mode=args.sort_mode,
                    side_bit=args.side_bit,
                )
                write_parquet(rows, path)
                partition_records[key] = partition_row(
                    trade_date,
                    symbol,
                    path,
                    resumed=False,
                    output_rows=len(rows),
                )
                logger.info("completed %s:%s rows=%s", trade_date, symbol, len(rows))
            except Exception as exc:  # pragma: no cover - operational guardrail
                failure = {"date": trade_date, "symbol": f"HK.{symbol}", "error": repr(exc)}
                failures.append(failure)
                logger.exception("failed %s:%s", trade_date, symbol)
            completed += 1
            maybe_write_heartbeat(args.heartbeat, heartbeat_path, total_tasks, completed, failures)

    write_partitions(partitions_path, partition_records)
    summary = build_summary(args, list(partition_records.values()), failures)
    write_json(summary_path, summary)
    if args.write_research_report:
        report_path = write_handoff_report(args.research_root, summary)
        summary["research_report"] = str(report_path)
        write_json(summary_path, summary)
        write_json_summary(args.research_root, summary)
    maybe_write_heartbeat(
        args.heartbeat, heartbeat_path, total_tasks, completed, failures, completed_status=True
    )
    print(json.dumps(compact_summary(summary), ensure_ascii=False, indent=2))
    return 1 if failures else 0


def select_dates(args: argparse.Namespace) -> list[str]:
    if args.dates:
        return sorted({value.strip() for value in args.dates.split(",") if value.strip()})
    dates = sorted(
        {
            path.name.split("=", 1)[1]
            for table in ("orders", "trades")
            for path in (args.stage_root / table).glob("date=*")
            if path.is_dir()
        }
    )
    if args.date_from:
        dates = [value for value in dates if value >= args.date_from]
    if args.date_to:
        dates = [value for value in dates if value <= args.date_to]
    return dates


def select_symbols(universe_path: Path, symbols_arg: str | None) -> list[str]:
    if universe_path.suffix == ".csv":
        frame = pl.read_csv(universe_path, infer_schema_length=None)
    else:
        frame = pl.read_parquet(universe_path)
    selected = frame.filter(pl.col("universe_status") == "included")
    if symbols_arg:
        requested = {symbol_code(value) for value in symbols_arg.split(",") if value.strip()}
        selected = selected.filter(
            pl.col("instrument_key").cast(pl.Utf8).str.zfill(5).is_in(requested)
        )
    return sorted(
        selected.get_column("instrument_key").cast(pl.Utf8).str.zfill(5).unique().to_list()
    )


def read_symbol_rows(
    stage_root: Path,
    trade_date: str,
    symbol: str,
    limit_rows: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        read_rows(
            default_table_path(stage_root, table="orders", date=trade_date),
            symbol,
            ORDER_COLUMNS,
            limit_rows,
        ),
        read_rows(
            default_table_path(stage_root, table="trades", date=trade_date),
            symbol,
            TRADE_COLUMNS,
            limit_rows,
        ),
    )


def read_rows(path: Path, symbol: str, columns: list[str], limit_rows: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    frame = pl.scan_parquet(str(path))
    selected = [column for column in columns if column in frame.collect_schema().names()]
    frame = frame.filter(pl.col("source_file").str.ends_with(source_suffix(symbol))).select(
        selected
    )
    if limit_rows > 0:
        frame = frame.limit(limit_rows)
    return frame.collect().to_dicts()


def read_date_symbol_rows(
    stage_root: Path,
    trade_date: str,
    *,
    table: str,
    columns: list[str],
    symbols: list[str],
    limit_rows: int,
) -> dict[str, list[dict[str, Any]]]:
    path = default_table_path(stage_root, table=table, date=trade_date)
    output = {symbol: [] for symbol in symbols}
    if not path.exists() or not symbols:
        return output
    prefix = "order" if table == "orders" else "trade"
    source_files = {f"{prefix}/{symbol_code(symbol)}.csv" for symbol in symbols}
    frame = pl.scan_parquet(str(path))
    selected = [column for column in columns if column in frame.collect_schema().names()]
    rows = (
        frame.filter(pl.col("source_file").is_in(source_files))
        .select(selected)
        .collect()
        .to_dicts()
    )
    counts: Counter[str] = Counter()
    for row in rows:
        symbol = symbol_code(str(row.get("source_file", "")).rsplit("/", 1)[-1].replace(".csv", ""))
        if symbol not in output:
            continue
        if limit_rows > 0 and counts[symbol] >= limit_rows:
            continue
        output[symbol].append(row)
        counts[symbol] += 1
    return output


def materialize_rows(
    *,
    date: str,
    symbol: str,
    order_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    sort_mode: str,
    side_bit: int,
) -> list[dict[str, Any]]:
    events = [("order", row) for row in order_rows] + [("trade", row) for row in trade_rows]
    event_time_counts = Counter(event_time(row) for _, row in events)
    events.sort(key=lambda item: event_sort_key(item[0], item[1], sort_mode=sort_mode))
    replay = HshareOrderBookReplay(side_bit=side_bit, sort_mode=sort_mode)
    rows: list[dict[str, Any]] = []
    for kind, row in events:
        if kind == "order":
            replay.apply_order(row)
        else:
            rows.append(handoff_row(date, symbol, replay, row, event_time_counts))
            replay.apply_trade_probe(row)
    return rows


def handoff_row(
    trade_date: str,
    symbol: str,
    replay: HshareOrderBookReplay,
    row: dict[str, Any],
    event_time_counts: Counter[str],
) -> dict[str, Any]:
    best_bid, bid_size, best_ask, ask_size = best_book_with_size(replay)
    trade_price = as_decimal(row.get("Price"))
    crossed = best_bid is not None and best_ask is not None and best_bid > best_ask
    same_ms_risk = event_time_counts[event_time(row)] > 1
    residue = crossed
    excluded = crossed
    has_positive_prices = (
        best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0
    )
    has_positive_sizes = (
        bid_size is not None and ask_size is not None and bid_size > 0 and ask_size > 0
    )
    valid = (
        has_positive_prices and not crossed and not residue and not excluded and not same_ms_risk
    )
    spread = (
        best_ask - best_bid if valid and best_ask is not None and best_bid is not None else None
    )
    mid = (best_bid + best_ask) / Decimal("2") if spread is not None else None
    quality_score = 1.0 if valid else 0.0
    eligible = (
        valid
        and quality_score == 1.0
        and has_positive_sizes
        and best_ask is not None
        and best_bid is not None
        and best_ask >= best_bid
    )
    return {
        "date": trade_date,
        "symbol": f"HK.{symbol_code(symbol)}",
        "namespace": NAMESPACE,
        "source_layer": "candidate_cleaned",
        "SendTime": stringify_time(row.get("SendTime")),
        "TickID": as_int(row.get("TickID")),
        "SeqNum": as_int(row.get("SeqNum")),
        "TradePrice": decimal_to_float(trade_price),
        "TradeVolume": as_int(row.get("Volume")),
        "BestBidReplay": decimal_to_float(best_bid),
        "BestBidSizeReplay": bid_size,
        "BestAskReplay": decimal_to_float(best_ask),
        "BestAskSizeReplay": ask_size,
        "ReplaySpread": decimal_to_float(spread),
        "ReplayMid": decimal_to_float(mid),
        "TopOfBookValidFlag": bool(valid),
        "ReplayQualityScore": quality_score,
        "CrossedWindowFlag": bool(crossed),
        "ReplayResidueFlag": bool(residue),
        "ReplayWindowExcludedFlag": bool(excluded),
        "SameMillisecondBatchRiskFlag": bool(same_ms_risk),
        "SizeSemanticsCaveat": size_caveat(crossed, same_ms_risk, has_positive_sizes),
        "StrategyHandoffEligibleFlag": bool(eligible),
    }


def best_book_with_size(
    replay: HshareOrderBookReplay,
) -> tuple[Decimal | None, int | None, Decimal | None, int | None]:
    bids = {price: volume for price, volume in replay.book["BID"].items() if volume > 0}
    asks = {price: volume for price, volume in replay.book["ASK"].items() if volume > 0}
    best_bid = max(bids) if bids else None
    best_ask = min(asks) if asks else None
    return (
        best_bid,
        bids.get(best_bid) if best_bid is not None else None,
        best_ask,
        asks.get(best_ask) if best_ask is not None else None,
    )


def size_caveat(crossed: bool, same_ms_risk: bool, has_positive_sizes: bool) -> str:
    if crossed:
        return "blocked_crossed_window_size_not_strategy_ready"
    if same_ms_risk:
        return "blocked_same_millisecond_batch_ordering_size_not_strategy_ready"
    if not has_positive_sizes:
        return "blocked_missing_best_bid_or_ask_size"
    return SIZE_CAVEAT


def event_time(row: dict[str, Any]) -> str:
    return stringify_time(row.get("SendTime") or row.get("Time"))


def decimal_to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, infer_schema_length=None).select(list(OUTPUT_SCHEMA)).write_parquet(
            path
        )
        return
    pl.DataFrame(schema=OUTPUT_SCHEMA).write_parquet(path)


def output_path(output_root: Path, *, date: str, symbol: str) -> Path:
    return (
        output_root
        / NAMESPACE
        / "top_of_book_events"
        / f"year={date[:4]}"
        / f"date={date}"
        / f"symbol={symbol_code(symbol)}"
        / "part-00000.parquet"
    )


def partition_key(trade_date: str, symbol: str) -> str:
    return f"{trade_date}:HK.{symbol_code(symbol)}"


def partition_row(
    trade_date: str,
    symbol: str,
    output: Path,
    *,
    resumed: bool,
    output_rows: int | None,
) -> dict[str, Any]:
    stats = partition_stats(output)
    if output_rows is None:
        output_rows = int(stats["total_rows"])
    return {
        "generated_at": iso_utc_now(),
        "date": trade_date,
        "symbol": f"HK.{symbol_code(symbol)}",
        "namespace": NAMESPACE,
        "path": str(output),
        "rows": output_rows,
        "eligible_rows": stats["eligible_rows"],
        "crossed_rows": stats["crossed_rows"],
        "same_ms_risk_rows": stats["same_ms_risk_rows"],
        "residue_rows": stats["residue_rows"],
        "no_size_rows": stats["no_size_rows"],
        "resumed": resumed,
    }


def partition_stats(path: Path) -> dict[str, int]:
    if not path.exists():
        return empty_stats()
    frame = pl.scan_parquet(str(path))
    if not frame.collect_schema().names():
        return empty_stats()
    row = (
        frame.select(
            pl.len().alias("total_rows"),
            pl.col("StrategyHandoffEligibleFlag").fill_null(False).sum().alias("eligible_rows"),
            pl.col("CrossedWindowFlag").fill_null(False).sum().alias("crossed_rows"),
            pl.col("SameMillisecondBatchRiskFlag")
            .fill_null(False)
            .sum()
            .alias("same_ms_risk_rows"),
            pl.col("ReplayResidueFlag").fill_null(False).sum().alias("residue_rows"),
            (
                (pl.col("BestBidSizeReplay").fill_null(0) <= 0)
                | (pl.col("BestAskSizeReplay").fill_null(0) <= 0)
            )
            .sum()
            .alias("no_size_rows"),
        )
        .collect()
        .to_dicts()[0]
    )
    return {key: int(value or 0) for key, value in row.items()}


def empty_stats() -> dict[str, int]:
    return {
        "total_rows": 0,
        "eligible_rows": 0,
        "crossed_rows": 0,
        "same_ms_risk_rows": 0,
        "residue_rows": 0,
        "no_size_rows": 0,
    }


def load_existing_partitions(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        rows[partition_key(row["date"], row["symbol"])] = row
    return rows


def write_partitions(path: Path, records: dict[str, dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    tmp = path.with_name(f"{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in sorted(records.values(), key=lambda item: (item["date"], item["symbol"])):
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    tmp.replace(path)


def reset_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def maybe_write_heartbeat(
    enabled: bool,
    path: Path,
    total_tasks: int,
    completed_tasks: int,
    failures: list[dict[str, Any]],
    *,
    completed_status: bool = False,
) -> None:
    if not enabled:
        return
    write_json(
        path,
        {
            "generated_at": iso_utc_now(),
            "status": "completed" if completed_status else "running",
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "pending_tasks": max(total_tasks - completed_tasks, 0),
            "failure_count": len(failures),
            "failures": failures[-20:],
        },
    )


def build_summary(
    args: argparse.Namespace,
    partitions: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    total_rows = sum(int(row["rows"]) for row in partitions)
    eligible_rows = sum(int(row["eligible_rows"]) for row in partitions)
    crossed_rows = sum(int(row["crossed_rows"]) for row in partitions)
    same_ms_rows = sum(int(row["same_ms_risk_rows"]) for row in partitions)
    residue_rows = sum(int(row["residue_rows"]) for row in partitions)
    no_size_rows = sum(int(row["no_size_rows"]) for row in partitions)
    per_symbol = aggregate(partitions, "symbol")
    per_date = aggregate(partitions, "date")
    blockers = []
    if eligible_rows == 0:
        blockers.append("no_strategy_handoff_eligible_rows_after_quality_gates")
    if same_ms_rows:
        blockers.append("same_millisecond_batch_ordering_present")
    if crossed_rows:
        blockers.append("crossed_book_windows_present")
    if no_size_rows:
        blockers.append("missing_or_non_positive_best_bid_ask_size_present")
    return {
        "generated_at": iso_utc_now(),
        "pipeline": "build_opend_agent_strategy_handoff",
        "namespace": NAMESPACE,
        "source_layer": "candidate_cleaned",
        "output_root": str(args.output_root / NAMESPACE),
        "universe_path": str(args.universe_path),
        "dates": sorted({row["date"] for row in partitions}),
        "symbols": sorted({row["symbol"] for row in partitions}),
        "partition_count": len(partitions),
        "failure_count": len(failures),
        "failures": failures,
        "total_rows": total_rows,
        "eligible_rows": eligible_rows,
        "eligible_ratio": rate(eligible_rows, total_rows),
        "crossed_ratio": rate(crossed_rows, total_rows),
        "same_ms_risk_ratio": rate(same_ms_rows, total_rows),
        "residue_ratio": rate(residue_rows, total_rows),
        "no_size_ratio": rate(no_size_rows, total_rows),
        "per_symbol": per_symbol,
        "per_date": per_date,
        "blockers": blockers,
        "size_semantics_boundary": SIZE_CAVEAT,
    }


def aggregate(partitions: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    totals: dict[str, Counter[str]] = defaultdict(Counter)
    for row in partitions:
        item = totals[str(row[key])]
        item["rows"] += int(row["rows"])
        item["eligible_rows"] += int(row["eligible_rows"])
        item["crossed_rows"] += int(row["crossed_rows"])
        item["same_ms_risk_rows"] += int(row["same_ms_risk_rows"])
        item["residue_rows"] += int(row["residue_rows"])
        item["no_size_rows"] += int(row["no_size_rows"])
    output = []
    for value, counter in sorted(totals.items()):
        rows = int(counter["rows"])
        output.append(
            {
                key: value,
                "rows": rows,
                "eligible_rows": int(counter["eligible_rows"]),
                "eligible_ratio": rate(counter["eligible_rows"], rows),
                "crossed_ratio": rate(counter["crossed_rows"], rows),
                "same_ms_risk_ratio": rate(counter["same_ms_risk_rows"], rows),
                "residue_ratio": rate(counter["residue_rows"], rows),
                "no_size_ratio": rate(counter["no_size_rows"], rows),
            }
        )
    return output


def rate(numerator: int | float, denominator: int | float) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def write_handoff_report(research_root: Path, summary: dict[str, Any]) -> Path:
    path = research_root / "Reports" / f"opend_agent_strategy_handoff_{datetime.now():%Y%m%d}.md"
    ensure_dir(path.parent)
    lines = [
        "# OpenD Agent Strategy Handoff",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- namespace: `{summary['namespace']}`",
        f"- output_root: `{summary['output_root']}`",
        f"- processed_symbols: {len(summary['symbols'])}",
        f"- processed_dates: {len(summary['dates'])}",
        f"- total_rows: {summary['total_rows']}",
        f"- eligible_rows: {summary['eligible_rows']}",
        f"- eligible_ratio: {summary['eligible_ratio']}",
        f"- crossed_ratio: {summary['crossed_ratio']}",
        f"- same_ms_risk_ratio: {summary['same_ms_risk_ratio']}",
        f"- residue_ratio: {summary['residue_ratio']}",
        f"- no_size_ratio: {summary['no_size_ratio']}",
        f"- blockers: {', '.join(summary['blockers']) if summary['blockers'] else 'none'}",
        "",
        "## Per Symbol Eligibility",
        "",
        "| symbol | rows | eligible | eligible_ratio | crossed_ratio | "
        "same_ms_risk_ratio | residue_ratio | no_size_ratio |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["per_symbol"]:
        lines.append(
            f"| {row['symbol']} | {row['rows']} | {row['eligible_rows']} | "
            f"{row['eligible_ratio']} | {row['crossed_ratio']} | "
            f"{row['same_ms_risk_ratio']} | {row['residue_ratio']} | "
            f"{row['no_size_ratio']} |"
        )
    lines += [
        "",
        "## Per Date Eligibility",
        "",
        "| date | rows | eligible | eligible_ratio | crossed_ratio | "
        "same_ms_risk_ratio | residue_ratio | no_size_ratio |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["per_date"]:
        lines.append(
            f"| {row['date']} | {row['rows']} | {row['eligible_rows']} | {row['eligible_ratio']} | "
            f"{row['crossed_ratio']} | {row['same_ms_risk_ratio']} | "
            f"{row['residue_ratio']} | {row['no_size_ratio']} |"
        )
    lines += [
        "",
        "## Boundary",
        "",
        "- Downstream must filter `StrategyHandoffEligibleFlag == true` and "
        "retain every quality flag.",
        "- Size is active-order replay volume at the best price, not verified "
        "executable queue size.",
        "- Any crossed, same-millisecond, residue, missing-size, or non-positive "
        "price row is fail-closed.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_json_summary(research_root: Path, summary: dict[str, Any]) -> Path:
    path = (
        research_root
        / "Reports"
        / f"opend_agent_strategy_handoff_summary_{datetime.now():%Y%m%d}.json"
    )
    write_json(path, summary)
    return path


def compact_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "namespace": summary["namespace"],
        "partition_count": summary["partition_count"],
        "total_rows": summary["total_rows"],
        "eligible_rows": summary["eligible_rows"],
        "eligible_ratio": summary["eligible_ratio"],
        "blockers": summary["blockers"],
        "output_root": summary["output_root"],
    }


if __name__ == "__main__":
    raise SystemExit(main())
