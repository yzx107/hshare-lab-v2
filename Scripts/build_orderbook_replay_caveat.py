from __future__ import annotations

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.hshare_orderbook_replay import (
    SORT_MODES,
    ActiveOrder,
    HshareOrderBookReplay,
    as_decimal,
    as_int,
    default_table_path,
    event_sort_key,
    ext_side,
    source_suffix,
    stringify_time,
    symbol_code,
)
from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    append_jsonl,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_OUTPUT_ROOT = DEFAULT_DATA_ROOT / "caveat"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_ROOT = REPO_ROOT / "Research" / "Reports"
NAMESPACE = "orderbook_replay__caveat_lifecycle_linkage"
SEMANTIC_RELEASE = "orderbook_replay_semantic_release_2026-05-23"
REPLAY_DEPTH_ADMISSION = "blocked_until_crossed_book_residue_is_explained_or_bounded"

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize caveat-only orderbook replay lifecycle/linkage tables."
    )
    parser.add_argument("--dates", help="Comma-separated trading dates, e.g. 2026-05-22.")
    parser.add_argument("--symbols", help="Comma-separated symbols, e.g. HK.01879,HK.01609.")
    parser.add_argument("--stage-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument("--sort-mode", choices=sorted(SORT_MODES), default="send_seq_order_first")
    parser.add_argument("--side-bit", type=int, default=0)
    parser.add_argument("--limit-rows", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--write-research-report", action="store_true")
    parser.add_argument("--print-plan", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="build_orderbook_replay_caveat",
            purpose="Materialize caveat-only lifecycle and trade-linkage replay evidence.",
            responsibilities=[
                "Replay OrderType lifecycle with Ext[0] side candidate.",
                "Write lifecycle_events and trade_linkage parquet tables.",
                "Keep reconstructed depth blocked; only emit replay evidence fields.",
                "Write resumable manifests and optional research summary.",
            ],
            inputs=[
                "candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet",
                "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "caveat/orderbook_replay__caveat_lifecycle_linkage/lifecycle_events/...",
                "caveat/orderbook_replay__caveat_lifecycle_linkage/trade_linkage/...",
                "caveat/orderbook_replay__caveat_lifecycle_linkage/manifests/...",
            ],
        )
        return 0

    if args.side_bit != 0:
        raise SystemExit("Only side-bit 0 is released for caveat materialization.")
    dates = parse_values(args.dates)
    symbols = [symbol_code(value) for value in parse_values(args.symbols)]
    if not dates or not symbols:
        raise SystemExit("--dates and --symbols are required")

    logger = configure_logger(
        "build_orderbook_replay_caveat",
        args.log_root / NAMESPACE / "last_run.log",
    )
    manifest_dir = args.output_root / NAMESPACE / "manifests"
    ensure_dir(manifest_dir)
    partitions_path = manifest_dir / "partitions.jsonl"
    heartbeat_path = manifest_dir / "heartbeat.json"
    summary_path = manifest_dir / "summary.json"
    if args.overwrite_existing:
        reset_files([partitions_path, heartbeat_path, summary_path])

    total_tasks = len(dates) * len(symbols)
    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    completed = 0
    for date in dates:
        for symbol in symbols:
            task_key = f"{date}:HK.{symbol}"
            lifecycle_path = output_path(
                args.output_root,
                table="lifecycle_events",
                date=date,
                symbol=symbol,
            )
            linkage_path = output_path(
                args.output_root,
                table="trade_linkage",
                date=date,
                symbol=symbol,
            )
            if args.resume and not args.overwrite_existing:
                if lifecycle_path.exists() and linkage_path.exists():
                    rows.append(
                        partition_row(date, symbol, lifecycle_path, linkage_path, resumed=True)
                    )
                    completed += 1
                    write_heartbeat(heartbeat_path, total_tasks, completed, failures)
                    continue
            try:
                order_rows, trade_rows = read_symbol_rows(
                    args.stage_root,
                    date,
                    symbol,
                    args.limit_rows,
                )
                lifecycle_rows, linkage_rows = materialize_rows(
                    date=date,
                    symbol=symbol,
                    order_rows=order_rows,
                    trade_rows=trade_rows,
                    sort_mode=args.sort_mode,
                    side_bit=args.side_bit,
                )
                write_parquet(lifecycle_rows, lifecycle_path)
                write_parquet(linkage_rows, linkage_path)
                row = partition_row(
                    date,
                    symbol,
                    lifecycle_path,
                    linkage_path,
                    lifecycle_rows=len(lifecycle_rows),
                    linkage_rows=len(linkage_rows),
                    resumed=False,
                )
                append_jsonl(partitions_path, row)
                rows.append(row)
                logger.info(
                    "completed %s lifecycle_rows=%s linkage_rows=%s",
                    task_key,
                    len(lifecycle_rows),
                    len(linkage_rows),
                )
            except Exception as exc:  # pragma: no cover - operational guardrail
                failure = {"date": date, "symbol": f"HK.{symbol}", "error": repr(exc)}
                failures.append(failure)
                logger.exception("failed %s", task_key)
            completed += 1
            write_heartbeat(heartbeat_path, total_tasks, completed, failures)

    summary = build_summary(args, rows, failures)
    write_json(summary_path, summary)
    if args.write_research_report:
        report_path = write_research_report(args, summary)
        summary["research_report"] = str(report_path)
        write_json(summary_path, summary)
    print(json.dumps(compact_stdout_summary(summary), ensure_ascii=False, indent=2))
    return 1 if failures else 0


def parse_values(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def read_symbol_rows(
    stage_root: Path,
    date: str,
    symbol: str,
    limit_rows: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        read_rows(
            default_table_path(stage_root, table="orders", date=date),
            symbol,
            ORDER_COLUMNS,
            limit_rows=limit_rows,
        ),
        read_rows(
            default_table_path(stage_root, table="trades", date=date),
            symbol,
            TRADE_COLUMNS,
            limit_rows=limit_rows,
        ),
    )


def read_rows(
    path: Path,
    symbol: str,
    columns: list[str],
    *,
    limit_rows: int,
) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    frame = pl.scan_parquet(str(path))
    names = frame.collect_schema().names()
    selected = [column for column in columns if column in names]
    frame = frame.filter(pl.col("source_file").str.ends_with(source_suffix(symbol))).select(
        selected
    )
    if limit_rows > 0:
        frame = frame.limit(limit_rows)
    return frame.collect().to_dicts()


def materialize_rows(
    *,
    date: str,
    symbol: str,
    order_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
    sort_mode: str,
    side_bit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events = [("order", row) for row in order_rows] + [("trade", row) for row in trade_rows]
    events.sort(key=lambda item: event_sort_key(item[0], item[1], sort_mode=sort_mode))
    replay = HshareOrderBookReplay(side_bit=side_bit, sort_mode=sort_mode)
    lifecycle_rows: list[dict[str, Any]] = []
    linkage_rows: list[dict[str, Any]] = []
    for kind, row in events:
        if kind == "order":
            lifecycle_rows.append(lifecycle_row(date, symbol, replay, row, sort_mode, side_bit))
            replay.apply_order(row)
        else:
            linkage_rows.append(linkage_row(date, symbol, replay, row, sort_mode, side_bit))
            replay.apply_trade_probe(row)
    return lifecycle_rows, linkage_rows


def lifecycle_row(
    date: str,
    symbol: str,
    replay: HshareOrderBookReplay,
    row: dict[str, Any],
    sort_mode: str,
    side_bit: int,
) -> dict[str, Any]:
    order_id = as_int(row.get("OrderId"))
    before = replay.active_orders.get(order_id) if order_id is not None else None
    volume_pre = as_int(row.get("VolumePre"))
    return {
        **base_metadata(date, symbol, sort_mode, side_bit),
        "event_kind": "order",
        "SendTime": stringify_time(row.get("SendTime")),
        "Time": row.get("Time"),
        "SeqNum": as_int(row.get("SeqNum")),
        "OrderId": order_id,
        "OrderType": as_int(row.get("OrderType")),
        "Ext": row.get("Ext"),
        "OrderSideVendor": ext_side(row.get("Ext"), side_bit=side_bit),
        "Price": decimal_to_float(as_decimal(row.get("Price"))),
        "Volume": as_int(row.get("Volume")),
        "Level": as_int(row.get("Level")),
        "VolumePre": volume_pre,
        "active_order_side_before": active_side(before),
        "active_order_price_before": active_price(before),
        "active_order_volume_before": active_volume(before),
        "volume_pre_matches_prior_active_order": (
            bool(before is not None and volume_pre == before.volume)
            if volume_pre is not None and volume_pre > 0
            else None
        ),
    }


def linkage_row(
    date: str,
    symbol: str,
    replay: HshareOrderBookReplay,
    row: dict[str, Any],
    sort_mode: str,
    side_bit: int,
) -> dict[str, Any]:
    bid_id = as_int(row.get("BidOrderID"))
    ask_id = as_int(row.get("AskOrderID"))
    bid_present = bid_id is not None and bid_id > 0
    ask_present = ask_id is not None and ask_id > 0
    bid_order = replay.active_orders.get(bid_id) if bid_present else None
    ask_order = replay.active_orders.get(ask_id) if ask_present else None
    best_bid, best_ask = replay.best_bid_ask()
    trade_price = as_decimal(row.get("Price"))
    inside_book = None
    if trade_price is not None and best_bid is not None and best_ask is not None:
        inside_book = best_bid <= best_ask and best_bid <= trade_price <= best_ask
    return {
        **base_metadata(date, symbol, sort_mode, side_bit),
        "event_kind": "trade",
        "SendTime": stringify_time(row.get("SendTime")),
        "Time": row.get("Time"),
        "SeqNum": as_int(row.get("SeqNum")),
        "TickID": as_int(row.get("TickID")),
        "Price": decimal_to_float(trade_price),
        "Volume": as_int(row.get("Volume")),
        "BidOrderID": bid_id,
        "AskOrderID": ask_id,
        "bid_orderid_present": bid_present,
        "bid_order_active": bid_order is not None,
        "bid_order_side": active_side(bid_order),
        "bid_order_price": active_price(bid_order),
        "bid_order_volume": active_volume(bid_order),
        "bid_order_side_matches": bid_order.side == "BID" if bid_order else None,
        "ask_orderid_present": ask_present,
        "ask_order_active": ask_order is not None,
        "ask_order_side": active_side(ask_order),
        "ask_order_price": active_price(ask_order),
        "ask_order_volume": active_volume(ask_order),
        "ask_order_side_matches": ask_order.side == "ASK" if ask_order else None,
        "best_bid": decimal_to_float(best_bid),
        "best_ask": decimal_to_float(best_ask),
        "crossed_book_at_trade": (
            best_bid > best_ask if best_bid is not None and best_ask is not None else None
        ),
        "trade_price_inside_book": inside_book,
    }


def base_metadata(date: str, symbol: str, sort_mode: str, side_bit: int) -> dict[str, Any]:
    return {
        "date": date,
        "symbol": f"HK.{symbol}",
        "namespace": NAMESPACE,
        "source_layer": "candidate_cleaned",
        "admission_rule": "admit_now_plus_caveat_only",
        "contains_caveat_fields": True,
        "semantic_release": SEMANTIC_RELEASE,
        "replay_depth_admission": REPLAY_DEPTH_ADMISSION,
        "sort_mode": sort_mode,
        "side_bit": side_bit,
    }


def active_side(order: ActiveOrder | None) -> str | None:
    return order.side if order else None


def active_price(order: ActiveOrder | None) -> float | None:
    return decimal_to_float(order.price) if order else None


def active_volume(order: ActiveOrder | None) -> int | None:
    return order.volume if order else None


def decimal_to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


def output_path(output_root: Path, *, table: str, date: str, symbol: str) -> Path:
    year = date[:4]
    return (
        output_root
        / NAMESPACE
        / table
        / f"year={year}"
        / f"date={date}"
        / f"symbol={symbol}"
        / "part-00000.parquet"
    )


def partition_row(
    date: str,
    symbol: str,
    lifecycle_path: Path,
    linkage_path: Path,
    *,
    lifecycle_rows: int | None = None,
    linkage_rows: int | None = None,
    resumed: bool,
) -> dict[str, Any]:
    if lifecycle_rows is None and lifecycle_path.exists():
        lifecycle_rows = pl.read_parquet(lifecycle_path).height
    if linkage_rows is None and linkage_path.exists():
        linkage_rows = pl.read_parquet(linkage_path).height
    return {
        "generated_at": iso_utc_now(),
        "date": date,
        "symbol": f"HK.{symbol}",
        "namespace": NAMESPACE,
        "lifecycle_path": str(lifecycle_path),
        "trade_linkage_path": str(linkage_path),
        "lifecycle_rows": lifecycle_rows or 0,
        "trade_linkage_rows": linkage_rows or 0,
        "admission_rule": "admit_now_plus_caveat_only",
        "contains_caveat_fields": True,
        "replay_depth_admission": REPLAY_DEPTH_ADMISSION,
        "resumed": resumed,
    }


def write_heartbeat(
    path: Path,
    total_tasks: int,
    completed_tasks: int,
    failures: list[dict[str, Any]],
) -> None:
    write_json(
        path,
        {
            "generated_at": iso_utc_now(),
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "pending_tasks": max(total_tasks - completed_tasks, 0),
            "failure_count": len(failures),
            "failures": failures[-20:],
        },
    )


def build_summary(
    args: argparse.Namespace,
    rows: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "generated_at": iso_utc_now(),
        "pipeline": "build_orderbook_replay_caveat",
        "namespace": NAMESPACE,
        "source_layer": "candidate_cleaned",
        "output_root": str(args.output_root / NAMESPACE),
        "dates": sorted({row["date"] for row in rows}),
        "symbols": sorted({row["symbol"] for row in rows}),
        "partition_count": len(rows),
        "failure_count": len(failures),
        "failures": failures,
        "lifecycle_rows": sum(int(row["lifecycle_rows"]) for row in rows),
        "trade_linkage_rows": sum(int(row["trade_linkage_rows"]) for row in rows),
        "admission_rule": "admit_now_plus_caveat_only",
        "contains_caveat_fields": True,
        "semantic_release": SEMANTIC_RELEASE,
        "replay_depth_admission": REPLAY_DEPTH_ADMISSION,
    }


def write_research_report(args: argparse.Namespace, summary: dict[str, Any]) -> Path:
    dates = summary["dates"] or parse_values(args.dates)
    stem = dates[0].replace("-", "") if len(dates) == 1 else "multi_date"
    path = args.research_root / f"orderbook_replay_caveat_{stem}.md"
    ensure_dir(path.parent)
    lines = [
        f"# Orderbook Replay Caveat Materialization {', '.join(dates)}",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- namespace: `{summary['namespace']}`",
        f"- source_layer: `{summary['source_layer']}`",
        f"- output_root: `{summary['output_root']}`",
        f"- partition_count: {summary['partition_count']}",
        f"- lifecycle_rows: {summary['lifecycle_rows']}",
        f"- trade_linkage_rows: {summary['trade_linkage_rows']}",
        f"- admission_rule: `{summary['admission_rule']}`",
        f"- contains_caveat_fields: `{summary['contains_caveat_fields']}`",
        f"- replay_depth_admission: `{summary['replay_depth_admission']}`",
        "",
        "## Boundary",
        "",
        "- This materialization is caveat-only DQA/replay evidence.",
        "- It does not admit reconstructed depth into strategy or production replay.",
        "- `Level`, full `Ext`, and broker semantics remain vendor-defined / unverified.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def compact_stdout_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "namespace": summary["namespace"],
        "partition_count": summary["partition_count"],
        "failure_count": summary["failure_count"],
        "lifecycle_rows": summary["lifecycle_rows"],
        "trade_linkage_rows": summary["trade_linkage_rows"],
        "output_root": summary["output_root"],
        "research_report": summary.get("research_report"),
    }


def reset_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
