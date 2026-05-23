from __future__ import annotations

import argparse
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.field_release_registry import (
    DEFAULT_REGISTRY_PATH,
    FieldReleaseRegistryError,
    assert_release_objects_for_namespace,
    load_registry,
)
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
NAMESPACE = "orderbook_replay__top_of_book_only"
BUILDER = "python -m Scripts.build_orderbook_top_of_book_only"
REPLAY_DEPTH_ADMISSION = "full_reconstructed_depth_blocked"
RELEASE_OBJECTS = [
    "orderbook_replay__top_of_book_only",
    "BestBidReplay",
    "BestAskReplay",
    "ReplaySpread",
    "ReplayMid",
    "TradeInsideBestBookFlag",
    "TopOfBookValidFlag",
    "ReplayQualityScore",
    "CrossedWindowFlag",
    "ReplayResidueFlag",
    "ReplayWindowExcludedFlag",
    "SameMillisecondBatchRiskFlag",
]

ORDER_COLUMNS = [
    "SendTime",
    "Time",
    "SeqNum",
    "OrderId",
    "OrderType",
    "Ext",
    "Price",
    "Volume",
    "source_file",
]
TRADE_COLUMNS = [
    "SendTime",
    "Time",
    "SeqNum",
    "TickID",
    "Price",
    "Volume",
    "source_file",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize top-of-book-only replay objects with quality gates."
    )
    parser.add_argument("--dates", help="Comma-separated trading dates, e.g. 2026-05-22.")
    parser.add_argument("--symbols", help="Comma-separated symbols, e.g. HK.01879,HK.01609.")
    parser.add_argument("--stage-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument("--field-release-registry", type=Path, default=DEFAULT_REGISTRY_PATH)
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
            name="build_orderbook_top_of_book_only",
            purpose="Materialize gated top-of-book replay objects without full depth.",
            responsibilities=[
                "Replay active orders with released Ext[0] side proxy.",
                "Emit only best bid/ask, spread, mid, inside-book flag, and quality gates.",
                "Keep full depth, queue semantics, Level, and execution realism blocked.",
            ],
            inputs=[
                "candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet",
                "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "caveat/orderbook_replay__top_of_book_only/top_of_book_events/...",
                "caveat/orderbook_replay__top_of_book_only/manifests/...",
            ],
        )
        return 0

    if args.side_bit != 0:
        raise SystemExit("Only side-bit 0 is released for top-of-book-only materialization.")
    enforce_release_registry(args.field_release_registry)
    dates = parse_values(args.dates)
    symbols = [symbol_code(value) for value in parse_values(args.symbols)]
    if not dates or not symbols:
        raise SystemExit("--dates and --symbols are required")

    logger = configure_logger(
        "build_orderbook_top_of_book_only",
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
    for trade_date in dates:
        for symbol in symbols:
            path = output_path(args.output_root, date=trade_date, symbol=symbol)
            if args.resume and not args.overwrite_existing and path.exists():
                rows.append(partition_row(trade_date, symbol, path, resumed=True))
                completed += 1
                write_heartbeat(heartbeat_path, total_tasks, completed, failures)
                continue
            try:
                order_rows, trade_rows = read_symbol_rows(
                    args.stage_root,
                    trade_date,
                    symbol,
                    args.limit_rows,
                )
                output_rows = materialize_rows(
                    date=trade_date,
                    symbol=symbol,
                    order_rows=order_rows,
                    trade_rows=trade_rows,
                    sort_mode=args.sort_mode,
                    side_bit=args.side_bit,
                )
                write_parquet(output_rows, path)
                row = partition_row(
                    trade_date, symbol, path, output_rows=len(output_rows), resumed=False
                )
                append_jsonl(partitions_path, row)
                rows.append(row)
                logger.info(
                    "completed %s:HK.%s top_of_book_rows=%s", trade_date, symbol, len(output_rows)
                )
            except Exception as exc:  # pragma: no cover - operational guardrail
                failure = {"date": trade_date, "symbol": f"HK.{symbol}", "error": repr(exc)}
                failures.append(failure)
                logger.exception("failed %s:HK.%s", trade_date, symbol)
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


def enforce_release_registry(registry_path: Path) -> None:
    registry = load_registry(registry_path)
    try:
        assert_release_objects_for_namespace(
            registry,
            object_names=["orderbook_replay__top_of_book_only"],
            namespace=NAMESPACE,
            allowed_buckets={"admit_top_of_book_only"},
            expected_builder=BUILDER,
            require_dossier=True,
        )
        assert_release_objects_for_namespace(
            registry,
            object_names=RELEASE_OBJECTS[1:],
            namespace=NAMESPACE,
            allowed_buckets={"admit_top_of_book_only", "admit_with_explicit_caveat_only"},
        )
    except FieldReleaseRegistryError as exc:
        raise SystemExit(str(exc)) from exc


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
            limit_rows,
        ),
        read_rows(
            default_table_path(stage_root, table="trades", date=date),
            symbol,
            TRADE_COLUMNS,
            limit_rows,
        ),
    )


def read_rows(path: Path, symbol: str, columns: list[str], limit_rows: int) -> list[dict[str, Any]]:
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
            rows.append(
                top_of_book_row(date, symbol, replay, row, sort_mode, side_bit, event_time_counts)
            )
            replay.apply_trade_probe(row)
    return rows


def top_of_book_row(
    date: str,
    symbol: str,
    replay: HshareOrderBookReplay,
    row: dict[str, Any],
    sort_mode: str,
    side_bit: int,
    event_time_counts: Counter[str],
) -> dict[str, Any]:
    best_bid, best_ask = replay.best_bid_ask()
    trade_price = as_decimal(row.get("Price"))
    crossed = best_bid is not None and best_ask is not None and best_bid > best_ask
    same_ms_risk = event_time_counts[event_time(row)] > 1
    residue = crossed
    excluded = crossed
    valid = (
        best_bid is not None
        and best_ask is not None
        and not crossed
        and not residue
        and not same_ms_risk
    )
    spread = best_ask - best_bid if valid else None
    mid = (best_bid + best_ask) / Decimal("2") if valid else None
    inside = None
    if valid and trade_price is not None:
        inside = bool(best_bid <= trade_price <= best_ask)
    quality_score = 1.0 if valid else 0.0
    return {
        "date": date,
        "symbol": f"HK.{symbol}",
        "namespace": NAMESPACE,
        "source_layer": "candidate_cleaned",
        "release_bucket": "admit_top_of_book_only",
        "admission_rule": "top_of_book_only_with_quality_gates",
        "contains_caveat_fields": True,
        "replay_depth_admission": REPLAY_DEPTH_ADMISSION,
        "sort_mode": sort_mode,
        "side_bit": side_bit,
        "SendTime": stringify_time(row.get("SendTime")),
        "Time": row.get("Time"),
        "SeqNum": as_int(row.get("SeqNum")),
        "TickID": as_int(row.get("TickID")),
        "TradePrice": decimal_to_float(trade_price),
        "TradeVolume": as_int(row.get("Volume")),
        "BestBidReplay": decimal_to_float(best_bid),
        "BestAskReplay": decimal_to_float(best_ask),
        "ReplaySpread": decimal_to_float(spread),
        "ReplayMid": decimal_to_float(mid),
        "CrossedWindowFlag": bool(crossed),
        "ReplayResidueFlag": bool(residue),
        "ReplayWindowExcludedFlag": bool(excluded),
        "SameMillisecondBatchRiskFlag": bool(same_ms_risk),
        "TopOfBookValidFlag": bool(valid),
        "ReplayQualityScore": quality_score,
        "TradeInsideBestBookFlag": inside,
    }


def event_time(row: dict[str, Any]) -> str:
    return stringify_time(row.get("SendTime") or row.get("Time"))


def decimal_to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    ensure_dir(path.parent)
    if rows:
        pl.from_dicts(rows, infer_schema_length=None).write_parquet(path)
    else:
        pl.DataFrame().write_parquet(path)


def output_path(output_root: Path, *, date: str, symbol: str) -> Path:
    return (
        output_root
        / NAMESPACE
        / "top_of_book_events"
        / f"year={date[:4]}"
        / f"date={date}"
        / f"symbol={symbol}"
        / "part-00000.parquet"
    )


def partition_row(
    date: str,
    symbol: str,
    output_path: Path,
    *,
    output_rows: int | None = None,
    resumed: bool,
) -> dict[str, Any]:
    if output_rows is None and output_path.exists():
        output_rows = pl.read_parquet(output_path).height
    return {
        "generated_at": iso_utc_now(),
        "date": date,
        "symbol": f"HK.{symbol}",
        "namespace": NAMESPACE,
        "top_of_book_path": str(output_path),
        "top_of_book_rows": output_rows or 0,
        "release_bucket": "admit_top_of_book_only",
        "admission_rule": "top_of_book_only_with_quality_gates",
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
        "pipeline": "build_orderbook_top_of_book_only",
        "namespace": NAMESPACE,
        "source_layer": "candidate_cleaned",
        "output_root": str(args.output_root / NAMESPACE),
        "dates": sorted({row["date"] for row in rows}),
        "symbols": sorted({row["symbol"] for row in rows}),
        "partition_count": len(rows),
        "failure_count": len(failures),
        "failures": failures,
        "top_of_book_rows": sum(int(row["top_of_book_rows"]) for row in rows),
        "release_bucket": "admit_top_of_book_only",
        "admission_rule": "top_of_book_only_with_quality_gates",
        "contains_caveat_fields": True,
        "replay_depth_admission": REPLAY_DEPTH_ADMISSION,
    }


def write_research_report(args: argparse.Namespace, summary: dict[str, Any]) -> Path:
    dates = summary["dates"] or parse_values(args.dates)
    stem = dates[0].replace("-", "") if len(dates) == 1 else "multi_date"
    path = args.research_root / f"orderbook_top_of_book_only_{stem}.md"
    ensure_dir(path.parent)
    lines = [
        f"# Top-of-Book Only Replay 物化报告 {', '.join(dates)}",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- namespace: `{summary['namespace']}`",
        f"- source_layer: `{summary['source_layer']}`",
        f"- output_root: `{summary['output_root']}`",
        f"- partition_count: {summary['partition_count']}",
        f"- top_of_book_rows: {summary['top_of_book_rows']}",
        f"- release_bucket: `{summary['release_bucket']}`",
        f"- admission_rule: `{summary['admission_rule']}`",
        f"- contains_caveat_fields: `{summary['contains_caveat_fields']}`",
        f"- replay_depth_admission: `{summary['replay_depth_admission']}`",
        "",
        "## 边界",
        "",
        "- 本物化只输出 top-of-book-only replay objects。",
        "- `ReplayQualityScore` 只是 bounded gate：`1.0` 表示 "
        "`TopOfBookValidFlag=true`，`0.0` 表示该行不能作为默认样本消费。",
        "- 不输出 full depth、queue position、Level semantics 或 execution realism。",
        "- 下游消费必须保留所有 quality flags。",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def compact_stdout_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "namespace": summary["namespace"],
        "partition_count": summary["partition_count"],
        "failure_count": summary["failure_count"],
        "top_of_book_rows": summary["top_of_book_rows"],
        "output_root": summary["output_root"],
        "research_report": summary.get("research_report"),
    }


def reset_files(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
