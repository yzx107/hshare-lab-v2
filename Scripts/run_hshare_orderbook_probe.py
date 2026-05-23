from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from Scripts.hshare_orderbook_replay import (
    SORT_MODES,
    default_table_path,
    rate,
    replay_orderbook_semantics,
    source_suffix,
    symbol_code,
)
from Scripts.runtime import (
    DEFAULT_DATA_ROOT,
    DEFAULT_LOG_ROOT,
    configure_logger,
    ensure_dir,
    iso_utc_now,
    print_scaffold_plan,
    write_json,
)

DEFAULT_STAGE_ROOT = DEFAULT_DATA_ROOT / "candidate_cleaned"
DEFAULT_DQA_ROOT = DEFAULT_DATA_ROOT / "dqa"
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_ROOT = REPO_ROOT / "Research" / "Audits"
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
        description="Probe Hshare order-book reconstruction semantics."
    )
    parser.add_argument("--dates", help="Comma-separated trading dates, e.g. 2026-05-22.")
    parser.add_argument("--symbols", help="Comma-separated symbols, e.g. HK.01879,HK.01609.")
    parser.add_argument(
        "--discover-by",
        choices=["none", "top-orders", "top-trades"],
        default="none",
        help="Optionally add top symbols by row count for each date.",
    )
    parser.add_argument("--max-symbols", type=int, default=20)
    parser.add_argument("--min-order-rows", type=int, default=1)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_STAGE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_DQA_ROOT)
    parser.add_argument("--research-root", type=Path, default=DEFAULT_RESEARCH_ROOT)
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT)
    parser.add_argument(
        "--sort-modes",
        default="send_seq_order_first",
        help=f"Comma-separated sort modes: {','.join(sorted(SORT_MODES))}.",
    )
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
            name="run_hshare_orderbook_probe",
            purpose="Validate Hshare active-order-book reconstruction on bounded samples.",
            responsibilities=[
                "Replay OrderType lifecycle per OrderId.",
                "Compare Ext[0] and Ext[1] as side candidates.",
                "Emit resumable DQA artifacts outside the repository.",
                "Write an optional compact Research/Audits markdown summary.",
            ],
            inputs=[
                "candidate_cleaned/orders/date=YYYY-MM-DD/*.parquet",
                "candidate_cleaned/trades/date=YYYY-MM-DD/*.parquet",
            ],
            outputs=[
                "dqa/orderbook_reconstruction/date=YYYY-MM-DD/symbol=XXXXX/sort=MODE/result.json",
                "dqa/orderbook_reconstruction/last_run_summary.json",
                "Research/Audits/hshare_orderbook_reconstruction_probe_*.md",
            ],
        )
        return 0

    dates = parse_values(args.dates)
    if not dates:
        raise SystemExit("--dates is required")
    sort_modes = parse_values(args.sort_modes)
    unknown_sort_modes = sorted(set(sort_modes) - SORT_MODES)
    if unknown_sort_modes:
        raise SystemExit(f"unsupported sort modes: {','.join(unknown_sort_modes)}")

    logger = configure_logger(
        "hshare_orderbook_probe",
        args.log_root / "orderbook_reconstruction" / "last_run.log",
    )
    symbols = [symbol_code(value) for value in parse_values(args.symbols)]
    for date in dates:
        symbols.extend(discover_symbols(args, date, logger=logger))
    symbols = unique_symbols(symbols)
    if not symbols:
        raise SystemExit("--symbols is required unless --discover-by is used")

    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    total_tasks = len(dates) * len(symbols) * len(sort_modes)
    completed_tasks = 0
    heartbeat_path = args.output_root / "orderbook_reconstruction" / "heartbeat.json"
    logger.info(
        "orderbook probe started dates=%s symbols=%s sort_modes=%s total_tasks=%s",
        ",".join(dates),
        ",".join(symbols),
        ",".join(sort_modes),
        total_tasks,
    )

    for date in dates:
        for symbol in symbols:
            loaded_rows: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None
            for sort_mode in sort_modes:
                result_path = result_path_for(args.output_root, date, symbol, sort_mode)
                if args.resume and result_path.exists() and not args.overwrite_existing:
                    result = json.loads(result_path.read_text(encoding="utf-8"))
                    results.append(result)
                    completed_tasks += 1
                    write_heartbeat(heartbeat_path, total_tasks, completed_tasks, failures)
                    continue
                try:
                    if loaded_rows is None:
                        loaded_rows = read_symbol_rows(
                            args.input_root,
                            date,
                            symbol,
                            args.limit_rows,
                        )
                    result = run_probe_for_rows(
                        date=date,
                        symbol=symbol,
                        sort_mode=sort_mode,
                        input_root=args.input_root,
                        order_rows=loaded_rows[0],
                        trade_rows=loaded_rows[1],
                    )
                    write_json(result_path, result)
                    results.append(result)
                    logger.info(
                        "completed date=%s symbol=%s sort_mode=%s orders=%s trades=%s",
                        date,
                        symbol,
                        sort_mode,
                        result["order_rows"],
                        result["trade_rows"],
                    )
                except Exception as exc:  # pragma: no cover - operational guardrail
                    failure = {
                        "date": date,
                        "symbol": symbol,
                        "sort_mode": sort_mode,
                        "error": repr(exc),
                    }
                    failures.append(failure)
                    logger.exception("failed %s", failure)
                completed_tasks += 1
                write_heartbeat(heartbeat_path, total_tasks, completed_tasks, failures)

    summary = build_summary(results, failures, args=args)
    summary_path = args.output_root / "orderbook_reconstruction" / "last_run_summary.json"
    results_jsonl_path = args.output_root / "orderbook_reconstruction" / "last_run_results.jsonl"
    write_json(summary_path, summary)
    write_jsonl(results_jsonl_path, results)
    if args.write_research_report:
        report_path = write_research_report(summary, results, args=args)
        summary["research_report"] = str(report_path)
        write_json(summary_path, summary)
    print(json.dumps(compact_stdout_summary(summary), ensure_ascii=False, indent=2))
    return 1 if failures else 0


def parse_values(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def unique_symbols(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    output = []
    for symbol in symbols:
        code = symbol_code(symbol)
        if code not in seen:
            seen.add(code)
            output.append(code)
    return output


def discover_symbols(args: argparse.Namespace, date: str, *, logger: Any) -> list[str]:
    if args.discover_by == "none":
        return []
    table = "orders" if args.discover_by == "top-orders" else "trades"
    path = default_table_path(args.input_root, table=table, date=date)
    if not path.exists():
        logger.warning("cannot discover symbols; missing %s", path)
        return []
    frame = (
        pl.scan_parquet(str(path))
        .group_by("source_file")
        .agg(pl.len().alias("row_count"))
        .sort("row_count", descending=True)
    )
    if table == "orders" and args.min_order_rows > 1:
        frame = frame.filter(pl.col("row_count") >= args.min_order_rows)
    rows = frame.head(args.max_symbols).collect().to_dicts()
    symbols = [
        symbol_code(str(row["source_file"]).rsplit("/", 1)[-1].replace(".csv", ""))
        for row in rows
    ]
    logger.info("discovered %s symbols for %s by %s", len(symbols), date, args.discover_by)
    return symbols


def read_symbol_rows(
    input_root: Path,
    date: str,
    symbol: str,
    limit_rows: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    order_path = default_table_path(input_root, table="orders", date=date)
    trade_path = default_table_path(input_root, table="trades", date=date)
    return (
        read_rows(order_path, symbol, ORDER_COLUMNS, limit_rows=limit_rows),
        read_rows(trade_path, symbol, TRADE_COLUMNS, limit_rows=limit_rows),
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
    if "source_file" not in names:
        raise ValueError(f"source_file column missing in {path}")
    selected = [column for column in columns if column in names]
    frame = frame.filter(pl.col("source_file").str.ends_with(source_suffix(symbol))).select(
        selected
    )
    if limit_rows > 0:
        frame = frame.limit(limit_rows)
    return frame.collect().to_dicts()


def run_probe_for_rows(
    *,
    date: str,
    symbol: str,
    sort_mode: str,
    input_root: Path,
    order_rows: list[dict[str, Any]],
    trade_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    result = replay_orderbook_semantics(
        order_rows=order_rows,
        trade_rows=trade_rows,
        sort_mode=sort_mode,
    )
    result.update(
        {
            "date": date,
            "symbol": f"HK.{symbol}",
            "order_rows": len(order_rows),
            "trade_rows": len(trade_rows),
            "order_path": str(default_table_path(input_root, table="orders", date=date)),
            "trade_path": str(default_table_path(input_root, table="trades", date=date)),
            "generated_at": iso_utc_now(),
        }
    )
    return result


def result_path_for(output_root: Path, date: str, symbol: str, sort_mode: str) -> Path:
    return (
        output_root
        / "orderbook_reconstruction"
        / f"date={date}"
        / f"symbol={symbol}"
        / f"sort={sort_mode}"
        / "result.json"
    )


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


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def build_summary(
    results: list[dict[str, Any]],
    failures: list[dict[str, Any]],
    *,
    args: argparse.Namespace,
) -> dict[str, Any]:
    aggregate = aggregate_candidates(results)
    task_rows = [task_summary(row) for row in results]
    return {
        "generated_at": iso_utc_now(),
        "input_root": str(args.input_root),
        "output_root": str(args.output_root / "orderbook_reconstruction"),
        "dates": sorted({row["date"] for row in results}),
        "symbols": sorted({row["symbol"] for row in results}),
        "sort_modes": sorted({row["sort_mode"] for row in results}),
        "result_count": len(results),
        "failure_count": len(failures),
        "failures": failures,
        "recommended_side_bit_counts": recommended_side_bit_counts(results),
        "aggregate_candidate_results": aggregate,
        "task_results": task_rows,
    }


def aggregate_candidates(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, int], dict[str, Any]] = {}
    for result in results:
        for candidate in result["candidate_results"]:
            key = (candidate["sort_mode"], candidate["side_bit"])
            bucket = buckets.setdefault(
                key,
                {
                    "sort_mode": candidate["sort_mode"],
                    "side_bit": candidate["side_bit"],
                    "task_count": 0,
                    "orders_total": 0,
                    "add_count": 0,
                    "modify_count": 0,
                    "delete_count": 0,
                    "raw_counts": defaultdict(int),
                },
            )
            bucket["task_count"] += 1
            for column in ("orders_total", "add_count", "modify_count", "delete_count"):
                bucket[column] += candidate[column]
            for column, value in candidate["raw_counts"].items():
                bucket["raw_counts"][column] += value

    rows = []
    for bucket in buckets.values():
        raw = dict(bucket["raw_counts"])
        rows.append(
            {
                "sort_mode": bucket["sort_mode"],
                "side_bit": bucket["side_bit"],
                "task_count": bucket["task_count"],
                "orders_total": bucket["orders_total"],
                "add_count": bucket["add_count"],
                "modify_count": bucket["modify_count"],
                "delete_count": bucket["delete_count"],
                "modify_active_rate": rate(raw["modify_with_active_order"], bucket["modify_count"]),
                "delete_active_rate": rate(raw["delete_with_active_order"], bucket["delete_count"]),
                "volume_pre_match_rate": rate(raw["volume_pre_matches"], raw["volume_pre_checks"]),
                "level_match_rate": rate(raw["level_matches"], raw["level_checks"]),
                "crossed_book_rate": rate(
                    raw["crossed_book_observations"],
                    raw["book_observations"],
                ),
                "trade_price_inside_book_rate": rate(
                    raw["trades_price_inside_book"],
                    raw["trades_with_book"],
                ),
                "bid_orderid_active_rate": rate(
                    raw["bid_orderid_active"],
                    raw["bid_orderid_checks"],
                ),
                "ask_orderid_active_rate": rate(
                    raw["ask_orderid_active"],
                    raw["ask_orderid_checks"],
                ),
                "bid_orderid_side_match_rate": rate(
                    raw["bid_orderid_side_matches"],
                    raw["bid_orderid_active"],
                ),
                "ask_orderid_side_match_rate": rate(
                    raw["ask_orderid_side_matches"],
                    raw["ask_orderid_active"],
                ),
                "raw_counts": raw,
            }
        )
    return sorted(rows, key=lambda row: (row["sort_mode"], row["side_bit"]))


def recommended_side_bit_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for result in results:
        counts[f"Ext[{result['recommended_side_bit']}]"] += 1
    return dict(sorted(counts.items()))


def task_summary(result: dict[str, Any]) -> dict[str, Any]:
    chosen = next(
        item
        for item in result["candidate_results"]
        if item["side_bit"] == result["recommended_side_bit"]
    )
    return {
        "date": result["date"],
        "symbol": result["symbol"],
        "sort_mode": result["sort_mode"],
        "order_rows": result["order_rows"],
        "trade_rows": result["trade_rows"],
        "recommended_side_bit": result["recommended_side_bit"],
        "crossed_book_rate": chosen["crossed_book_rate"],
        "bid_orderid_active_rate": chosen["bid_orderid_active_rate"],
        "ask_orderid_active_rate": chosen["ask_orderid_active_rate"],
        "bid_orderid_side_match_rate": chosen["bid_orderid_side_match_rate"],
        "ask_orderid_side_match_rate": chosen["ask_orderid_side_match_rate"],
        "volume_pre_match_rate": chosen["volume_pre_match_rate"],
        "level_match_rate": chosen["level_match_rate"],
        "trade_price_inside_book_rate": chosen["trade_price_inside_book_rate"],
    }


def write_research_report(
    summary: dict[str, Any],
    results: list[dict[str, Any]],
    *,
    args: argparse.Namespace,
) -> Path:
    dates = summary["dates"] or parse_values(args.dates)
    if len(dates) == 1:
        stem = dates[0].replace("-", "")
    else:
        stem = f"{dates[0].replace('-', '')}_{dates[-1].replace('-', '')}"
    path = args.research_root / f"hshare_orderbook_reconstruction_probe_{stem}.md"
    ensure_dir(path.parent)
    lines = [
        f"# Hshare Order Book Reconstruction Probe {', '.join(dates)}",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- input_root: `{summary['input_root']}`",
        f"- output_root: `{summary['output_root']}`",
        f"- result_count: {summary['result_count']}",
        f"- failure_count: {summary['failure_count']}",
        f"- recommended_side_bit_counts: {summary['recommended_side_bit_counts']}",
        "",
        "## Aggregate By Side Candidate",
        "",
        "| sort_mode | side | tasks | crossed | bid_active | ask_active | bid_side | "
        "ask_side | volume_pre | level | trade_inside |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary["aggregate_candidate_results"]:
        lines.append(
            "| {sort_mode} | Ext[{side_bit}] | {task_count} | {crossed_book_rate} | "
            "{bid_orderid_active_rate} | {ask_orderid_active_rate} | "
            "{bid_orderid_side_match_rate} | {ask_orderid_side_match_rate} | "
            "{volume_pre_match_rate} | {level_match_rate} | "
            "{trade_price_inside_book_rate} |".format(**format_rate_row(row))
        )
    lines.extend(
        [
            "",
            "## Task Detail",
            "",
            "| date | symbol | sort_mode | chosen_side | orders | trades | crossed | "
            "bid_active | ask_active | bid_side | ask_side | volume_pre | level | "
            "trade_inside |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | "
            "---: | ---: | ---: |",
        ]
    )
    for row in summary["task_results"]:
        lines.append(
            "| {date} | {symbol} | {sort_mode} | Ext[{recommended_side_bit}] | "
            "{order_rows} | {trade_rows} | {crossed_book_rate} | "
            "{bid_orderid_active_rate} | {ask_orderid_active_rate} | "
            "{bid_orderid_side_match_rate} | {ask_orderid_side_match_rate} | "
            "{volume_pre_match_rate} | {level_match_rate} | "
            "{trade_price_inside_book_rate} |".format(**format_rate_row(row))
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This report is DQA/semantic evidence only, not strategy admission.",
            "- `OrderType` is replayed as lifecycle events: `1=add`, `2=modify`, `3=delete`.",
            "- `Ext[0]` and `Ext[1]` are compared as side candidates; only `Ext[0]` "
            "is currently expected to behave like order side.",
            "- `Level` is treated as a vendor hint until larger replay sanity checks pass.",
            "- Non-zero crossed-book rates remain the main blocker before reconstructed "
            "depth enters replay.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def format_rate_row(row: dict[str, Any]) -> dict[str, Any]:
    output = dict(row)
    for key, value in row.items():
        if key.endswith("_rate") or key in {"crossed_book_rate", "trade_price_inside_book_rate"}:
            output[key] = format_rate(value)
    return output


def format_rate(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4%}"


def compact_stdout_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "result_count": summary["result_count"],
        "failure_count": summary["failure_count"],
        "dates": summary["dates"],
        "symbols": summary["symbols"],
        "sort_modes": summary["sort_modes"],
        "recommended_side_bit_counts": summary["recommended_side_bit_counts"],
        "summary_path": str(Path(summary["output_root"]) / "last_run_summary.json"),
        "research_report": summary.get("research_report"),
    }


if __name__ == "__main__":
    raise SystemExit(main())
