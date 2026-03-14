from __future__ import annotations

import argparse

from Scripts.runtime import print_scaffold_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scaffold for building verified research-ready tables.")
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="build_verified_layer",
            purpose="Materialize research-ready tables from mechanically safe and semantically verified fields.",
            responsibilities=[
                "Read candidate_cleaned inputs and semantic verification decisions.",
                "Build verified_trades, verified_orders, and linkage tables.",
                "Keep manifest, DuckDB audit tables, and research reports in sync.",
            ],
            inputs=[
                "candidate_cleaned partitions.",
                "Semantic verification matrix and DQA results.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/verified",
                "Research/Reports verified-layer summaries.",
            ],
        )
        return 0

    print("Scaffold only. Use --print-plan until the verified-layer contract is finalized.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
