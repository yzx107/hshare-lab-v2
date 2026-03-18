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
                "Read candidate_cleaned inputs and verified admission policy decisions.",
                "Build verified_orders and verified_trades from admit-now fields only.",
                "Keep manifest, policy versions, and excluded-field lists in sync.",
                "Defer verified_trade_order_linkage and broker enrichment until a later phase.",
            ],
            inputs=[
                "candidate_cleaned partitions.",
                "Semantic verification matrix and DQA results.",
                "Verified admission matrix and verified field policy.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/verified",
                "Research/Reports verified-layer summaries.",
                "Verified manifest with year-level caveats and policy versions.",
            ],
        )
        return 0

    print("Scaffold only. Use --print-plan until verified v1 admit-now implementation is started.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
