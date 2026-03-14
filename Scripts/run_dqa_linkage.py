from __future__ import annotations

import argparse

from Scripts.runtime import print_scaffold_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scaffold for DQA linkage feasibility checks.")
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_dqa_linkage",
            purpose="Measure cross-table linkage feasibility before semantic claims are made.",
            responsibilities=[
                "Estimate BidOrderID and AskOrderID matchability into OrderId.",
                "Profile linkage lag, unmatched rate, and broker consistency.",
                "Persist drill-down tables for failed or ambiguous matches.",
            ],
            inputs=[
                "candidate_cleaned trades and orders partitions.",
                "Golden sample definitions and broker reference artifacts.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/dqa/linkage",
                "Research/Audits linkage feasibility reports.",
            ],
        )
        return 0

    print("Scaffold only. Use --print-plan until linkage rules are finalized.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
