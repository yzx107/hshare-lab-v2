from __future__ import annotations

import argparse

from Scripts.runtime import print_scaffold_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scaffold for DQA schema integrity checks.")
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_dqa_schema",
            purpose="Check schema drift, type stability, and nullability against the contract.",
            responsibilities=[
                "Snapshot schema per partition and compare against the frozen contract.",
                "Detect missing columns, type drift, and nullability anomalies.",
                "Store reproducible schema reports in dqa and research outputs.",
            ],
            inputs=[
                "candidate_cleaned parquet partitions.",
                "Contract snapshots from DATA_CONTRACT.md and CLEANING_SPEC.md.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/dqa/schema",
                "Research/Audits schema integrity reports.",
            ],
        )
        return 0

    print("Scaffold only. Use --print-plan until schema integrity rules are fixed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
