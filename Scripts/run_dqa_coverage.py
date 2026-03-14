from __future__ import annotations

import argparse

from Scripts.runtime import print_scaffold_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scaffold for DQA ingestion completeness.")
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="run_dqa_coverage",
            purpose="Measure ingestion completeness from raw to candidate_cleaned.",
            responsibilities=[
                "Compare raw file inventory against candidate_cleaned partitions.",
                "Track date coverage, empty partitions, and unusually small outputs.",
                "Persist checkpointed metrics for reruns and drill-downs.",
            ],
            inputs=[
                "Raw inventory outputs.",
                "candidate_cleaned manifests.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/dqa/coverage",
                "Research/Audits coverage summaries and exceptions.",
            ],
        )
        return 0

    print("Scaffold only. Use --print-plan until DQA coverage metrics are fully specified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
