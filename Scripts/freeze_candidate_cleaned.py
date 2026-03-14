from __future__ import annotations

import argparse

from Scripts.runtime import print_scaffold_plan


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scaffold for freezing candidate_cleaned_2025_v1."
    )
    parser.add_argument("--print-plan", action="store_true", help="Print the intended pipeline plan.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.print_plan:
        print_scaffold_plan(
            name="freeze_candidate_cleaned",
            purpose="Freeze the mechanical cleaned layer without injecting unverified semantics.",
            responsibilities=[
                "Materialize candidate_cleaned partitions from raw inputs.",
                "Lock schema, partition spec, and candidate key assumptions.",
                "Write manifests, checkpoints, and run logs alongside outputs.",
                "Expose the golden-sample slice for semantic verification.",
            ],
            inputs=[
                "Raw layer manifests and source metadata.",
                "CLEANING_SPEC.md and DATA_CONTRACT.md rules.",
            ],
            outputs=[
                "/Volumes/Data/港股Tick数据/candidate_cleaned",
                "/Volumes/Data/港股Tick数据/manifests/candidate_cleaned_*",
            ],
        )
        return 0

    print("Scaffold only. Use --print-plan until the candidate_cleaned contract is finalized.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
