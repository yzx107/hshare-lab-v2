# Semantic TradeDir Contrast 2026

- generated_at: 2026-03-15T09:00:35+00:00
- scope: representative sample `2026-01-05 / 2026-02-24 / 2026-03-13`
- session_basis: `Time`-derived buckets because current stage inputs do not carry a `Session` column
- turnover_basis: `turnover_proxy = Price * Volume`
- observed_dir_values: `0,1,2`
- status: `candidate_directional_signal`
- admissibility_impact: `requires_manual_review`

## Key Findings

- `TradeDir` is a stable 3-value code `{0,1,2}` on the `2026` representative sample.
- `Dir=1` and `Dir=2` are not separated by linkage-side structure:
  - `dir1_vs_dir2_linkage_gap_avg = 0.0`
  - both values keep `both_present_rate = 1.0` across the representative sample
- `Dir=1` and `Dir=2` do show stable contrast in `previous-trade price move`:
  - `dir1_vs_dir2_uptick_gap_avg = -0.042857355658386144`
  - `dir1_vs_dir2_downtick_gap_avg = 0.003862084301872737`
  - `dir1_vs_dir2_sameprice_gap_avg = 0.03899527135651339`
  - `dir1_vs_dir2_uptick_gap_sign_consistent_flag = true`
  - `dir1_vs_dir2_bucket_uptick_consistent_day_count = 3`
- `Dir=0` is clearly a separate category:
  - `dir0_specialness_score = 0.4032756015451462`
  - `Dir=0` has `bid_present_rate = ask_present_rate = both_present_rate = 0.0`
  - `Dir=0` concentrates in `0900_0929` and `1600_plus` buckets that `Dir=1/2` do not occupy

## Daily Contrast

### 2026-01-05

- `dir1_vs_dir2_uptick_gap = -0.03802037925616081`
- `dir1_vs_dir2_bucket_uptick_gap_summary = 0930_1159:-0.031,1300_1559:-0.0472`
- `dir0_specialness = 0.4098339207162774`

### 2026-02-24

- `dir1_vs_dir2_uptick_gap = -0.04827058472082757`
- `dir1_vs_dir2_bucket_uptick_gap_summary = 0930_1159:-0.0487,1300_1559:-0.0469`
- `dir0_specialness = 0.4018964825717752`

### 2026-03-13

- `dir1_vs_dir2_uptick_gap = -0.04228110299817006`
- `dir1_vs_dir2_bucket_uptick_gap_summary = 0930_1159:-0.0393,1300_1559:-0.0458`
- `dir0_specialness = 0.398096401347386`

## Interpretation

- `TradeDir` should no longer be described as merely “unknown but stable”.
- The most defensible current interpretation is:
  - `TradeDir` carries a candidate directional signal
  - the contrast between `Dir=1` and `Dir=2` is related to short-horizon price movement, not linkage-side structure
  - `Dir=0` should be treated as a separate class rather than mixed into the `Dir=1/2` symmetry hypothesis

## Research Boundary

- Safe to say:
  - `TradeDir` likely carries some direction-related short-horizon coding signal
  - `Dir=1/2` contrast is stable on representative samples
  - `Dir=0` is structurally distinct
- Not safe to say:
  - `Dir=1` equals buy aggressor
  - `Dir=2` equals sell aggressor
  - `TradeDir` is ready for signed-flow alpha or confirmed aggressor-side research

## Project Wording

> For 2026 representative samples, `TradeDir` is a stable 3-value code `{0,1,2}`.
> Contrast probing suggests a candidate directional signal between `Dir=1` and `Dir=2` based on previous-trade price-move behavior, but linkage structure does not identify a confirmed signed-side mapping.
> Current admissibility remains `requires_manual_review`.
