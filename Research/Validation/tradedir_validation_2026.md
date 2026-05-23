# TradeDir Validation 2026

- generated_at: 2026-03-25T18:40:44+00:00
- decision: `candidate_directional_signal_only`
- manual_review_required: `true`
- signed_flow_status: `blocked`
- aggressor_side_status: `blocked`
- rationale: TradeDir is a stable vendor direction code with consistent contrast evidence, but signed-side mapping remains blocked.

## Evidence

- doc note: [vendor_hkex_doc_analysis_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/vendor_hkex_doc_analysis_2026-03-15.md)
- vendor_defined_flag: `true`
- no_aggressor_truth_flag: `true`
- vendor_export_field_flag: `true`
- semantic probe status: `weak_pass`
- semantic probe days_run: `53`
- semantic nonnull avg: `1`
- semantic distinct values: `3`
- contrast probe status: `candidate_directional_signal`
- contrast days_run: `3`
- contrast uptick gap avg: `-0.042857`
- contrast linkage gap avg: `0`

## Boundary

- Allowed:
  - descriptive distribution checks
  - candidate directional signal exploration with explicit caveat
  - manual-review-gated feature ideation
- Blocked:
  - signed flow factor production
  - aggressor-side truth labeling
  - verified-layer default admission

## Witness Dates

- `2026-01-05` uptick_gap=`-0.03802` dir0_specialness=`0.409834`
  bucket_gap_summary: `0930_1159:-0.031,1300_1559:-0.0472`
- `2026-02-24` uptick_gap=`-0.048271` dir0_specialness=`0.401896`
  bucket_gap_summary: `0930_1159:-0.0487,1300_1559:-0.0469`
- `2026-03-13` uptick_gap=`-0.042281` dir0_specialness=`0.398096`
  bucket_gap_summary: `0930_1159:-0.0393,1300_1559:-0.0458`
