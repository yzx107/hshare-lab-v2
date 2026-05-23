# TradeDir Validation 2025

- generated_at: 2026-03-25T18:40:44+00:00
- decision: `stable_code_structure_only`
- manual_review_required: `true`
- signed_flow_status: `blocked`
- aggressor_side_status: `blocked`
- rationale: TradeDir is structurally stable enough for descriptive checks, but not ready for directional truth claims.

## Evidence

- doc note: [vendor_hkex_doc_analysis_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Notes/vendor_hkex_doc_analysis_2026-03-15.md)
- vendor_defined_flag: `true`
- no_aggressor_truth_flag: `true`
- vendor_export_field_flag: `true`
- semantic probe status: `weak_pass`
- semantic probe days_run: `246`
- semantic nonnull avg: `1`
- semantic distinct values: `3`
- contrast probe status: `None`
- contrast days_run: `None`
- contrast uptick gap avg: `na`
- contrast linkage gap avg: `na`

## Boundary

- Allowed:
  - descriptive distribution checks
  - candidate directional signal exploration with explicit caveat
  - manual-review-gated feature ideation
- Blocked:
  - signed flow factor production
  - aggressor-side truth labeling
  - verified-layer default admission
