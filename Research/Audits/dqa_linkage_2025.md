# DQA Linkage 2025

- generated_at: 2026-03-16T17:07:00+00:00
- completed_count: 246
- failed_count: 0
- pending_count: 0

- pass_days: 0
- warn_days: 246
- fail_days: 0
- id_equality_pass_days: 246
- time_anchor_unavailable_days: 246
- lag_not_verifiable_days: 246

## Yearly conclusion

- `id_linkage_status = pass`
- `time_anchor_status = unavailable`
- `lag_linkage_status = not_verifiable`
- `research_time_grade = coarse_only`

This linkage audit now separates direct `OrderId` equality from lag validation that requires usable order-side `SendTime`. On the 2025 full year, all completed dates remained in the expected `ID-level pass + no usable order-side SendTime` regime.
