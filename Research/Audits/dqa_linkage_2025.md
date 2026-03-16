# DQA Linkage 2025

- report_scope: sample + partial full-year checkpoint
- sample_generated_at: 2026-03-15T17:05:26+00:00
- full_year_pause_recorded_at: 2026-03-16T08:39:22+08:00
- completed_count_at_pause: 195
- failed_count_at_pause: 0
- pending_count_at_pause: 45
- current_run_status: paused_at_resumable_checkpoint

## Representative sample

- pass_days: 0
- warn_days: 3
- fail_days: 0
- id_equality_pass_days: 3
- time_anchor_unavailable_days: 3
- lag_not_verifiable_days: 3

## Full-year partial checkpoint

- Full-year run used the 2025 lightweight linkage path.
- Completed days through checkpoint continue to show:
  - `bid_id_equal_rate = 1.0`
  - `bid_time_usable_rate = 0.0`
  - `status = warn`
- Latest completed day before pause: `2025-10-22`
- Active in-flight days at pause record:
  - `2025-02-13`
  - `2025-02-14`
  - `2025-02-17`
  - `2025-02-18`
  - `2025-02-26`
  - `2025-10-23`

## Resume

- Resume command:
  - `env POLARS_MAX_THREADS=1 OMP_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 python3 -m Scripts.run_dqa_linkage --year 2025 --workers 6 --executor thread --resume`

This linkage audit separates direct `OrderId` equality from lag validation that requires usable order-side `SendTime`. For `2025`, full-year execution is intentionally optimized toward `ID-level linkage` plus the known `time_anchor_unavailable` boundary.
