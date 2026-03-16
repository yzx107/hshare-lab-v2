# DQA Linkage 2026

- generated_at: 2026-03-16T00:27:45+08:00
- completed_count: 48
- failed_count: 0
- pending_count: 0

- pass_days: 48
- warn_days: 0
- fail_days: 0
- id_equality_pass_days: 48
- time_anchor_unavailable_days: 0
- lag_not_verifiable_days: 0

This linkage audit separates direct `OrderId` equality from lag validation that requires usable order-side `SendTime`. `2026` full-year linkage completed with `48/48` pass days and no observed yearly fallback into `time_anchor_unavailable`.
