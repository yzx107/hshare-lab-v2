# Semantic OrderType 2026

- generated_at: 2026-03-15T08:35:07+00:00
- scope: representative sample `2026-01-05 / 2026-02-24 / 2026-03-13`
- observed_ordertype_values: `1,2,3`
- status: `weak_pass`
- admissibility_impact: `allow_with_caveat`

## Key Findings

- `OrderType` is a stable 3-value code on the `2026` representative sample:
  - `distinct_ordertype_values_union = 3`
  - top values remain `1 / 3 / 2` on all three sample days
- Same-`OrderId` multi-value trajectories dominate the dataset:
  - `single_ordertype_orderid_rate_avg = 0.008788038383440383`
  - `multi_ordertype_orderid_rate_avg = 0.9912119616165596`
  - `ordertype_transition_pattern_count_total = 9`
- The leading transition patterns are stable across all three sample days:
  - `1,3` is the dominant trajectory
  - `1,2,3` is the second-largest trajectory
  - single-value `1` is a small residual pattern

## Daily Evidence

### 2026-01-05

- `distinct_ordertype_values = 3`
- `single_ordertype_orderid_rate = 0.009018891985718216`
- `multi_ordertype_orderid_rate = 0.9909811080142817`
- `top_ordertype_values = 1:56274762,3:55752113,2:4269164`
- `ordertype_transition_pattern_sample = 1,3:54092521,1,2,3:974756,1:507536`

### 2026-02-24

- `distinct_ordertype_values = 3`
- `single_ordertype_orderid_rate = 0.00914716340544133`
- `multi_ordertype_orderid_rate = 0.9908528365945587`
- `top_ordertype_values = 1:55816648,3:55294420,2:4187105`
- `ordertype_transition_pattern_sample = 1,3:53630655,1,2,3:1004156,1:510564`

### 2026-03-13

- `distinct_ordertype_values = 3`
- `single_ordertype_orderid_rate = 0.008198059759161602`
- `multi_ordertype_orderid_rate = 0.9918019402408385`
- `top_ordertype_values = 1:59948331,3:59443352,2:3966026`
- `ordertype_transition_pattern_sample = 1,3:57793348,1,2,3:1006092,1:491460`

## Interpretation

- `OrderType` should no longer be described as fully unknown on `2026`.
- The most defensible current interpretation is:
  - `OrderType` carries stable, repeated event-code-like structure
  - the field supports weak consistency checks and lifecycle-shape profiling
  - the field still does not support direct event semantics inference

## Research Boundary

- Safe to say:
  - `OrderType` is stable enough for descriptive / weak semantic checks
  - same-`OrderId` multi-value trajectories are a persistent structural fact in `2026`
  - lifecycle-shape studies by event-count profile can use the field with caveats
- Not safe to say:
  - `1 / 2 / 3` already map to confirmed event semantics
  - `OrderType` is sufficient on its own for execution-realism or fill-simulation research
  - `OrderType` can be used as a strict event-type label in latency-sensitive studies

## Project Wording

> For 2026 representative samples, `OrderType` is a stable 3-value code with persistent same-`OrderId` multi-value trajectories.
> This supports weak consistency checks and lifecycle-shape profiling, but it does not confirm event-type semantics.
> Current admissibility remains `allow_with_caveat`.
