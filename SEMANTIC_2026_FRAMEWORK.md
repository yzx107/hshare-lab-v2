# Semantic 2026 Framework

## Scope

This round only covers:

- `orderid_lifecycle` framework
- `tradedir` probe skeleton
- `ordertype` probe skeleton
- `session` probe skeleton
- semantic report and admissibility bridge outputs

## Out Of Scope

- full-year DQA rescans
- stage contract changes
- heavy recomputation or long I/O jobs

## Semantic Areas

- `orderid_lifecycle`
- `tradedir`
- `ordertype`
- `session`

## Output Contract

- daily result: `dqa/semantic/year=<year>/semantic_*_daily.parquet`
- yearly summary: `dqa/semantic/year=<year>/semantic_*_summary.parquet`
- unified report outputs:
  - `dqa/semantic/year=<year>/semantic_daily_summary.parquet`
  - `dqa/semantic/year=<year>/semantic_yearly_summary.parquet`
  - `dqa/semantic/year=<year>/semantic_admissibility_bridge.parquet`
  - `Research/Audits/semantic_<year>_summary.md`

## Relationship To Research Admissibility

Semantic probes do not directly assert research conclusions.
They provide gating signals that downstream admissibility rules can consume when deciding which modules are:

- allowed
- allowed with caveat
- blocked
- session-split sensitive
- manual-review only
