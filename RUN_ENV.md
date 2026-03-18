# Run Environment

## Purpose

This document records the operational environment for Hshare Lab v2 so collaborators do not need to reconstruct machine roles from chat history.

## Roles

- Primary compute node: Mac mini
- Secondary mobile node: MacBook Air

## Execution Rule

- All heavy jobs run on the primary compute node.
- Heavy jobs include:
  - full-year staging
  - full-year DQA
  - semantic jobs with large parquet scans
  - verified-layer batch builds

## Secondary Node Usage

- The secondary mobile node is used as:
  - a remote terminal
  - a control surface for logs, heartbeats, and commands
  - an optional physical host for the external `Data` disk

## Data Attachment Rule

- The `Data` disk may be physically attached to the secondary node.
- When that happens, the disk is exported to the primary compute node over the local remote-sharing path.
- Even in that setup, heavy jobs are still launched from the primary compute node.

## Collaboration Rule

- Repo code, contracts, and batch orchestration are maintained from the primary compute node.
- The secondary node should not be treated as an independent heavy-compute environment.
- Query and light inspection may be initiated remotely, but production runs remain primary-node only.

## Operational Discipline

- Long jobs must remain:
  - visible
  - resumable
  - traceable
  - observable
- Performance work may optimize execution paths, task layout, and materialization strategy,
  but may not reduce DQA / semantic metric quality or replace formal metrics with weaker proxies
  unless the contract explicitly allows it.
- New heavy pipelines should scale gradually:
  - single-day smoke
  - 1 sample
  - 3 sample
  - then full-year

## Notes

- Keep machine-specific private details such as personal IPs, local account names, and private network topology out of public repo documentation.
- This file intentionally documents roles and rules, not private infrastructure secrets.
