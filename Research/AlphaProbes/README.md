# Alpha Probes

This directory contains experimental alpha research and candidate factor generation.

Unlike the main infrastructure pipeline, Alpha Probes are designed to move fast and test specific investment hypotheses using the `candidate_cleaned` or `verified` datasets.

Probes may use fields whose semantic verification status is still `candidate`, provided that the resulting report clearly caveats this dependency. The ultimate goal of an Alpha Probe is to find predicting power (edge).

## Current Probes

- `broker_flow_v0`: Investigates net buying/selling pressure based on `BrokerNo` and `Dir` (TradeDir).
- `darkpool_ratio_v0`: Investigates the proportion of algorithmic/institutional off-exchange trading using the `HKDarkPool` source group.
