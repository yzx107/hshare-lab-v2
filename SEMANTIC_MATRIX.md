# SEMANTIC_MATRIX

> 关键字段语义状态矩阵

## Project-Level Boundary

- `raw inventory = inventory_closed`
- `golden sample = frozen`
- `field / reference / verified admission policy` 已成文
- `Query / report policy bridge` 已成文
- 当前矩阵仍不得绕过 `semantic verification` 直接放行高风险字段

| Field | Candidate Meaning | Status | Blocking | Notes |
|---|---|---|---|---|
| `TradeDir` | candidate directional code | `candidate_directional_signal` | `yes` | `2026` representative sample 上稳定为 `{0,1,2}`；`Dir=1/2` 在 `previous-trade price move` 上有稳定差异，但 signed-side mapping 仍未确认，保持 `requires_manual_review` |
| `BrokerNo` | trade-side / reporting / one-side broker | `unknown` | `partial` | broker alpha 阻塞 |
| `BidOrderID` | linked bid order id | `unknown` | `yes` | linkage research 阻塞 |
| `AskOrderID` | linked ask order id | `unknown` | `yes` | linkage research 阻塞 |
| `OrderId` | order identity key | `unknown` | `yes` | lifecycle / linkage 阻塞 |
| `OrderType` | order event code | `weak_pass` | `partial` | `2026` representative sample 上稳定为 `3` 值编码，且同一 `OrderId` 多值轨迹占绝大多数；支持弱一致性检查，但 `event_semantics_inference` 仍阻塞 |
| `Session` | session tag | `unknown` | `partial` | 研究切分依赖 |
| `TickID` | trade identity / event key | `unknown` | `partial` | 候选唯一键待验证 |
| `Level` | depth / level indicator | `unknown` | `yes` | 禁止直接用于 book depth |
| `VolumePre` | queue-ahead proxy | `unknown` | `yes` | 禁止直接用于 queue analysis |
| `Type` | message subtype | `unknown` | `partial` | 语义未确认 |

## 状态定义

- `pass`: 可直接进入 verified layer
- `fail`: 不得进入研究主线
- `weak_pass`: 可支持弱一致性 / 轮廓研究，但不能直接放行强语义解释
- `unknown`: 只能保留在 candidate cleaned，不得默认解释
- `candidate_directional_signal`: 存在稳定的候选方向相关信号，但还不能直接映射为 confirmed signed side，默认仍需人工审查

## Related Policy Docs

- [verified_admission_boundary_2026-03-15.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/verified_admission_boundary_2026-03-15.md)
- [query_report_policy_bridge_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/query_report_policy_bridge_2026-03-17.md)
- [broker_reference_readonly_boundary_2026-03-17.md](/Users/yxin/AI_Workstation/Hshare_Lab_v2/Research/Validation/broker_reference_readonly_boundary_2026-03-17.md)
