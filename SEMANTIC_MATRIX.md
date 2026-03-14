# SEMANTIC_MATRIX

> 关键字段语义状态矩阵

| Field | Candidate Meaning | Status | Blocking | Notes |
|---|---|---|---|---|
| `TradeDir` | aggressor side | `unknown` | `yes` | 未验证前不得用于 signed flow |
| `BrokerNo` | trade-side / reporting / one-side broker | `unknown` | `partial` | broker alpha 阻塞 |
| `BidOrderID` | linked bid order id | `unknown` | `yes` | linkage research 阻塞 |
| `AskOrderID` | linked ask order id | `unknown` | `yes` | linkage research 阻塞 |
| `OrderId` | order identity key | `unknown` | `yes` | lifecycle / linkage 阻塞 |
| `OrderType` | order event type | `unknown` | `partial` | state model 阻塞 |
| `Session` | session tag | `unknown` | `partial` | 研究切分依赖 |
| `TickID` | trade identity / event key | `unknown` | `partial` | 候选唯一键待验证 |
| `Level` | depth / level indicator | `unknown` | `yes` | 禁止直接用于 book depth |
| `VolumePre` | queue-ahead proxy | `unknown` | `yes` | 禁止直接用于 queue analysis |
| `Type` | message subtype | `unknown` | `partial` | 语义未确认 |

## 状态定义

- `pass`: 可直接进入 verified layer
- `fail`: 不得进入研究主线
- `unknown`: 只能保留在 candidate cleaned，不得默认解释
