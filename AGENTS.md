# AGENTS.md

## 项目定位

Hshare Lab v2 是港股 tick / order / trade 研究基础设施重启版。

当前主线：

`raw -> candidate cleaned -> DQA -> semantic verification -> verified layer`

## 目录规范

- 所有脚本放在 `/Scripts`
- 所有研究输出放在 `/Research`
- 大体量 manifest 与 logs 默认写外置盘，不直接堆在仓库里

## 核心原则

- `raw` 不可变
- cleaning 只做 mechanical transformation
- 未验证字段不得默认解释
- 长任务必须 `visible + resumable`
- 所有关键结论必须能追溯到 contract 和报告

## 当前禁区

- 不沿用旧 repo 的 cleaned / DQA / feature 结论
- 不在未验证前直接做复杂 alpha
- 不把 `TradeDir`、`BrokerNo`、`Level`、`VolumePre` 当已知语义

## 当前优先级

1. raw inventory
2. candidate cleaned contract
3. DQA
4. semantic verification
5. verified layer

## 大数据管路管理
1. 不可黑箱要visilbe
2. 可恢复resumable
3. 可追溯traceable
4. 可观测

