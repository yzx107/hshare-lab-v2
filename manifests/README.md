# manifests

本目录保留仓库内的 manifest 规范与样例。

真正的大体量 manifest 数据优先写入外置盘：
`/Volumes/Data/港股Tick数据/manifests`

当前仓库内保留的轻量 release registry：

- `field_release_registry.json`：research object / derived field / caveat namespace 的 machine-readable release registry
- `orderbook_research_handoff.json`：`orderbook_replay__top_of_book_only` 给下游研究消费的 machine-readable handoff contract
