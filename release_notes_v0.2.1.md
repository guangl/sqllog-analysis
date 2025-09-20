# v0.2.1 — 2025-09-20

What's new

- 增加 DuckDB 批量写入器（基于 Appender API），支持可配置的分块大小（`--duckdb-chunk-size`）。
- 增加 `IndexReport`，对每个索引的创建结果进行报告（耗时 + 错误信息）。
- 在 CLI 中添加导出 `IndexReport` 为 JSON 的能力：`--duckdb-path`, `--duckdb-report`（使用 `-` 输出到 stdout）。
- 增加针对索引创建失败的可注入测试钩子，用于可靠地覆盖失败路径的集成测试。
- 在 CI 中启用严格的 clippy（将警告视为错误，并启用 pedantic/nursery/cargo 规则），保持更高的代码质量。

Highlights

- 通过先批量插入再创建索引的策略，显著提升了批量写入性能。
- `IndexReport` 有助于在自动化流程中捕获索引创建的回归与异常，并可直接写为 JSON 以便集成到监控或上报系统。

Breaking changes

- `Sqllog` 中用于时间与计数的数值字段迁移为带符号的 `i64`（数据库字段相应改为 `BIGINT`）。如果你依赖这些字段的二进制格式或外部脚本，请注意更新。

Quick usage

Write parsed logs to DuckDB and print index report to stdout as JSON:

```powershell
sqllog-analysis.exe --duckdb-path=out.duckdb --duckdb-report=- --duckdb-chunk-size=500
```

Notes

- This is a draft release; you can review and publish it on GitHub. For full details see `CHANGELOG.md`.
