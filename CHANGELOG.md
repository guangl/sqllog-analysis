# 变更日志

本文件记录该项目的重要变更。
## [v0.2.1] - 2025-09-20
### 新增
- 基于 DuckDB Appender 的 `Sqllog` 批量写入器，支持可配置的 chunk 大小。
- `IndexReport`：用于记录每个索引创建的耗时与错误信息。
- 新增 CLI 选项以 JSON 格式导出 `IndexReport`：`--duckdb-path`、`--duckdb-report`、`--duckdb-chunk-size`。
- 为 DuckDB 写入器和索引失败注入添加集成测试。
- 在 GitHub Actions 工作流中启用严格的 Clippy 检查（pedantic / nursery / cargo）。

### 变更
- `Sqllog` 中的数值字段迁移为带符号的 `i64`，数据库模式同步更新为 `BIGINT`。
- 更新 README 和文档，加入使用示例与 CI 状态徽章。

### 修复
- 解决 Clippy pedantic 报告的问题并进行若干小的重构。

[v0.2.1]: https://github.com/guangl/sqllog-analysis/releases/tag/v0.2.1

## [v0.3.0] - 2025-09-21
### 新增 / 调整
- 对用户可见的文档与日志进行中文化翻译：包括 `README.md`、`CHANGELOG.md`、`config.toml.example` 以及程序在运行时输出给用户或 CI 的部分日志信息（注意：仅替换字符串/注释，不改变代码标识符或逻辑）。
- 将包版本从 `0.1.0` 提升到 `0.3.0`。

### 兼容性说明
- 配置字段名、SQL 语句与导出格式 token 保持不变，现有 `config.toml` 文件应与本次发布兼容。

[v0.3.0]: https://github.com/guangl/sqllog-analysis/releases/tag/v0.3.0
