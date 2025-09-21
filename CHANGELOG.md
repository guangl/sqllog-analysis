# 变更日志

本文件记录该项目的重要变更。

## [v0.4.0] - 2025-09-22

### ✨ 新功能
- **优雅停止控制**：实现 Ctrl-C 信号处理和交互式 "stop" 命令，支持安全退出所有工作线程
- **解析错误收集**：新增可配置的解析错误写入功能，支持 JSONL 格式输出错误详情
- **专用错误写入线程**：使用 mpsc channel 和 BufWriter 实现高性能、非阻塞的错误写入
- **线程安全停止标志**：使用 `Arc<AtomicBool>` 实现并发安全的停止控制

### 🔧 代码质量提升
- **零 Clippy 警告**：通过严格的 Clippy 检查（all + pedantic + nursery + cargo 级别）
- **函数重构**：拆分长函数，将 `process_files`、`merge_to_runtime_config` 等大函数分解为更小的职责单一函数
- **类型优化**：修复 `ref_option` 等 Clippy 建议，提高代码惯用性
- **完整中文文档**：为所有主要模块和函数添加详细的中文注释和文档

### ⚙️ 配置扩展
- **错误写入配置**：新增 `sqllog.write_errors` 开关和 `sqllog.errors_out_path` 路径配置
- **线程数配置**：支持通过 `sqllog.parser_threads` 配置解析器线程数量
- **配置示例更新**：更新 `config.toml.example`，标明开发中功能的状态

### 📚 文档改进
- **功能状态澄清**：README 准确反映已实现和开发中的功能状态
- **使用说明完善**：添加详细的停止控制和错误诊断使用说明
- **技术特性说明**：详细介绍性能优化、并发安全和代码质量特性

### ⚠️ 重要说明
- **DuckDB 功能状态**：澄清 DuckDB 数据库连接和数据写入功能尚未实现，当前版本主要提供解析和错误诊断功能
- **向后兼容**：所有配置选项完全向后兼容，无破坏性变更

### 🛠️ 技术改进
- **并发性能**：优化线程池使用，减少锁争用
- **内存优化**：改进内存使用模式，减少不必要的分配
- **错误处理**：完整的错误链传播和用户友好的错误信息

[v0.4.0]: https://github.com/guangl/sqllog-analysis/releases/tag/v0.4.0
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

## [v0.3.1] - 2025-09-22
### 新增 / 改进
- 增加流式（chunked）解析 API：提供 `Sqllog::parse_all` 与 `Sqllog::parse_in_chunks`，支持按行读取原始字节（保留 CR/LF）、分块回调（成功/错误回调分离）以降低内存占用并支持实时处理。
- 将原有解析逻辑拆分为更小的 helper，并移除过时的 `get_raw_line`，部分解析实现迁移到 `src/sqllog/*` 模块内以改善代码结构。
- 在 `config.toml` 中新增 `log.level` 配置，用于设置运行时的日志等级（支持：error/warn/info/debug/trace/off），并将该配置注入到日志初始化流程中。
- 更新并修复基准（benches）和单元测试以配合新的 API，新增针对分块解析的测试用例。

### 修复
- 解决空白段被误报为 Format 错误的问题（跳过仅包含空白/换行的段落）。
- 修复并通过 Clippy 的若干警告（包括格式化和字符比较的改进）。

### 重构
- 将大量老旧的 sqllog 解析和 IO 代码重构为多个文件（`io.rs`、`parser.rs`、`utils.rs`、`types.rs`），移除部分不再使用的 DuckDB 写入器代码。

### 兼容性说明
- 新增的解析 API 向后兼容大多数使用场景，但旧的 `from_file_with_errors` 等已被移除或替换，使用前请查看 API 文档并更新调用处。

[v0.3.1]: https://github.com/guangl/sqllog-analysis/releases/tag/v0.3.1
