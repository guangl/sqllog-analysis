# Release v0.3.1

此版本为 v0.3.0 的后续小版本，包含中文化相关的发布信息与补充说明，并作为对已推送翻译变更的正式发布通知。

主要变更：

- 将 `README.md`、`CHANGELOG.md`、`config.toml.example` 等用户文档翻译为中文。
- 将运行时的用户可见日志与错误信息翻译为中文（`src/config.rs`、`src/main.rs`、`src/duckdb_writer.rs` 等）。
- 将包版本在源文件中提升（工作分支提为 0.3.0）；发布为 `v0.3.1` 以避免覆盖已有的远程 `v0.3.0` tag。

兼容性：

- 配置字段名、数据库 SQL 语句与导出格式 token 未更改，现有 `config.toml` 文件继续兼容。

如何验证：

```powershell
cargo check --manifest-path D:\code\sqllog-analysis\Cargo.toml
cargo test --manifest-path D:\code\sqllog-analysis\Cargo.toml
```

（本地验证已通过。）

发布日期：2025-09-21
