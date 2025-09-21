# Release v0.3.0

本次发布主要包含对用户可见文档与日志的中文化、本地化处理，以及一些代码内的提示/日志文本翻译（不改变程序行为或 API）。

主要变更：

- 将 `README.md`、`CHANGELOG.md`、`config.toml.example` 中的用户文档翻译为中文，并保留示例命令与代码片段。
- 将 `src/config.rs`、`src/main.rs`、`src/duckdb_writer.rs` 中的用户可见日志与错误消息翻译为中文（仅修改字符串/注释，不改标识符或逻辑）。
- 保持所有配置字段名、SQL 语句与导出格式 token 不变，确保向后兼容。
- 版本号由 `0.1.0` 提升到 `0.3.0`。

兼容性与注意事项：

- config 文件字段名未更改；现有 `config.toml` 文件仍然兼容。
- 如果 CI 或脚本依赖 README 中英文内容，请在合并后相应更新脚本或文档引用。

如何验证（本地）：

```powershell
cargo check --manifest-path D:\code\sqllog-analysis\Cargo.toml
cargo test --manifest-path D:\code\sqllog-analysis\Cargo.toml
```

以上命令在本地运行均通过。

感谢使用与测试！
