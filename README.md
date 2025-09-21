# sqllog 分析

[![Release v0.2.1](https://img.shields.io/badge/release-v0.2.1-blue)](https://github.com/guangl/sqllog-analysis/releases/tag/v0.2.1) [![Rust Tests](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml/badge.svg)](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml)

本工具用于分析达梦数据库产生的 sqllog 文件。它可读取日志文件、将解析结果导入 DuckDB 以便后续分析（例如导出事务相关信息）。

## 日志（Logging）

本项目使用 `tracing` / `tracing-subscriber` 进行日志记录。

- 默认会把日志写入当前工作目录下的 `logs/` 目录（若目录不存在会尝试创建）。
- 默认日志文件名为 `sqllog-analysis-YYYY-MM-DD.log`，其中 `YYYY-MM-DD` 为程序启动当天的日期。
- 同时日志也会输出到终端（stdout），方便交互或 CI 时直接查看。

可用命令行参数：

- `--log-file=<path>`：指定日志文件或日志目录。如果传入目录，程序会在该目录下创建默认文件名；也可以传入完整文件路径。
- `--no-log`：禁用日志输出（文件与终端）。
- `--log-level=<level>`：指定日志等级（error、warn、info、debug、trace）。默认 `info`。

示例：

```powershell
# 使用默认 logs 目录
.\sqllog-analysis.exe --input-file logs/example.log

.\sqllog-analysis.exe --log-file D:\mylogs --input-file logs/example.log
.\sqllog-analysis.exe --log-file D:\mylogs\my-sqllog.log --input-file logs/example.log

# 关闭日志
```

注意事项：

- 日志文件名按程序启动日期决定；长期运行的进程不会自动按天轮换（当前实现只在启动时创建当天的日志文件）。如需轮换，请使用外部工具（systemd、logrotate）或替换为支持轮换的库（例如 `flexi_logger`）。
- 当程序无法创建日志目录或日志文件时，会在标准错误输出一条提示并继续运行（此时日志将被禁用）。

### 示例日志片段

下面是程序在终端或日志文件中可能输出的若干示例行（包含 `info` / `warn` / `error` 等等级）：

```
Sep 20 12:34:56 INFO  sqllog_analysis: 日志功能已启用，等级: Info
Sep 20 12:35:01 INFO  sqllog_analysis: 处理文件: test.log (parsed=4, errors=0)
Sep 20 12:35:02 WARN  sqllog_analysis: 无法创建日志目录 logs: Permission denied
Sep 20 12:35:03 ERROR sqllog_analysis: 读取文件 failed.log: IO错误: No such file or directory
```

字段说明：

- 时间（示例中为 `Sep 20 12:34:56`）：事件发生时间（本地时间，由 tracing 格式化输出）。
- 级别（INFO/WARN/ERROR）：日志等级。
- 目标（`sqllog_analysis`）：日志记录源（crate 名或模块）。
- 消息：具体日志内容，可能包括解析统计、错误信息或调试提示。

这些日志行也会写入默认的日志文件 `logs/sqllog-analysis-YYYY-MM-DD.log`，便于长期保存与分析。

## DuckDB 写入器（duckdb_writer）

仓库包含一个将解析后的 `Sqllog` 记录写入 DuckDB 的写入器，使用 DuckDB 的 Appender API 进行批量插入以提高性能。

主要 API（位于 crate 根的 `duckdb_writer` 模块）：

- `write_sqllogs_to_duckdb<P: AsRef<Path>>(db_path: P, records: &[Sqllog]) -> Result<()>`：将记录批量写入指定的 DuckDB 数据库文件。当前实现不使用外部 chunk 流式写入，而通过 DuckDB Appender 批量插入提高性能。

注意：早期版本包含基于 chunk 的分块写入与索引创建报告的 API（例如 `write_sqllogs_to_duckdb_with_chunk_and_report`），这些接口已被简化或移除。如需历史实现或索引创建报告，请参阅变更日志（CHANGELOG.md）或在 issue 中提出请求。

如何运行注入式失败测试（无需额外环境变量）

项目提供了测试 helper 用于在集成测试中注入失败，用以验证错误处理逻辑。示例：`tests/duckdb_index_failure.rs` 中通过调用：

```rust
// 在测试开始处启用注入
sqllog_analysis::duckdb_writer::set_inject_bad_index(true);

// 运行测试（本地或 CI）
// 这会触发已注入的失败语句并验证 IndexReport 的错误处理逻辑
// 在 CI 中可以直接运行：
// cargo test --test duckdb_index_failure -- --nocapture
```

通常 CI 不需要额外环境变量；只需在 CI job 中运行相应测试即可。若确实需要仿真等效的外部注入（仅在特殊验证场景），可以采用自定义脚本或额外的测试 binary，但默认建议使用 crate 提供的测试 helper。

注意：索引通常在批量插入之后创建以获得更佳插入性能。若需在索引创建失败时回滚整个批次，或希望将索引创建改为一次性原子操作，可在调用处实现更高层的事务控制或调整写入器行为（当前实现为每个索引独立短事务，并在 `IndexReport` 中报告失败）。

示例（Rust 片段）：

```rust
use sqllog_analysis::duckdb_writer;

fn write_and_report(db_path: &str, records: &[Sqllog]) -> anyhow::Result<()> {
    // chunk_size = 500, create indexes = true
    let reports = duckdb_writer::write_sqllogs_to_duckdb_with_chunk_and_report(db_path, records, 500, true)?;

    for r in reports {
        match r.error {
            Some(err) => eprintln!("index '{}' failed: {}", r.statement, err),
            None => println!("index '{}' created in {:?} ms", r.statement, r.elapsed_ms),
        }
    }

    Ok(())
}
```