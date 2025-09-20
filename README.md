# sqllog analysis

[![Release v0.2.1](https://img.shields.io/badge/release-v0.2.1-blue)](https://github.com/guangl/sqllog-analysis/releases/tag/v0.2.1) [![Rust Tests](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml/badge.svg)](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml)

对达梦数据库的 sqllog 进行分析，输入路径，导入到 duckdb，从而分析 sql 日志的内容，比如导出 trx

## Logging / 日志

本工具使用 `tracing`/`tracing-subscriber` 进行日志记录。

- 默认会把日志写入当前工作目录下的 `logs/` 目录（若目录不存在会尝试创建）。
- 默认日志文件名为 `sqllog-analysis-YYYY-MM-DD.log`，其中 `YYYY-MM-DD` 为程序启动当天的日期。
- 同时日志也会输出到终端（stdout），以便在交互或 CI 中直接查看。

可用命令行参数：

- `--log-file=<path>`: 指定日志文件（或目录）路径。如果传入目录，将在该目录下创建默认文件名；也可以直接传入完整文件路径。
- `--no-log`: 禁用文件/终端日志输出。
- `--log-level=<level>`: 指定日志级别（error, warn, info, debug, trace）。默认为 `info`。

示例：

```powershell
# 使用默认 logs 目录
.\sqllog-analysis.exe --input-file logs/example.log

# 指定日志目录
.\sqllog-analysis.exe --log-file D:\mylogs --input-file logs/example.log

# 指定完整日志文件路径
.\sqllog-analysis.exe --log-file D:\mylogs\my-sqllog.log --input-file logs/example.log

# 关闭日志
.\sqllog-analysis.exe --no-log --input-file logs/example.log
```

注意事项：

- 日志文件名按启动日期决定；长期运行进程不会自动按每天轮换（当前实现是在启动时创建当天文件）。如需运行时轮换，请在外部（systemd/logrotate）或替换为支持轮换的库（例如 `flexi_logger`）。
- 当程序无法创建日志目录或日志文件时，会在标准错误输出一条提示并继续（日志将被禁用）。

### 示例日志片段 / Sample log lines

下面是程序在终端或日志文件中可能输出的几行示例（使用 `info` / `warn` / `error` 级别）：

```
Sep 20 12:34:56 INFO  sqllog_analysis: 日志功能已启用，等级: Info
Sep 20 12:35:01 INFO  sqllog_analysis: 处理文件: test.log (parsed=4, errors=0)
Sep 20 12:35:02 WARN  sqllog_analysis: 无法创建日志目录 logs: Permission denied
Sep 20 12:35:03 ERROR sqllog_analysis: 读取文件 failed.log: IO错误: No such file or directory
```

字段说明：

- 时间（示例中为 `Sep 20 12:34:56`）：日志事件发生的本地时间（tracing 格式化输出）。
- 级别（INFO/WARN/ERROR）：日志级别。
- 目标（`sqllog_analysis`）：日志记录源（crate 名或模块）。
- 消息：具体日志内容，可能包括解析统计、错误信息或调试提示。

这些行同时会写入默认的日志文件 `logs/sqllog-analysis-YYYY-MM-DD.log`，便于长期保存和分析。

## DuckDB 写入器 (duckdb_writer)

本仓库提供了一个将解析后的 `Sqllog` 记录直接写入 DuckDB 的写入器，使用 DuckDB 的 Appender API 批量插入以提高性能。

主要 API（位于 crate 根的 `duckdb_writer` 模块）：

- `write_sqllogs_to_duckdb<P: AsRef<Path>>(db_path: P, records: &[Sqllog]) -> Result<()>`：默认使用 chunk 大小 1000，依据环境变量决定是否创建索引。
- `write_sqllogs_to_duckdb_with_chunk<P: AsRef<Path>>(db_path: P, records: &[Sqllog], chunk_size: usize) -> Result<()>`：手动指定 chunk 大小（`0` 会被归一为 `1`）。
- `write_sqllogs_to_duckdb_with_chunk_and_report<P: AsRef<Path>>(db_path: P, records: &[Sqllog], chunk_size: usize, create_indexes: bool) -> Result<Vec<IndexReport>>`：显式控制是否创建索引，并返回每个索引创建的 `IndexReport` 列表。

IndexReport 结构体字段：

- `statement: String`：执行的 CREATE INDEX 语句。
- `elapsed_ms: Option<u128>`：创建成功时的耗时（毫秒）。失败时为 `None`。
- `error: Option<String>`：若创建失败，此处包含错误字符串；成功时为 `None`。

索引与日志控制的环境变量：

- `SQLOG_CREATE_INDEXES`：是否在写入后创建索引。默认开启（未设置或非 "0" 时为 true）。将其设置为 `0` 则跳过索引创建。
- `SQLOG_INDEX_LOG_LEVEL`：索引创建期间的日志级别。默认 `info`；设置为 `debug` 将使用 `debug!` 打印更详细的索引创建信息。

示例（PowerShell）：

```powershell
# 在 /tmp/my.duckdb 写入，并显式创建索引，chunk=500
.
# 运行示例（伪代码）
# 调用 Rust API： duckdb_writer::write_sqllogs_to_duckdb_with_chunk_and_report("/tmp/my.duckdb", &records, 500, true)

# 在环境中开启 debug 日志用于索引创建
$env:SQLOG_INDEX_LOG_LEVEL = "debug"

# 或者禁止索引创建
$env:SQLOG_CREATE_INDEXES = "0"
```

注意：索引通常会在批量插入之后创建以获得更好的插入性能。若你希望索引创建失败能回滚整个批次，或者希望把索引创建改为一次性原子操作，可以在调用处实现更高层的事务控制或修改写入器行为（当前实现为每个索引独立短事务，并在 `IndexReport` 中报告失败）。

示例（Rust 代码片段）：

```rust
use sqllog_analysis::duckdb_writer;
use sqllog_analysis::sqllog::Sqllog;

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

CI 运行（GitHub Actions）：如果你想在 CI 中也执行“失败路径”测试，可以在 workflow 中设置环境变量：

```yaml
name: Rust

on: [push, pull_request]

jobs:
	test:
		runs-on: ubuntu-latest
		steps:
			- uses: actions/checkout@v4
			- name: Install Rust
				uses: dtolnay/rust-toolchain@v1
			- name: Run tests
				run: |
					cargo test --all -- --nocapture

	# Separate job to exercise the injected bad-index test so it doesn't
	# interfere with normal test runs or environment expectations.
	index-failure-test:
		runs-on: ubuntu-latest
		steps:
			- uses: actions/checkout@v4
			- name: Install Rust
				uses: dtolnay/rust-toolchain@v1
			- name: Run injected-index test
				env:
					SQLOG_INJECT_BAD_INDEX: "1"
					SQLOG_CREATE_INDEXES: "1"
				run: |
					cargo test --test duckdb_index_failure -- --nocapture

```
