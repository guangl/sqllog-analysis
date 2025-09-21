# sqllog 分析

[![Release v0.2.1](https://img.shields.io/badge/release-v0.2.1-blue)](https://github.com/guangl/sqllog-analysis/releases/tag/v0.2.1) [![Rust Tests](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml/badge.svg)](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml)

本工具用于分析达梦数据库产生的 sqllog 文件。它可读取日志文件、将解析结果导入 DuckDB 以便后续分析（例如导出事务相关信息）。

## TODO

目前需要的功能有
- [ ] 多线程并发分析 sqllog，每个文件都有一个线程用来分析（线程数量可以配置）；
- [ ] 多线程并发插入到 duckdb 中的 sqllogs 表里面；
- [ ] duckdb 可以导出成 excel/csv/json（导出类型可以配置）；
- [ ] duckdb 可以使用内存数据库或者磁盘数据库；

## 日志（Logging）

本项目使用 `tracing` / `tracing-subscriber` 进行日志记录。

- 默认会把日志写入当前工作目录下的 `logs/` 目录（若目录不存在会尝试创建）。
- 默认日志文件名为 `sqllog-analysis-YYYY-MM-DD.log`，其中 `YYYY-MM-DD` 为程序启动当天的日期。
- 同时日志也会输出到终端（stdout），方便交互或 CI 时直接查看。

日志行为现在由配置文件中的 `[log]` 节控制（请编辑 `config.toml` 或 `config.toml.example`）：

- `enable_stdout`：是否将日志同时输出到终端（stdout）。在未显式设置时，调试构建下通常为开启；在发布构建请显式设置为 `true` 或 `false`。
- `log_dir`：日志文件所在目录（相对或绝对路径），程序会在该目录写入按日期命名的日志文件。

示例（`config.toml` 中的 `[log]` 节）：

```toml
[log]
# 是否在 stdout 打印日志（true/false）
enable_stdout = true
# 日志目录，支持相对路径或绝对路径
log_dir = "logs"
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

## sqllog 目录配置

新增的 `[sqllog]` 配置节用于指定 sqllog 文件存放目录：

- `sqllog_dir`：sqllog 目录路径，支持相对路径或绝对路径。

优先级与回退规则：

1. 如果在配置文件（或通过环境/CLI）指定了 `[sqllog].sqllog_dir`，程序将使用该路径。
2. 否则，程序会尝试从 `[database].db_path` 推导父目录作为 sqllog 目录。
3. 如果仍然无法推导（例如 `db_path` 为空或没有父目录），程序将使用相对目录 `sqllog`（即运行目录下的 `sqllog/`）。

示例（config.toml 中）：

```toml
[sqllog]
sqllog_dir = "sqllog"
```

说明：将 `sqllog_dir` 设置为 `"sqllog"` 是默认且推荐的简单用法，适合在同一目录下管理日志与解析数据的场景。若需要把 sqllog 存放在集中日志服务器或不同分区，请使用绝对路径。

## 导出配置说明（重要）

在 `[export]` 节中可以设置 `file_size_bytes` 来指定单个导出文件的大小上限（以字节为单位）。注意：

- `export.file_size_bytes` 不能设置为 0。配置为 `0` 会被视为配置错误，程序在启动时解析配置时会打印错误并以非零退出码退出。要表示“无上限”，请删除该项或不在配置中设置它；要限制大小，请设置为大于 0 的正整数。

示例：

```toml
[export]
# 导出开关
enabled = true
# 导出目标格式：csv/json/excel
format = "csv"
# 导出目标路径
out_path = "exports/out.csv"
# 单个导出文件大小上限（字节），注意：不能为 0；删除此行以表示无上限
file_size_bytes = 104857600
```

注意和变更说明：

- `export.file_size_bytes` 不能设置为 `0`。如果在配置文件中设置为 `0`，程序在启动时会视为配置错误并以非零退出码终止。要表示“无上限”，请删除该项或注释掉它。
- 内存导出（in-memory export）行为变更：当配置或运行时选择使用内存导出路径（`use_in_memory`），程序会先在内存中的 DuckDB 写入数据，然后——默认情况下——不会把内存数据库自动 ATTACH 到磁盘并 CTAS 导出回磁盘文件。换言之，内存路径现在是“内存写入仅保留在内存”。如果你需要旧的将内存数据写回磁盘的行为，请使用相应的配置开关（见 `config.toml.example` 中的说明）。

基准（Benchmark）说明：

本仓库包含一个 Criterion 基准（`benches/duckdb_write_bench.rs`），测试三种写入路径的性能：

- `appender_direct`：直接通过写入 API 将记录插入磁盘上的 DuckDB。
- `in_memory_ctas`：在内存中写入，然后（旧行为）CTAS 导出到磁盘；在当前默认实现下内存写入不会导出到磁盘，基准仍衡量内存写入成本。
- `csv_copy`：先写入临时 CSV，然后用 DuckDB 的 COPY FROM 导入。

运行基准：

```powershell
cargo bench --bench duckdb_write_bench
```

基准会在多个规模下测量（例如 10k, 50k, 200k 条记录），并输出 Criterion 的报告。下面我会运行基准并把结果绘制为 PNG 图表以便直观比较。