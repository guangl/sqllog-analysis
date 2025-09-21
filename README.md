# sqllog 分析

[![Release v0.2.1](https://img.shields.io/badge/release-v0.2.1-blue)](https://github.com/guangl/sqllog-analysis/releases/tag/v0.2.1) [![Rust Tests](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml/badge.svg)](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml)

本工具用于分析达梦数据库产生的 sqllog 文件。它可读取日志文件、将解析结果导入 DuckDB 以便后续分析（例如导出事务相关信息）。

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