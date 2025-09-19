# sqllog analysis

[![Rust Tests](https://github.com/[你的GitHub用户名]/sqllog-analysis/actions/workflows/rust.yml/badge.svg)](https://github.com/[你的GitHub用户名]/sqllog-analysis/actions/workflows/rust.yml)

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
