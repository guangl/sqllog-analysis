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
