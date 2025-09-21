# sqllog 分析# sqllog 分析



[![Release v0.4.0](https://img.shields.io/badge/release-v0.4.0-blue)](https://github.com/guangl/sqllog-analysis/releases/tag/v0.4.0) [![Rust Tests](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml/badge.svg)](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml)[![Release v0.3.0](https://img.shields.io/badge/release-v0.3.0-blue)](https://github.com/guangl/sqllog-analysis/releases/tag/v0.3.0) [![Rust Tests](https://github.com/guangl/## 导出配置说明（开发中）



本工具用于分析达梦数据库产生的 sqllog 文件。它可以解析日志文件、验证格式，并收集解析错误用于诊断。> **⚠️ 注注意和变更说明：



> **⚠️ 开发状态**：DuckDB 数据存储和导出功能正在开发中。当前版本主要提供 sqllog 文件解析和错误诊断功能。- `export.file_size_bytes` 不能设置为 `0`。如果在配置文件中设置为 `0`，程序在启动时会视为配置错误并以非零退出码终止。要表示"无上限"，请删除该项或注释掉它。

- **数据库导出功能开发中**：当前版本只解析 sqllog 文件，不会创建 DuckDB 数据库或导出数据。导出相关配置已预留，待后续版本实现。

## ✨ 新功能 (v0.4.0)

## 性能基准测试

### 停止控制

- **Ctrl-C 支持**：程序运行时按 Ctrl-C 可优雅停止所有解析任务本仓库包含性能基准测试：

- **交互式停止**：在交互式终端中输入 "stop" 并回车也可停止程序

- **并发安全**：所有工作线程会在收到停止信号后安全退出```powershell

# 运行 sqllog 解析性能基准

### 解析错误收集cargo bench --bench sqllog_bench

- **错误写入功能**：可配置将解析失败的日志行写入单独的错误文件

- **JSONL 格式**：错误信息以 JSON Lines 格式保存，便于后续分析# 运行日期时间解析性能基准

- **线程安全**：使用专用写入线程，避免主解析流程阻塞cargo bench --bench datetime_bench

```

### 代码质量提升

- **零 Clippy 警告**：代码通过严格的 Clippy 检查（all + pedantic + nursery + cargo）当前基准测试主要测量：

- **函数重构**：拆分长函数，提高代码可读性和可维护性- sqllog 文件解析性能

- **中文文档**：添加详细的中文注释和文档- 日期时间字符串解析性能



## 使用说明> **计划中**：待 DuckDB 写入功能实现后，将添加数据库写入性能基准测试。已就绪，但实际的 DuckDB 数据导出功能尚未实现。



### 基本运行在 `[export]` 节中可以设置 `file_size_bytes` 来指定单个导出文件的大小上限（以字节为单位）。注意：



```powershell- `export.file_size_bytes` 不能设置为 0。配置为 `0` 会被视为配置错误，程序在启动时解析配置时会打印错误并以非零退出码退出。要表示"无上限"，请删除该项或不在配置中设置它；要限制大小，请设置为大于 0 的正整数。g-analysis/actions/workflows/rust.yml/badge.svg)](https://github.com/guangl/sqllog-analysis/actions/workflows/rust.yml)

# 使用默认配置运行

cargo run --release本工具用于分析达梦数据库产生的 sqllog 文件。它可以解析日志文件、验证格式，并收集解析错误用于诊断。



# 或者直接运行编译后的程序> **⚠️ 开发状态**：DuckDB 数据存储和导出功能正在开发中。当前版本主要提供 sqllog 文件解析和错误诊断功能。

./target/release/sqllog-analysis.exe

```## ✨ 新功能 (v0.3.0)



### 停止程序### 停止控制

- **Ctrl-C 支持**：程序运行时按 Ctrl-C 可优雅停止所有解析任务

程序提供多种停止方式：- **交互式停止**：在交互式终端中输入 "stop" 并回车也可停止程序

- **并发安全**：所有工作线程会在收到停止信号后安全退出

1. **Ctrl-C**：在程序运行时按 `Ctrl-C` 可优雅停止

2. **交互式停止**：在交互式终端中输入 "stop" 并按回车### 解析错误收集

3. **自动完成**：当所有文件处理完成后程序会自动退出- **错误写入功能**：可配置将解析失败的日志行写入单独的错误文件

- **JSONL 格式**：错误信息以 JSON Lines 格式保存，便于后续分析

### 错误诊断- **线程安全**：使用专用写入线程，避免主解析流程阻塞



如果启用了错误写入功能（`write_errors = true`），您可以检查错误文件来诊断解析问题：### 代码质量提升

- **零 Clippy 警告**：代码通过严格的 Clippy 检查（all + pedantic + nursery + cargo）

```powershell- **函数重构**：拆分长函数，提高代码可读性和可维护性

# 查看错误文件内容- **中文文档**：添加详细的中文注释和文档

type parse_errors.jsonl

## 使用说明

# 统计错误数量

(Get-Content parse_errors.jsonl).Count### 基本运行

```

```powershell

## 配置说明# 使用默认配置运行

cargo run --release

### sqllog 配置

# 或者直接运行编译后的程序

```toml./target/release/sqllog-analysis.exe

[sqllog]```

# sqllog 文件目录

sqllog_dir = "sqllog"### 停止程序

# 解析器线程数量（默认：10）

parser_threads = 10程序提供多种停止方式：

# 是否写入解析错误到文件

write_errors = true1. **Ctrl-C**：在程序运行时按 `Ctrl-C` 可优雅停止

# 错误输出文件路径   - 所有正在进行的解析任务会安全完成当前文件

errors_out_path = "parse_errors.jsonl"   - 数据库连接会正确关闭

```   - 临时文件会被清理



### 解析错误收集功能2. **交互式停止**：在交互式终端中输入 "stop"

   ```

当启用 `write_errors = true` 时，程序会将无法解析的 sqllog 行保存到指定的错误文件中，格式为 JSONL（每行一个 JSON 对象）：   正在处理文件...

   stop  [按回车]

```json   收到停止信号，正在安全退出...

{"path":"sqllog/test.log","line":42,"error":"Invalid datetime format","raw":"malformed log line..."}   ```

{"path":"sqllog/test.log","line":43,"error":"Missing required field","raw":"incomplete log entry"}

```3. **自动完成**：当所有文件处理完成后程序会自动退出



## TODO### 错误诊断



目前需要的功能有：如果启用了错误写入功能（`write_errors = true`），您可以检查错误文件来诊断解析问题：

- [x] 多线程并发分析 sqllog，每个文件都有一个线程用来分析（线程数量可以配置）

- [ ] **多线程并发插入到 duckdb 中的 sqllogs 表里面**```powershell

- [ ] **duckdb 可以导出成 excel/csv/json（导出类型可以配置）**# 查看错误文件内容

- [ ] **duckdb 可以使用内存数据库或者磁盘数据库**type parse_errors.jsonl

- [x] 支持 Ctrl-C 和交互式停止

- [x] 解析错误收集和导出功能# 统计错误数量

(Get-Content parse_errors.jsonl).Count

### ⚠️ 重要说明

# 使用 jq 分析错误类型（需要安装 jq）

**DuckDB 数据库功能尚未完成**：type parse_errors.jsonl | jq '.error' | sort | uniq -c

- ✅ DuckDB 依赖和配置已添加```

- ✅ 数据库路径配置已实现

- ❌ **实际的数据库连接和数据插入功能尚未实现**## TODO

- ❌ **数据导出功能尚未实现**

目前需要的功能有

目前程序只能：- [x] 多线程并发分析 sqllog，每个文件都有一个线程用来分析（线程数量可以配置）；

1. 解析 sqllog 文件并验证格式- [ ] **多线程并发插入到 duckdb 中的 sqllogs 表里面**；

2. 收集解析错误到 JSONL 文件- [ ] **duckdb 可以导出成 excel/csv/json（导出类型可以配置）**；

3. 支持优雅停止（Ctrl-C 或交互式 "stop"）- [ ] **duckdb 可以使用内存数据库或者磁盘数据库**；

- [x] 支持 Ctrl-C 和交互式停止

## 技术特性- [x] 解析错误收集和导出功能



### 性能优化### ⚠️ 重要说明

- **并行处理**：使用 rayon 线程池并行解析多个 sqllog 文件

- **异步日志**：使用 tracing 的非阻塞写入器，避免 I/O 阻塞主线程**DuckDB 数据库功能尚未完成**：

- **专用错误写入线程**：错误收集使用单独线程和 channel，避免解析线程阻塞- ✅ DuckDB 依赖和配置已添加

- ✅ 数据库路径配置已实现

### 并发安全- ❌ **实际的数据库连接和数据插入功能尚未实现**

- **原子停止标志**：使用 Arc<AtomicBool> 实现线程安全的停止控制- ❌ **数据导出功能尚未实现**

- **信号处理**：注册 Ctrl-C 处理器，支持优雅停止

- **无锁设计**：错误写入使用 channel 而非锁，避免争用目前程序只能：

1. 解析 sqllog 文件并验证格式

### 代码质量2. 收集解析错误到 JSONL 文件

- **零警告**：通过最严格的 Clippy 检查（all + pedantic + nursery + cargo）3. 支持优雅停止（Ctrl-C 或交互式 "stop"）

- **函数式设计**：拆分长函数，提高可读性和可维护性

- **错误处理**：完整的错误链传播和用户友好的错误信息**下一步开发计划**：

1. 实现 DuckDB 数据库连接管理

## 性能基准测试2. 添加解析数据到数据库的插入逻辑

3. 实现多格式数据导出功能（CSV/JSON/Excel）

```powershell

# 运行 sqllog 解析性能基准## 日志（Logging）

cargo bench --bench sqllog_bench

本项目使用 `tracing` / `tracing-subscriber` 进行日志记录。

# 运行日期时间解析性能基准

cargo bench --bench datetime_bench- 默认会把日志写入当前工作目录下的 `logs/` 目录（若目录不存在会尝试创建）。

```- 默认日志文件名为 `sqllog-analysis-YYYY-MM-DD.log`，其中 `YYYY-MM-DD` 为程序启动当天的日期。

- 同时日志也会输出到终端（stdout），方便交互或 CI 时直接查看。

当前基准测试主要测量：

- sqllog 文件解析性能日志行为现在由配置文件中的 `[log]` 节控制（请编辑 `config.toml` 或 `config.toml.example`）：

- 日期时间字符串解析性能

- `enable_stdout`：是否将日志同时输出到终端（stdout）。在未显式设置时，调试构建下通常为开启；在发布构建请显式设置为 `true` 或 `false`。

> **计划中**：待 DuckDB 写入功能实现后，将添加数据库写入性能基准测试。- `log_dir`：日志文件所在目录（相对或绝对路径），程序会在该目录写入按日期命名的日志文件。

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

新增的 `[sqllog]` 配置节用于指定 sqllog 文件存放目录和错误处理：

- `sqllog_dir`：sqllog 目录路径，支持相对路径或绝对路径。
- `write_errors`：是否将解析失败的行写入错误文件（默认：false）
- `errors_out_path`：错误输出文件路径（默认：parse_errors.log）

### 解析错误收集功能

当启用 `write_errors = true` 时，程序会将无法解析的 sqllog 行保存到指定的错误文件中，格式为 JSONL（每行一个 JSON 对象）：

```json
{"path":"sqllog/test.log","line":42,"error":"Invalid datetime format","raw":"malformed log line..."}
{"path":"sqllog/test.log","line":43,"error":"Missing required field","raw":"incomplete log entry"}
```

每个错误记录包含：
- `path`：源文件路径
- `line`：出错的行号
- `error`：具体错误描述
- `raw`：原始日志行内容

优先级与回退规则：

1. 如果在配置文件（或通过环境/CLI）指定了 `[sqllog].sqllog_dir`，程序将使用该路径。
2. 否则，程序会尝试从 `[database].db_path` 推导父目录作为 sqllog 目录。
3. 如果仍然无法推导（例如 `db_path` 为空或没有父目录），程序将使用相对目录 `sqllog`（即运行目录下的 `sqllog/`）。

示例（config.toml 中）：

```toml
[sqllog]
# sqllog 文件目录
sqllog_dir = "sqllog"
# 是否写入解析错误到文件
write_errors = true
# 错误输出文件路径
errors_out_path = "parse_errors.jsonl"
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

## 🔧 技术特性

### 性能优化
- **并行处理**：使用 rayon 线程池并行解析多个 sqllog 文件
- **异步日志**：使用 tracing 的非阻塞写入器，避免 I/O 阻塞主线程
- **专用错误写入线程**：错误收集使用单独线程和 channel，避免解析线程阻塞
- **缓冲写入**：使用 BufWriter 提高文件写入性能

### 并发安全
- **原子停止标志**：使用 Arc<AtomicBool> 实现线程安全的停止控制
- **信号处理**：注册 Ctrl-C 处理器，支持优雅停止
- **无锁设计**：错误写入使用 channel 而非锁，避免争用

### 代码质量
- **零警告**：通过最严格的 Clippy 检查（all + pedantic + nursery + cargo）
- **函数式设计**：拆分长函数，提高可读性和可维护性
- **错误处理**：完整的错误链传播和用户友好的错误信息
- **类型安全**：充分利用 Rust 类型系统避免运行时错误

### 配置灵活性
- **多层配置**：支持配置文件、环境变量和默认值
- **路径智能解析**：自动处理相对路径和绝对路径
- **格式多样**：支持 CSV、JSON、Excel 多种导出格式