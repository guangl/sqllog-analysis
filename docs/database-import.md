# 数据库导入功能

## 概述

sqllog-analysis v0.4.0 新增了强大的数据库导入功能，支持：

- **多数据库支持**: 可扩展的数据库抽象层，目前支持 DuckDB
- **多线程插入**: 使用 rayon 线程池并行解析和插入数据
- **批量操作**: 高性能的批量数据插入
- **多格式导出**: 支持 JSON、CSV、Excel 格式导出
- **内存/磁盘模式**: 可选择内存数据库或持久化磁盘数据库

## 快速开始

### 1. 配置设置

在 `config.toml` 中添加数据库配置：

```toml
[database]
# 数据库文件路径（磁盘模式）
db_path = "sqllogs.duckdb"

# 数据库类型（目前支持 "duckdb"）
db_type = "duckdb"

# 是否使用内存数据库（true=内存，false=磁盘）
use_in_memory = false
```

### 2. 基本使用

```rust
use sqllog_analysis::config::Config;
use sqllog_analysis::database::{
    ParallelProcessor, ParallelProcessConfig, ErrorStrategy
};

// 加载配置
let config = Config::load();

// 配置并行处理参数
let process_config = ParallelProcessConfig {
    num_threads: 4,           // 使用 4 个线程
    batch_size: 1000,        // 每批处理 1000 条记录
    show_progress: true,     // 显示进度信息
    error_strategy: ErrorStrategy::ContinueOnError,
};

// 创建处理器并处理文件
let processor = ParallelProcessor::new(config, process_config);
let files = vec!["sqllog/file1.log", "sqllog/file2.log"];
let stats = processor.process_files(&files)?;
```

### 3. 目录批量处理

```rust
use sqllog_analysis::database::process_directory;

// 处理整个目录中的所有 .log 文件
let stats = process_directory(
    "sqllog",           // 目录路径
    config,             // 运行时配置
    process_config      // 处理配置
)?;

stats.print_report(); // 打印统计报告
```

### 4. 数据导出

```rust
use sqllog_analysis::database::ExportFormat;

// 导出到不同格式
processor.export_to_file(ExportFormat::Json, "output.json")?;
processor.export_to_file(ExportFormat::Csv, "output.csv")?;
processor.export_to_file(ExportFormat::Excel, "output.xlsx")?;
```

## 数据库表结构

数据会被存储到 `sqllogs` 表中，包含以下字段：

| 字段名 | 类型 | 描述 |
|--------|------|------|
| id | BIGSERIAL | 主键，自增ID |
| occurrence_time | VARCHAR(32) | 日志发生时间 |
| ep | INTEGER | EP标识 |
| session | VARCHAR(64) | 会话ID |
| thread | VARCHAR(64) | 线程ID |
| username | VARCHAR(128) | 用户名 |
| trx_id | VARCHAR(64) | 事务ID |
| statement | VARCHAR(64) | 语句指针 |
| appname | VARCHAR(256) | 应用名 |
| ip | VARCHAR(45) | 客户端IP |
| sql_type | VARCHAR(32) | SQL类型 |
| description | TEXT | 语句描述 |
| execute_time | BIGINT | 执行时间（毫秒） |
| rowcount | BIGINT | 影响行数 |
| execute_id | BIGINT | 执行ID |
| created_at | TIMESTAMP | 记录创建时间 |

## 性能特性

- **并行解析**: 使用 rayon 线程池并行处理多个文件
- **批量插入**: 每批最多处理 1000 条记录，减少数据库调用
- **内存优化**: 支持内存数据库模式，提高处理速度
- **索引优化**: 自动创建索引提高查询性能

## 错误处理策略

支持三种错误处理策略：

1. **StopOnError**: 遇到错误立即停止处理
2. **ContinueOnError**: 记录错误但继续处理其他文件
3. **IgnoreErrors**: 忽略所有错误，静默处理

## 统计报告

处理完成后会生成详细的统计报告：

```
=== 处理统计报告 ===
文件处理数量: 5
解析记录总数: 10000
插入记录总数: 9950
解析错误数量: 50
插入错误数量: 0
总处理耗时: 2500 毫秒
处理速度: 4000.00 记录/秒
插入成功率: 99.5%
```

## 架构设计

### 多数据库支持

使用 trait 抽象层设计，易于扩展：

```rust
pub trait DatabaseProvider {
    fn initialize(&mut self) -> Result<()>;
    fn insert_batch(&mut self, records: &[Sqllog]) -> Result<usize>;
    fn export_data(&self, format: ExportFormat, path: &str) -> Result<()>;
    // ... 其他方法
}
```

### 多线程架构

- **文件级并行**: 多个线程同时解析不同文件
- **独立数据库连接**: 每个线程使用独立的数据库连接避免冲突
- **原子统计**: 使用原子操作收集跨线程的统计信息

## 命令行使用

可以通过命令行工具使用数据库功能：

```bash
# 导入单个文件
sqllog-analysis --import sqllog/file.log

# 批量导入目录
sqllog-analysis --import-dir sqllog/

# 导出数据
sqllog-analysis --export json output.json
sqllog-analysis --export csv output.csv

# 显示统计信息
sqllog-analysis --stats
```

## 未来扩展

- 支持 PostgreSQL、MySQL 等其他数据库
- 增加数据压缩和归档功能
- 支持实时流式处理
- 添加 Web API 接口
- 集成监控和告警功能