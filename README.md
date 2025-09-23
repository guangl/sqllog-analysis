# dm-sqllog-parser v1.0.0 🎉

[![Release v1.0.0](https://img.shields.io/badge/release-v1.0.0-green)](https://github.com/guangl/dm-sqllog-parser/releases/tag/v1.0.0) [![Rust Tests](https://github.com/guangl/dm-sqllog-parser/actions/workflows/rust.yml/badge.svg)](https://github.com/guangl/dm-sqllog-parser/actions/workflows/rust.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**一个企业级的达梦数据库 SQL 日志解析工具**，专门用于高性能处理大规模数据库日志文件，提供完整的解析、存储、分析和导出解决方案。

本工具用于解析达梦数据库产生的 sqllog 文件。它可以解析日志文件、验证格式，并收集解析错误用于诊断。

> **⚠️ 注意和变更说明：**
> - `export.file_size_bytes` 不能设置为 `0`。如果在配置文件中设置为 `0`，程序在启动时会视为配置错误并以非零退出码终止。要表示"无上限"，请删除该项或注释掉它。

## ✨ 核心特性

### 🚀 高性能处理

- **并行解析**：多线程并发处理多个日志文件
- **智能拼接**：自动处理跨行的 SQL 语句和多行描述
- **批量处理**：支持目录级别的批量文件发现和处理
- **内存优化**：高效的内存使用和缓冲策略

### 📊 数据质量保证

> **⚠️ 开发状态**：DuckDB 数据存储和导出功能正在开发中。当前版本主要提供 sqllog 文件解析和错误诊断功能。

- **数据库导出功能开发中**：当前版本只解析 sqllog 文件，不会创建 DuckDB 数据库或导出数据。导出相关配置已预留，待后续版本实现。
- **严格验证**：确保只有格式完整的记录进入数据库
- **错误追踪**：详细记录解析失败的具体原因和位置
- **格式检查**：验证 EXECTIME/ROWCOUNT/EXEC_ID 参数的完整性
- **质量报告**：生成详细的处理统计和错误分析

### 🔧 灵活配置

- **多格式导出**：支持 CSV、JSON、Excel 等格式
- **可配置处理**：支持线程数、块大小等性能调优
- **智能路径处理**：自动处理相对路径和绝对路径
- **错误输出控制**：可配置的错误信息收集和输出

### 🛡️ 稳定可靠

- **优雅停止**：支持 Ctrl-C 和交互式停止
- **错误隔离**：单个文件处理失败不影响其他文件
- **线程安全**：完整的并发安全保证
- **资源管理**：自动清理临时文件和数据库连接
- **Ctrl-C 支持**：程序运行时按 Ctrl-C 可优雅停止所有解析任务
- **并发安全**：所有工作线程会在收到停止信号后安全退出

## 📈 性能基准测试

本仓库包含性能基准测试：

```powershell
# 运行 sqllog 解析性能基准
cargo bench --bench sqllog_bench

# 运行核心函数性能基准
cargo bench --bench core_functions_bench

# 运行日期时间处理基准
cargo bench --bench datetime_bench
```

## 🚀 快速开始

### 安装

确保您已安装 Rust 1.70 或更高版本：

```bash
# 克隆仓库
git clone https://github.com/guangl/dm-sqllog-parser.git
cd dm-sqllog-parser

# 构建项目
cargo build --release
```

### 基本使用

```bash
# 解析单个日志文件
cargo run -- --input-path ./sqllog/dmsql_OA01_20250916_200253.log -o ./output

# 解析整个目录中的所有日志文件
cargo run -- --input-path D:\data\dmsql\logs -o ./output_all

# 使用配置文件
cargo run -- --config config.toml
```

### 配置文件

复制并修改示例配置：

```bash
copy config.toml.example config.toml
```

编辑 `config.toml` 以满足您的需求：

```toml
[input]
path = "D:\\data\\dmsql\\logs"

[output]
base_dir = "./output"

[processing]
num_threads = 8
chunk_size = 8192

[export]
formats = ["csv", "json"]
file_size_bytes = 104857600  # 100MB，设为 0 会导致启动失败
```

## 📋 命令行选项

```
OPTIONS:
    -i, --input-path <PATH>    输入文件或目录路径
    -o, --output <DIR>         输出目录（可选，默认为 './output'）
    -c, --config <FILE>        配置文件路径（可选）
    -h, --help                 显示帮助信息
    -V, --version              显示版本信息
```

## 🔍 解析错误收集

工具会自动收集和记录解析过程中遇到的错误：

- **日志级别**：ERROR、WARN、INFO、DEBUG
- **错误分类**：格式错误、字段缺失、数据类型不匹配
- **详细位置**：文件名、行号、具体错误内容
- **统计报告**：成功/失败计数、错误分布统计

## 🧪 测试

运行完整的测试套件：

```bash
# 运行所有测试
cargo test --all-features

# 运行测试覆盖率分析
cargo tarpaulin --all-features --skip-clean
```

当前测试覆盖率：**35.64%** (619/1737 lines)

## 📊 输出格式

### CSV 导出

```csv
timestamp,exec_id,sql_statement,exectime,rowcount
2025-09-16 20:02:53,12345,SELECT * FROM users,0.015,100
```

### JSON 导出

```json
{
  "records": [
    {
      "timestamp": "2025-09-16T20:02:53",
      "exec_id": "12345",
      "sql_statement": "SELECT * FROM users",
      "exectime": 0.015,
      "rowcount": 100
    }
  ]
}
```

## 🔧 开发环境

### 依赖要求

- Rust 1.70+
- Cargo

### 开发工具

```bash
# 安装开发依赖
cargo install cargo-tarpaulin  # 测试覆盖率
cargo install cargo-bench     # 性能基准测试

# 代码格式化
cargo fmt

# 代码检查
cargo clippy -- -D warnings
```

## 📄 许可证

本项目采用 MIT 许可证 - 详情请查看 [LICENSE](LICENSE) 文件。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建您的特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交您的修改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开一个 Pull Request

## 📞 支持

如果您在使用过程中遇到问题，请：

1. 查看 [Issues](https://github.com/guangl/dm-sqllog-parser/issues) 页面
2. 创建新的 Issue 详细描述问题
3. 或者联系维护者

## 📝 更新日志

查看 [CHANGELOG.md](CHANGELOG.md) 了解详细的版本更新信息。