//! 应用程序主逻辑模块 - 文件扫描与批处理流程
//!
//! 本模块实现了应用程序的核心业务逻辑，包括文件发现、批处理调度
//! 和结果导出的完整流程。
//!
//! ## 核心功能
//!
//! ### 1. 智能文件发现
//! - **模式匹配**：自动识别以 `dmsql_` 开头的 `.log` 文件
//! - **递归扫描**：支持指定目录下的文件批量发现
//! - **扩展名过滤**：不区分大小写的 `.log` 扩展名匹配
//!
//! ### 2. 批处理管道
//! ```text
//! 文件扫描 → 独立数据库处理 → 结果合并 → 数据导出
//!     ↓            ↓              ↓          ↓
//!  目录遍历    并行解析处理      临时库整合   CSV输出
//!  文件筛选    错误收集归档      统计汇总     格式转换
//! ```
//!
//! ### 3. 统一错误处理
//! - **错误隔离**：单个文件的处理失败不影响其他文件
//! - **错误聚合**：收集所有处理过程中的错误信息
//! - **错误报告**：生成详细的错误统计和诊断信息
//!
//! ## 处理流程
//!
//! 1. **配置加载**：从配置文件和命令行参数构建运行时配置
//! 2. **文件发现**：扫描指定目录，收集符合规则的日志文件
//! 3. **批处理执行**：使用独立数据库策略并行处理文件
//! 4. **结果导出**：将处理结果导出为指定格式（如 CSV）
//! 5. **统计报告**：输出处理统计信息和性能指标
//!
//! ## 设计原则
//!
//! - **可扩展性**：支持不同的文件格式和导出格式
//! - **容错性**：优雅处理各种异常情况
//! - **性能优化**：并行处理和内存效率优化
//! - **监控友好**：丰富的日志和统计信息

use sqllog_analysis::config::Config;
use sqllog_analysis::database::DuckDbProvider;
use sqllog_analysis::database::{
    DatabaseProvider, ExportFormat, process_files_with_independent_databases,
};

use std::fs;
use std::path;

/// 在指定目录中收集符合命名规则的 sqllog 日志文件。
///
/// ## 文件识别规则
///
/// 为了确保处理的是正确的 SQL 日志文件，采用了严格的文件名模式匹配：
///
/// - **前缀匹配**：文件名必须以 `dmsql_` 开头
/// - **扩展名检查**：必须是 `.log` 扩展名（不区分大小写）
/// - **文件类型**：只处理常规文件，忽略目录和符号链接
///
/// ## 典型文件名示例
///
/// ✅ **匹配的文件**:
/// - `dmsql_OA01_20250922_120000.log`
/// - `dmsql_backup.LOG`
/// - `dmsql_test.log`
///
/// ❌ **不匹配的文件**:
/// - `sqllog_data.log` (前缀不对)
/// - `dmsql_data.txt` (扩展名不对)
/// - `dmsql_backup` (无扩展名)
///
/// ## 性能考虑
///
/// - **单次扫描**：避免递归搜索，只扫描指定目录
/// - **延迟过滤**：在迭代过程中过滤，减少内存使用
/// - **错误恢复**：单个文件访问失败不会中断整个扫描过程
///
/// 参数：
/// - `sqllog_dir`：要扫描的目录路径。
///
/// 返回：符合规则的文件路径列表（按目录迭代顺序）。
fn collect_sqllog_files(sqllog_dir: &path::Path) -> Vec<path::PathBuf> {
    let mut files: Vec<path::PathBuf> = Vec::new();
    if let Ok(iter) = fs::read_dir(sqllog_dir) {
        for entry in iter.flatten() {
            let p = entry.path();
            if p.is_file() {
                if let Some(n) = p.file_name().and_then(|s| s.to_str()) {
                    // 使用 Path 的 extension 并进行不区分大小写的比较
                    if n.starts_with("dmsql_")
                        && std::path::Path::new(n)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
                    {
                        files.push(p);
                    }
                }
            }
        }
    }
    files
}

/// 程序主逻辑入口（由 `main` 调用），负责加载配置并触发文件扫描与解析。
pub fn run() {
    let runtime = Config::load();
    if let Some(sqllog_dir) = runtime.sqllog_dir.clone() {
        let files = collect_sqllog_files(&sqllog_dir);

        if files.is_empty() {
            log::warn!("在 {} 中未找到 dmsql_*.log 文件", sqllog_dir.display());
            return;
        }

        log::info!("发现 {} 个待处理文件", files.len());

        // 使用独立数据库处理所有文件（每个线程独立数据库，最后合并）
        match process_files_with_independent_databases(&files, &runtime) {
            Ok(stats) => {
                log::info!("所有文件处理完成！统计信息:");
                log::info!("  - 处理记录数: {}", stats.records_processed);
                log::info!("  - 插入记录数: {}", stats.records_inserted);
                log::info!("  - 处理文件数: {}", stats.files_processed);
                log::info!(
                    "  - 临时数据库数: {}",
                    stats.temp_databases_created
                );

                // 如果启用了导出功能，执行数据导出
                if runtime.export_enabled {
                    if let Some(export_path) = &runtime.export_out_path {
                        log::info!("开始导出数据...");

                        // 创建数据库提供者进行导出
                        match DuckDbProvider::new(&runtime) {
                            Ok(provider) => {
                                // 解析导出格式
                                if let Ok(format) = runtime
                                    .export_format
                                    .parse::<ExportFormat>()
                                {
                                    let path_str =
                                        export_path.to_string_lossy();
                                    match provider
                                        .export_data(format, &path_str)
                                    {
                                        Ok(()) => {
                                            log::info!(
                                                "数据导出完成: {path_str}"
                                            );
                                        }
                                        Err(e) => {
                                            log::error!("数据导出失败: {e}");
                                        }
                                    }
                                } else {
                                    log::error!(
                                        "不支持的导出格式: {}",
                                        runtime.export_format
                                    );
                                }
                            }
                            Err(e) => {
                                log::error!("创建数据库提供者失败: {e}");
                            }
                        }
                    } else {
                        log::warn!("导出功能已启用，但未指定导出路径");
                    }
                } else {
                    log::debug!("导出功能未启用");
                }
            }
            Err(e) => {
                log::error!("处理文件失败: {e}");
                std::process::exit(1);
            }
        }
    } else {
        log::warn!("未配置 sqllog_dir，跳过解析");
    }
}
