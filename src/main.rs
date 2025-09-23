//! SQL 日志分析工具 - 程序入口点
//!
//! 这是一个高性能的 SQL 日志分析工具，专门用于处理大规模的数据库日志文件，
//! 提供解析、存储、分析和导出功能。
//!
//! ## 工具概述
//!
//! 本工具能够：
//! - **批量处理**：自动发现和处理指定目录下的所有 SQL 日志文件
//! - **并行解析**：使用多线程并行处理，显著提升大文件的处理速度
//! - **错误恢复**：智能处理格式异常的日志条目，生成详细的错误报告
//! - **灵活导出**：支持多种格式的数据导出，便于后续分析
//!
//! ## 典型使用场景
//!
//! ### 1. 数据库性能分析
//! ```bash
//! # 处理性能日志，分析慢查询
//! dm-sqllog-parser --input /logs/sqllog/ --export slow_queries.csv
//! ```
//!
//! ### 2. 数据质量检查
//! ```bash
//! # 检查日志格式一致性，生成错误报告
//! dm-sqllog-parser --input /logs/ --config quality_check.toml
//! ```
//!
//! ### 3. 批量数据迁移
//! ```bash
//! # 将日志数据导入数据库供后续分析
//! dm-sqllog-parser --input /archive/ --database analytics.db
//! ```
//!
//! ## 程序架构
//!
//! ```text
//! main() → 配置加载 → 日志初始化 → 异常处理 → app::run()
//!   ↓         ↓          ↓           ↓          ↓
//! 入口点   TOML解析   tracing设置   panic钩子   业务逻辑
//! ```
//!
//! ## 错误处理策略
//!
//! - **配置错误**：立即退出，提供详细的错误信息
//! - **日志初始化失败**：视为严重错误，退出码 2
//! - **运行时异常**：记录详细日志和回溯信息，优雅退出
//! - **数据处理错误**：隔离错误，继续处理其他数据

mod app;

use dm_sqllog_parser::core::analysis_log::LogConfig;
use dm_sqllog_parser::core::{Config, RuntimeConfig};
use std::{backtrace::Backtrace, panic, process};

fn main() {
    let runtime = load_runtime_config();
    init_logging(&runtime);
    set_panic_hook();

    app::run();
}

/// 载入运行时配置。
///
/// 目前直接调用 `Config::load()` 并返回 `RuntimeConfig`。
fn load_runtime_config() -> RuntimeConfig {
    Config::load()
}

/// 初始化日志系统并在初始化失败时退出进程。
///
/// 参数：
/// - `runtime`：运行时配置，包含日志级别、是否输出到 stdout、日志目录等。
fn init_logging(runtime: &RuntimeConfig) {
    let log_config = LogConfig {
        enable_stdout: runtime.enable_stdout,
        log_file: runtime.log_dir.clone(),
        level: runtime.log_level,
        ..Default::default()
    };
    // 在初始化日志之前先打印当前日志相关配置（便于在 enable_stdout=false 时也能看到等级）
    println!(
        "日志等级配置: {:?}, stdout: {}",
        log_config.level, log_config.enable_stdout
    );
    if let Err(e) = log_config.init() {
        eprintln!("日志初始化失败: {e}");
        // 无法初始化日志属于严重错误，退出
        process::exit(2);
    }
}

/// 设置全局 panic hook，用于在 panic 时记录错误信息与回溯信息。
///
/// 该 hook 不会阻止进程继续退出，但会将 panic 信息记录到日志中，便于后续排查。
fn set_panic_hook() {
    panic::set_hook(Box::new(|info| {
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            log::error!("发生 panic：{s}");
        } else if let Some(s) = info.payload().downcast_ref::<String>() {
            log::error!("发生 panic：{s}");
        } else {
            log::error!("发生 panic：{info}");
        }
        let bt = Backtrace::force_capture();
        log::error!("回溯信息:\n{bt:?}");
    }));
}
