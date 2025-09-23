//! # SQL日志分析库
//!
//! 这是一个用于解析和分析达梦数据库 SQL 日志的 Rust 库。
//!
//! ## 功能特性
//!
//! - 完整的 SQL 日志解析功能
//! - 统一的日志系统（通过 logging feature）
//! - 多种数据导出格式（CSV、JSON、SQLite、DuckDB）
//! - 并发导出支持
//! - 丰富的错误处理
//! - 类型安全的 API
//!
//! ## 快速开始
//!
//! ### 同步解析
//!
//! ```no_run
//! use sqllog_analysis::sqllog::{SyncSqllogParser, ParseResult};
//!
//! let mut result = ParseResult::new();
//! SyncSqllogParser::parse_with_hooks("path/to/logfile.log", 1000, |records, errors| {
//!     // 处理解析结果
//!     println!("解析到 {} 条记录，{} 个错误", records.len(), errors.len());
//! }).unwrap();
//! ```
//!
//! ### 多格式并发导出
//!
//! ```no_run
//! # #[cfg(all(feature = "exporter-csv", feature = "exporter-json"))]
//! # fn example() {
//! use sqllog_analysis::prelude::*;
//!
//! let mut multi_exporter = SyncMultiExporter::new();
//! multi_exporter.add_exporter(SyncCsvExporter::new("output.csv").unwrap());
//! multi_exporter.add_exporter(SyncJsonExporter::new("output.json").unwrap());
//!
//! // 解析并导出数据
//! // ...
//! # }
//! ```
//!
//! ## Feature 说明
//!
//! ### 核心功能
//! - `logging` (默认启用) - 启用日志系统功能
//!
//! ### 导出器功能
//! - `exporter-csv` - CSV 导出器
//! - `exporter-json` - JSON 导出器
//! - `exporter-sqlite` - SQLite 导出器
//! - `exporter-duckdb` - DuckDB 导出器
//! - `all-exporters` - 启用所有导出器
//!
//! ## 模块结构
//!
//! - [`error`] - 错误类型定义
//! - [`sqllog`] - SQL 日志解析相关功能
//!   - [`parser`](sqllog::parser) - 同步解析器
//!   - [`sync_parser`](sqllog::sync_parser) - 文件 I/O 处理
//!   - [`types`](sqllog::types) - 数据类型定义
//! - [`exporter`] - 数据导出相关功能（需要相应的 exporter 特性）
//!

pub mod config;
pub mod error;
#[cfg(feature = "logging")]
pub mod logging;
pub mod sqllog;

// 导出器模块
#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
pub mod exporter;

// 重新导出常用类型和函数
pub use error::{Result, SqllogError};
pub use sqllog::types::Sqllog;

/// 库版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// 库名称
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// 库描述
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

/// 预导入模块
///
/// 包含了最常用的类型和函数，方便用户快速开始使用。
///
/// ```no_run
/// use sqllog_analysis::prelude::*;
///
/// // 现在你可以直接使用所有常用的类型和函数
/// ```
pub mod prelude {
    pub use crate::error::{Result, SqllogError};
    pub use crate::sqllog::types::Sqllog;
    pub use crate::sqllog::{
        ConcurrentParser, ParseError, ParseResult, SqllogParser,
        SyncSqllogParser,
    };

    // 导出器相关
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    pub use crate::exporter::{ExportStats, SyncExporter, SyncMultiExporter};

    #[cfg(feature = "exporter-csv")]
    pub use crate::exporter::sync_impl::SyncCsvExporter;

    #[cfg(feature = "exporter-json")]
    pub use crate::exporter::sync_impl::SyncJsonExporter;

    #[cfg(feature = "exporter-sqlite")]
    pub use crate::exporter::sync_impl::SyncSqliteExporter;

    #[cfg(feature = "exporter-duckdb")]
    pub use crate::exporter::sync_impl::SyncDuckdbExporter;
}
