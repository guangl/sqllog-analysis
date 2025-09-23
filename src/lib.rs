//! # SQL日志分析库
//!
//! 这是一个用于解析和分析达梦数据库 SQL 日志的 Rust 库。
//!
//! ## 功能特性
//!
//! - 完整的 SQL 日志解析功能
//! - 统一的日志系统（通过 logging feature）
//! - 异步支持（通过 async feature）
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
//! ### 异步解析
//!
//! ```no_run
//! # #[cfg(feature = "async")]
//! # async fn example() {
//! use sqllog_analysis::sqllog::AsyncSqllogParser;
//!
//! let (mut record_rx, mut error_rx) = AsyncSqllogParser::parse_with_hooks("path/to/logfile.log", 1000).await.unwrap();
//! while let Some(records) = record_rx.recv().await {
//!     println!("接收到 {} 条记录", records.len());
//! }
//! # }
//! ```
//!
//! ## Feature 说明
//!
//! - `logging` (默认启用) - 启用日志系统功能
//! - `tokio` - 启用 tokio 运行时支持
//! - `async` - 启用异步解析功能（包含 tokio 和 logging）
//!
//! ## 模块结构
//!
//! - [`error`] - 错误类型定义
//! - [`sqllog`] - SQL 日志解析相关功能
//!   - [`parser`](sqllog::parser) - 同步解析器
//!   - [`async_parser`](sqllog::async_parser) - 异步解析器（需要 `async` feature）
//!   - [`sync_parser`](sqllog::sync_parser) - 文件 I/O 处理
//!   - [`types`](sqllog::types) - 数据类型定义
//!

pub mod error;
#[cfg(feature = "logging")]
pub mod logging;
pub mod sqllog;

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
        SqllogParser, ParseError, ParseResult, SyncSqllogParser,
    };

    #[cfg(feature = "async")]
    pub use crate::sqllog::AsyncSqllogParser;
}
