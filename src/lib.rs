//! # SQL日志分析库
//!
//! 这是一个用于解析和分析达梦数据库 SQL 日志的 Rust 库。
//!
//! ## 功能特性
//!
//! - 完整的 SQL 日志解析功能
//! - 统一的日志系统（通过 logging feature）
//! - 异步支持（通过 tokio feature）
//! - 丰富的错误处理
//! - 类型安全的 API
//!
//! ## 快速开始
//!
//! ```no_run
//! use sqllog_analysis::sqllog::types::Sqllog;
//!
//! // 使用 SQL 日志解析功能
//! // ... 你的代码
//! ```
//!
//! ## Feature 说明
//!
//! - `logging` (默认启用) - 启用日志系统功能
//! - `tokio` - 启用异步 tokio 运行时支持
//!
//! ## 模块结构
//!
//! - [`error`] - 错误类型定义
//! - [`sqllog`] - SQL 日志解析相关功能
//!

pub mod error;
#[cfg(feature = "logging")]
mod logging;
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
}
