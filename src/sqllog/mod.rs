//! SQL 日志解析模块
//!
//! 提供 SQL 日志的解析、类型定义和工具函数

pub mod parser;
pub mod sync_parser;
pub mod types;
pub mod utils;

#[cfg(feature = "async")]
pub mod async_parser;

// 重新导出核心类型和函数
pub use parser::SqllogParser;
pub use sync_parser::{ParseError, ParseResult, SyncSqllogParser};
pub use types::{DescNumbers, Sqllog};
pub use utils::{find_first_row_pos, is_first_row, line_bytes_to_str_impl};

#[cfg(feature = "async")]
pub use async_parser::AsyncSqllogParser;
