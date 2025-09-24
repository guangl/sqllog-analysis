//! SQL 日志解析模块
//!
//! 提供 SQL 日志的解析、类型定义和工具函数

pub mod concurrent;
pub mod parser;
pub mod sync_parser;
pub mod types;
pub mod utils;

// 重新导出核心类型和函数
pub use concurrent::{ConcurrentParser, ParseBatch, ParseTask};
pub use parser::SqllogParser;
pub use sync_parser::{ParseError, ParseResult, SyncSqllogParser};
pub use types::{DescNumbers, Sqllog};
pub use utils::{find_first_row_pos, is_first_row, line_bytes_to_str_impl};

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
pub use concurrent::{
    ExportTask, ProcessingSummary, parse_and_export_concurrent,
};

pub use concurrent::parse_files_concurrent;
