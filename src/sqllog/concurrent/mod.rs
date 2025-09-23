//! 并发处理模块
//!
//! 提供多线程并发解析和导出功能

pub mod parse_workers;
pub mod types;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
pub mod export_workers;

pub mod concurrent_parser;

// 重新导出主要类型和接口
pub use concurrent_parser::ConcurrentParser;
pub use types::{ParseBatch, ParseTask};

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
pub use types::{ExportTask, ProcessingSummary};
