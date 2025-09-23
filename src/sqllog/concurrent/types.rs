//! 并发处理相关的数据类型定义

use crate::sqllog::{sync_parser::ParseError, types::Sqllog};
use std::path::PathBuf;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use std::time::Duration;

/// 并发处理总结结果
#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
#[derive(Debug, Clone)]
pub struct ProcessingSummary {
    /// 总处理时间
    pub total_duration: Duration,
    /// 解析耗时
    pub parse_duration: Duration,
    /// 导出耗时
    pub export_duration: Duration,
    /// 解析错误
    pub parse_errors: Vec<ParseError>,
}

/// 解析任务结构
#[derive(Debug, Clone)]
pub struct ParseTask {
    /// 文件路径
    pub file_path: PathBuf,
    /// 批处理大小
    pub batch_size: usize,
}

/// 解析批次结果
#[derive(Debug, Clone)]
pub struct ParseBatch {
    /// 解析出的记录
    pub records: Vec<Sqllog>,
    /// 解析错误
    pub errors: Vec<ParseError>,
    /// 源文件路径
    pub source_file: PathBuf,
    /// 批次ID
    pub batch_id: usize,
}

/// 导出任务结构
#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
#[derive(Debug, Clone)]
pub struct ExportTask {
    /// 要导出的记录
    pub records: Vec<Sqllog>,
    /// 任务ID
    pub task_id: usize,
    /// 源文件路径
    pub source_file: PathBuf,
}
