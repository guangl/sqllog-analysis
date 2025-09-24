//! 并发 SQL 日志解析器主实现

use crate::config::SqllogConfig;
use crate::error::Result;
use std::path::PathBuf;

#[cfg(any(
    feature = "exporter-csv",
    feature = "exporter-json",
    feature = "exporter-sqlite",
    feature = "exporter-duckdb"
))]
use crate::exporter::sync_impl::SyncExporter;

/// 并发 SQL 日志解析器
#[derive(Debug, Clone)]
pub struct ConcurrentParser {
    /// 线程数
    pub thread_count: usize,
    /// 批处理大小
    pub batch_size: usize,
    /// 可选的错误输出路径（JSONL）
    pub errors_out: Option<String>,
}

impl ConcurrentParser {
    /// 创建新的并发解析器
    pub fn new(config: SqllogConfig) -> Self {
        #[cfg(feature = "logging")]
        tracing::debug!("创建并发解析器，配置: {:?}", config);

        Self {
            thread_count: config.thread_count.unwrap_or(0),
            batch_size: config.batch_size,
            errors_out: config.errors_out.clone(),
        }
    }

    /// 并发解析和流水线导出文件
    #[cfg(any(
        feature = "exporter-csv",
        feature = "exporter-json",
        feature = "exporter-sqlite",
        feature = "exporter-duckdb"
    ))]
    pub fn parse_and_export_streaming<E>(
        &self,
        file_paths: &[PathBuf],
        exporter: E,
    ) -> Result<Vec<(usize, usize)>>
    where
        E: SyncExporter + Send + 'static,
    {
        super::parse_workers::parse_and_export_concurrent(
            file_paths,
            exporter,
            self.batch_size,
            self.thread_count,
            self.errors_out.clone(),
        )
    }

    /// 并发解析多个文件（不导出）
    pub fn parse_files_concurrent(
        &self,
        file_paths: &[PathBuf],
    ) -> Result<(
        Vec<crate::sqllog::types::Sqllog>,
        Vec<crate::sqllog::sync_parser::ParseError>,
    )> {
        super::parse_workers::parse_files_concurrent(
            file_paths,
            self.batch_size,
            self.thread_count,
            self.errors_out.clone(),
        )
    }
}

impl Default for ConcurrentParser {
    fn default() -> Self {
        Self::new(SqllogConfig::default())
    }
}
