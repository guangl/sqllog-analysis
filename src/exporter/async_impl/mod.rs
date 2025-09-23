//! 异步导出器实现模块

use crate::error::Result;
use crate::exporter::ExportStats;
use crate::sqllog::types::Sqllog;
use async_trait::async_trait;

/// 数据导出器的统一接口 (异步版本)
#[async_trait]
pub trait AsyncExporter: Send + Sync {
    /// 导出器名称
    fn name(&self) -> &str;

    /// 导出单个记录
    async fn export_record(&mut self, record: &Sqllog) -> Result<()>;

    /// 批量导出记录
    async fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        for record in records {
            self.export_record(record).await?;
        }
        Ok(())
    }

    /// 完成导出，清理资源
    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }

    /// 获取导出统计信息
    fn get_stats(&self) -> ExportStats {
        ExportStats::default()
    }
}

#[cfg(feature = "exporter-csv")]
pub mod csv;
#[cfg(feature = "exporter-json")]
pub mod json;
pub mod multi_exporter;
#[cfg(feature = "exporter-sqlite")]
pub mod sqlite;

#[cfg(feature = "exporter-csv")]
pub use csv::AsyncCsvExporter;
#[cfg(feature = "exporter-json")]
pub use json::AsyncJsonExporter;
pub use multi_exporter::AsyncMultiExporter;
#[cfg(feature = "exporter-sqlite")]
pub use sqlite::AsyncSqliteExporter;
