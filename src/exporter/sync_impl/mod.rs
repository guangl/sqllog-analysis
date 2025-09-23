//! 同步导出器实现模块

use crate::error::Result;
use crate::sqllog::types::Sqllog;
use crate::exporter::ExportStats;

/// 同步数据导出器的统一接口
pub trait SyncExporter: Send + Sync {
    /// 导出器名称
    fn name(&self) -> &str;

    /// 导出单个记录
    fn export_record(&mut self, record: &Sqllog) -> Result<()>;

    /// 批量导出记录
    fn export_batch(&mut self, records: &[Sqllog]) -> Result<()> {
        for record in records {
            self.export_record(record)?;
        }
        Ok(())
    }

    /// 完成导出，清理资源
    fn finalize(&mut self) -> Result<()> {
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
pub use csv::SyncCsvExporter;
#[cfg(feature = "exporter-json")]
pub use json::SyncJsonExporter;
pub use multi_exporter::SyncMultiExporter;
#[cfg(feature = "exporter-sqlite")]
pub use sqlite::SyncSqliteExporter;
